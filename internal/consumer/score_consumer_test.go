// Unit tests for the score consumer's consume-loop policies, driven by
// in-memory fakes: no Kafka broker, Redis server, PostgreSQL, or Docker
// is required.
//
// What these tests prove:
//   - ordering: an event is applied to the projection BEFORE its offset
//     is committed (process-then-commit), and only after processing
//     succeeded;
//   - a failing projection update never commits and is retried with the
//     existing capped backoff, or abandoned uncommitted on shutdown;
//   - malformed events follow skip-and-commit (never applied, offset
//     committed so the partition keeps moving);
//   - reprocessing the same event re-sends the identical absolute score
//     (set semantics — no increments or accumulated deltas in this layer);
//   - offset commit failures retry in place (same event, no reprocessing)
//     up to commitMaxAttempts; exhausting them STOPS the consumer, because
//     Kafka commits are per-partition monotonic — acking a later offset
//     would implicitly commit the unresolved event and lose it. The fake
//     models this with per-partition committed positions (ack at offset N
//     advances the position to N+1, covering all lower offsets).
//
// What these tests do NOT prove:
//   - Redis server-side ZADD GT behavior. The fake mirrors the
//     max(best, score) contract in memory; real GT semantics were verified
//     separately in the runtime integration tests (duplicate re-apply after
//     crash recovery left Redis unchanged);
//   - broker-side commit durability, network partitions, or rebalance
//     handling — those remain runtime concerns.
package consumer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"real_time_leaderboard/internal/publisher"
)

func TestMain(m *testing.M) {
	log.SetOutput(io.Discard) // keep the fake-driven assertions readable
	os.Exit(m.Run())
}

// ---------------------------------------------------------------------------
// shared event log: records apply/commit operations in the exact order the
// consumer performed them, so ordering assertions are exact-sequence checks.
// ---------------------------------------------------------------------------

type seqLog struct {
	mu     sync.Mutex
	events []string
}

func (l *seqLog) add(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, fmt.Sprintf(format, args...))
}

func (l *seqLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]string, len(l.events))
	copy(out, l.events)
	return out
}

func sameSequence(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// fakes
// ---------------------------------------------------------------------------

// fakeUpdater stands in for LeaderboardRepository.UpdateBestScore. It
// records every call's arguments and applies the projection's contract
// in memory: absolute best-score set (max), never an increment.
type fakeUpdater struct {
	mu            sync.Mutex
	log           *seqLog
	best          map[int]int
	args          [][2]int
	calls         int
	failFirst     int            // fail the first N calls, then succeed
	failAlways    bool           // fail every call until the test ends
	onFailureCall func(call int) // hook invoked (under no lock) when a call fails
}

func newFakeUpdater(lg *seqLog) *fakeUpdater {
	return &fakeUpdater{log: lg, best: make(map[int]int)}
}

func (f *fakeUpdater) UpdateBestScore(userID int, score int) error {
	f.mu.Lock()
	f.calls++
	call := f.calls
	f.args = append(f.args, [2]int{userID, score})
	shouldFail := f.failAlways || call <= f.failFirst
	hook := f.onFailureCall
	f.mu.Unlock()

	if hook != nil {
		hook(call)
	}
	if shouldFail {
		f.log.add("update-fail user=%d score=%d", userID, score)
		return errors.New("redis unavailable")
	}

	f.mu.Lock()
	if cur, ok := f.best[userID]; !ok || score > cur {
		f.best[userID] = score
	}
	f.mu.Unlock()
	f.log.add("update-ok user=%d score=%d", userID, score)
	return nil
}

func (f *fakeUpdater) argSnapshot() [][2]int {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([][2]int, len(f.args))
	copy(out, f.args)
	return out
}

func (f *fakeUpdater) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *fakeUpdater) bestOf(userID int) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.best[userID]
}

// fakeReader stands in for *kafka.Reader. Messages are served from an
// in-memory queue; when the queue is empty FetchMessage blocks until ctx
// is cancelled (mirroring a real reader waiting for the next record).
//
// Offset model: Kafka commits are per-partition monotonic — committing
// offset N durably stores N+1 as the partition's next position and
// implicitly covers every lower offset in that partition. positions
// mirrors that: a successful CommitMessages advances positions[partition]
// to max(current, offset+1). Attempts (including rejected ones) are
// recorded separately so tests can count retries; only acked calls move
// the committed position.
//
// CommitMessages failures are scripted with commitFailFirst: the first N
// calls return an error (coordinator rejected the offset), later calls
// succeed.
type fakeReader struct {
	mu              sync.Mutex
	log             *seqLog
	queue           []kafka.Message
	fetched         int
	commitCalls     int
	commitFailFirst int
	attempts        []kafka.Message
	positions       map[int]int64 // partition -> next committed offset (offset+1 of last ack)
	acks            int           // CommitMessages calls that returned nil
	closes          int
}

func newFakeReader(lg *seqLog, msgs ...kafka.Message) *fakeReader {
	return &fakeReader{log: lg, queue: msgs, positions: make(map[int]int64)}
}

func (r *fakeReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	r.mu.Lock()
	if len(r.queue) > 0 {
		msg := r.queue[0]
		r.queue = r.queue[1:]
		r.fetched++
		r.mu.Unlock()
		return msg, nil
	}
	r.mu.Unlock()

	<-ctx.Done()
	return kafka.Message{}, ctx.Err()
}

func (r *fakeReader) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.commitCalls++
	fail := r.commitCalls <= r.commitFailFirst
	for _, m := range msgs {
		r.attempts = append(r.attempts, m)
		if fail {
			r.log.add("commit-fail partition=%d offset=%d", m.Partition, m.Offset)
			continue
		}
		// Acked: advance the partition's committed position to offset+1.
		// Monotonic — a higher ack covers all lower offsets in the
		// partition, exactly like the broker.
		if next := m.Offset + 1; next > r.positions[m.Partition] {
			r.positions[m.Partition] = next
		}
		r.acks++
		r.log.add("commit partition=%d offset=%d", m.Partition, m.Offset)
	}
	if fail {
		return errors.New("broker coordinator unavailable")
	}
	return nil
}

func (r *fakeReader) Close() error {
	r.mu.Lock()
	r.closes++
	r.mu.Unlock()
	return nil
}

// commitAckCount returns how many CommitMessages calls were acknowledged
// (returned nil). Acks count attempts that the broker accepted; for
// message-loss questions the per-partition position matters, see
// committedPosition.
func (r *fakeReader) commitAckCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.acks
}

// committedPosition returns the partition's committed position: the next
// offset the broker would deliver after a restart. kafka.FirstOffset (-1)
// means nothing has ever been acked for the partition.
func (r *fakeReader) committedPosition(partition int) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	if pos, ok := r.positions[partition]; ok {
		return pos
	}
	return kafka.FirstOffset
}

// attemptSnapshot returns every CommitMessages call in order, whether it
// was acked or rejected.
func (r *fakeReader) attemptSnapshot() []kafka.Message {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]kafka.Message, len(r.attempts))
	copy(out, r.attempts)
	return out
}

func attemptsFor(msgs []kafka.Message, offset int64) int {
	n := 0
	for _, m := range msgs {
		if m.Offset == offset {
			n++
		}
	}
	return n
}

func (r *fakeReader) fetchCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.fetched
}

func (r *fakeReader) closeCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.closes
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// scoreEvent builds one well-formed score.submitted message with the given
// partition offset. Timestamps are fixed so tests are fully deterministic.
func scoreEvent(t *testing.T, userID, score int, offset int64) kafka.Message {
	t.Helper()
	payload, err := json.Marshal(publisher.ScoreSubmittedEvent{
		Event:     "score.submitted",
		UserID:    userID,
		Score:     score,
		ScoreID:   int(offset) + 1,
		CreatedAt: "2026-10-09T12:00:00Z",
	})
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	return kafka.Message{
		Topic:     "score.submitted",
		Partition: 0,
		Offset:    offset,
		Key:       []byte(strconv.Itoa(userID)),
		Value:     payload,
	}
}

// runConsumerUntil starts the consume loop, waits until cond holds (or
// fails the test after 5s), then cancels the context and waits for Run to
// shut down cleanly (which also exercises the deferred reader Close).
func runConsumerUntil(t *testing.T, c *ScoreConsumer, cond func() bool) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		c.Run(ctx)
		close(done)
	}()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("consumer Run did not stop after context cancel")
		}
	}()

	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not reached within 5s")
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// ---------------------------------------------------------------------------
// goal 1: valid event applies, and commits only after successful processing
// ---------------------------------------------------------------------------

func TestRun_ValidEvent_AppliesBeforeCommit(t *testing.T) {
	lg := &seqLog{}
	up := newFakeUpdater(lg)
	rd := newFakeReader(lg, scoreEvent(t, 32, 9500, 7))
	c := &ScoreConsumer{reader: rd, lbRepo: up}

	runConsumerUntil(t, c, func() bool { return rd.committedPosition(0) == 8 })

	// Exact sequence: the projection was updated first, the offset
	// committed second — never the other way around.
	want := []string{
		"update-ok user=32 score=9500",
		"commit partition=0 offset=7",
	}
	if got := lg.snapshot(); !sameSequence(got, want) {
		t.Fatalf("sequence mismatch:\n got: %v\nwant: %v", got, want)
	}

	if args := up.argSnapshot(); len(args) != 1 || args[0] != [2]int{32, 9500} {
		t.Fatalf("updater calls = %v, want exactly [[32 9500]]", args)
	}
	// One acked commit advancing partition 0's position to offset+1 = 8.
	if pos := rd.committedPosition(0); pos != 8 {
		t.Fatalf("committed position partition 0 = %d, want 8 (offset 7 + 1)", pos)
	}
	if rd.commitAckCount() != 1 {
		t.Fatalf("commit acks = %d, want 1", rd.commitAckCount())
	}
	if rd.closeCount() != 1 {
		t.Fatalf("reader Close called %d times on shutdown, want 1", rd.closeCount())
	}
}

// ---------------------------------------------------------------------------
// goal 2: projection failure never commits; retry follows the design
// ---------------------------------------------------------------------------

func TestRun_TransientFailure_RetriesWithoutCommit(t *testing.T) {
	lg := &seqLog{}
	up := newFakeUpdater(lg)
	up.failFirst = 2 // two failures, then success
	rd := newFakeReader(lg, scoreEvent(t, 31, 4000, 3))
	c := &ScoreConsumer{reader: rd, lbRepo: up}

	start := time.Now()
	runConsumerUntil(t, c, func() bool { return rd.committedPosition(0) == 4 })
	elapsed := time.Since(start)

	// Exact sequence: no commit may appear between the failed attempts
	// and the successful apply.
	want := []string{
		"update-fail user=31 score=4000",
		"update-fail user=31 score=4000",
		"update-ok user=31 score=4000",
		"commit partition=0 offset=3",
	}
	if got := lg.snapshot(); !sameSequence(got, want) {
		t.Fatalf("sequence mismatch:\n got: %v\nwant: %v", got, want)
	}

	if n := up.callCount(); n != 3 {
		t.Fatalf("updater attempts = %d, want 3 (2 retries + success)", n)
	}
	// Two failures mean the design's backoff sleeps (500ms then 1s) ran in
	// full before the successful attempt.
	if elapsed < 1500*time.Millisecond {
		t.Fatalf("elapsed %s before commit, want >= 1.5s (capped backoff sleeps)", elapsed)
	}
}

func TestRun_TransientFailure_ShutdownLeavesEventUncommitted(t *testing.T) {
	lg := &seqLog{}
	up := newFakeUpdater(lg)
	up.failAlways = true

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Cancel from inside the first failed attempt: the loop is then sitting
	// in its first backoff sleep and must abandon the event without commit.
	up.onFailureCall = func(call int) { cancel() }

	rd := newFakeReader(lg, scoreEvent(t, 32, 9999, 9))
	c := &ScoreConsumer{reader: rd, lbRepo: up}

	done := make(chan struct{})
	go func() {
		c.Run(ctx)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not stop after context cancel during retry")
	}

	if n := rd.commitAckCount(); n != 0 {
		t.Fatalf("committed %d offsets while the update was failing, want 0", n)
	}
	if n := up.callCount(); n != 1 {
		t.Fatalf("updater attempts = %d, want 1 (cancelled during first backoff)", n)
	}
	// Fetched once, zero commit attempts: the event was in flight when the
	// loop shut down, so the broker still owns it and will redeliver it to
	// the next generation — the at-least-once redelivery state.
	if fetched, acks := rd.fetchCount(), rd.commitAckCount(); fetched != 1 || acks != 0 {
		t.Fatalf("fetched=%d acks=%d, want fetched=1 acks=0 (uncommitted => redeliverable)", fetched, acks)
	}
	if pos := rd.committedPosition(0); pos != kafka.FirstOffset {
		t.Fatalf("committed position partition 0 = %d, want FirstOffset (nothing acked)", pos)
	}
}

// ---------------------------------------------------------------------------
// goal 3: malformed events are skipped with a commit, never applied
// ---------------------------------------------------------------------------

func TestRun_MalformedEvents_SkipAndCommit(t *testing.T) {
	lg := &seqLog{}
	up := newFakeUpdater(lg)

	badJSON := kafka.Message{
		Topic: "score.submitted", Partition: 0, Offset: 4,
		Value: []byte("{not json"),
	}
	negativeScore := scoreEvent(t, 5, -1, 5)
	zeroUser := scoreEvent(t, 0, 100, 6)
	good := scoreEvent(t, 7, 1234, 7)

	rd := newFakeReader(lg, badJSON, negativeScore, zeroUser, good)
	c := &ScoreConsumer{reader: rd, lbRepo: up}

	runConsumerUntil(t, c, func() bool { return rd.committedPosition(0) == 8 }) // last of 4,5,6,7 is offset 7 -> pos 8

	// Exact sequence: each malformed event is committed (skipped) without
	// ever reaching the projection; the valid event still applies normally,
	// proving one bad event cannot block the partition.
	want := []string{
		"commit partition=0 offset=4",
		"commit partition=0 offset=5",
		"commit partition=0 offset=6",
		"update-ok user=7 score=1234",
		"commit partition=0 offset=7",
	}
	if got := lg.snapshot(); !sameSequence(got, want) {
		t.Fatalf("sequence mismatch:\n got: %v\nwant: %v", got, want)
	}

	if args := up.argSnapshot(); len(args) != 1 || args[0] != [2]int{7, 1234} {
		t.Fatalf("updater calls = %v, want exactly [[7 1234]] (malformed must never be applied)", args)
	}
	if n := up.bestOf(5); n != 0 {
		t.Fatalf("user 5 best = %d, want 0 (negative-score event must not be applied)", n)
	}
}

// ---------------------------------------------------------------------------
// goal 4: reprocessing the same event stays idempotent at this layer
// ---------------------------------------------------------------------------

func TestRun_DuplicateEvent_ReappliesSameAbsoluteScore(t *testing.T) {
	lg := &seqLog{}
	up := newFakeUpdater(lg)
	ev := scoreEvent(t, 32, 9500, 12)
	// Same message delivered twice — the at-least-once redelivery shape.
	rd := newFakeReader(lg, ev, ev)
	c := &ScoreConsumer{reader: rd, lbRepo: up}

	runConsumerUntil(t, c, func() bool { return rd.commitAckCount() == 2 })

	// Both passes send the identical ABSOLUTE score: this layer never
	// increments, accumulates, or derives deltas, so a store honoring
	// best-score-set semantics cannot double-count the reapply.
	wantArgs := [][2]int{{32, 9500}, {32, 9500}}
	if args := up.argSnapshot(); len(args) != 2 || args[0] != wantArgs[0] || args[1] != wantArgs[1] {
		t.Fatalf("updater args = %v, want %v (identical absolute scores)", args, wantArgs)
	}
	if n := up.bestOf(32); n != 9500 {
		t.Fatalf("best score after duplicate processing = %d, want 9500 (no double-count)", n)
	}

	// Each delivery was applied then committed; both commits target the
	// same partition/offset. The committed position advances to 13 on the
	// first ack and stays there — re-committing the same offset is a no-op
	// for the monotonic position.
	want := []string{
		"update-ok user=32 score=9500",
		"commit partition=0 offset=12",
		"update-ok user=32 score=9500",
		"commit partition=0 offset=12",
	}
	if got := lg.snapshot(); !sameSequence(got, want) {
		t.Fatalf("sequence mismatch:\n got: %v\nwant: %v", got, want)
	}
	if pos := rd.committedPosition(0); pos != 13 {
		t.Fatalf("committed position partition 0 = %d, want 13 (offset 12 + 1, monotonic)", pos)
	}
	if n := rd.commitAckCount(); n != 2 {
		t.Fatalf("commit acks = %d, want 2", n)
	}
}

// ---------------------------------------------------------------------------
// commit-failure policy: bounded retries around the offset commit itself
// ---------------------------------------------------------------------------

// TestRun_CommitFailure_RetriesUntilAck_WithoutReprocessing covers the
// commit()-retry loop when the coordinator rejects an offset temporarily:
// the commit is retried in place, the event is NOT re-applied to the
// projection (process happens once per fetch), and the offset counts as
// committed only when CommitMessages finally returns nil.
func TestRun_CommitFailure_RetriesUntilAck_WithoutReprocessing(t *testing.T) {
	lg := &seqLog{}
	up := newFakeUpdater(lg)
	rd := newFakeReader(lg, scoreEvent(t, 32, 9500, 8))
	rd.commitFailFirst = 2 // coordinator rejects attempts 1-2, acks attempt 3
	c := &ScoreConsumer{reader: rd, lbRepo: up}

	start := time.Now()
	runConsumerUntil(t, c, func() bool { return rd.committedPosition(0) == 9 })
	elapsed := time.Since(start)

	// Exact sequence: one apply, then commit-fail, commit-fail, acked
	// commit. The apply appears exactly ONCE — the commit attempts must
	// not drag the event back through processing.
	want := []string{
		"update-ok user=32 score=9500",
		"commit-fail partition=0 offset=8",
		"commit-fail partition=0 offset=8",
		"commit partition=0 offset=8",
	}
	if got := lg.snapshot(); !sameSequence(got, want) {
		t.Fatalf("sequence mismatch:\n got: %v\nwant: %v", got, want)
	}

	// Attempt vs ack distinction: 3 attempts, exactly 1 ack, and the ack
	// advanced the partition position to offset+1 = 9.
	if n := len(rd.attemptSnapshot()); n != 3 {
		t.Fatalf("commit attempts = %d, want 3 (2 rejections + 1 ack)", n)
	}
	if n := rd.commitAckCount(); n != 1 {
		t.Fatalf("commit acks = %d, want exactly 1 ack of offset 8", n)
	}
	if pos := rd.committedPosition(0); pos != 9 {
		t.Fatalf("committed position partition 0 = %d, want 9 (offset 8 + 1)", pos)
	}

	// No unnecessary reprocessing within the same processing attempt:
	// the updater ran exactly once with the original absolute score.
	if n := up.callCount(); n != 1 {
		t.Fatalf("updater calls = %d, want 1 (commit retries must not re-apply)", n)
	}
	if args := up.argSnapshot(); len(args) != 1 || args[0] != [2]int{32, 9500} {
		t.Fatalf("updater args = %v, want exactly [[32 9500]] (absolute score, once)", args)
	}
	// Absolute best-score semantics: state equals the event score exactly —
	// a failed commit neither increments nor re-applies it.
	if v := up.bestOf(32); v != 9500 {
		t.Fatalf("best score = %d, want exactly 9500 (absolute set, no increment)", v)
	}

	// The design's backoff slept 500ms after each of the two rejections.
	if elapsed < 1*time.Second {
		t.Fatalf("elapsed %s before ack, want >= 1s (two 500ms backoff sleeps)", elapsed)
	}
}

// TestRun_CommitFailure_ExhaustionStops_NoImplicitOffsetAdvance is the
// regression test for per-partition monotonic commit semantics: Kafka
// committing offset N implicitly commits every lower offset in that
// partition. If offset 10 exhausts its commit attempts, the consumer must
// STOP — continuing to offset 11 and acking it would silently mark offset
// 10 committed too, losing the message after a restart.
//
// The fake models this: an acked commit advances the partition's position
// to offset+1; committedPosition stays at FirstOffset only if nothing was
// ever acked for that partition.
func TestRun_CommitFailure_ExhaustionStops_NoImplicitOffsetAdvance(t *testing.T) {
	var logs bytes.Buffer
	log.SetOutput(&logs)
	defer log.SetOutput(io.Discard) // restore TestMain's discard after this test

	lg := &seqLog{}
	up := newFakeUpdater(lg)
	// Two events in the SAME partition. Offset 10's commit is rejected on
	// all 5 attempts; offset 11 must never be fetched or committed.
	rd := newFakeReader(lg, scoreEvent(t, 32, 9500, 10), scoreEvent(t, 31, 4000, 11))
	rd.commitFailFirst = 5
	c := &ScoreConsumer{reader: rd, lbRepo: up}

	start := time.Now()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		c.Run(ctx)
		close(done)
	}()
	// Run must stop on its own after the give-up (not hang, not continue).
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not stop after exhausting commit attempts")
	}
	elapsed := time.Since(start)

	// Exact sequence: msg1 applied, rejected exactly 5 times, then the
	// loop stops. There is NO entry for msg2 — it was never fetched.
	want := []string{
		"update-ok user=32 score=9500",
		"commit-fail partition=0 offset=10",
		"commit-fail partition=0 offset=10",
		"commit-fail partition=0 offset=10",
		"commit-fail partition=0 offset=10",
		"commit-fail partition=0 offset=10",
	}
	if got := lg.snapshot(); !sameSequence(got, want) {
		t.Fatalf("sequence mismatch:\n got: %v\nwant: %v", got, want)
	}

	// Exactly commitMaxAttempts (5) attempts for offset 10...
	all := rd.attemptSnapshot()
	if n := attemptsFor(all, 10); n != 5 {
		t.Fatalf("commit attempts for offset 10 = %d, want exactly 5 (commitMaxAttempts)", n)
	}
	if n := attemptsFor(all, 11); n != 0 {
		t.Fatalf("commit attempts for offset 11 = %d, want 0 (later offset must not be committed)", n)
	}
	// ...ZERO acks, and the partition position never advanced: a restart
	// would redeliver offset 10 (at-least-once preserved, no message loss).
	// Under the pre-fix behavior this position would have been 12,
	// silently covering the unresolved offset 10.
	if n := rd.commitAckCount(); n != 0 {
		t.Fatalf("commit acks = %d, want 0 (no false ack)", n)
	}
	if pos := rd.committedPosition(0); pos != kafka.FirstOffset {
		t.Fatalf("committed position partition 0 = %d, want FirstOffset (%d) — "+
			"a later offset must never advance the position past an unresolved event",
			pos, kafka.FirstOffset)
	}
	if n := rd.fetchCount(); n != 1 {
		t.Fatalf("messages fetched = %d, want 1 (loop must stop, not fetch offset 11)", n)
	}

	// The give-up is reported honestly: one warning per attempt plus the
	// explicit stop message — never a success claim for offset 10.
	out := logs.String()
	if n := strings.Count(out, "offset commit failed"); n != 5 {
		t.Fatalf("commit-failure warnings = %d, want 5; logs:\n%s", n, out)
	}
	if !strings.Contains(out, "offset commit not acknowledged after 5 attempts") ||
		!strings.Contains(out, "stopping consumer to avoid committing a later offset") {
		t.Fatalf("missing explicit stop-on-exhaustion log; logs:\n%s", out)
	}

	// No reprocessing and no increment: the applied event was applied
	// exactly once with its original absolute score.
	if n := up.callCount(); n != 1 {
		t.Fatalf("updater calls = %d, want 1", n)
	}
	if args := up.argSnapshot(); len(args) != 1 || args[0] != [2]int{32, 9500} {
		t.Fatalf("updater args = %v, want exactly [[32 9500]]", args)
	}
	if v := up.bestOf(32); v != 9500 {
		t.Fatalf("best score = %d, want exactly 9500 (absolute set, no increment)", v)
	}

	// Five backoff sleeps (500ms after each rejection) precede the stop.
	if elapsed < 2500*time.Millisecond {
		t.Fatalf("elapsed %s before stop, want >= 2.5s (5 x 500ms backoff)", elapsed)
	}
}
