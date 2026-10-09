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
//     (set semantics — no increments or accumulated deltas in this layer).
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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
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
type fakeReader struct {
	mu      sync.Mutex
	log     *seqLog
	queue   []kafka.Message
	fetched int
	commits []kafka.Message
	closes  int
}

func newFakeReader(lg *seqLog, msgs ...kafka.Message) *fakeReader {
	return &fakeReader{log: lg, queue: msgs}
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
	for _, m := range msgs {
		r.commits = append(r.commits, m)
		r.log.add("commit partition=%d offset=%d", m.Partition, m.Offset)
	}
	r.mu.Unlock()
	return nil
}

func (r *fakeReader) Close() error {
	r.mu.Lock()
	r.closes++
	r.mu.Unlock()
	return nil
}

func (r *fakeReader) commitSnapshot() []kafka.Message {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]kafka.Message, len(r.commits))
	copy(out, r.commits)
	return out
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

	runConsumerUntil(t, c, func() bool { return len(rd.commitSnapshot()) == 1 })

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
	if commits := rd.commitSnapshot(); len(commits) != 1 ||
		commits[0].Partition != 0 || commits[0].Offset != 7 {
		t.Fatalf("commits = %+v, want exactly one for partition 0 offset 7", commits)
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
	runConsumerUntil(t, c, func() bool { return len(rd.commitSnapshot()) == 1 })
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

	if n := len(rd.commitSnapshot()); n != 0 {
		t.Fatalf("committed %d offsets while the update was failing, want 0", n)
	}
	if n := up.callCount(); n != 1 {
		t.Fatalf("updater attempts = %d, want 1 (cancelled during first backoff)", n)
	}
	// Fetched once, committed zero times: the event was in flight when the
	// loop shut down, so the broker still owns it and will redeliver it to
	// the next generation — the at-least-once redelivery state.
	if fetched, committed := rd.fetchCount(), len(rd.commitSnapshot()); fetched != 1 || committed != 0 {
		t.Fatalf("fetched=%d commits=%d, want fetched=1 commits=0 (uncommitted => redeliverable)", fetched, committed)
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

	runConsumerUntil(t, c, func() bool { return len(rd.commitSnapshot()) == 4 })

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

	runConsumerUntil(t, c, func() bool { return len(rd.commitSnapshot()) == 2 })

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
	// same partition/offset, which is harmless for the coordinator.
	want := []string{
		"update-ok user=32 score=9500",
		"commit partition=0 offset=12",
		"update-ok user=32 score=9500",
		"commit partition=0 offset=12",
	}
	if got := lg.snapshot(); !sameSequence(got, want) {
		t.Fatalf("sequence mismatch:\n got: %v\nwant: %v", got, want)
	}
	commits := rd.commitSnapshot()
	if len(commits) != 2 || commits[0].Offset != commits[1].Offset {
		t.Fatalf("commits = %+v, want two commits of the same offset", commits)
	}
}
