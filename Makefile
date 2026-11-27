# Repo-wide task entry points. Currently: protobuf regeneration.

PROTO_DIR := proto
GEN_DIR := internal/gen

# proto regenerates the committed Go sources from the .proto contract.
# paths=source_relative keeps outputs inside $(GEN_DIR)/leaderboard/v1/
# (mirroring the proto tree) — never at the repository root.
.PHONY: proto
proto:
	mkdir -p $(GEN_DIR)
	protoc \
		--proto_path=$(PROTO_DIR) \
		--go_out=$(GEN_DIR) --go_opt=paths=source_relative \
		--go-grpc_out=$(GEN_DIR) --go-grpc_opt=paths=source_relative \
		$(PROTO_DIR)/leaderboard/v1/leaderboard.proto
