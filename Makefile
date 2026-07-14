# Convenience targets for the FarLabs dispatcher stack.
# `up/down/logs/ps` manage the stack; `chaos` runs the harness; `test` runs the pytest suite.

.PHONY: up down logs ps chaos test

# Duration (s) and rate (jobs/s) for `make chaos`; override on the CLI, e.g.
#   make chaos DURATION=60
# For a fresh (non-seed-tuned) run set HARNESS_SEED, e.g. `make chaos HARNESS_SEED=42`.
DURATION ?= 600
RATE ?= 50

# Bring the whole stack up (builds coordinator images). One command, no manual steps.
up:
	docker compose up -d --build

# Tear everything down and drop volumes (fresh Postgres/Redis next time).
down:
	docker compose down -v

# Follow logs for all services.
logs:
	docker compose logs -f

# Show container status.
ps:
	docker compose ps

# Run the chaos harness against the running stack, then copy its report to the repo root.
# The harness container writes to the mounted /out dir (./harness_out), which we then surface
# as ./chaos_report.txt. Uses container names so the harness can DNS-resolve + docker-kill them.
# The harness exits non-zero when it records invariant violations; we capture that code but
# STILL copy the report to the repo root (so a failing run is always inspectable), then
# re-raise the harness exit code so `make chaos` reflects the true verdict.
chaos:
	mkdir -p harness_out
	@docker compose --profile chaos run --rm harness chaos_harness.py \
		--base http://lb:8080 \
		--coords c1,c2,c3 \
		--workers w1,w2,w3,w4,w5 \
		--duration $(DURATION) \
		--rate $(RATE) \
		--report /out/chaos_report.txt; \
	rc=$$?; \
	cp harness_out/chaos_report.txt chaos_report.txt 2>/dev/null && echo "report -> chaos_report.txt" || echo "no report produced"; \
	exit $$rc

# Run the pytest invariant suite in the harness image against the compose Postgres/Redis.
# Covers dedup (one id per key), strictly-increasing fences, exactly-once commit, expired-
# lease rejection, reaper higher-fence requeue, and drop_acks single-accept. Needs the stack
# up (make up) for Postgres; the ENTRYPOINT is python3, so `-m pytest` runs the suite.
test:
	docker compose --profile chaos run --rm harness -m pytest -q
