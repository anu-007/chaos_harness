# Convenience targets for the FarLabs dispatcher stack.
# `chaos` and `test` are wired in later steps (23/24); `up/down/logs/ps` work now.

.PHONY: up down logs ps

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
