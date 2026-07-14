# Chaos-harness runner image.
#
# The harness both talks HTTP to the stack (aiohttp) AND shells out to the `docker` CLI to
# kill/start coordinators and workers (see _docker() in chaos_harness.py). So the image needs
# the docker client binary; the daemon is reached via the mounted /var/run/docker.sock, which
# lets `docker kill c2` from inside this container act on sibling containers on the host.
FROM python:3.11-slim

# docker CLI only (no daemon) — used to kill/start peer containers via the mounted socket.
# NOTE: the Debian `docker.io` apt package is unusable on python:3.11-slim — the slim image
# ships /etc/dpkg/dpkg.cfg.d/docker path-excludes that strip the /usr/bin/docker binary, so
# `docker` ends up "not found" even though dpkg reports it installed. Instead pull the official
# static docker CLI binary (client only) for the target arch straight from download.docker.com.
ARG DOCKER_CLI_VERSION=27.3.1
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends ca-certificates curl; \
    arch="$(dpkg --print-architecture)"; \
    case "$arch" in \
        amd64) dl_arch="x86_64" ;; \
        arm64) dl_arch="aarch64" ;; \
        *) echo "unsupported arch: $arch" >&2; exit 1 ;; \
    esac; \
    curl -fsSL "https://download.docker.com/linux/static/stable/${dl_arch}/docker-${DOCKER_CLI_VERSION}.tgz" -o /tmp/docker.tgz; \
    tar -xzf /tmp/docker.tgz -C /tmp; \
    install -m 0755 /tmp/docker/docker /usr/local/bin/docker; \
    rm -rf /tmp/docker /tmp/docker.tgz; \
    apt-get purge -y curl; apt-get autoremove -y; rm -rf /var/lib/apt/lists/*; \
    docker --version

WORKDIR /app

COPY harness/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# The harness file is also bind-mounted in compose so edits don't require a rebuild; copying
# it here keeps the image runnable standalone.
COPY chaos_harness.py ./chaos_harness.py

# No default CMD: the harness is always launched via `docker compose run harness ...` with the
# explicit flags from the Makefile, so its args aren't baked into the image.
ENTRYPOINT ["python3"]
