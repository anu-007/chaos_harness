# Chaos-harness runner image.
#
# The harness both talks HTTP to the stack (aiohttp) AND shells out to the `docker` CLI to
# kill/start coordinators and workers (see _docker() in chaos_harness.py). So the image needs
# the docker client binary; the daemon is reached via the mounted /var/run/docker.sock, which
# lets `docker kill c2` from inside this container act on sibling containers on the host.
FROM python:3.11-slim

# docker CLI only (no daemon) — used to kill/start peer containers via the mounted socket.
RUN apt-get update \
    && apt-get install -y --no-install-recommends docker.io \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY harness/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# The harness file is also bind-mounted in compose so edits don't require a rebuild; copying
# it here keeps the image runnable standalone.
COPY chaos_harness.py ./chaos_harness.py

# No default CMD: the harness is always launched via `docker compose run harness ...` with the
# explicit flags from the Makefile, so its args aren't baked into the image.
ENTRYPOINT ["python3"]
