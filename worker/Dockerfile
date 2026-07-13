# Worker image. Dials the LB outbound over WebSocket; needs only aiohttp.
FROM python:3.11-slim

WORKDIR /app

COPY worker/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY worker/ ./worker/

WORKDIR /app/worker

CMD ["python", "worker.py"]
