# Coordinator image. Postgres schema (db/schema.sql) is applied at runtime on startup,
# so the whole repo's db/ dir is copied in and resolved relative to the app.
FROM python:3.11-slim

WORKDIR /app

# Install deps first for better layer caching.
COPY coordinator/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Application code and the shared schema.
COPY coordinator/ ./coordinator/
COPY db/ ./db/

# Run from the coordinator package dir so `from db import Database` resolves to
# coordinator/db.py, and the default schema path (../db/schema.sql) resolves to /app/db.
WORKDIR /app/coordinator

EXPOSE 8080

CMD ["python", "app.py"]
