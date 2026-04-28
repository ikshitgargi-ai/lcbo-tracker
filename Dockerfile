# LCBO Tracker — Flask backend
# Runs on Fly.io (default), Railway, or any container host.
FROM python:3.11.11-slim

# System deps for psycopg2-binary + general health
RUN apt-get update && apt-get install -y --no-install-recommends \
      curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python deps first for layer caching
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy app source
COPY . .

# Defaults — Fly.io / Railway override $PORT at runtime
ENV PORT=8080 \
    PYTHONUNBUFFERED=1 \
    FLASK_DEBUG=false

EXPOSE 8080

# 1800s timeout for the Daily-A SOD sync (1.5M rows ~22 min on small instance);
# single worker so APScheduler runs once (no duplicate cron firings).
CMD ["sh", "-c", "gunicorn app:app --bind 0.0.0.0:${PORT} --timeout 1800 --workers 1 --access-logfile - --error-logfile -"]
