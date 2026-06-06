FROM python:3.11-slim

# Force Python to flush print() output immediately so Docker logs are never blank.
# Without this, Python buffers stdout and nothing appears in `docker logs`.
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ ./src/

# Create data directory for ledger persistence
RUN mkdir -p /app/data

# Default: run as a consensus node
CMD ["python", "-m", "src.node"]
