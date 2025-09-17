# Use slim Python base
FROM python:3.11-slim

# System deps (optional but handy for scientific stacks)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl && \
    rm -rf /var/lib/apt/lists/*

# App dir
WORKDIR /app

# Copy code & requirements
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy your notebooks/scripts
COPY participation_nb.py /app/participation_nb.py
COPY stage_calculation.py /app/stage_calculation.py

# Persist on container filesystem
RUN mkdir -p /app/.cache

# Env
ENV PYTHONUNBUFFERED=1 \
    MARIMO_DISABLE_TELEMETRY=1

# Expose marimo port
EXPOSE 2718

# Launch marimo serving the notebook
CMD ["python", "-m", "marimo", "run", "/app/participation_nb.py", "--host", "0.0.0.0", "--port", "2718"]

