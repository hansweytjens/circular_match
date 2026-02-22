FROM python:3.12

# Set working directory (matches devcontainer workspace)
WORKDIR /workspace

# Optional basic tools
RUN apt-get update && apt-get install -y git \
    && rm -rf /var/lib/apt/lists/*

# Install dependencies only (not your source code)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

