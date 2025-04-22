FROM python:3.10.16-slim

RUN apt-get update && apt-get install -y \
    build-essential git curl\
    && rm -rf /var/lib/apt/lists/*

# Install the PM2 process manager for Node.js
RUN pip install poetry

# Copy the application's dependencies files
COPY poetry.lock pyproject.toml ./

# Install the Python dependencies
RUN poetry install --no-root

# Copy the rest of the application's files
COPY . .

# Make the shell scripts executable
RUN chmod +x ./snapshotter_autofill.sh ./init_processes.sh ./bootstrap.sh
