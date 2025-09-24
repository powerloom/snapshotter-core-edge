FROM python:3.12-slim

RUN apt-get update && apt-get install -y \
    build-essential git curl\
    && rm -rf /var/lib/apt/lists/*

# Install CA certificates
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

# Install poetry
RUN pip install poetry

# Copy the application's dependencies files
COPY poetry.lock pyproject.toml ./

# Install the Python dependencies
RUN poetry install --no-root

# Copy the rest of the application's files
COPY . .

RUN git clone --branch bds_eth_uniswapv3_core --single-branch --depth 1 https://github.com/powerloom/snapshotter-computes /computes

# Make the shell scripts executable
RUN chmod +x ./snapshotter_autofill.sh ./init_processes.sh ./bootstrap.sh
