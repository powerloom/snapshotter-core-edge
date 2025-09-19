## Table of Contents
- [Table of Contents](#table-of-contents)
- [Overview](#overview)
- [Major Components](#major-components)
  - [Epoch Syncer Service](#epoch-syncer-service)
  - [Processor Distributor](#processor-distributor)
  - [Worker Services](#worker-services)
    - [Snapshot Workers](#snapshot-workers)
    - [Aggregation Workers](#aggregation-workers)
    - [Cacher Worker](#cacher-worker)
  - [Core API](#core-api)
    - [Health and Status Endpoints](#health-and-status-endpoints)
    - [Epoch Information Endpoints](#epoch-information-endpoints)
    - [Snapshot Data Endpoints](#snapshot-data-endpoints)
    - [Time Series Data Endpoint](#time-series-data-endpoint)
    - [Authentication API Endpoints](#authentication-api-endpoints)
    - [Uniswap V3 API Endpoints](#uniswap-v3-api-endpoints)
  - [Periphery Services](#periphery-services)
    - [Block Fetcher Service](#block-fetcher-service)
    - [Transaction Processor Service](#transaction-processor-service)
    - [Epoch Syncer Service](#epoch-syncer-service-1)
    - [Rate Limiter Service](#rate-limiter-service)
- [Setup](#setup)
  - [Quick Start](#quick-start)
- [Increase IPFS memory limits (for complex use cases)](#increase-ipfs-memory-limits-for-complex-use-cases)
- [Development setup and instructions](#development-setup-and-instructions)
  - [Configuration](#configuration)
    - [3. Environment Variables](#3-environment-variables)
    - [4. Automatic Service Generation](#4-automatic-service-generation)
  - [Periphery Services Configuration](#periphery-services-configuration)
    - [Block Fetcher Service Configuration](#block-fetcher-service-configuration)
    - [Transaction Processor Service Configuration](#transaction-processor-service-configuration)
    - [Epoch Syncer Service Configuration](#epoch-syncer-service-configuration)
    - [Rate Limiter Service Configuration](#rate-limiter-service-configuration)
    - [Docker Compose Configuration](#docker-compose-configuration)
    - [Integration with Core Snapshotter](#integration-with-core-snapshotter)
- [Testing Environment Setup](#testing-environment-setup)
  - [Prerequisites](#prerequisites)
  - [Step-by-Step Setup](#step-by-step-setup)
  - [Test Configuration Fields](#test-configuration-fields)
    - [**RPC Settings (Required)**](#rpc-settings-required)
    - [\*\*Anchor RPC Settings \*\*](#anchor-rpc-settings-)
    - [**IPFS Settings (Required)**](#ipfs-settings-required)
    - [**Redis Settings (Required)**](#redis-settings-required)
    - [**Core API Settings**](#core-api-settings)
    - [**Protocol Settings (Required)**](#protocol-settings-required)
    - [**External API Settings (Optional)**](#external-api-settings-optional)
  - [Environment Verification](#environment-verification)
  - [Test-Specific Environment Variables](#test-specific-environment-variables)
  - [Running the Configuration Loading Test](#running-the-configuration-loading-test)
  - [Troubleshooting](#troubleshooting)
  - [API Documentation](#api-documentation)
- [Find us](#find-us)

## Overview

Snapshotter Core Edge is the next iteration of Snapshotter Core ([Pooler](https://github.com/PowerLoom/snapshotter-core)) repository. 

**Key Architecture Improvements:**
* **Message Queue**: The system now uses Redis with Dramatiq instead of RabbitMQ for improved performance and simplified deployment
* **Modular Services**: New Periphery services (Block Fetcher, Transaction Processor, Epoch Syncer) are introduced and are separate components that work together to provide efficient blockchain data processing.
* **Dynamic Worker Generation**: Worker services are automatically generated based on your project and aggregator configurations
* **Streamlined Directory Structure**: Compute modules linked to [snapshotter-computes](https://github.com/PowerLoom/snapshotter-computes/) and configurations in [snapshotter-configs](https://github.com/PowerLoom/snapshotter-configs/) live in `/computes` and `/config` respectively

## Major Components

### Epoch Syncer Service

The Epoch Syncer service (Periphery service) replaces the previous System Event Detector and provides enhanced functionality:
- Monitors both source chain blocks and protocol state events
- Verifies data availability in Redis cache before triggering snapshot generation
- Ensures all required blocks, transactions, and receipts are cached
- Sends events to the Processor Distributor via Redis/Dramatiq queues
- Prevents wasted computation by checking cache completeness

Related information and other services depending on these can be found in previous sections: [State Transitions](#state-transitions), [Configuration](#configuration).

### Processor Distributor
The Processor Distributor, defined in [`processor_distributor.py`](snapshotter/processor_distributor.py), is the central coordinator of the snapshot generation process.

* It loads the preloader, base snapshotting, and aggregator config information from the settings files
* It receives events from the Epoch Syncer via the Redis-backed Dramatiq queue: `f'powerloom-event-detector_{settings.namespace}_{settings.instance_id}'`
* It creates and distributes processing messages based on:
  - Preloader configuration in `config/preloader.json`
  - Project configuration in `config/projects.json`
  - Aggregator configuration in `config/aggregator.json`
* For `EpochReleased` events:
  - Executes preloaders if configured to prepare data
  - Distributes work to snapshot workers for each project type
  - Each project type has its own dedicated worker pool
* For `ProcessingComplete` events:
  - Triggers aggregation workers to process completed base snapshots
  - Routes messages based on aggregation dependencies


### Worker Services

The system uses specialized workers for different tasks, all communicating through Redis/Dramatiq message queues. Worker services are automatically generated based on your configuration files.

#### Snapshot Workers
- **Purpose**: Build base snapshots according to `config/projects.json`
- **Auto-generation**: Each project type gets its own worker service (e.g., `snapshotter-worker-trade-volume`, `snapshotter-worker-pair-total-reserves`)
- **Queue**: Listen on project-specific queues: `f'powerloom-snapshotter_{settings.namespace}_{settings.instance_id}-{project_type}'`
- **Compute Logic**: Execute compute modules from `/computes/` directory
- **Implementation**: [`snapshotter/utils/snapshot_worker.py`](snapshotter/utils/snapshot_worker.py)

#### Aggregation Workers
- **Purpose**: Build aggregate snapshots according to `config/aggregator.json`
- **Processing**: Transform completed base snapshots into higher-order aggregates
- **Queue**: Listen on: `f'powerloom-aggregator_{settings.namespace}_{settings.instance_id}'`
- **Compute Logic**: Execute modules from `/computes/aggregates/` directory
- **Implementation**: [`snapshotter/utils/aggregation_worker.py`](snapshotter/utils/aggregation_worker.py)

#### Cacher Worker
- **Purpose**: Manage snapshot data caching and state updates
- **Events Handled**: `SnapshotSubmitted`, `SnapshotFinalized`, `SnapshotBatchSubmitted`
- **Queue**: Listen on: `f'powerloom-cacher_{settings.namespace}_{settings.instance_id}'`
- **Functionality**: Maintains project data in Redis with TTL management
- **Implementation**: [`snapshotter/cacher.py`](snapshotter/cacher.py)

Upon receiving a message from the processor distributor, the workers validate inputs and call the `compute()` function on the configured compute class to generate snapshots.

### Core API

This component is one of the most important and allows you to access the finalized protocol state on the smart contract running on the anchor chain. Find it in [`core_api.py`](snapshotter/core_api.py).

The Core API service exposes several endpoints for accessing snapshot data and system information. All API endpoints are accessible via HTTP on port 8002 by default.

#### Health and Status Endpoints

| Endpoint | Method | Description |
|----------|---------|-------------|
| `/health` | GET | Check the health status of the Snapshotter service |
| `/current_epoch` | GET | Get the current epoch data from the protocol state contract |
| `/latest_epoch_info` | GET | Get the latest epoch information for active pools |

#### Epoch Information Endpoints

| Endpoint | Method | Description |
|----------|---------|-------------|
| `/epoch/{epoch_id}` | GET | Get epoch information for a specific epoch ID |
| `/last_finalized_epoch/{project_id}` | GET | Get the last finalized epoch information for a given project |
| `/get_previous_epoch_info/{epoch_id}` | GET | Get previous epoch information for a given epoch ID |

#### Snapshot Data Endpoints

| Endpoint | Method | Description |
|----------|---------|-------------|
| `/data/{epoch_id}/{project_id}/` | GET | Get snapshot data for a given epoch and project ID |
| `/cid/{epoch_id}/{project_id}/` | GET | Get the finalized CID for a given epoch and project ID |
| `/previous_snapshots_data/{pool_address}/{epoch_id}` | GET | Get previous snapshots data for a specific pool address |

#### Time Series Data Endpoint

| Endpoint | Method | Description |
|----------|---------|-------------|
| `/time_series/{epoch_id}/{start_time}/{step_seconds}/{project_id}` | GET | Get time series data points at specified intervals |

**Parameters:**
- `epoch_id`: The epoch ID to end the series at
- `start_time`: Unix timestamp in seconds of when to begin data
- `step_seconds`: Length of time in seconds between data points
- `project_id`: The ID of the project to get data for

**Limitations:**
- Maximum 200 observations per request
- Start time must be before the epoch timestamp

#### Authentication API Endpoints

The authentication service (`snapshotter/auth/server_entry.py`) provides user management and API key functionality:

| Endpoint | Method | Description |
|----------|---------|-------------|
| `/user` | POST | Create a new user |
| `/user/{email}/api_key` | POST | Generate an API key for a user |
| `/user/{email}/api_key` | DELETE | Delete a user's API key |
| `/user/{email}` | GET | Get user information |
| `/users` | GET | Get all users (admin only) |

**Note:** Authentication endpoints require proper authorization headers and are typically used for managing access to the Core API endpoints.

#### Uniswap V3 API Endpoints

The Uniswap V3 compute modules expose a rich set of endpoints for accessing detailed data. These are defined in `computes/api/router.py`.

| Endpoint | Method | Description |
|----------|---------|-------------|
| `/pool/{pool_address}/metadata` | GET | Retrieve metadata for a specific Uniswap V3 pool. |
| `/token/{token_address}/pools` | GET | Retrieve all pools associated with a specific token. |
| `/ethPrice` or `/ethPrice/{block_number}` | GET | Retrieve the ETH price snapshot for the latest or a specific block. |
| `/token/price/{token_address}/{pool_address}` | GET | Retrieve the price of a token in a specific pool, optionally at a specific block. |
| `/snapshot/base_all_pools/{token_address}` | GET | Retrieve base snapshots for all pools of a given token. |
| `/snapshot/base/{pool_address}` | GET | Retrieve the base snapshot for a specific pool, optionally at a specific block. |
| `/snapshot/trades/{pool_address}` | GET | Retrieve the trades snapshot for a specific pool, optionally at a specific block. |
| `/snapshot/allTrades` | GET | Retrieve the trades snapshot for all pools, optionally at a specific block. |
| `/tokenPrices/all/{token_address}` | GET | Retrieve all price snapshots for a token, optionally at a specific block. |
| `/tradeVolumeAllPools/{token_address}/{time_interval}` | GET | Retrieve aggregated trade volume for all pools of a token over a time interval. |
| `/tradeVolume/{pool_address}/{time_interval}` | GET | Retrieve aggregated trade volume for a specific pool over a time interval. |
| `/poolTrades/{pool_address}/{start_timestamp}/{end_timestamp}` | GET | Retrieve all trades for a pool between two timestamps. |
| `/timeSeries/{token_address}/{pool_address}/{time_interval}/{step_seconds}` | GET | Retrieve a time series of token prices. |
| `/dailyActiveTokens` | GET | Get a paginated list of daily active tokens with frequencies. |
| `/dailyActivePools` | GET | Get a paginated list of daily active pools with frequencies. |



### Periphery Services

The Snapshotter includes several specialized Periphery services that handle specific aspects of blockchain data processing. These services work together to provide efficient, scalable, and reliable data collection and processing.

#### Block Fetcher Service

The Block Fetcher Service (`snapshotter-periphery-blockfetcher`)[https://github.com/powerloom/snapshotter-periphery-blockfetcher/] is responsible for continuously fetching blockchain blocks and caching them in Redis for downstream processing.

**Key Features:**
- **Continuous Block Monitoring**: Tracks the latest blocks on the source blockchain
- **Efficient Caching**: Stores complete block data in Redis for quick access
- **Configurable Polling**: Adjustable polling intervals for different network conditions
- **Test Mode**: Special mode for development and testing with single block processing
- **Graceful Shutdown**: Handles shutdown signals properly to ensure data consistency

**Architecture:**
```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Blockchain RPC │◄────┤  Block Fetcher  │────▶│  Redis Cache    │
│                 │     │    Service      │     │                 │
└─────────────────┘     └─────────────────┘     └─────────────────┘
```

**Local Testing:**
```bash
cd snapshotter-periphery-blockfetcher
docker-compose --profile local up --build
```

#### Transaction Processor Service

The Transaction Processor Service (`snapshotter-periphery-txprocessor`)[https://github.com/powerloom/snapshotter-periphery-txprocessor/] processes transactions from cached blocks and extracts relevant information for snapshot generation.

**Key Features:**
- **Transaction Receipt Processing**: Fetches and caches detailed transaction receipts
- **Event Log Extraction**: Extracts and indexes event logs from transactions
- **Redis-based Queue System**: Consumes processing tasks from Redis queues
- **Parallel Processing**: Handles multiple transactions concurrently for efficiency
- **Comprehensive Logging**: Detailed logging for monitoring and debugging

**Architecture:**
```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Redis Queue    │────▶│  TX Processor   │────▶│  Redis Cache    │
│  (Block Data)   │     │    Service      │     │  (TX Receipts)  │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                                │
                                ▼
                        ┌─────────────────┐
                        │  Blockchain RPC │
                        └─────────────────┘
```

#### Epoch Syncer Service

The Epoch Syncer Service (`snapshotter-periphery-epochsyncer`)[https://github.com/powerloom/snapshotter-periphery-epochsyncer/] monitors blockchain events and ensures data availability before triggering snapshot generation.

**Key Features:**
- **Dual Chain Monitoring**: Monitors both source chain blocks and protocol state events
- **Cache Verification**: Ensures both block and transaction data are cached before processing
- **Event Detection**: Detects `DayStartedEvent` and `SnapshotBatchSubmitted` events
- **Dramatiq Integration**: Uses Dramatiq for reliable message queue processing
- **Adaptive Polling**: Automatically adjusts polling intervals based on throughput
- **Complete Cache Validation**: Verifies both block and transaction cache completeness

**Architecture:**
```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│                 │     │                 │     │                 │
│  Blockchain RPC │◄────┤  EpochSyncer    │◄────┤  Rate Limiter   │
│                 │     │                 │     │                 │
└─────────────────┘     └────────┬────────┘     └─────────────────┘
                                 │
                                 ▼
┌─────────────────┐     ┌─────────────────┐     
│                 │     │                 │     
│  Redis Cache    │◄────┤  Cache Checker  │     
│                 │     │  (Background)   │     
└─────────────────┘     └────────┬────────┘     
                                 │
                                 ▼
                        ┌─────────────────┐
                        │                 │
                        │  Dramatiq       │
                        │  Workers        │
                        │                 │
                        └─────────────────┘
```

**Key Components:**
1. **Source Chain Block Detection**: Monitors source blockchain for new blocks
2. **Protocol Event Detection**: Monitors protocol state contract for epoch events
3. **Cache Completeness Verification**: Ensures all required data is cached
4. **Message Queue Integration**: Sends messages to downstream workers via Dramatiq

#### Rate Limiter Service

The Rate Limiter Service (`rate-limiter`)[https://github.com/powerloom/rate-limiter/] provides centralized rate limiting for all RPC calls across the Snapshotter ecosystem.

**Key Features:**
- **Multiple Rate Limits**: Configure different rate limits for different keys
- **Statistics Tracking**: Track hourly and daily usage for each key
- **In-Memory Storage**: Fast, efficient rate limit checking
- **RESTful API**: Simple HTTP API for rate limit management
- **Health Monitoring**: Built-in health check endpoint

**API Endpoints:**

| Endpoint | Method | Description |
|----------|---------|-------------|
| `/check/{key}` | GET | Check if a key is within its rate limit |
| `/configure` | POST | Configure a custom rate limit for a key |
| `/stats/{key}` | GET | Get usage statistics for a key |
| `/health` | GET | Health check endpoint |

**Rate Limit Format:**
```
{number}/{unit}
```
Where:
- `number`: A positive integer
- `unit`: One of "second", "minute", "hour", "day"

Examples: `10/second`, `100/minute`, `1000/hour`, `5000/day`

**Usage Example:**
```bash
# Check rate limit
curl http://localhost:8000/check/my-api-key

# Configure custom rate limit
curl -X POST http://localhost:8000/configure \
  -H "Content-Type: application/json" \
  -d '{"key": "my-api-key", "limit": "100/minute"}'

# Get statistics
curl http://localhost:8000/stats/my-api-key
```

## Setup

The snapshotter is a distributed system with multiple moving parts. The easiest way to get started is by using the Docker-based setup.

### Quick Start

1. **Clone the repository**:
   ```bash
   git clone https://github.com/PowerLoom/snapshotter-core-edge.git
   cd snapshotter-core-edge
   ```
2. **Configure your environment**:
   ```bash
   cp env.example .env
   # Edit .env with your settings
   ```

3. **Run the bootstrap script** to set up Periphery services:
   ```bash
   ./bootstrap.sh
   ```

4. **Build and run**:
   ```bash
   ./build.sh
   ```

The `bootstrap.sh` script automatically:
- Sets up all Periphery services (Block Fetcher, TX Processor, Epoch Syncer)
- Configures Redis and IPFS
- Generates the docker-compose.yaml with all required services
- Creates worker services based on your project configuration

**Note** - RPC usage is highly use-case specific. If your use case is complicated and needs to make a lot of RPC calls, it is recommended to run your own RPC node instead of using third-party RPC services as it can be expensive.


## Increase IPFS memory limits (for complex use cases)

If you want to increase the memory limits for IPFS, you can do so by running the following commands, this will reset on system restart though:

```bash
sudo sysctl -w net.core.rmem_max=8388608
sudo sysctl -w net.core.wmem_max=8388608
sudo sysctl -w net.ipv4.udp_mem='8388608 8388608 8388608'
sudo sysctl -w net.core.netdev_max_backlog=5000
sudo sysctl -w net.ipv4.tcp_rmem='4096 87380 8388608'
sudo sysctl -w net.ipv4.tcp_wmem='4096 87380 8388608'
```

To make these changes permanent, add or modify the following lines in /etc/sysctl.conf:

```
net.core.rmem_max=8388608
net.core.wmem_max=8388608
net.ipv4.udp_mem=8388608 8388608 8388608
net.core.netdev_max_backlog=5000
net.ipv4.tcp_rmem=4096 87380 8388608
net.ipv4.tcp_wmem=4096 87380 8388608
```

Apply the changes with:

```bash
sudo sysctl -p
```

Restart the docker service

```bash
sudo systemctl restart docker
```

Finally, bring up your Docker Compose stack again:

```bash
./clean_stop.sh
./build.sh
```
## Development setup and instructions
### Configuration
The snapshotter needs the following config files to be present:

* **`config/auth_settings.json`**: Authentication configuration. Copy from `config/auth_settings.example.json`. This enables an authentication layer over the core API.

* **`config/settings.json`**: The primary configuration file. Copy from `config/settings.example.json` and configure:
  - `instance_id`: Your unique node identifier
  - `namespace`: Project namespace for consensus
  - RPC endpoints and rate limits
  - Redis connection settings
  - Protocol state contract details

* **`config/projects.json`**: Defines base snapshot computation tasks. Each entry maps a project type to a compute module.
* **`config/aggregator.json`**: Defines aggregation tasks over base snapshots. Copy `config/aggregator.example.json` to `config/aggregator.json`.
* **`config/preloader.json`**: Optional preloading configuration for data that needs to be cached before snapshot generation. Copy `config/preloader.example.json` to `config/preloader.json`.

#### 3. Environment Variables

The `.env` file contains all the runtime configuration. Key variables include:

```bash
# Source Chain RPC
SOURCE_RPC_URL=https://your-ethereum-rpc
SOURCE_RPC_RATE_LIMIT="100/second"

# PowerLoom Protocol  
NAMESPACE=your_namespace
INSTANCE_ID=your_unique_instance_id
SIGNER_ACCOUNT_ADDRESS=0x...
SIGNER_ACCOUNT_PRIVATE_KEY=0x...

# Redis Configuration
REDIS_HOST=redis
REDIS_PORT=6379
REDIS_DB=0

# IPFS Configuration
IPFS_URL=http://ipfs:5001
IPFS_READER_URL=http://ipfs:8080

# Protocol State Contract
PROTOCOL_STATE_CONTRACT=0x...
POWERLOOM_RPC_URL=https://rpc-prost.powerloom.io
```

#### 4. Automatic Service Generation

When you run `./build.sh`, it:
1. Reads your `projects.json` and creates a worker service for each project type
2. Reads your `aggregator.json` and configures aggregation workers
3. Generates the final `docker-compose.yaml` with all services properly configured
4. Sets up networking and dependencies between services

For example, if you have project types `trade_volume` and `pair_reserves`, the build script will create:
- `snapshotter-worker-trade-volume` service
- `snapshotter-worker-pair-reserves` service
- Plus all the Periphery services (block fetcher, tx processor, epoch syncer)

### Periphery Services Configuration

The new distributed architecture requires configuration for each Periphery service. These services work together to provide efficient blockchain data processing.

#### Block Fetcher Service Configuration

The Block Fetcher service requires the following environment variables:

```bash
# RPC Configuration
SOURCE_RPC_URL=https://your-rpc-endpoint
SOURCE_RPC_RATE_LIMIT="100/second"
SOURCE_RPC_REQUEST_TIMEOUT=30
SOURCE_RPC_RETRY_COUNT=3

# Redis Configuration
REDIS_URL=redis://localhost:6379/0

# Service Configuration
NAMESPACE=your_namespace
POLLING_INTERVAL=1.0
TEST_MODE=false  # Set to true for single block testing
```

#### Transaction Processor Service Configuration

The Transaction Processor service configuration:

```bash
# RPC Configuration
SOURCE_RPC_URL=https://your-rpc-endpoint
SOURCE_RPC_RATE_LIMIT="100/second"

# Redis Configuration
REDIS_URL=redis://localhost:6379/0

# Service Configuration
NAMESPACE=your_namespace
CONCURRENT_WORKERS=10
BATCH_SIZE=50
```

#### Epoch Syncer Service Configuration

The Epoch Syncer service requires both source chain and PowerLoom chain configuration:

```bash
# Source Chain RPC
SOURCE_RPC_URL=https://your-source-rpc-endpoint
SOURCE_RPC_RATE_LIMIT="100/second"

# PowerLoom Chain RPC
POWERLOOM_RPC_URL=https://your-powerloom-rpc-endpoint
POWERLOOM_RPC_RATE_LIMIT="50/second"

# Redis Configuration
REDIS_URL=redis://localhost:6379/0

# Protocol Configuration
PROTOCOL_STATE_CONTRACT_ADDRESS=0x...
DATA_MARKET_CONTRACT_ADDRESS=0x...
NAMESPACE=your_namespace
INSTANCE_ID=your_instance_id

# Performance Settings
BATCH_SIZE=50
MAX_CONCURRENT_CACHE_CHECKS=20
ADAPTIVE_POLLING=true
```

#### Rate Limiter Service Configuration

The Rate Limiter service configuration is minimal:

```bash
# Default rate limit for all keys
DEFAULT_RATE_LIMIT="10/second"

# Service port
PORT=8000
```

#### Docker Compose Configuration

For local development, each Periphery service includes a Docker Compose configuration. To run services locally:

```bash
# Block Fetcher
cd snapshotter-periphery-blockfetcher
docker-compose --profile local up --build

# Transaction Processor
cd snapshotter-periphery-txprocessor
docker-compose up --build

# Epoch Syncer
cd snapshotter-periphery-epochsyncer
docker-compose up --build

# Rate Limiter
cd rate-limiter
docker build -t rate-limiter .
docker run -p 8000:8000 rate-limiter
```

#### Integration with Core Snapshotter

The Periphery services integrate with the core snapshotter through:

1. **Shared Redis Cache**: All services use the same Redis instance for data sharing
2. **Message Queues**: Services communicate through Redis-based queues and Dramatiq
3. **Rate Limiting**: All RPC calls go through the centralized rate limiter
4. **Namespace Consistency**: All services must use the same namespace configuration

Ensure all services are configured with:
- Same Redis connection parameters
- Same namespace value
- Compatible RPC endpoints
- Proper network connectivity between services

## Testing Environment Setup

To ensure a consistent and correct testing environment, follow these steps to configure your virtual environment and verify the test configuration loading mechanism.

### Prerequisites

This project uses Poetry 2.0+ for dependency management and requires Python 3.12.

**Required Tools:**
- **Python 3.12.x**: Using `pyenv` for Python version management is strongly recommended
- **Poetry 2.0+**: Modern Python dependency management tool

### Step-by-Step Setup

**1. Install Python 3.12 with pyenv (Recommended)**

If you don't have `pyenv` installed, follow the [pyenv installation guide](https://github.com/pyenv/pyenv#installation).

```bash
# Install Python 3.12 (use the latest available patch version)
pyenv install 3.12.11

# Verify installation
pyenv versions
```

**2. Install Poetry**

If you don't have Poetry installed, follow the [official Poetry installation guide](https://python-poetry.org/docs/#installation).

```bash
# Verify Poetry version (should be 2.0+)
poetry --version
```

**3. Set Up Project Environment**

Navigate to the project root directory and set up the Python version:

```bash
# Navigate to project root
cd /path/to/snapshotter-core-edge

# Set Python version for this project
pyenv local 3.12.11

# Verify correct Python version is active
python --version  # Should show Python 3.12.11
```

**4. Install Dependencies**

```bash
# Install all dependencies including development tools
poetry install

# Verify installation
poetry env info  # Shows virtual environment details
```

**5. Activate Environment**

With Poetry 2.0, you have several options to work with the virtual environment:

```bash
# Option A: Use poetry run for individual commands
poetry run python --version
poetry run pytest tests/

# Option B: Get activation command (recommended for development)
poetry env activate
# Then source the provided activation command

# Option C: Spawn a new shell with environment activated (Requires the shell plugin)
poetry shell
```

**6. Create Test Environment Configuration**

Before running tests, create your test environment configuration:

```bash
# Copy the test environment template
cp env.test.example .env.test

# Edit .env.test with your test configuration values
```

### Test Configuration Fields

The `.env.test` file contains all the configuration values needed for running tests. Here's a breakdown of each section and what values to use:

#### **RPC Settings (Required)**
These settings configure the main blockchain RPC endpoints for testing:

```bash
# Main RPC endpoint - use a reliable Ethereum RPC provider
TEST_RPC_URL_FULL_NODE_1=https://eth-mainnet.alchemyapi.io/v2/YOUR_API_KEY
# Archive node (optional) - for historical data queries
TEST_RPC_URL_ARCHIVE_NODE_1=https://eth-mainnet.alchemyapi.io/v2/YOUR_ARCHIVE_KEY

# Connection settings (defaults are usually fine)
TEST_RPC_REQUEST_TIMEOUT=30          # Request timeout in seconds
TEST_RPC_RETRY_COUNT=3               # Number of retry attempts
TEST_RPC_MAX_CONNECTIONS=100         # Max concurrent connections
TEST_RPC_MAX_KEEPALIVE_CONNECTIONS=50 # Max persistent connections
TEST_RPC_KEEPALIVE_EXPIRY=300        # Connection keep-alive time
```

#### **Anchor RPC Settings **
These configure the Powerloom anchor chain (if different from main RPC):

```bash
# Powerloom-specific anchor chain endpoint
TEST_ANCHOR_RPC_URL_FULL_NODE_1=
TEST_ANCHOR_RPC_URL_ARCHIVE_NODE_1=

# Lower connection limits for anchor chain
TEST_ANCHOR_RPC_MAX_CONNECTIONS=5
TEST_ANCHOR_RPC_MAX_KEEPALIVE_CONNECTIONS=2
```

#### **IPFS Settings (Required)**
Configure IPFS for data storage and retrieval:

```bash
# Local IPFS node (recommended for testing)
TEST_IPFS_URL=/ip4/127.0.0.1/tcp/5001

# Or remote IPFS service
# TEST_IPFS_URL=/dns/your-ipfs-provider.com/tcp/443/https

# IPFS connection settings
TEST_IPFS_TIMEOUT=60                 # Request timeout
TEST_IPFS_MAX_RETRIES=3              # Retry attempts
```

#### **Redis Settings (Required)**
Configure Redis for caching and state management:

```bash
TEST_REDIS_HOST=localhost            # Redis server host
TEST_REDIS_PORT=6379                 # Redis server port
TEST_REDIS_DB=0                      # Database number (0-15)
TEST_REDIS_PASSWORD=                 # Password (empty for no auth)
TEST_REDIS_TIMEOUT=5                 # Connection timeout
```

#### **Core API Settings**
Configure the snapshotter core API:

```bash
TEST_CORE_API_PORT=8002              # Port for core API server
TEST_BLOCK_SHIFT_FOR_BITMAP_INDEX=22400000  # Block indexing offset
```

#### **Protocol Settings (Required)**
Set the contract addresses and namespace:

```bash
# Your unique namespace identifier
TEST_NAMESPACE=my_test_namespace

# Smart contract addresses
TEST_PROTOCOL_STATE_CONTRACT_ADDRESS=0x3B5A0FB70ef68B5dd677C7d614dFB89961f97401
TEST_DATA_MARKET_CONTRACT_ADDRESS=0xae32c4FA72E2e5F53ed4D214E4aD049286Ded16f

# Chain Wrapped ETH
TEST_WETH_ADDRESS=0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2
```

#### **External API Settings (Optional)**
Configure external data providers:

```bash
# Etherscan API
TEST_ETHERSCAN_API_KEY=your_etherscan_api_key_here
TEST_ETHERSCAN_URL=https://api.etherscan.io/v2/

# CoinMarketCap API (for price data)
COINMARKETCAP_API_KEY=your_cmc_api_key_here
COINMARKETCAP_API_URL=https://pro-api.coinmarketcap.com
COINMARKETCAP_API_PRICE_TOLERANCE=5  # Acceptable price variance %
```

**7. Run Tests**

```bash
# Run all tests
poetry run pytest

# Run specific test files
poetry run pytest tests/shared_fixtures/test_config_loading.py

# Run with verbose output
poetry run pytest -v tests/shared_fixtures/test_config_loading.py::test_ipfs_settings_are_correct
```

### Environment Verification

To verify your environment is set up correctly:

```bash
# Check Python version
poetry run python --version

# Check that pytest is available
poetry run pytest --version

# Verify test configuration loads correctly
poetry run pytest tests/shared_fixtures/test_config_loading.py::test_app_settings_loaded_successfully -v
```

### Test-Specific Environment Variables

Tests require specific environment variables to be set, which control aspects like RPC endpoints, contract addresses, and other test parameters. These are loaded from a `.env.test` file located in the project root.

*   **Create `.env.test`**:
    *   Copy the example file `env.test.example` to `.env.test` in the project root directory:
        ```bash
        cp env.test.example .env.test
        ```
    *   **Crucially, edit `.env.test`** and replace all placeholder values (like `your_actual_test_rpc_url`, `0xYourTestContractAddress...`) with valid data for your testing environment. The tests will not pass with placeholder values.

### Running the Configuration Loading Test

A dedicated test suite verifies that the test configuration mechanism (driven by `tests/shared_fixtures/conftest.py` and your `.env.test` file) works correctly. This test ensures that the application's main configuration files (e.g., `config/settings.json`) are correctly populated with test-specific values at runtime.

*   **Run the Test**:
    *   Ensure your virtual environment is activated.
    *   From the project root directory, run the following Pytest command:
        ```bash
        poetry run pytest tests/shared_fixtures/test_config_loading.py -s
        ```
        The `-s` flag is optional but helpful as it shows `print` statements from your `conftest.py` and tests, which can aid in debugging if issues arise.
*   **Expected Outcome**:
    *   All tests within `test_config_loading.py` should pass.
    *   You should see output from `conftest.py` indicating:
        *   The project root being added to `sys.path`.
        *   Loading of environment variables from `.env.test`.
        *   Copying of `*.example.json` files to their active names (e.g., `settings.json`).
        *   Population of `settings.json` and `auth_settings.json` with test data.
        *   At the end of the session, restoration of original config files (or removal of test-generated ones if no originals existed).
    *   If all tests pass, your environment is correctly set up for running the broader test suite, as the core mechanism for providing test-specific configurations to the application is working.

### Troubleshooting
*   **`FileNotFoundError: .env.test`**: Ensure `.env.test` exists in the project root.
*   **Tests Failing with Placeholder Values**: Double-check that you've replaced all placeholder values in your `.env.test` with actual, valid data.
*   **`ModuleNotFoundError`**:
    *   Ensure your virtual environment is active (`source .venv/bin/activate` or `poetry shell`).
    *   Confirm that `poetry install --with dev` completed successfully.
    *   The `conftest.py` automatically adds the project root to `sys.path`. If module import issues persist, verify the `PROJECT_ROOT` definition in `tests/shared_fixtures/conftest.py` correctly points to your project's top-level directory.
*   **Errors during config file population/restoration**: The print statements from `pytest_sessionstart` and `pytest_sessionfinish` in `conftest.py` should provide details on which file operations are failing. Check file permissions and paths.


### API Documentation

The Core API service provides interactive API documentation through FastAPI's built-in SwaggerUI:

```
http://localhost:8002/docs
```

This interface allows you to:
- Explore all available endpoints
- Test API calls directly from the browser
- View request/response schemas
- Understand parameter requirements

![Snapshotter API SwaggerUI](snapshotter/static/docs/assets/SnapshotterSwaggerUI.png)


## Find us

* [Discord](https://powerloom.io/discord)
* [Twitter](https://twitter.com/powerloom)
* [Github](https://github.com/PowerLoom)
* [Careers](https://wellfound.com/company/powerloom/jobs)
* [Blog](https://blog.powerloom.io/)
* [Medium Engineering Blog](https://medium.com/powerloom)
