# Gemini Codebase Overview: snapshotter-core

This document provides a comprehensive overview of the components within the `snapshotter-core` repository. It is intended to be a living document that helps in understanding the architecture and locating key files and logic.

## Core Architecture

The system follows a microservices-like architecture with several periphery services supporting a central `snapshotter` core. The general data flow is as follows:

1.  `snapshotter-periphery-blockfetcher`: Fetches raw block data.
2.  `snapshotter-periphery-txprocessor`: Processes transactions from the fetched blocks to extract events.
3.  `snapshotter-periphery-epochsyncer`: Detects when a full epoch's data is ready.
4.  `snapshotter`: Consumes the "ready" signal and orchestrates the main snapshotting and aggregation logic defined in `computes`.
5.  `snapshotter-lite-local-collector`: A Go-based service for P2P data submission.
6.  `rate-limiter`: A utility service for rate limiting.

---

## Component Details

### 1. `snapshotter` (Core)

*   **Purpose**: The main application that orchestrates the snapshotting process, including distributing tasks to workers, caching data, and exposing a core API.
*   **Key Technologies**: Python, Dramatiq, FastAPI, Redis.
*   **Key Files**:
    *   `processor_distributor.py`: Distributes processing tasks for epochs.
    *   `cacher.py`: Manages the lifecycle of snapshots, from submission to finalization.
    *   `core_api.py`: Exposes the main API endpoints.
    *   `snapshot_worker.py`, `aggregation_worker.py`: Dramatiq workers for snapshotting and aggregation.
*   **Digest File**: `snapshotter/digest.txt`

### 2. `computes`

*   **Purpose**: Contains the core business logic for calculating various data points and metrics, primarily for Uniswap V3. These are the "computes" that the `snapshotter` orchestrates.
*   **Key Technologies**: Python.
*   **Key Files**:
    *   `active_pools.py`, `active_tokens.py`: Processors to track active pools and tokens.
    *   `eth_price.py`: Processor for ETH price calculation.
    *   `trades.py`: Processor for trade volume.
    *   `pair_total_reserves.py`: Processor for calculating token reserves.
    *   `api/router.py`: FastAPI router that exposes the computed data.
*   **Digest File**: `computes/digest.txt`

### 3. `config`

*   **Purpose**: Centralized JSON configuration for all components.
*   **Key Files**:
    *   `settings.json`: Main application settings (RPC, Redis, IPFS).
    *   `projects.json`: Defines the snapshotting "projects" (tasks).
    *   `aggregator.json`: Defines aggregation logic.
    *   `preloader.json`: Configures preloader tasks.
*   **Digest File**: `config/digest.txt`

### 4. `snapshotter-periphery-blockfetcher`

*   **Purpose**: A periphery service to fetch new blocks from a source blockchain.
*   **Key Technologies**: Python, Docker.
*   **Key Files**:
    *   `main.py`: Main entrypoint for the service.
    *   `utils/block_fetcher.py`: Core logic for fetching and caching blocks.
    *   `utils/preloaders/block_details.py`: Preloader hook to dump block details to Redis.
*   **Digest File**: `snapshotter-periphery-blockfetcher/digest.txt`

### 5. `snapshotter-periphery-txprocessor`

*   **Purpose**: A periphery service to process transactions from a queue.
*   **Key Technologies**: Python, Docker.
*   **Key Files**:
    *   `main.py`: Main entrypoint for the service.
    *   `utils/tx_processor.py`: Core logic for consuming from Redis and processing receipts.
    *   `utils/preloaders/event_filter.py`: Key preloader hook that decodes logs based on ABI definitions.
*   **Digest File**: `snapshotter-periphery-txprocessor/digest.txt`

### 6. `snapshotter-periphery-epochsyncer`

*   **Purpose**: A periphery service that monitors the anchor chain for `EpochReleased` events and ensures data is cached before notifying the core `snapshotter`.
*   **Key Technologies**: Python, Dramatiq, Docker.
*   **Key Files**:
    *   `event_detector.py`: The core logic that listens for blockchain events, checks for data readiness in Redis, and enqueues tasks for the `snapshotter` core.
*   **Digest File**: `snapshotter-periphery-epochsyncer/digest.txt`

### 7. `snapshotter-lite-local-collector`

*   **Purpose**: A local data collector service operating in a P2P network.
*   **Key Technologies**: Go, gRPC, libp2p, Docker.
*   **Key Files**:
    *   `cmd/main.go`: Main entrypoint for the server.
    *   `pkgs/service/msg_server.go`: gRPC server implementation for handling submissions.
    *   `pkgs/service/discovery.go`: Logic for discovering and connecting to peers.
*   **Digest File**: `snapshotter-lite-local-collector/digest.txt`

### 8. `rate-limiter`

*   **Purpose**: A general-purpose, standalone rate limiting service.
*   **Key Technologies**: Python, FastAPI, Docker.
*   **Key Files**:
    *   `app.py`: The FastAPI application defining the rate limiting logic and API endpoints.
*   **Digest File**: `rate-limiter/digest.txt`