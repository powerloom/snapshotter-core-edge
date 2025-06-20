# Snapshotter System Architecture Diagrams

This directory contains comprehensive PlantUML diagrams explaining the caching and querying layers of the snapshotter system. These diagrams are designed to help both beginners and experienced developers understand the complete data flow architecture.

## ✅ All PlantUML Format
All diagrams have been converted to PlantUML format for superior rendering quality, consistency, and professional presentation. PlantUML provides excellent support for component diagrams, sequence diagrams, activity diagrams, and class diagrams.

## Diagram Overview

### 1. Overall System Architecture (`01_overall_system_architecture.puml`)
**Purpose**: High-level component view of all system components and their relationships

**Key Components Shown**:
- External systems (Blockchain, IPFS, API Clients)
- Event detection and queuing layer
- Caching layer with the Cacher process
- Storage layer (Redis cache + IPFS)
- API query layer (FastAPI router + data utilities)
- Response processing

**Use This Diagram To**:
- Get an overview of the entire system
- Understand how components interact
- See the separation of concerns between layers

### 2. Cacher Event Processing (`02_cacher_event_processing.puml`)
**Purpose**: Detailed sequence diagram showing how the Cacher processes blockchain events

**Key Flows Shown**:
- SnapshotSubmitted event processing
- SnapshotFinalized event processing
- SnapshotBatchSubmitted event processing
- Special project type handling (activePools, activeTokens, baseSnapshot)
- Background task management
- Detailed CID processing with recursive previous snapshot handling

**Use This Diagram To**:
- Understand how blockchain events are processed
- See the Redis pipeline optimization strategies
- Learn about special aggregation logic for different project types
- Understand error handling and graceful degradation

### 3. Redis Cache Structure (`03_redis_cache_structure.puml`)
**Purpose**: Detailed class diagram view of Redis data organization and key patterns

**Key Data Types Shown**:
- Project data storage (hashmaps for epochs and CIDs)
- Aggregated data storage (active pools, tokens, trade volumes)
- Block and timestamp mappings
- System management data (health, expiry tracking)
- IPFS cache layer
- Key naming patterns and examples

**Use This Diagram To**:
- Understand Redis data organization
- Learn key naming conventions
- See how different data types are structured
- Understand TTL and expiry management

### 4. API Query Flow (`04_api_query_flow.puml`)
**Purpose**: Detailed sequence diagram showing API request processing from client to response

**Key Flows Shown**:
- Request validation and routing
- Cache-first data retrieval strategy
- Incremental data updates for aggregated endpoints
- Parallel metadata fetching
- Pagination and response formatting
- Error handling with fallbacks

**Use This Diagram To**:
- Understand API request lifecycle
- See intelligent caching strategies
- Learn about performance optimizations
- Understand error handling in API layer


### 6. Data Lifecycle Flow (`06_data_lifecycle_flow.puml`) ⭐ **COMPREHENSIVE**
**Purpose**: Complete implementation-level component diagram with extensive technical details

**New Comprehensive Features**:
- **Exact Method Names**: `model_validate_json()`, `get_project_epoch_snapshot_bulk()`, `anchor_rpc_helper.get_transaction_from_hash()`
- **Complete Redis Key Formats**: `project_data:{projectId}`, `active_pool_data:{interval}:{epoch}:{namespace}`, `epoch_id_project_to_state_mapping:{epochId}:{stateId}`
- **Precise Intervals & Timeouts**: 30s task cleanup, 60s health reporting, 3600s data expiry cleanup
- **Detailed Data Structures**: JSON formats, SnapshotterStateUpdate objects, expiry tracking mechanisms
- **Background Task Management**: Task tracking with `_active_tasks Set[(timestamp, task)]`, worker thread monitoring
- **Complete Storage Operations**: `hset`, `hget`, `hdel`, `zadd`, `zrangebyscore`, `zrem` with pipeline batching

**Enhanced Event Processing**:
- **SnapshotSubmitted**: Pipeline operations, IPFS unpin scheduling, special project routing by prefix
- **SnapshotFinalized**: Max epochId calculation, state tracking updates, atomic commits
- **SnapshotBatchSubmitted**: Transaction fetching, contract ABI decoding, batch processing with `zip(projectIds, snapshotCids)`

**Special Project Processing Details**:
- **ActivePools**: 24h sliding window cache, incremental updates every 10 epochs, pool frequency aggregation
- **ActiveTokens**: Token frequency tracking with namespace isolation
- **TradeVolume**: Dual interval processing (24h + 7d), volume calculation with `totalTrade` field extraction

**Implementation-Level Details**:
- **Error Handling**: Full exception traceback logging, timeout management with `future.result()`
- **Task Lifecycle**: `_create_tracked_task()` with timestamp tracking, automatic cleanup of completed/timed-out tasks
- **Health Monitoring**: `_worker_thread.is_alive()` checks, `service_health_timestamps` updates
- **Data Expiry**: Automated cleanup with `zrangebyscore()`, batch deletion via `hdel()` operations

**Use This Diagram To**:
- **Implementation Reference**: Get exact method names and parameters for development
- **Debugging**: Trace through complete processing flows with technical accuracy
- **Performance Analysis**: Understand pipeline optimizations and background task management
- **Architecture Deep Dive**: See how Redis operations, async tasks, and error handling work together
- **Code Review**: Reference actual implementation patterns and data structures

## How to Use These Diagrams

### For System Overview
1. Start with **Diagram 1** (Overall System Architecture) to get the big picture
2. Move to **Diagram 6** (Data Lifecycle Flow) to understand the complete technical implementation
3. Use **Diagram 5** (Component Architecture) for architectural relationships

### For Implementation Details
1. Study **Diagram 6** (Data Lifecycle Flow) for comprehensive technical reference - **START HERE FOR IMPLEMENTATION**
2. Examine **Diagram 2** (Cacher Event Processing) for sequence-based event flows
3. Review **Diagram 3** (Redis Cache Structure) for data organization patterns
4. Use **Diagram 4** (API Query Flow) for API-specific implementation details

### For Development & Debugging
- **Primary Reference**: **Diagram 6** contains exact method names, parameters, Redis operations, and implementation logic
- Use **Diagram 2** to trace event processing sequences and error flows
- Use **Diagram 4** to debug API performance and caching issues
- Use **Diagram 3** to understand Redis key patterns and data structures

## Key Architectural Concepts

### Cache-First Architecture
The system prioritizes checking Redis cache before making expensive blockchain or IPFS calls. This is evident in:
- Project data lookup patterns with `project_data:{projectId}` hashmaps
- Metadata caching strategies using `project_last_finalized_epoch` tracking
- Aggregated data storage with namespace isolation via `settings.namespace`

### Intelligent Aggregation
For active pools and tokens data:
- Uses cached aggregated data when available via incremental cache strategy
- Applies incremental updates for efficiency with epoch filtering (`epochId % 10 == 0`)
- Falls back to full calculation using `get_project_epoch_snapshot_bulk()` when needed
- Maintains sliding time windows (86400s for 24h, 604800s for 7d)

### Graceful Degradation
The system handles failures gracefully:
- Continues processing on IPFS errors with exception logging
- Returns partial results when possible using `_active_tasks` tracking
- Marks failed operations with comprehensive error details
- Uses retry mechanisms with timeout management (`task_timeout` configuration)

### Performance Optimizations
- **Redis Pipelines**: Batch operations using `pipeline.execute()` for atomic multi-writes
- **Parallel Processing**: Concurrent operations with `_create_tracked_task()` async task management
- **Task Management**: Background cleanup via `_cleanup_tasks()` every 30s with timeout handling
- **Data Expiry**: Automated cleanup using `data_expiry_zset` sorted set with TTL management (7 days)

### Advanced Implementation Features
- **Background Task Tracking**: `_active_tasks Set[(timestamp, task)]` with automatic cleanup
- **Health Monitoring**: `service_health_timestamps` updates every 60s with worker thread health checks
- **Atomic Operations**: Redis pipeline batching for consistency across related updates
- **Namespace Isolation**: Settings-based namespace separation for multi-tenant deployments
- **Contract Integration**: ABI decoding via `protocol_state_contract.decode_function_input()`

## Viewing the Diagrams

### PlantUML Diagrams (.puml files)
All diagrams can be viewed in:
- **VS Code** with PlantUML extension (recommended)
- **IntelliJ IDEA** with PlantUML plugin
- **Online at plantuml.com** for quick viewing
- **Any PlantUML-compatible renderer**

**Note**: PlantUML provides superior rendering quality with professional layouts, consistent styling, and excellent support for complex diagrams.

### PlantUML Extension Setup
For the best experience, install the PlantUML extension in your IDE:
- **VS Code**: Install "PlantUML" extension by jebbs
- **IntelliJ**: Install "PlantUML integration" plugin
- **Eclipse**: Install PlantUML plugin from marketplace

## System Benefits

These diagrams illustrate several key system benefits:

1. **Scalability**: Async processing with Dramatiq message queues and `CACHER_QUEUE_NAME` routing
2. **Performance**: Multi-layer caching with intelligent invalidation via `data_expiry_zset` management
3. **Reliability**: Comprehensive error handling with full traceback logging and timeout management
4. **Maintainability**: Clear separation of concerns with dedicated processors for special project types
5. **Efficiency**: Batch operations using Redis pipelines and parallel processing with tracked async tasks

## Professional Documentation Features

PlantUML diagrams provide:
- **Implementation Accuracy**: Exact method names, parameters, and Redis operations from actual code
- **Consistent Styling**: Professional appearance across all diagrams with color-coded component types
- **Rich Annotations**: Detailed technical notes explaining implementation specifics
- **Comprehensive Coverage**: Complete system flows from blockchain events to API responses
- **Scalable Rendering**: High-quality output at any resolution for presentations and documentation
- **Version Control Friendly**: Text-based format that diffs well for tracking architectural changes

## Technical Reference Guide

**Diagram 6** now serves as a complete technical reference containing:
- **Method Signatures**: Exact function names and parameters for development reference
- **Redis Operations**: Complete CRUD operations with key patterns and data structures
- **Background Processes**: Task management, health monitoring, and data cleanup with precise intervals
- **Error Handling**: Exception management, timeout handling, and graceful degradation patterns
- **Performance Patterns**: Pipeline optimization, async task tracking, and cache management strategies

Understanding these diagrams will help you:
- Navigate the codebase more effectively with exact technical references
- Debug issues across the system using implementation-accurate flow diagrams
- Make informed decisions about optimizations based on actual performance patterns
- Understand the design rationale behind architectural choices with technical depth
- Present system architecture to stakeholders with professional, comprehensive documentation
- Implement new features following established patterns and best practices 