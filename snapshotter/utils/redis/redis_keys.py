from snapshotter.settings.config import settings

# Redis key for cached block details at a specific height
cached_block_details_at_height = f'block_cache:{settings.namespace}'

# Redis key for the last processed block by the event detector
event_detector_last_processed_block = 'SystemEventDetector:lastProcessedBlock'


def block_number_to_timestamp_key(namespace: str) -> str:
    return f'blockNumberToTimestamp:{namespace}'


def timestamp_to_block_number_key(namespace: str) -> str:
    return f'timestampToBlockNumber:{namespace}'


def project_data_hmap(project_id: str) -> str:
    """
    Generate Redis key for project data hashmap.
    """
    return f'projectID:{project_id}:data'


def cid_cache(cid: str) -> str:
    """
    Generate Redis key for CID cache.
    """
    return f'CIDCache:{cid}'


def blank_epochs_bitmap(project_id: str) -> str:
    """
    Generate Redis key for blank epochs bitmap.
    """
    return f'BlankEpochsBitmap:{project_id}'


def cid_not_found_key(cid: str) -> str:
    """
    Generate Redis key for CID not found.

    Args:
        cid (str): The CID of the file.

    Returns:
        str: Redis key for the CID not found.
    """
    return f'CIDNotFound:{cid}'


def project_first_epoch_hmap() -> str:
    """
    Generate Redis key for project first epoch hashmap.

    Returns:
        str: Redis key for the project first epoch hashmap.
    """
    return 'projectFirstEpoch'


def source_chain_id_key() -> str:
    """
    Generate Redis key for source chain ID.

    Returns:
        str: Redis key for the source chain ID.
    """
    return 'sourceChainId'


def source_chain_block_time_key() -> str:
    """
    Generate Redis key for source chain block time.

    Returns:
        str: Redis key for the source chain block time.
    """
    return 'sourceChainBlockTime'


def source_chain_epoch_size_key() -> str:
    """
    Generate Redis key for source chain epoch size.

    Returns:
        str: Redis key for the source chain epoch size.
    """
    return 'sourceChainEpochSize'


def project_last_finalized_epoch_hmap() -> str:
    """
    Generate Redis key for project's last finalized epoch hashmap.
    """
    return 'projectLastFinalizedEpoch'


def snapshots_to_unpin_zset_name() -> str:
    """
    Generate Redis key for unpinned snapshots zset.

    Returns:
        str: Redis key for the unpinned snapshots zset.
    """
    return 'snapshotsToUnpin'


def epoch_id_project_to_state_mapping(epoch_id: int, state_id: str) -> str:
    """
    Generate Redis key for epoch-project state mapping.

    Args:
        epoch_id (int): The ID of the epoch.
        state_id (str): The ID of the state.

    Returns:
        str: Redis key for the epoch-project state mapping.
    """
    return f'epochID:{epoch_id}:stateID:{state_id}:processingStatus'


def last_submitted_snapshot_data_key(project_id: str) -> str:
    """
    Generate Redis key for last submitted snapshot data.

    Args:
        project_id (str): The ID of the project.

    Returns:
        str: Redis key for the last submitted snapshot data.
    """
    return f'lastSubmittedSnapshotData:{project_id}'


def last_snapshot_processing_complete_timestamp_key() -> str:
    """
    Generate Redis key for last snapshot processing complete timestamp.

    Returns:
        str: Redis key for the last snapshot processing complete timestamp.
    """
    return f'lastSnapshotProcessingCompleteTimestamp:{settings.namespace}'


def last_epoch_detected_timestamp_key() -> str:
    """
    Generate Redis key for last epoch detected timestamp.

    Returns:
        str: Redis key for the last epoch detected timestamp.
    """
    return f'lastEpochDetectedTimestamp:{settings.namespace}'


def last_epoch_detected_epoch_id_key() -> str:
    """
    Generate Redis key for last detected epoch ID.

    Returns:
        str: Redis key for the last detected epoch ID.
    """
    return f'lastEpochDetectedEpochID:{settings.namespace}'


def data_expiry_zset() -> str:
    """
    Generate Redis key for project data expiry zset.
    This zset tracks expiration times for individual hash entries in project data hashmaps.

    Returns:
        str: Redis key for the project data expiry zset.
    """
    return f'DataExpiry:{settings.namespace}'


def callback_last_sent_by_issue(issue_type: str) -> str:
    """
    Generate Redis key for callback last sent timestamp. Stores the last sent timestamp for each issueType.

    Returns:
        str: Redis key for the callback last sent timestamp.
    """
    return f'callbackLastSentTimestamp:{settings.namespace}:{issue_type}'


def service_health_timestamps_key() -> str:
    """
    Generate Redis key for the service health timestamps hash.

    This key points to a Redis hash that stores the last reported health timestamp
    for each service instance (e.g., worker, API). The field is the service's
    hostname, and the value is the Unix timestamp of the last health ping.

    Returns:
        str: Redis key for the service health timestamps hash.
    """
    return f'{settings.namespace}:service_health_timestamps'


def cids_to_cache_set() -> str:
    """
    Generate Redis key for the set of CIDs to be cached.

    This key points to a Redis set that acts as a queue for snapshot CIDs
    that need to be fetched from IPFS and cached in Redis. A worker process
    (e.g., Cacher) monitors this set, processes the CIDs, and removes them
    upon successful caching.

    Returns:
        str: Redis key for the CIDs to cache set.
    """
    return f'cidsToCache:{settings.namespace}'


def active_pool_data_processing_key(time_interval: int) -> str:
    """
    Generate Redis key for active pool data processing status.
    """
    return f"active_pool_data:{time_interval}:processing"


def active_pool_data_latest_epoch_key(time_interval: int) -> str:
    """
    Generate Redis key for the latest epoch of active pool data.
    """
    return f"active_pool_data:{time_interval}:latest:epoch"


def active_pool_data_indexed_key(time_interval: int, epoch: int) -> str:
    """
    Generate Redis key for indexed active pool data for a specific epoch.
    """
    return f"active_pool_data:{time_interval}:{epoch}:{settings.namespace}"


def active_pools_sorted_set_key(time_interval: int) -> str:
    """
    Generate Redis key for the sorted set of active pools.
    """
    return f"active:pools:sorted:{time_interval}:{settings.namespace}"


def active_token_data_processing_key(project_id: str, time_interval: int) -> str:
    """
    Generate Redis key for active token data processing status.
    """
    return f"active_token_data:{project_id}:{time_interval}:processing"


def active_token_data_latest_epoch_key(time_interval: int) -> str:
    """
    Generate Redis key for the latest epoch of active token data.
    """
    return f"active_token_data:{time_interval}:latest:epoch"


def active_token_data_indexed_key(time_interval: int, epoch: int) -> str:
    """
    Generate Redis key for indexed active token data for a specific epoch.
    """
    return f"active_token_data:{time_interval}:{epoch}:{settings.namespace}"


def active_tokens_sorted_set_key(time_interval: int) -> str:
    """
    Generate Redis key for the sorted set of active tokens.
    """
    return f"active:tokens:sorted:{time_interval}:{settings.namespace}"


def trade_volume_data_processing_key(project_id: str, time_interval: int) -> str:
    """
    Generate Redis key for trade volume data processing status.
    """
    return f"trade_volume_data:{project_id}:{time_interval}:processing"


def trade_volume_data_latest_epoch_key(project_id: str, time_interval: int) -> str:
    """
    Generate Redis key for the latest epoch of trade volume data.
    """
    return f"trade_volume_data:{project_id}:{time_interval}:latest:epoch"


def trade_volume_data_indexed_key(project_id: str, time_interval: int, epoch: int) -> str:
    """
    Generate Redis key for indexed trade volume data for a specific epoch.
    """
    return f"trade_volume_data:{project_id}:{time_interval}:{epoch}:{settings.namespace}"


def agg_volume_pool_key(project_id: str, time_interval: int) -> str:
    """
    Generate Redis key for aggregated trade volume for a specific pool.
    """
    return f"agg:volume:pool:{project_id}:{time_interval}"

def metadata_project_id(pool_address: str) -> str:
    """
    Generate project ID for metadata snapshots.
    """
    return f"metadata:{pool_address}:{settings.namespace}"

def metadata_pool_key(pool_address: str) -> str:
    """
    Generate Redis key for pool metadata.
    """
    return f"metadata:pool:{pool_address}"


def metadata_token_key(token_address: str) -> str:
    """
    Generate Redis key for token metadata.
    """
    return f"metadata:token:{token_address}"


def agg_snapshot_base_all_pools_key(token_address: str) -> str:
    """
    Generate Redis key for aggregated base snapshots for all pools of a token.
    """
    return f"agg:snapshot:base_all_pools:{token_address}"


def agg_snapshot_token_prices_key(token_address: str) -> str:
    """
    Generate Redis key for aggregated token prices across all pools for a token.
    """
    return f"agg:snapshot:token_prices:{token_address}"


def agg_snapshot_all_trades_key() -> str:
    """
    Generate Redis key for aggregated trades snapshot across all pools.
    """
    return f"agg:snapshot:all_trades"


def base_snapshot_project_id(pool_address: str) -> str:
    """
    Generate project ID for base snapshots.
    """
    return f"baseSnapshot:{pool_address}:{settings.namespace}"


def trades_snapshot_project_id(pool_address: str) -> str:
    """
    Generate project ID for trades snapshots.
    """
    return f"tradesSnapshot:{pool_address}:{settings.namespace}"


def all_trades_snapshot_project_id() -> str:
    """
    Generate project ID for all trades snapshots.
    """
    return f"allTradesSnapshot:{settings.namespace}"


def token_pools_project_id(token_address: str) -> str:
    """
    Generate project ID for token pools snapshots.
    """
    return f"tokenPools:{token_address}:{settings.namespace}"


def eth_price_project_id() -> str:
    """
    Generate project ID for ETH price snapshots.
    """
    return f'price:ETH:{settings.namespace}'


def series_price_key(pool_address: str, token_address: str, time_interval: int, step_seconds: int) -> str:
    """
    Generate Redis key for time series price data.
    """
    return f"series:price:{pool_address}:{token_address}:{time_interval}:{step_seconds}"


def series_trades_key(pool_address: str, start_timestamp: int, end_timestamp: int) -> str:
    """
    Generate Redis key for time series trades data.
    """
    return f"series:trades:{pool_address}:{start_timestamp}:{end_timestamp}"