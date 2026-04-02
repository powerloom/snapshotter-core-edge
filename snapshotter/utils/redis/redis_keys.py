from snapshotter.settings.config import settings

# Redis key for cached block details at a specific height
cached_block_details_at_height = f'block_cache:{settings.namespace}'

# Redis key for the last processed block by the event detector
event_detector_last_processed_block = 'SystemEventDetector:lastProcessedBlock'


def block_number_to_timestamp_key(namespace: str) -> str:
    return f'blockNumberToTimestamp:{namespace}'


def timestamp_to_block_number_key(namespace: str) -> str:
    return f'timestampToBlockNumber:{namespace}'


def project_data_hmap(project_id):
    """
    Generate Redis key for project data hashmap.
    """
    return f'projectID:{project_id}:data'


def cid_cache(cid: str):
    """
    Generate Redis key for CID cache.
    """
    return f'CIDCache:{cid}'


def blank_epochs_bitmap(project_id: str):
    """
    Generate Redis key for blank epochs bitmap.
    """
    return f'BlankEpochsBitmap:{project_id}'


def cid_not_found_key(cid):
    """
    Generate Redis key for CID not found.

    Args:
        cid (str): The CID of the file.

    Returns:
        str: Redis key for the CID not found.
    """
    return f'CIDNotFound:{cid}'


def project_first_epoch_hmap():
    """
    Generate Redis key for project first epoch hashmap.

    Returns:
        str: Redis key for the project first epoch hashmap.
    """
    return 'projectFirstEpoch'


def source_chain_id_key():
    """
    Generate Redis key for source chain ID.

    Returns:
        str: Redis key for the source chain ID.
    """
    return 'sourceChainId'


def source_chain_block_time_key():
    """
    Generate Redis key for source chain block time.

    Returns:
        str: Redis key for the source chain block time.
    """
    return 'sourceChainBlockTime'


def source_chain_epoch_size_key():
    """
    Generate Redis key for source chain epoch size.

    Returns:
        str: Redis key for the source chain epoch size.
    """
    return 'sourceChainEpochSize'


def project_last_finalized_epoch_hmap():
    """
    Generate Redis key for project's last finalized epoch hashmap.
    """
    return 'projectLastFinalizedEpoch'


def snapshot_finalized_channel(project_id):
    """Redis pub/sub channel published when a project snapshot is finalized."""
    return f'snapshot:finalized:{project_id}'


def snapshots_to_unpin_zset_name():
    """
    Generate Redis key for unpinned snapshots zset.

    Returns:
        str: Redis key for the unpinned snapshots zset.
    """
    return 'snapshotsToUnpin'


def epoch_id_project_to_state_mapping(epoch_id, state_id):
    """
    Generate Redis key for epoch-project state mapping.

    Args:
        epoch_id (str): The ID of the epoch.
        state_id (str): The ID of the state.

    Returns:
        str: Redis key for the epoch-project state mapping.
    """
    return f'epochID:{epoch_id}:stateID:{state_id}:processingStatus'


def last_submitted_snapshot_data_key(project_id):
    """
    Generate Redis key for last submitted snapshot data.

    Args:
        project_id (str): The ID of the project.

    Returns:
        str: Redis key for the last submitted snapshot data.
    """
    return f'lastSubmittedSnapshotData:{project_id}'


def last_submitted_snapshot_raw_data_key(project_id):
    """
    Generate Redis key for last submitted snapshot raw data.
    """
    return f'lastSubmittedSnapshotRawData:{project_id}'


def last_snapshot_processing_complete_timestamp_key():
    """
    Generate Redis key for last snapshot processing complete timestamp.

    Returns:
        str: Redis key for the last snapshot processing complete timestamp.
    """
    return f'lastSnapshotProcessingCompleteTimestamp:{settings.namespace}'


def last_epoch_detected_timestamp_key():
    """
    Generate Redis key for last epoch detected timestamp.

    Returns:
        str: Redis key for the last epoch detected timestamp.
    """
    return f'lastEpochDetectedTimestamp:{settings.namespace}'


def last_epoch_detected_epoch_id_key():
    """
    Generate Redis key for last detected epoch ID.

    Returns:
        str: Redis key for the last detected epoch ID.
    """
    return f'lastEpochDetectedEpochID:{settings.namespace}'


def data_expiry_zset():
    """
    Generate Redis key for project data expiry zset.
    This zset tracks expiration times for individual hash entries in project data hashmaps.

    Returns:
        str: Redis key for the project data expiry zset.
    """
    return f'DataExpiry:{settings.namespace}'


def callback_last_sent_by_issue(issue_type):
    """
    Generate Redis key for callback last sent timestamp. Stores the last sent timestamp for each issueType.

    Returns:
        str: Redis key for the callback last sent timestamp.
    """
    return f'callbackLastSentTimestamp:{settings.namespace}:{issue_type}'


def service_health_timestamps_key():
    """
    Generate Redis key for the service health timestamps hash.

    This key points to a Redis hash that stores the last reported health timestamp
    for each service instance (e.g., worker, API). The field is the service's
    hostname, and the value is the Unix timestamp of the last health ping.

    Returns:
        str: Redis key for the service health timestamps hash.
    """
    return f'{settings.namespace}:service_health_timestamps'


def cids_to_cache_set():
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