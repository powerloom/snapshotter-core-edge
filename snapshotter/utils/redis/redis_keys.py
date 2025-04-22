from snapshotter.settings.config import settings

# Redis key for cached block details at a specific height
cached_block_details_at_height = (
    'block_cache:' + settings.namespace
)

# Redis key for the last processed epoch by the epoch detector
epoch_detector_last_processed_epoch = 'SystemEpochDetector:lastProcessedEpoch'

# Redis key for the last processed block by the event detector
event_detector_last_processed_block = 'SystemEventDetector:lastProcessedBlock'


# Redis keys for various RPC call counters
rpc_json_rpc_calls = (
    'rpc:jsonRpc:' + settings.namespace + ':calls'
)

rpc_get_event_logs_calls = (
    'rpc:eventLogsCount:' + settings.namespace + ':calls'
)

rpc_web3_calls = (
    'rpc:web3:' + settings.namespace + ':calls'
)

rpc_blocknumber_calls = (
    'rpc:blocknumber:' + settings.namespace + ':calls'
)

rpc_get_block_number_calls = (
    'rpc:blockNumber:' + settings.namespace + ':calls'
)

rpc_get_transaction_receipt_calls = (
    'rpc:transactionReceipt:' + settings.namespace + ':calls'
)

# Redis key for cached epoch process report
epoch_process_report_cached_key = 'epochProcessReport'

# Redis key for service health timestamps
service_health_timestamps_key = f'service:health:timestamps'


def project_finalized_data_zset(project_id):
    """
    Generate Redis key for project finalized data zset.

    Args:
        project_id (str): The ID of the project.

    Returns:
        str: Redis key for the project's finalized data zset.
    """
    return f'projectID:{project_id}:finalizedData'


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


def project_last_finalized_epoch_key(project_id):
    """
    Generate Redis key for project's last finalized epoch.

    Args:
        project_id (str): The ID of the project.

    Returns:
        str: Redis key for the project's last finalized epoch.
    """
    return f'projectID:{project_id}:lastFinalizedEpoch'


def project_successful_snapshot_submissions_suffix():
    """
    Generate Redis key suffix for successful snapshot submissions count.

    Returns:
        str: Redis key suffix for successful snapshot submissions count.
    """
    return 'totalSuccessfulSnapshotCount'


def project_incorrect_snapshot_submissions_suffix():
    """
    Generate Redis key suffix for incorrect snapshot submissions count.

    Returns:
        str: Redis key suffix for incorrect snapshot submissions count.
    """
    return 'totalIncorrectSnapshotCount'


def project_missed_snapshot_submissions_suffix():
    """
    Generate Redis key suffix for missed snapshot submissions count.

    Returns:
        str: Redis key suffix for missed snapshot submissions count.
    """
    return 'totalMissedSnapshotCount'


def unpinned_snapshots_zset_name():
    """
    Generate Redis key for unpinned snapshots zset.

    Returns:
        str: Redis key for the unpinned snapshots zset.
    """
    return 'snapshotsToUnpin'


def project_snapshotter_status_report_key(project_id):
    """
    Generate Redis key for project's snapshotter status report.

    Args:
        project_id (str): The ID of the project.

    Returns:
        str: Redis key for the project's snapshotter status report.
    """
    return f'projectID:{project_id}:snapshotterStatusReport'


def stored_projects_key():
    """
    Generate Redis key for stored project IDs.

    Returns:
        str: Redis key for stored project IDs.
    """
    return 'storedProjectIds'


def epoch_txs_htable(epoch_id):
    """
    Generate Redis key for epoch transaction receipts hashtable.

    Args:
        epoch_id (str): The ID of the epoch.

    Returns:
        str: Redis key for the epoch's transaction receipts hashtable.
    """
    return f'epochID:{epoch_id}:txReceipts'


def epoch_id_epoch_released_key(epoch_id):
    """
    Generate Redis key for epoch release status.

    Args:
        epoch_id (str): The ID of the epoch.

    Returns:
        str: Redis key for the epoch's release status.
    """
    return f'epochID:{epoch_id}:epochReleased'


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


def submitted_base_snapshots_key(epoch_id, project_id):
    """
    Generate Redis key for submitted base snapshots.

    Args:
        epoch_id (str): The ID of the epoch.
        project_id (str): The ID of the project.

    Returns:
        str: Redis key for the submitted base snapshots.
    """
    return f'submittedBaseSnapshots:{epoch_id}:{project_id}'


def submitted_unfinalized_snapshot_cids(project_id):
    """
    Generate Redis key for submitted unfinalized snapshot CIDs.

    Args:
        project_id (str): The ID of the project.

    Returns:
        str: Redis key for the submitted unfinalized snapshot CIDs.
    """
    return f'projectID:{project_id}:unfinalizedSnapshots'


def callback_last_sent_by_issue(issue_type):
    """
    Generate Redis key for callback last sent timestamp. Stores the last sent timestamp for each issueType.

    Returns:
        str: Redis key for the callback last sent timestamp.
    """
    return f'callbackLastSentTimestamp:{settings.namespace}:{issue_type}'
