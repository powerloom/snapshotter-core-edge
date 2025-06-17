import asyncio
import json
import time
import tenacity

from redis import asyncio as aioredis
from rpc_helper.rpc import RpcHelper
from tenacity import retry
from tenacity import retry_if_exception_type
from tenacity import stop_after_attempt
from tenacity import wait_random_exponential
from typing import List, Optional, Tuple, Dict, Any
from web3 import Web3
from ipfs_client.main import AsyncIPFSClient

from snapshotter.utils.models.data_models import (
    EpochSnapshotResponse, 
    ExactEpochSnapshot, 
    ClosestEpochs, 
    EpochIdentifier,
)
from snapshotter.settings.config import settings
from snapshotter.utils.models.data_models import BlockSearchType
from snapshotter.utils.default_logger import default_logger
from snapshotter.utils.redis.redis_keys import block_number_to_timestamp_key
from snapshotter.utils.redis.redis_keys import cid_not_found_key
from snapshotter.utils.redis.redis_keys import project_first_epoch_hmap
from snapshotter.utils.redis.redis_keys import project_last_finalized_epoch_hmap
from snapshotter.utils.redis.redis_keys import project_data_hmap
from snapshotter.utils.redis.redis_keys import last_submitted_snapshot_data_key
from snapshotter.utils.redis.redis_keys import source_chain_block_time_key
from snapshotter.utils.redis.redis_keys import source_chain_epoch_size_key
from snapshotter.utils.redis.redis_keys import source_chain_id_key
from snapshotter.utils.redis.redis_keys import data_expiry_zset
from snapshotter.utils.redis.redis_keys import cid_cache
from snapshotter.utils.redis.redis_keys import blank_epochs_bitmap
from snapshotter.utils.redis.redis_bitmap import RedisBitmap
from snapshotter.utils.redis.redis_keys import timestamp_to_block_number_key
from snapshotter.settings.config import projects_config
from snapshotter.settings.config import aggregator_config
from snapshotter.utils.models.data_models import SnapshotStatus
import traceback

logger = default_logger.bind(module='data_helper')
PROJECT_DATA_ENTRY_EXPIRY = 60 * 60 * 24 * 7  # 7 days in seconds
BLOCK_SHIFT_FOR_BITMAP_INDEX = 22400000
MAX_RECURSION_DEPTH = 50

redis_bitmap = RedisBitmap(epoch_offset=BLOCK_SHIFT_FOR_BITMAP_INDEX)


def retry_state_callback(retry_state: tenacity.RetryCallState):
    """
    Callback function to handle retry attempts for IPFS cat operation.

    This function logs a warning message when an IPFS cat exception occurs during a retry attempt.

    Args:
        retry_state (tenacity.RetryCallState): The current state of the retry call.

    Returns:
        None
    """
    logger.warning(f'Encountered IPFS cat exception: {retry_state.outcome.exception()}')


def get_project_config(project_id: str):
    """
    Get the project config for a given project ID.
    """
    if not project_id:
        return None
    primary_identifier = project_id.split(':')[0]
    for config in projects_config:
        if config.project_name.startswith(primary_identifier):
            return config
    for config in aggregator_config:
        if config.project_name.startswith(primary_identifier):
            return config
    return None


async def get_project_finalized_cid(
    redis_conn: aioredis.Redis, 
    state_contract_obj, 
    rpc_helper, 
    ipfs_reader, 
    epoch_id, 
    project_id
):
    """
    Get the CID of the finalized data for a given project and epoch.

    This function first checks if the epoch is valid for the project. If so, it attempts to retrieve the CID
    from a Redis cache. If not found in the cache, it fetches and caches the CID from the blockchain.

    Args:
        redis_conn (aioredis.Redis): Redis connection object.
        state_contract_obj: Contract object for the state contract.
        rpc_helper: Helper object for making RPC calls.
        epoch_id (int): Epoch ID for which to get the CID.
        project_id (str): ID of the project for which to get the CID.

    Returns:
        str: CID of the finalized data for the given project and epoch, or None if not found.
    """

    # Check if the epoch is valid for the project
    project_first_epoch = await get_project_first_epoch(
        redis_conn, state_contract_obj, rpc_helper, project_id,
    )
    if epoch_id < project_first_epoch:
        return None

    # Try to get the CID from Redis cache
    cid_data_raw = await redis_conn.hget(
        project_data_hmap(project_id=project_id),
        epoch_id,
    )

    cid_data = json.loads(cid_data_raw) if cid_data_raw else None

    if cid_data:
        cid = cid_data["snapshot_cid"]
    else:
        blank_epochs_bitmap_key = blank_epochs_bitmap(project_id)
        # check if epoch is in blank epochs set
        is_blank_epoch = await redis_bitmap.get_bit(redis_conn, blank_epochs_bitmap_key, epoch_id)
        if is_blank_epoch:
            cid = f'null_{epoch_id}'
        else:
            # If not in cache, fetch from blockchain and cache it
            cid, _ = await w3_get_and_cache_finalized_cid(redis_conn, state_contract_obj, rpc_helper, ipfs_reader, epoch_id, project_id)

    # Return None if CID is None (consensus not yet available) or contains 'null'
    if cid is None or 'null' in cid:
        return None
    return cid


async def get_project_last_finalized_epoch(
    redis_conn: aioredis.Redis, 
    state_contract_obj, 
    rpc_helper, 
    project_id, 
    force_update=False
):
    """
    Get the last finalized epoch for a given project.
    """
    if not force_update:
        last_finalized_epoch = await redis_conn.hget(project_last_finalized_epoch_hmap(), project_id)
        if last_finalized_epoch:
            return int(last_finalized_epoch)

    [project_last_finalized_epoch] = await rpc_helper.web3_call(
        tasks=[
            ('lastSequencerFinalizedSnapshot', [Web3.to_checksum_address(settings.data_market), project_id]),
        ],
        contract_addr=state_contract_obj.address,
        abi=state_contract_obj.abi,
    )
    if project_last_finalized_epoch == 0:
        return 0
    await redis_conn.hset(project_last_finalized_epoch_hmap(), project_id, project_last_finalized_epoch)
    return project_last_finalized_epoch


async def get_last_submitted_snapshot_data(redis_conn: aioredis.Redis, project_id: str):
    last_submitted_snapshot_data = await redis_conn.get(last_submitted_snapshot_data_key(project_id))
    if last_submitted_snapshot_data:
        return json.loads(last_submitted_snapshot_data)
    return None


async def get_project_finalized_cids_bulk(
    redis_conn: aioredis.Redis,
    state_contract_obj,
    rpc_helper: RpcHelper,
    ipfs_reader,
    epoch_id_min: int,
    epoch_id_max: int,
    project_id: str,
) -> Tuple[List[str], int]:
    """
    Retrieves CIDs for multiple epochs in bulk.

    Args:
        redis_conn (aioredis.Redis): Redis connection object.
        state_contract_obj: Contract object for the state contract.
        rpc_helper (RpcHelper): Helper object for making RPC calls.
        epoch_ids (List[int]): List of epoch IDs.
        project_id (str): Project ID.

    Returns:
        Tuple[List[str], int]: List of CIDs and the project's first epoch.
    """
    project_config = get_project_config(project_id)

    # Adjust epoch_id_min if it's less than the project's first epoch
    project_first_epoch = await get_project_first_epoch(
        redis_conn, state_contract_obj, rpc_helper, project_id,
    )

    last_submitted_snapshot_data = await get_last_submitted_snapshot_data(redis_conn, project_id)
    if last_submitted_snapshot_data:
        max_epoch_with_data = last_submitted_snapshot_data['epochId']
    else:
        max_epoch_with_data = await get_project_last_finalized_epoch(
            redis_conn=redis_conn,
            state_contract_obj=state_contract_obj,
            rpc_helper=rpc_helper, 
            project_id=project_id,
        )

    empty_epochs_with_cids = []
    if max_epoch_with_data < epoch_id_max:
        logger.info(f'Max epoch with data {max_epoch_with_data} is less than epoch_id_max {epoch_id_max}. Adjusting epoch_id_max to {max_epoch_with_data}')
        empty_epochs_with_cids = [(f'null_{epoch_id}', epoch_id) for epoch_id in range(max_epoch_with_data + 1, epoch_id_max + 1)]
        epoch_id_max = max(epoch_id_min, max_epoch_with_data)

    logger.info(f'Project first epoch: {project_first_epoch}')

    if epoch_id_min < project_first_epoch:
        logger.warning(
            f'Min. Epoch ID: {epoch_id_min} is less than the project first epoch {project_first_epoch}.',
            f'Adjusting min epoch to {project_first_epoch}.',
        )
        epoch_id_min = project_first_epoch
        
        # If the adjusted min is greater than max, return empty list
        if epoch_id_min > epoch_id_max:
            logger.warning(
                f'Adjusted min epoch {epoch_id_min} is greater than max epoch {epoch_id_max}.',
                'Returning empty list.',
            )
            return [], project_first_epoch

    epoch_ids_set = set(range(epoch_id_min, epoch_id_max + 1))

    # Check Redis cache for existing CIDs
    epoch_ids_to_fetch = list(range(epoch_id_min, epoch_id_max + 1))
    logger.info(f'Fetching CIDs for {len(epoch_ids_to_fetch)} epochs for project {project_id}')
    if not epoch_ids_to_fetch:
        return [], project_first_epoch
 
    data_raw = await redis_conn.hmget(
        project_data_hmap(project_id=project_id),
        epoch_ids_to_fetch
    )
    data = []
    for data_raw in data_raw:
        if data_raw:
            data.append(json.loads(data_raw))
        else:
            data.append(dict())

    cid_data_with_epochs = []
    for data, epoch_id in zip(data, epoch_ids_to_fetch):
        if "snapshot_cid" in data:
            cid_data_with_epochs.append((data["snapshot_cid"], epoch_id))
        
    logger.info(f'Found {len(cid_data_with_epochs)} CIDs for project {project_id}')

    existing_epochs = set([epoch_id for _, epoch_id in cid_data_with_epochs])
    missing_epochs_with_blanks = sorted(list(epoch_ids_set.difference(existing_epochs)))
    logger.info(f'Found {len(missing_epochs_with_blanks)} missing epochs with blanks for project {project_id}')
    missing_epochs = []

    blank_epochs_bitmap_key = blank_epochs_bitmap(project_id)
    blank_epochs = await redis_bitmap.get_bits_in_range(redis_conn, blank_epochs_bitmap_key, missing_epochs_with_blanks)
    
    for epoch_id, is_blank in blank_epochs:
        if is_blank:
            cid_data_with_epochs.append((f'null_{epoch_id}', epoch_id))
        else:
            missing_epochs.append(epoch_id)
    logger.info(f'Found {len(missing_epochs)} missing epochs without blanks for project {project_id}')
    # batch_web3_contract_calls
    if missing_epochs:
        if project_config.keep_previous_snapshot_data:
            logger.info(f'Fetching CIDs for {len(missing_epochs)} epochs for project {project_id}')
            missing_cids_with_epochs = await w3_get_and_cache_finalized_cid_bulk_using_previous_snapshots(
                redis_conn, state_contract_obj, rpc_helper, ipfs_reader, missing_epochs, project_id,
            )
        else:
            # Batch fetch CIDs from the blockchain
            missing_cids_with_epochs = await w3_get_and_cache_finalized_cid_bulk(
                redis_conn=redis_conn,
                state_contract_obj=state_contract_obj,
                rpc_helper=rpc_helper,
                epoch_ids=missing_epochs,
                project_id=project_id,
            )

        # Merge existing and missing CIDs
        all_cids_with_epochs = cid_data_with_epochs + missing_cids_with_epochs + empty_epochs_with_cids
        all_cids_with_epochs.sort(key=lambda x: x[1])
    else:
        all_cids_with_epochs = cid_data_with_epochs + empty_epochs_with_cids
        all_cids_with_epochs.sort(key=lambda x: x[1])
    return [cid for cid, _ in all_cids_with_epochs], project_first_epoch


@retry(
    reraise=True,
    retry=retry_if_exception_type(Exception),
    wait=wait_random_exponential(multiplier=1, max=10),
    stop=stop_after_attempt(3),
)
async def w3_get_and_cache_finalized_cid(
    redis_conn: aioredis.Redis,
    state_contract_obj,
    rpc_helper: RpcHelper,
    ipfs_reader,
    epoch_id,
    project_id,
):
    """
    Retrieves the consensus status and the snapshot CID for a given project and epoch.

    This function interacts with the blockchain to get the snapshot status and CID, then caches the result in Redis.
    It supports both legacy v1 and new v2 protocols for consensus status.

    Args:
        redis_conn (aioredis.Redis): Redis connection object
        state_contract_obj: Contract object for the protocol state contract
        rpc_helper: Helper object for making web3 calls
        epoch_id (int): Epoch ID
        project_id (int): Project ID

    Returns:
        Tuple[str, int]: The CID and epoch ID if the consensus status is not PENDING, 
        or the null value and epoch ID if the consensus status is PENDING.
    """

    # Add to expiry tracking sorted set with TTL
    expiry_time = int(time.time()) + PROJECT_DATA_ENTRY_EXPIRY
    pipeline = redis_conn.pipeline()
    expiry_keys = []
    blank_epochs_bitmap_key = blank_epochs_bitmap(project_id)

    # Fetch consensus status and CID from the blockchain
    [consensus_status, current_epoch] = await rpc_helper.web3_call(
        tasks=[
            ('snapshotStatus', [Web3.to_checksum_address(settings.data_market), project_id, epoch_id]),
            ('currentEpoch', [Web3.to_checksum_address(settings.data_market)]),
        ],
        contract_addr=state_contract_obj.address,
        abi=state_contract_obj.abi,
    )
    logger.info(f'consensus status for project {project_id} and epoch {epoch_id} is {consensus_status}')

    # Extract status and CID from the ConsensusStatus struct
    status, cid, timestamp = consensus_status
    null_cid = f'null_{epoch_id}'

    # Only return without caching if the epoch is less than 10 epochs behind the current epoch
    if epoch_id > current_epoch[2] - 10 and timestamp == 0:
        logger.info(f'Consensus status not yet available for project {project_id} and epoch {epoch_id}')
        return null_cid, epoch_id

    # Process and cache the result only if we have a valid timestamp
    if cid and "null" not in cid:
        # First check if the key already exists
        project_hmap_key = project_data_hmap(project_id=project_id)

        # Create a pipeline for batch operations

        pipeline.hset(
            project_hmap_key,
            epoch_id,
            json.dumps({"snapshot_cid": cid, "status": status + 1}),
        )

        expiry_keys.append(f"{project_id}|{epoch_id}")

        # Process previousSnapshots if available
        try:
            snapshot_data = await get_submission_data(cid, ipfs_reader, False)
            if snapshot_data and "previousSnapshots" in snapshot_data and len(snapshot_data["previousSnapshots"]) > 0:
                data_to_cache = {}
                min_previous_snapshot_key = snapshot_data["previousSnapshots"][0][0]
                all_previous_snapshot_keys = set(range(min_previous_snapshot_key, epoch_id + 1))
                # Process each previous snapshot
                for (epoch_id, snapshot_cid) in snapshot_data["previousSnapshots"]:
                    epoch_id = int(epoch_id)
                    data_to_cache[epoch_id] = json.dumps({
                        "snapshot_cid": snapshot_cid,
                        "status": status + 1
                    })
                    all_previous_snapshot_keys.discard(epoch_id)
                    expiry_keys.append(f"{project_id}|{epoch_id}")

                # Add to pipeline if we have data to cache
                if data_to_cache:
                    pipeline.hset(
                        project_hmap_key,
                        mapping=data_to_cache,
                    )

                blank_epochs = sorted(list(all_previous_snapshot_keys))
                await redis_bitmap.set_bits_in_range(redis_conn, blank_epochs_bitmap_key, blank_epochs)
                
            if expiry_keys:
                expiry_data = {key: expiry_time for key in expiry_keys}
                pipeline.zadd(
                    name=data_expiry_zset(),
                    mapping=expiry_data,
                )
            
            # processing snapshot cid
            logger.info(f"Processing snapshot cid: {cid} for project {project_id} at epoch {epoch_id}")
            await process_snapshot_cid(redis_conn, ipfs_reader, project_id, cid, epoch_id, epoch_id)
        
        except Exception as e:
            logger.opt(exception=True).error(f'Error while fetching data from IPFS | CID {cid} | Error: {e}')
            pipeline.set(cid_not_found_key(cid), 'true', ex=86400)
            await pipeline.execute()
            return null_cid, epoch_id

        # Execute all redis operations in the pipeline
        await pipeline.execute()
        return cid, epoch_id
    else:
        await redis_bitmap.set_bit(redis_conn, blank_epochs_bitmap_key, epoch_id)

        await pipeline.execute()

        return null_cid, epoch_id


@retry(
    reraise=True,
    retry=retry_if_exception_type(Exception),
    wait=wait_random_exponential(multiplier=1, max=10),
    stop=stop_after_attempt(3),
)
async def w3_get_and_cache_finalized_cid_bulk_using_previous_snapshots(
    redis_conn: aioredis.Redis,
    state_contract_obj,
    rpc_helper: RpcHelper,
    ipfs_reader,
    epoch_ids: List[int],
    project_id: str,
):
    """
    Retrieves and caches the consensus status and snapshot CID for multiple epochs of a given project.

    This function interacts with the blockchain to get the snapshot status for multiple epochs,
    then caches the results in Redis.

    Args:
        redis_conn (aioredis.Redis): Redis connection object
        state_contract_obj: Contract object for the protocol state contract
        rpc_helper (RpcHelper): Helper object for making web3 calls
        epoch_ids (List[int]): List of epoch IDs to fetch
        project_id (str): Project ID

    Returns:
        List[Tuple[str, int]]: List of tuples containing (CID, epoch_id) for each epoch
    """
    try:
        logger.info(f'Fetching CIDs for {len(epoch_ids)} epochs for project {project_id}')
        blank_epochs_bitmap_key = blank_epochs_bitmap(project_id)
        missing_epochs = []
        cid_data_with_epochs = []

        blank_epochs = await redis_bitmap.get_bits_in_range(
            redis_conn,
            blank_epochs_bitmap_key,
            epoch_ids
        )

        for epoch_id, is_blank in blank_epochs:
            if is_blank:
                cid_data_with_epochs.append((f'null_{epoch_id}', epoch_id))
            else:
                missing_epochs.append(epoch_id)
        logger.info(f'Found {len(cid_data_with_epochs)} CIDs for project {project_id}')
        logger.info(f'Found {len(missing_epochs)} missing epochs without blanks for project {project_id}')
        # Get the project hashmap key
        project_hmap_key = project_data_hmap(project_id=project_id)

        while True:
            logger.info(f'Fetching CIDs for {len(missing_epochs)} epochs for project {project_id}')
            if len(missing_epochs) == 0:
                break
            sorted_missing_epochs = sorted(list(missing_epochs))
            epoch_to_fetch = sorted_missing_epochs[-1]
            
            cid, epoch_id = await w3_get_and_cache_finalized_cid(redis_conn, state_contract_obj, rpc_helper, ipfs_reader, epoch_to_fetch, project_id)
            cid_data_with_epochs.append((cid, epoch_id))
            missing_epochs.remove(epoch_to_fetch)
            if cid and "null" not in cid:
                if not missing_epochs:
                    break
                missing_epoch_list = sorted(list(missing_epochs))
                redis_cache_data = await redis_conn.hmget(project_hmap_key, missing_epoch_list)

                blank_epochs = await redis_bitmap.get_bits_in_range(
                    redis_conn,
                    blank_epochs_bitmap_key,
                    missing_epoch_list
                )

                for epoch_id, is_blank in blank_epochs:
                    if is_blank:
                        cid_data_with_epochs.append((f'null_{epoch_id}', epoch_id))
                        missing_epochs.remove(epoch_id)

                data = []
                for data_raw_item in redis_cache_data:
                    if data_raw_item:
                        data.append(json.loads(data_raw_item))
                    else:
                        data.append(dict())
                    
                for snapshot_data, epoch_id_from_list in zip(data, missing_epoch_list):
                    if "snapshot_cid" in snapshot_data:
                        cid_data_with_epochs.append((snapshot_data["snapshot_cid"], epoch_id_from_list))
                        if epoch_id_from_list in missing_epochs:
                            missing_epochs.remove(epoch_id_from_list)
                else:
                    logger.debug(f"missing_epoch_list is empty after fetching CID for {epoch_to_fetch}. Skipping hmget for this iteration.")                    

        return cid_data_with_epochs

    except Exception as e:
        logger.error(f'Error in w3_get_and_cache_finalized_cid_bulk_using_previous_snapshots: {str(e)}')
        raise


@retry(
    reraise=True,
    retry=retry_if_exception_type(Exception),
    wait=wait_random_exponential(multiplier=1, max=10),
    stop=stop_after_attempt(3),
)
async def w3_get_and_cache_finalized_cid_bulk(
    redis_conn: aioredis.Redis,
    state_contract_obj,
    rpc_helper: RpcHelper,
    epoch_ids: List[int],
    project_id: str,
):
    """
    Retrieves and caches the consensus status and snapshot CID for multiple epochs of a given project.

    This function interacts with the blockchain to get the snapshot status for multiple epochs,
    then caches the results in Redis.

    Args:
        redis_conn (aioredis.Redis): Redis connection object
        state_contract_obj: Contract object for the protocol state contract
        rpc_helper (RpcHelper): Helper object for making web3 calls
        epoch_ids (List[int]): List of epoch IDs to fetch
        project_id (str): Project ID

    Returns:
        List[Tuple[str, int]]: List of tuples containing (CID, epoch_id) for each epoch
    """
    BATCH_SIZE = 50
    try:
        pipeline = redis_conn.pipeline()
        blank_epochs_bitmap_key = blank_epochs_bitmap(project_id)
        all_results = []

        for i in range(0, len(epoch_ids), BATCH_SIZE):
            batch_epoch_ids = epoch_ids[i:i + BATCH_SIZE]

            # Prepare tasks for batch call
            tasks = [
                ('snapshotStatus', [Web3.to_checksum_address(settings.data_market), project_id, epoch_id])
                for epoch_id in batch_epoch_ids
            ]

            # Make batch call
            batch_results = await rpc_helper.batch_web3_contract_calls(
                tasks=tasks,
                contract_obj=state_contract_obj,
            )
            all_results.extend(batch_results)

        # Process results and prepare for caching
        cids_with_epochs = []
        
        # Get the project hashmap key
        project_hmap_key = project_data_hmap(project_id=project_id)
        
        # Create mappings only for non-existing keys
        data_to_cache = {}
        blank_epochs = []
        expiry_keys = []
        expiry_time = int(time.time()) + PROJECT_DATA_ENTRY_EXPIRY

        for i, epoch_id in enumerate(epoch_ids):
            consensus_status = all_results[i]

            # Extract status and CID from the ConsensusStatus struct
            status, cid, timestamp = consensus_status

            # Skip if consensus status is not yet available
            null_cid = f'null_{epoch_id}'
            if timestamp == 0:
                logger.debug(f'Consensus status not yet available for project {project_id} and epoch {epoch_id}')
                cids_with_epochs.append((null_cid, epoch_id))
                continue

            if cid:
                data_to_cache[epoch_id] = json.dumps({"snapshot_cid": cid, "status": status})
                expiry_keys.append(f"{project_id}|{epoch_id}")
                cids_with_epochs.append((cid, epoch_id))
            else:
                cids_with_epochs.append((null_cid, epoch_id))
                blank_epochs.append(epoch_id)

        # Use pipeline for Redis operations if we have keys to update
        if data_to_cache:

            pipeline.hset(
                project_hmap_key,
                mapping=data_to_cache,
            )

        if blank_epochs:
            await redis_bitmap.set_bits_in_range(
                redis_conn,
                blank_epochs_bitmap_key,
                blank_epochs,
            )

        # Add to expiry tracking sorted set with TTL
        if expiry_keys:
            expiry_data = {key: expiry_time for key in expiry_keys}
            pipeline.zadd(
                name=data_expiry_zset(),
                mapping=expiry_data,
            )

        await pipeline.execute()

        return cids_with_epochs

    except Exception as e:
        logger.error(f'Error in w3_get_and_cache_finalized_cid_bulk: {str(e)}')
        raise


async def get_project_first_epoch(redis_conn: aioredis.Redis, state_contract_obj, rpc_helper: RpcHelper, project_id):
    """
    Get the first epoch for a given project ID.

    This function first checks a Redis cache for the first epoch. If not found, it fetches the information
    from the blockchain and caches it in Redis for future use.

    Args:
        redis_conn (aioredis.Redis): Redis connection object.
        state_contract_obj: Contract object for the state contract.
        rpc_helper: RPC helper object.
        project_id (str): ID of the project.

    Returns:
        int: The first epoch for the given project ID.
    """
    # Try to get the first epoch from Redis cache
    first_epoch_data = await redis_conn.hget(
        project_first_epoch_hmap(),
        project_id,
    )
    if first_epoch_data:
        first_epoch = int(first_epoch_data)
        return first_epoch
    else:
        # If not in cache, fetch from blockchain
        [first_epoch] = await rpc_helper.web3_call(
            tasks=[
                ('projectFirstEpochId', [Web3.to_checksum_address(settings.data_market), project_id]),
            ],
            contract_addr=state_contract_obj.address,
            abi=state_contract_obj.abi,
        )
        logger.debug(f'first epoch for project {project_id} is {first_epoch}')

        # Cache the result if it's not 0
        if first_epoch != 0:
            await redis_conn.hset(
                project_first_epoch_hmap(),
                project_id,
                first_epoch,
            )

        return first_epoch


@retry(
    reraise=True,
    retry=retry_if_exception_type(Exception),
    wait=wait_random_exponential(multiplier=0.5, max=10),
    stop=stop_after_attempt(3),
    before_sleep=retry_state_callback,
)
async def _fetch_file_from_ipfs(ipfs_reader, cid):
    """
    Fetches a file from IPFS using the given IPFS reader and CID.

    This function is decorated with a retry mechanism to handle potential IPFS errors.

    Args:
        ipfs_reader: An IPFS reader object.
        cid: The CID of the file to fetch.

    Returns:
        The contents of the file as bytes.
    """
    return await ipfs_reader.cat(cid)


async def fetch_file_from_ipfs(ipfs_reader, cid):
    """
    Fetches a file from IPFS using the given IPFS reader and CID.

    Uses _fetch_file_from_ipfs under the hood, if it is unable to fetch file from IPFS, it will mark the cid as not found in redis.
    """
    try:
        data = await _fetch_file_from_ipfs(ipfs_reader, cid)
        return json.loads(data)
    except Exception as e:
        logger.opt(exception=True).error(f'Error while fetching data from IPFS | CID {cid} | Error: {e}')
        return dict()


async def get_submission_data(cid, ipfs_reader, cleanup_previous_snapshots: bool = True) -> dict:
    """
    Fetches submission data from cache or IPFS.

    This function first attempts to read the data from a local cache. If not found,
    it fetches the data from IPFS and then caches it locally.

    Args:
        redis_conn (aioredis.Redis): Redis connection object.
        cid (str): IPFS content ID.
        ipfs_reader (ipfshttpclient.client.Client): IPFS client object.
        project_id (str): ID of the project.

    Returns:
        dict: Submission data.
    """
    if not cid or 'null' in cid:
        return dict()

    data = await fetch_file_from_ipfs(ipfs_reader, cid)
    if isinstance(data, str):
        data = json.loads(data)
    if data:
        if cleanup_previous_snapshots and "previousSnapshots" in data:
            data["previousSnapshots"] = []
        return data
    else:
        return dict()


async def get_submission_data_bulk(
    redis_conn: aioredis.Redis,
    cids: List[str],
    ipfs_reader,
    project_id: str,
    ensure_complete: bool = False,
) -> List[dict]:
    """
    Retrieves submission data for multiple submissions in bulk.

    This function processes the submissions in batches to optimize performance.

    Args:
        redis_conn (aioredis.Redis): Redis connection object.
        cids (List[str]): List of submission CIDs.
        ipfs_reader: IPFS reader object.

    Returns:
        List[dict]: List of submission data dictionaries.
    """
    BATCH_SIZE = 1000
    all_snapshot_data = {}
    cid_keys = [cid_cache(cid) for cid in cids]
    # try to get data from redis cache
    cid_cache_data = await redis_conn.mget(cid_keys)

    project_config = get_project_config(project_id)
    pipeline = redis_conn.pipeline()

    missing_cids = []
    for cid, data in zip(cids, cid_cache_data):
        if data:
            all_snapshot_data[cid] = json.loads(data)
        else:
            missing_cids.append(cid)

    # Process submissions in batches
    for i in range(0, len(missing_cids), BATCH_SIZE):
        batch_cids = missing_cids[i:i + BATCH_SIZE]
        batch_snapshot_data = await asyncio.gather(
            *[
                get_submission_data(cid, ipfs_reader)
                for cid in batch_cids
            ],
        )

        for cid, data in zip(batch_cids, batch_snapshot_data):

            all_snapshot_data[cid] = data
            if project_config and project_config.cache_cids:
                pipeline.set(
                    name=cid_cache(cid),
                    value=json.dumps(data),
                    ex=PROJECT_DATA_ENTRY_EXPIRY,
                )
        await pipeline.execute()

        if ensure_complete:
            missing_cids = [
                cid for cid, data in zip(batch_cids, batch_snapshot_data)
                if data == dict()
            ]
            if missing_cids:
                logger.error(f'Incomplete ipfs data for CIDs: {missing_cids}')
                return []

    final_snapshot_data = []
    for cid in cids:
        final_snapshot_data.append(all_snapshot_data[cid])

    return final_snapshot_data


async def get_project_epoch_snapshot(
    redis_conn: aioredis.Redis, state_contract_obj, rpc_helper, ipfs_reader, epoch_id, project_id, seek=False
) -> EpochSnapshotResponse:
    """
    Retrieves the epoch snapshot for a given project.

    This function first gets the finalized CID for the given epoch and project,
    then fetches the corresponding submission data. If no CID is found for the exact epoch,
    it will find the closest epochs before and after the requested epoch if seek=True.

    Args:
        redis_conn (aioredis.Redis): Redis connection object.
        state_contract_obj: State contract object.
        rpc_helper: RPC helper object.
        ipfs_reader: IPFS reader object.
        epoch_id (int): Epoch ID.
        project_id (str): Project ID.
        seek (bool): If True and no exact match found, find closest epochs.

    Returns:
        EpochSnapshotResponse: A response object containing either:
            1. An exact match for the requested epoch
            2. The closest epochs when seek=True and no exact match exists
            3. No data (empty response)
    """
    cid = await get_project_finalized_cid(redis_conn, state_contract_obj, rpc_helper, ipfs_reader, epoch_id, project_id)
    if cid and 'null' not in cid:
        data = await get_submission_data(cid, ipfs_reader)
        return EpochSnapshotResponse(
            exact_match=ExactEpochSnapshot(
                epoch_id=epoch_id,
                snapshot_cid=cid,
                data=data
            )
        )
    elif seek:
        # Get all finalized epoch IDs for this project
        project_hmap_key = project_data_hmap(project_id=project_id)
        keys = await redis_conn.hkeys(project_hmap_key)
        epoch_ids = []
        for key in keys:
            try:
                epoch_ids.append(int(key.decode('utf-8')))
            except (ValueError, AttributeError) as e:
                logger.warning(f"Invalid epoch ID in Redis: {key}, Error: {e}")
                continue
        
        if not epoch_ids:
            logger.info(f"No finalized epochs found for project {project_id}")
            return EpochSnapshotResponse()

        # Sort epoch IDs to find closest ones
        epoch_ids.sort()
        
        # Find closest epochs before and after the requested epoch_id
        prev_epoch = None
        next_epoch = None
        
        for e_id in epoch_ids:
            if e_id <= epoch_id:
                prev_epoch = e_id
            else:
                next_epoch = e_id
                break

        closest_epochs = ClosestEpochs()
        
        # If we found closest epochs, get their CIDs and data
        if prev_epoch is not None:
            prev_data_raw = await redis_conn.hget(project_hmap_key, str(prev_epoch))
            if prev_data_raw:
                prev_data = json.loads(prev_data_raw)
                prev_cid = prev_data.get("snapshot_cid")
                if prev_cid and 'null' not in prev_cid:
                    closest_epochs.previous = EpochIdentifier(
                        epoch_id=prev_epoch,
                        snapshot_cid=prev_cid
                    )
        
        if next_epoch is not None:
            next_data_raw = await redis_conn.hget(project_hmap_key, str(next_epoch))
            if next_data_raw:
                next_data = json.loads(next_data_raw)
                next_cid = next_data.get("snapshot_cid")
                if next_cid and 'null' not in next_cid:
                    closest_epochs.next = EpochIdentifier(
                        epoch_id=next_epoch,
                        snapshot_cid=next_cid
                    )
        
        return EpochSnapshotResponse(closest_epochs=closest_epochs)
    else:
        return EpochSnapshotResponse()


async def get_source_chain_id(redis_conn: aioredis.Redis, state_contract_obj, rpc_helper: RpcHelper):
    """
    Retrieves the source chain ID from Redis cache if available, otherwise fetches it from the state contract and caches it in Redis.

    Args:
        redis_conn (aioredis.Redis): Redis connection object.
        state_contract_obj: State contract object.
        rpc_helper: RPC helper object.

    Returns:
        int: The source chain ID.
    """
    # Try to get the source chain ID from Redis cache
    source_chain_id_data = await redis_conn.get(
        source_chain_id_key(),
    )
    if source_chain_id_data:
        source_chain_id = int(source_chain_id_data.decode('utf-8'))
        return source_chain_id
    else:
        # If not in cache, fetch from blockchain
        [source_chain_id] = await rpc_helper.web3_call(
            tasks=[
                ('SOURCE_CHAIN_ID', [Web3.to_checksum_address(settings.data_market)]),
            ],
            contract_addr=state_contract_obj.address,
            abi=state_contract_obj.abi,
        )

        # Cache the result in Redis
        await redis_conn.set(
            source_chain_id_key(),
            source_chain_id,
        )
        return source_chain_id


async def get_source_chain_epoch_size(redis_conn: aioredis.Redis, state_contract_obj, rpc_helper: RpcHelper):
    """
    This function retrieves the epoch size of the source chain from the state contract.

    It first checks if the epoch size is cached in Redis. If not, it fetches from the blockchain and caches the result.

    Args:
        redis_conn (aioredis.Redis): Redis connection object.
        state_contract_obj: Contract object for the state contract.
        rpc_helper: Helper object for making RPC calls.

    Returns:
        int: The epoch size of the source chain.
    """
    # Try to get the epoch size from Redis cache
    source_chain_epoch_size_data = await redis_conn.get(
        source_chain_epoch_size_key(),
    )
    if source_chain_epoch_size_data:
        source_chain_epoch_size = int(source_chain_epoch_size_data.decode('utf-8'))
        return source_chain_epoch_size
    else:
        # If not in cache, fetch from blockchain
        [source_chain_epoch_size] = await rpc_helper.web3_call(
            tasks=[('EPOCH_SIZE', [Web3.to_checksum_address(settings.data_market)])],
            contract_addr=state_contract_obj.address,
            abi=state_contract_obj.abi,
        )

        # Cache the result in Redis
        await redis_conn.set(
            source_chain_epoch_size_key(),
            source_chain_epoch_size,
        )

        return source_chain_epoch_size


async def get_source_chain_block_time(redis_conn: aioredis.Redis, state_contract_obj, rpc_helper: RpcHelper):
    """
    Get the block time of the source chain.

    This function first checks Redis cache for the block time. If not found, it fetches from the blockchain
    and caches the result.

    Args:
        redis_conn (aioredis.Redis): Redis connection object.
        state_contract_obj: Contract object for the state contract.
        rpc_helper: RPC helper object.

    Returns:
        int: Block time of the source chain.
    """
    # Try to get the block time from Redis cache
    source_chain_block_time_data = await redis_conn.get(
        source_chain_block_time_key(),
    )
    if source_chain_block_time_data:
        source_chain_block_time = int(source_chain_block_time_data.decode('utf-8'))
        return source_chain_block_time
    else:
        # If not in cache, fetch from blockchain
        [source_chain_block_time] = await rpc_helper.web3_call(
            tasks=[('SOURCE_CHAIN_BLOCK_TIME', [Web3.to_checksum_address(settings.data_market)])],
            contract_addr=state_contract_obj.address,
            abi=state_contract_obj.abi,
        )
        source_chain_block_time = int(source_chain_block_time / 1e4)

        # Cache the result in Redis
        await redis_conn.set(
            source_chain_block_time_key(),
            source_chain_block_time,
        )

        return source_chain_block_time


async def get_tail_epoch_id(
        redis_conn: aioredis.Redis,
        state_contract_obj,
        rpc_helper,
        current_epoch_id,
        time_in_seconds,
        project_id,
):
    """
    Returns the tail epoch_id and a boolean indicating if tail contains the full time window.

    This function calculates the tail epoch ID based on the current epoch and a time window,
    ensuring it doesn't go below the project's first epoch.

    Args:
        redis_conn (aioredis.Redis): Redis connection object.
        state_contract_obj: State contract object.
        rpc_helper: RPC helper object.
        current_epoch_id (int): Current epoch ID.
        time_in_seconds (int): Time window in seconds.
        project_id (str): Project ID.

    Returns:
        Tuple[int, bool]: Tail epoch ID and a boolean indicating if tail contains the full time window.
    """
    # Get necessary chain parameters
    source_chain_epoch_size = await get_source_chain_epoch_size(redis_conn, state_contract_obj, rpc_helper)
    source_chain_block_time = await get_source_chain_block_time(redis_conn, state_contract_obj, rpc_helper)

    # Calculate tail epoch_id
    tail_epoch_id = current_epoch_id - int(time_in_seconds / (source_chain_epoch_size * source_chain_block_time))
    project_first_epoch = await get_project_first_epoch(redis_conn, state_contract_obj, rpc_helper, project_id)

    # Ensure tail_epoch_id is not less than project_first_epoch
    if tail_epoch_id < project_first_epoch:
        tail_epoch_id = project_first_epoch
        return tail_epoch_id, True

    logger.trace(
        'project ID {} tail epoch_id: {} against head epoch ID {} ',
        project_id, tail_epoch_id, current_epoch_id,
    )

    return tail_epoch_id, False


async def get_project_latest_snapshot(
    redis_conn: aioredis.Redis,
    state_contract_obj,
    rpc_helper,
    ipfs_reader,
    project_id,
) -> Optional[Dict[str, Any]]:
    """
    Retrieves the latest snapshot for a given project.

    This function first gets the latest epoch ID for the project, then fetches the snapshot data for that epoch.

    Args:
        redis_conn (aioredis.Redis): Redis connection object.
        state_contract_obj: State contract object.
        rpc_helper: RPC helper object.
        ipfs_reader: IPFS reader object.
        project_id: ID of the project to fetch snapshot data for.

    Returns:
        Optional[Dict[str, Any]]: The latest snapshot data for the given project, or None if not found.
    """
    last_submitted_snapshot_data = await get_last_submitted_snapshot_data(redis_conn, project_id)
    if last_submitted_snapshot_data:
        target_epoch = last_submitted_snapshot_data['epochId']
    else:
        target_epoch = await get_project_last_finalized_epoch(
            redis_conn=redis_conn,
            state_contract_obj=state_contract_obj,
            rpc_helper=rpc_helper,
            project_id=project_id,
        )

    if not target_epoch:
        logger.error(f"No last finalized epoch found for project {project_id}")
        return None
    else:
        logger.info(f"Using epoch {target_epoch} for fetch against project {project_id}")

    snapshot_response = await get_project_epoch_snapshot(
        redis_conn, state_contract_obj, rpc_helper, ipfs_reader, target_epoch, project_id
    )
    
    if snapshot_response.exact_match:
        return snapshot_response.exact_match.data
    return None


async def get_project_epoch_snapshot_bulk(
        redis_conn: aioredis.Redis,
        state_contract_obj,
        rpc_helper,
        ipfs_reader,
        epoch_id_min: int,
        epoch_id_max: int,
        project_id,
        ensure_complete: bool = False,
):
    """
    Fetches the snapshot data for a given project and epoch range.

    This function retrieves snapshot data in bulk, first checking Redis cache and then
    fetching missing data from the blockchain if necessary.

    Args:
        redis_conn (aioredis.Redis): Redis connection object.
        state_contract_obj: State contract object.
        rpc_helper: RPC helper object.
        ipfs_reader: IPFS reader object.
        epoch_id_min (int): Minimum epoch ID to fetch snapshot data for.
        epoch_id_max (int): Maximum epoch ID to fetch snapshot data for.
        project_id: ID of the project to fetch snapshot data for.

    Returns:
        A list of snapshot data for the given project and epoch range.
    """
    cid_data, project_first_epoch = await get_project_finalized_cids_bulk(
        redis_conn, state_contract_obj, rpc_helper, ipfs_reader, epoch_id_min, epoch_id_max, project_id,
    )

    epoch_id_min = max(epoch_id_min, project_first_epoch)

    cid_data_with_epochs = zip(cid_data, range(epoch_id_min, epoch_id_max + 1))
    # Filter out null CIDs
    valid_cid_data_with_epochs = [
        (cid, epoch_id) for cid, epoch_id in cid_data_with_epochs
        if cid and 'null' not in cid
    ]

    if ensure_complete and len(valid_cid_data_with_epochs) != epoch_id_max - epoch_id_min + 1:
        logger.error(f'Incomplete cids found for project {project_id} from epoch {epoch_id_min} to {epoch_id_max}')
        return []

    # Fetch snapshot data in bulk
    all_snapshot_data = await get_submission_data_bulk(
        redis_conn,
        [cid for cid, _ in valid_cid_data_with_epochs],
        ipfs_reader,
        project_id,
        ensure_complete=ensure_complete,
    )

    return all_snapshot_data


async def get_project_time_series_data(
        start_time: int,
        end_time: int,
        step_seconds: int,
        end_epoch_id: int,
        redis_conn: aioredis.Redis,
        state_contract_obj,
        rpc_helper,
        ipfs_reader,
        project_id,
) -> List[Dict[str, Any]]:
    """
    Returns a list of snapshot data containing equally spaced observations starting with the start_epoch id
    for the given project_id, and including epochs spaced step_seconds apart until the maximum observations has been reached.

    Args:
        start_time: Start time in seconds
        end_time: End time in seconds
        step_seconds: Time in seconds between each observation
        end_epoch_id: End epoch ID
        redis_conn (aioredis.Redis): Redis connection object.
        state_contract_obj: State contract object.
        rpc_helper: RPC helper object.
        ipfs_reader: IPFS reader object.
        project_id: ID of the project to fetch snapshot data for.

    Returns:
        List[Dict[str, Any]]: A list of snapshot data objects for the given project_id.
    """
    # get metadata for building steps
    [
        source_chain_epoch_size,
        source_chain_block_time,
        project_first_epoch,
    ] = await asyncio.gather(
        get_source_chain_epoch_size(
            redis_conn,
            state_contract_obj,
            rpc_helper,
        ),
        get_source_chain_block_time(
            redis_conn,
            state_contract_obj,
            rpc_helper,
        ),
        get_project_first_epoch(
            redis_conn,
            state_contract_obj,
            rpc_helper,
            project_id,
        ),
    )

    seek_stop_flag = False

    closest_step_time_gap = end_time % step_seconds
    closest_step_timestamp = end_time - closest_step_time_gap
    closest_step_epoch_id = end_epoch_id - \
        int(closest_step_time_gap / (source_chain_epoch_size * source_chain_block_time))
    if closest_step_epoch_id <= project_first_epoch:
        closest_step_epoch_id = project_first_epoch
        seek_stop_flag = True

    cid_tasks = []
    cid_tasks.append(
        get_project_finalized_cid(
            redis_conn,
            state_contract_obj,
            rpc_helper,
            ipfs_reader,
            closest_step_epoch_id,
            project_id,
        ),
    )

    remaining_observations = int((closest_step_timestamp - start_time) / step_seconds)

    count = 0
    head_epoch_id = closest_step_epoch_id
    while not seek_stop_flag and count < remaining_observations:
        tail_epoch_id = head_epoch_id - int(step_seconds / (source_chain_epoch_size * source_chain_block_time))
        if tail_epoch_id <= project_first_epoch:
            tail_epoch_id = project_first_epoch
            seek_stop_flag = True

        cid_tasks.append(
            get_project_finalized_cid(
                redis_conn,
                state_contract_obj,
                rpc_helper,
                ipfs_reader,
                tail_epoch_id,
                project_id,
            ),
        )

        head_epoch_id = tail_epoch_id
        count += 1

    all_cids = await asyncio.gather(*cid_tasks)

    return await get_submission_data_bulk(
        redis_conn=redis_conn,
        cids=all_cids,
        ipfs_reader=ipfs_reader,
        project_id=project_id,
    )


async def fetch_block_timestamps(
    redis_conn: aioredis.Redis,
    rpc_helper: RpcHelper,
    block_numbers: List[int],
) -> Optional[Dict[int, int]]:
    try:

        block_number_to_timestamp_mapping = {}
        missing_block_data = []
        RPC_BATCH_SIZE = 100
        mapping_bnt = {}
        mapping_tnb = {}

        prepared_rpc_calls = []

        for i in range(0, len(block_numbers), RPC_BATCH_SIZE):
            current_batch_block_numbers = block_numbers[i:i + RPC_BATCH_SIZE]
            if not current_batch_block_numbers:
                continue

            rpc_query = []
            request_id_counter = 1
            for block_num_in_batch in current_batch_block_numbers:
                rpc_query.append(
                    {
                        'jsonrpc': '2.0',
                        'method': 'eth_getBlockByNumber',
                        'params': [
                            hex(block_num_in_batch),
                            False,
                        ],
                        'id': request_id_counter,
                    },
                )
                request_id_counter += 1
            prepared_rpc_calls.append((current_batch_block_numbers, rpc_query))

        if not prepared_rpc_calls:
            return {}

        rpc_tasks = [rpc_helper._make_rpc_jsonrpc_call(query) for _, query in prepared_rpc_calls]
        
        logger.info(f"Concurrently fetching {len(rpc_tasks)} batches for a total of {len(block_numbers)} blocks.")
        all_batch_responses_or_exceptions = await asyncio.gather(*rpc_tasks, return_exceptions=True)

        for i, response_or_exception in enumerate(all_batch_responses_or_exceptions):
            current_batch_block_numbers, _ = prepared_rpc_calls[i]

            if isinstance(response_or_exception, Exception):
                logger.error(f"RPC call for batch starting with block {current_batch_block_numbers[0] if current_batch_block_numbers else 'N/A'} failed: {response_or_exception}")
                missing_block_data.extend(current_batch_block_numbers)
                continue

            batch_response_data = response_or_exception
            if isinstance(batch_response_data, list):
                for block_num, block_data_item in zip(current_batch_block_numbers, batch_response_data):
                    if block_data_item and 'result' in block_data_item:
                        result_data = block_data_item['result']
                        if result_data and 'timestamp' in result_data:
                            timestamp = int(result_data['timestamp'], 16)
                            block_number_to_timestamp_mapping[block_num] = timestamp
                            mapping_bnt[json.dumps(timestamp)] = block_num
                            mapping_tnb[str(block_num)] = timestamp
                        else:
                            logger.warning(f"No timestamp or null result in block data for block {block_num}: {result_data}")
                            missing_block_data.append(block_num)
                    else:
                        logger.warning(f"No block data or malformed response for block {block_num} in batch: {block_data_item}")
                        missing_block_data.append(block_num)
            else:
                logger.error(f"Unexpected response type from RPC batch call (expected list, got {type(batch_response_data)}): {str(batch_response_data)[:500]}. Affecting blocks: {current_batch_block_numbers}")
                missing_block_data.extend(current_batch_block_numbers)

        if mapping_bnt:
            try:
                await redis_conn.zadd(
                    block_number_to_timestamp_key(settings.namespace), 
                    mapping=mapping_bnt
                )
                logger.debug(f"Successfully cached {len(mapping_bnt)} entries to {block_number_to_timestamp_key(settings.namespace)}")
            except Exception as e:
                logger.error(f"Redis ZADD call failed for {block_number_to_timestamp_key(settings.namespace)}: {e}")

        if mapping_tnb:
            try:
                await redis_conn.zadd(
                    timestamp_to_block_number_key(settings.namespace), 
                    mapping=mapping_tnb
                )
                logger.debug(f"Successfully cached {len(mapping_tnb)} entries to {timestamp_to_block_number_key(settings.namespace)}")
            except Exception as e:
                logger.error(f"Redis ZADD call failed for {timestamp_to_block_number_key(settings.namespace)}: {e}")

        if missing_block_data:
            logger.warning(f"Missing block data for {len(missing_block_data)} blocks after RPC calls: {missing_block_data[:10]}{'...' if len(missing_block_data) > 10 else ''}")

        return block_number_to_timestamp_mapping

    except Exception as e:
        logger.error(f"Overall error in fetch_block_timestamps: {e}", exc_info=True)
        return {}


async def get_block_number_closest_to_timestamp(
    redis_conn: aioredis.Redis,
    target_timestamp: int,
    search_type: BlockSearchType = BlockSearchType.BEFORE_OR_AT,
) -> Optional[int]:
    """Finds the block number closest to the target_timestamp using a Redis ZSET.

    The ZSET is expected to have timestamps as scores and block numbers (as strings) as values.

    Args:
        redis_conn: Async Redis connection object.
        target_timestamp: The Unix timestamp to find the closest block.
        search_type: Determines the search direction.
                     BlockSearchType.BEFORE_OR_AT: finds the block with the largest timestamp <= target_timestamp.
                     BlockSearchType.AFTER_OR_AT: finds the block with the smallest timestamp >= target_timestamp.

    Returns:
        The block number, or None if no such block is found.
    """
    key = timestamp_to_block_number_key(settings.namespace)
    
    try:
        if search_type == BlockSearchType.BEFORE_OR_AT:
            # Find block with the largest timestamp <= target_timestamp
            result_raw = await redis_conn.zrevrangebyscore(
                key, 
                max=target_timestamp, 
                min='-inf', 
                start=0, 
                num=1, 
                withscores=True
            )
            log_message_prefix = f"Closest block at or before {target_timestamp}"
            warning_message_suffix = "too old or data is missing."
        elif search_type == BlockSearchType.AFTER_OR_AT:
            # Find block with the smallest timestamp >= target_timestamp
            result_raw = await redis_conn.zrangebyscore(
                key, 
                min=target_timestamp, 
                max='+inf', 
                start=0, 
                num=1, 
                withscores=True
            )
            log_message_prefix = f"Closest block at or after {target_timestamp}"
            warning_message_suffix = "too new or data is missing."
        else:
            raise ValueError(f"Invalid search_type: {search_type}")
        
        if result_raw:
            block_num_bytes, ts_float = result_raw[0]
            block_number = int(block_num_bytes.decode('utf-8'))
            timestamp_of_block = int(ts_float)
            logger.debug(
                f"{log_message_prefix} is {block_number} "
                f"(ts: {timestamp_of_block}) from key {key}"
            )
            return block_number
        else:
            logger.warning(
                f"No block found for timestamp {target_timestamp} with search_type {search_type.name} in ZSET {key}. "
                f"This may mean the target timestamp is {warning_message_suffix}"
            )
            return None

    except Exception as e:
        logger.error(
            f"Error querying Redis for closest block for timestamp {target_timestamp} (search_type: {search_type.name}) "
            f"using key {key}: {e}", exc_info=True
        )
        return None


    
async def process_snapshot_cid(redis_conn: aioredis.Redis, ipfs_reader: AsyncIPFSClient, project_id: str, snapshot_cid: str, epoch_id: int, original_epoch_id: int, rec_depth: int = 0):
    try:
        if rec_depth == 0:
            # mark in redis that this project is being processed
            # check if project is already being processed
            if await redis_conn.exists(f"project_processing:{project_id}"):
                logger.info(f"Project {project_id} is already being processed. Skipping.")
                return False
            await redis_conn.set(f"project_processing:{project_id}", "true", ex=300)

        logger.info(f"Processing snapshot cid: {snapshot_cid} for project {project_id} at epoch {epoch_id} (original epoch {original_epoch_id}), rec_depth {rec_depth}")

        project_config = get_project_config(project_id)
        if not project_config.keep_previous_snapshot_data:
            return
        snapshot_data = await get_submission_data(snapshot_cid, ipfs_reader, False)
        pipeline = redis_conn.pipeline()
        expiry_keys = []

        project_hmap_key = project_data_hmap(project_id=project_id)
        expiry_time = int(time.time()) + PROJECT_DATA_ENTRY_EXPIRY

        if snapshot_data:
            if "previousSnapshots" in snapshot_data and len(snapshot_data["previousSnapshots"]) > 0:    
                data_to_cache = {}
                all_previous_snapshot_keys = set(range(snapshot_data["previousSnapshots"][0][0], epoch_id))
                # Process each previous snapshot
                for (epoch_id, snapshot_cid) in snapshot_data["previousSnapshots"][::-1]:
                    epoch_id = int(epoch_id)
                    data_to_cache[epoch_id] = json.dumps({
                        "snapshot_cid": snapshot_cid,
                        "status": SnapshotStatus.SUBMITTED.value
                    })
                    all_previous_snapshot_keys.discard(epoch_id)
                    expiry_keys.append(f"{project_id}|{epoch_id}")

                # Add to pipeline if we have data to cache
                if data_to_cache:
                    pipeline.hset(
                        project_hmap_key,
                        mapping=data_to_cache,
                    )
                
                blank_epochs_bitmap_key = blank_epochs_bitmap(project_id)

                epochs_to_set = sorted(list(all_previous_snapshot_keys))
                await redis_bitmap.set_bits_in_range(redis_conn, blank_epochs_bitmap_key, epochs_to_set)

                if len(snapshot_data["previousSnapshots"]) > 0:
                    epoch_id = snapshot_data["previousSnapshots"][0][0]
                    epoch_cid = snapshot_data["previousSnapshots"][0][1]
                    # recursively process previous snapshots
                    within_recursion_depth = rec_depth < MAX_RECURSION_DEPTH
                    already_processed = await redis_conn.hexists(project_hmap_key, epoch_id)
                    # check if epoch_id is present in project_hmap_key and blank_epochs_set_key
                    if within_recursion_depth and not already_processed:
                        await process_snapshot_cid(redis_conn, ipfs_reader, project_id, epoch_cid, epoch_id, original_epoch_id, rec_depth=rec_depth + 1)

            if project_config.cache_cids:
                snapshot_data["previousSnapshots"] = []
                # cache lite snapshot in redis
                cid_cache_key = cid_cache(snapshot_cid)
                pipeline.set(
                    name=cid_cache_key,
                    value=json.dumps(snapshot_data),
                    ex=PROJECT_DATA_ENTRY_EXPIRY,
                )

        if expiry_keys:
            expiry_data = {key: expiry_time for key in expiry_keys}
            pipeline.zadd(
                name=data_expiry_zset(),
                mapping=expiry_data,
            )

        if rec_depth == 0:
            # remove the mark in redis that this project is being processed
            await redis_conn.delete(f"project_processing:{project_id}")

        await pipeline.execute()
    except Exception as e:
        logger.error(f'Error processing snapshot cid: {e}')
        logger.error(f'Detailed traceback:\n{traceback.format_exc()}')
        logger.error(f'Snapshot cid: {snapshot_cid}')


async def get_current_epoch_id(
    anchor_rpc_helper: RpcHelper,
    protocol_state_contract,
):
    """
    Retrieves the current epoch ID from the protocol state contract.
    
    Args:
        anchor_rpc_helper (RpcHelper): RPC helper for blockchain interactions
        protocol_state_contract: Smart contract object for protocol state
        
    Returns:
        int: The current epoch ID from the blockchain
    """
    [current_epoch_data] = await anchor_rpc_helper.web3_call(
        tasks=[
            ('currentEpoch', [Web3.to_checksum_address(settings.data_market)]),
        ],
        contract_addr=protocol_state_contract.address,
        abi=protocol_state_contract.abi,
    )
    return current_epoch_data[2]


async def fetch_single_epoch_snapshot(
    redis_conn: aioredis.Redis,
    protocol_state_contract,
    anchor_rpc_helper: RpcHelper,
    ipfs_reader: AsyncIPFSClient,
    epoch: int,
    project_id: str,
) -> Optional[Dict]:
    try:
        snapshot_response = await get_project_epoch_snapshot(
            redis_conn, protocol_state_contract, anchor_rpc_helper, 
            ipfs_reader, epoch, project_id, seek=True
        )
        
        if snapshot_response.exact_match:
            return snapshot_response.exact_match.data
        elif snapshot_response.has_closest_epochs and snapshot_response.closest_epochs.previous:
            prev_epoch = snapshot_response.closest_epochs.previous
            prev_snapshot = await get_submission_data(prev_epoch.snapshot_cid, ipfs_reader)
            return prev_snapshot if prev_snapshot else None
        else:
            return None
            
    except Exception as e:
        logger.warning(f"Failed to fetch snapshot for epoch {epoch}: {e}")
        return None


async def _fetch_snapshots_for_epochs(
    redis_conn: aioredis.Redis,
    protocol_state_contract,
    anchor_rpc_helper: RpcHelper,
    ipfs_reader: AsyncIPFSClient,
    target_epochs: List[int],
    project_id: str,
) -> List[Optional[Dict]]:
    """Fetch snapshots for target epochs concurrently with error handling."""
    

    # Fetch snapshots concurrently in batches to avoid overwhelming the system
    BATCH_SIZE = 50
    all_snapshots = []
    
    for i in range(0, len(target_epochs), BATCH_SIZE):
        batch_epochs = target_epochs[i:i + BATCH_SIZE]
        logger.debug(f"Fetching snapshots for epochs {batch_epochs[0]}-{batch_epochs[-1]}")
        
        try:
            batch_tasks = [
                fetch_single_epoch_snapshot(
                    redis_conn, protocol_state_contract, anchor_rpc_helper, 
                    ipfs_reader, epoch, project_id
                ) 
                for epoch in batch_epochs
            ]
            batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
            
            # Convert exceptions to None for consistent handling
            batch_snapshots = [
                result if not isinstance(result, Exception) else None
                for result in batch_results
            ]
            all_snapshots.extend(batch_snapshots)
            
        except Exception as e:
            logger.error(f"Batch snapshot fetch failed for epochs {batch_epochs}: {e}")
            # Add None for each epoch in the failed batch
            all_snapshots.extend([None] * len(batch_epochs))

    logger.info(f"Fetched {sum(1 for s in all_snapshots if s)} snapshots out of {len(target_epochs)} target epochs")
    return all_snapshots


async def _fetch_missing_timestamps(
    redis_conn: aioredis.Redis,
    rpc_helper: RpcHelper,
    blocks_of_interest: List[int],
    block_to_timestamp_map: Dict[int, int],
    project_id: str,
) -> Dict[int, int]:
    """Fetch missing block timestamps via RPC."""
    
    missing_blocks = [
        block for block in blocks_of_interest 
        if block not in block_to_timestamp_map
    ]
    
    if not missing_blocks:
        return {}
        
    logger.info(f"Fetching {len(missing_blocks)} missing timestamps for project {project_id}")
    
    try:
        fetched_timestamps = await fetch_block_timestamps(
            redis_conn=redis_conn,
            rpc_helper=rpc_helper,
            block_numbers=missing_blocks,
        )
        
        if fetched_timestamps:
            logger.info(f"Successfully fetched {len(fetched_timestamps)} timestamps via RPC")
            return fetched_timestamps
        else:
            logger.warning(f"RPC timestamp fetch returned no data for {len(missing_blocks)} blocks")
            return {}
            
    except Exception as e:
        logger.error(f"Failed to fetch timestamps via RPC: {e}")
        return {}


async def _fallback_fetch_block_at_timestamp(
    redis_conn: aioredis.Redis,
    anchor_rpc_helper: RpcHelper,
    rpc_helper: RpcHelper,
    protocol_state_contract,
    target_timestamp: int,
    search_type: BlockSearchType,
) -> int:
    """Common fallback logic for fetching block numbers when not found in cache."""

    BLOCK_RANGE_FOR_FALLBACK = 50

    source_chain_block_time = await get_source_chain_block_time(redis_conn, protocol_state_contract, anchor_rpc_helper)
    if not source_chain_block_time or source_chain_block_time <= 0:
        err_msg = f"Invalid source_chain_block_time for fallback."
        logger.error(err_msg)
        raise Exception(err_msg)

    latest_block_data = await rpc_helper.eth_get_block()
    if not latest_block_data:
        err_msg = f"Could not fetch valid latest block data for fallback estimation. Received: {latest_block_data}"
        logger.error(err_msg)
        raise Exception(err_msg)
 
    if not latest_block_data or not isinstance(latest_block_data, dict) or \
       'number' not in latest_block_data or 'timestamp' not in latest_block_data:
        err_msg = f"Could not fetch valid latest block data for fallback estimation. Received: {latest_block_data}"
        logger.error(err_msg)
        raise Exception(err_msg)

    current_block_number = int(latest_block_data['number'], 16)
    current_block_timestamp = int(latest_block_data['timestamp'], 16)

    timestamp_diff = current_block_timestamp - target_timestamp
    block_diff_estimate = int(timestamp_diff / source_chain_block_time)
    estimated_target_block = current_block_number - block_diff_estimate
    
    fetch_start_block = max(0, estimated_target_block - BLOCK_RANGE_FOR_FALLBACK)
    fetch_end_block = estimated_target_block + BLOCK_RANGE_FOR_FALLBACK

    blocks_to_fetch = list(range(fetch_start_block, fetch_end_block + 1))
    if not blocks_to_fetch:
        err_msg = f"No blocks to fetch in fallback range."
        logger.error(err_msg)
        raise Exception(err_msg)

    logger.info(f"Fallback for fetching block timestamps for range [{fetch_start_block}, {fetch_end_block}].")
    await fetch_block_timestamps(redis_conn, rpc_helper, blocks_to_fetch)

    block_number = await get_block_number_closest_to_timestamp(
        redis_conn=redis_conn,
        target_timestamp=target_timestamp,
        search_type=search_type,
    )
    
    if not block_number:
        err_msg = f"Fallback attempt failed to find a closest block."
        logger.error(err_msg)
        raise Exception(err_msg)
    else:
        logger.info(f"Fallback successful: Found block {block_number} after fetching block data.")
    
    return block_number