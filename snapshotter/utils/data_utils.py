import asyncio
import json
import time
import tenacity

from pydantic import BaseModel
from redis import asyncio as aioredis
from rpc_helper.rpc import RpcHelper
from tenacity import retry
from tenacity import retry_if_exception_type
from tenacity import stop_after_attempt
from tenacity import wait_random_exponential
from typing import List, Optional, Tuple, Type, Dict, Any
from web3 import Web3
from ipfs_client.main import AsyncIPFSClient

from computes.utils.models.message_models import UniswapBaseSnapshot, UniswapTradesSnapshot, TradeType
from snapshotter.utils.models.data_models import UniswapPoolMetadata, UniswapTokenPoolsSnapshot, UniswapEthPriceSnapshot, EpochSnapshotResponse, ExactEpochSnapshot, ClosestEpochs, EpochIdentifier
from snapshotter.settings.config import settings
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
from functools import lru_cache

logger = default_logger.bind(module='data_helper')
PROJECT_DATA_ENTRY_EXPIRY = 60 * 60 * 24 * 7  # 7 days in seconds
WETH = Web3.to_checksum_address('0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2')
BLOCK_SHIFT_FOR_BITMAP_INDEX = 22400000

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
    primary_identifier = project_id.split(':')[0]
    for config in projects_config:
        if config.project_name.startswith(primary_identifier):
            return config
    return None


async def get_project_finalized_cid(redis_conn: aioredis.Redis, state_contract_obj, rpc_helper, ipfs_reader, epoch_id, project_id):
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


async def get_project_last_finalized_epoch(redis_conn: aioredis.Redis, state_contract_obj, rpc_helper, project_id, force_update=False):
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
) -> List[str]:
    """
    Retrieves CIDs for multiple epochs in bulk.

    Args:
        redis_conn (aioredis.Redis): Redis connection object.
        state_contract_obj: Contract object for the state contract.
        rpc_helper (RpcHelper): Helper object for making RPC calls.
        epoch_ids (List[int]): List of epoch IDs.
        project_id (str): Project ID.

    Returns:
        List[str]: List of CIDs.
    """
    project_config = get_project_config(project_id)

    # Adjust epoch_id_min if it's less than the project's first epoch
    project_first_epoch = await get_project_first_epoch(
        redis_conn, state_contract_obj, rpc_helper, project_id,
    )

    logger.info(f'Project first epoch: {project_first_epoch}')

    if epoch_id_min < project_first_epoch:
        logger.warning(
            f'Min. Epoch ID: {epoch_id_min} is less than the project first epoch {project_first_epoch}.',
            'Cannot fetch CIDs for epochs before project first epoch.',
        )
        return None

    epoch_ids_set = set(range(epoch_id_min, epoch_id_max + 1))

    # Check Redis cache for existing CIDs
    epoch_ids_to_fetch = list(range(epoch_id_min, epoch_id_max + 1))
    logger.info(f'Fetching CIDs for {len(epoch_ids_to_fetch)} epochs for project {project_id}')
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

    existing_epochs = set([epoch_id for _, epoch_id in cid_data_with_epochs])
    missing_epochs_with_blanks = sorted(list(epoch_ids_set.difference(existing_epochs)))
    missing_epochs = []

    blank_epochs_bitmap_key = blank_epochs_bitmap(project_id)
    blank_epochs = await redis_bitmap.get_bits_in_range(redis_conn, blank_epochs_bitmap_key, missing_epochs_with_blanks)
    
    for epoch_id, is_blank in zip(missing_epochs_with_blanks, blank_epochs):
        if is_blank:
            cid_data_with_epochs.append((f'null_{epoch_id}', epoch_id))
        else:
            missing_epochs.append(epoch_id)

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
        all_cids_with_epochs = cid_data_with_epochs + missing_cids_with_epochs
        all_cids_with_epochs.sort(key=lambda x: x[1])
    else:
        all_cids_with_epochs = cid_data_with_epochs

    return [cid for cid, _ in all_cids_with_epochs]


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
    logger.trace(f'consensus status for project {project_id} and epoch {epoch_id} is {consensus_status}')

    # Extract status and CID from the ConsensusStatus struct
    status, cid, timestamp = consensus_status
    null_cid = f'null_{epoch_id}'

    # Only return without caching if the epoch is less than 10 epochs behind the current epoch
    if epoch_id > current_epoch[2] - 10 and timestamp == 0:
        logger.debug(f'Consensus status not yet available for project {project_id} and epoch {epoch_id}')
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

        blank_epochs_bitmap_key = blank_epochs_bitmap(project_id)
        missing_epochs = []
        cid_data_with_epochs = []

        blank_epochs = await redis_bitmap.get_bits_in_range(
            redis_conn,
            blank_epochs_bitmap_key,
            epoch_ids
        )

        for epoch_id, is_blank in zip(epoch_ids, blank_epochs):
            if is_blank:
                cid_data_with_epochs.append((f'null_{epoch_id}', epoch_id))
            else:
                missing_epochs.append(epoch_id)

        # Get the project hashmap key
        project_hmap_key = project_data_hmap(project_id=project_id)

        while True:
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

                for epoch_id, is_blank in zip(missing_epoch_list, blank_epochs):
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
            if project_config.cache_cids:
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
    cid_data = await get_project_finalized_cids_bulk(
        redis_conn, state_contract_obj, rpc_helper, ipfs_reader, epoch_id_min, epoch_id_max, project_id,
    )

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
) -> Optional[int]:
    """Finds the block number with the largest timestamp that is less than or equal to
    the target_timestamp using a Redis ZSET.

    The ZSET is expected to have timestamps as scores and block numbers (as strings) as values.

    Args:
        redis_conn: Async Redis connection object.
        target_timestamp: The Unix timestamp to find the closest block at or before.

    Returns:
        The block number at or immediately before the target timestamp, 
        or None if no such block is found.
    """
    key = timestamp_to_block_number_key(settings.namespace)
    
    try:
        # Find block with the largest timestamp <= target_timestamp
        result_before_raw = await redis_conn.zrevrangebyscore(
            key, 
            max=target_timestamp, 
            min='-inf', 
            start=0, 
            num=1, 
            withscores=True
        )
        
        if result_before_raw:
            block_num_bytes, ts_before_float = result_before_raw[0]
            block_number = int(block_num_bytes.decode('utf-8'))
            timestamp_of_block = int(ts_before_float)
            logger.debug(
                f"Closest block at or before {target_timestamp} is {block_number} "
                f"(ts: {timestamp_of_block}) from key {key}"
            )
            return block_number
        else:
            logger.warning(
                f"No block found at or before timestamp {target_timestamp} in ZSET {key}. "
                f"This may mean the target timestamp is too old or data is missing."
            )
            return None

    except Exception as e:
        logger.error(
            f"Error querying Redis for closest block at or before timestamp {target_timestamp} "
            f"using key {key}: {e}", exc_info=True
        )
        return None


### UNISWAP V3 SPECIFIC LOGIC ###
# TODO: consider packaging this as a separate plugin like computes since it uses compute specific logic and cache access
@lru_cache(maxsize=10000)
async def get_uniswap_v3_pool_metadata(
        pool_address: str, 
        redis_conn: aioredis.Redis, 
        anchor_rpc_helper: RpcHelper,
        ipfs_reader: AsyncIPFSClient,
        protocol_state_contract,
        
    ) -> Optional[UniswapPoolMetadata]:
    """
    Retrieves metadata for a Uniswap V3 pool from the snapshotter system.
    
    This function first checks the Redis cache for existing pool metadata. If not found,
    it fetches the latest snapshot data for the pool from the protocol state and 
    constructs the metadata object.
    
    Args:
        pool_address (str): The Ethereum address of the Uniswap V3 pool
        redis_conn (aioredis.Redis): Redis connection for caching
        anchor_rpc_helper (RpcHelper): RPC helper for blockchain interactions
        ipfs_reader (AsyncIPFSClient): IPFS client for reading snapshot data
        protocol_state_contract: Smart contract object for protocol state
        
    Returns:
        Optional[UniswapPoolMetadata]: Pool metadata object containing token information,
                                     decimals, symbols, etc. Returns None if metadata 
                                     cannot be retrieved.
                                     
    Raises:
        Exception: If there's an error fetching the latest snapshot data
    """
    # Check redis cache first for existing metadata
    project_id: str = 'metadata:{poolAddress}:{Namespace}'
    cache_key = f'pool_metadata:{pool_address}'
    cached_data = await redis_conn.get(cache_key)
    
    if cached_data:
        logger.info(f"Found cached metadata for pool {pool_address}")
        return UniswapPoolMetadata(**json.loads(cached_data))

    # If not cached, fetch from latest snapshot
    try:
        latest_snapshot = await get_project_latest_snapshot(
            redis_conn, protocol_state_contract, anchor_rpc_helper, ipfs_reader, project_id.format(poolAddress=pool_address, Namespace=settings.namespace)
        )
    except Exception as e:
        logger.opt(exception=e).error(f"Error getting latest snapshot for pool {pool_address} while processing metadata")
        return None
    return UniswapPoolMetadata(**latest_snapshot)


async def get_uniswapv3_snapshot(
    redis_conn: aioredis.Redis,
    anchor_rpc_helper: RpcHelper,
    ipfs_reader: AsyncIPFSClient,
    protocol_state_contract,
    project_id: str,
    message_model: Type[BaseModel],
    block_number: Optional[int] = None,
) -> Optional[Tuple[int, BaseModel]]:
    """
    Retrieves a Uniswap V3 snapshot for a given project and optionally a specific block number.
    
    This function handles the logic of determining the target epoch based on whether a block_number
    is provided or not. If no block_number is given, it uses the last finalized epoch. If a 
    block_number is provided, it seeks around that epoch to find the closest available data.
    
    Args:
        redis_conn (aioredis.Redis): Redis connection for data access
        anchor_rpc_helper (RpcHelper): RPC helper for blockchain interactions  
        ipfs_reader (AsyncIPFSClient): IPFS client for reading snapshot data
        protocol_state_contract: Smart contract object for protocol state
        project_id (str): The project identifier for the snapshot
        message_model (Type[BaseModel]): Pydantic model class to parse the snapshot data
        block_number (Optional[int]): Specific block number to target, if None uses latest
        
    Returns:
        Optional[Tuple[int, BaseModel]]: Tuple of (epoch_id, parsed_snapshot) if found,
                                        None if no valid snapshot data is available
                                        
    Note:
        When block_number is provided, the function assumes epoch equals block number
        in the data market contract configuration.
    """
    # Determine target epoch based on input parameters
    seek = False
    if not block_number:
        # Use last submitted or finalized epoch when no specific block requested
        last_submitted_snapshot_data = await get_last_submitted_snapshot_data(redis_conn, project_id)
        if last_submitted_snapshot_data:
            target_epoch = last_submitted_snapshot_data['epochId']
        else:
            target_epoch = await get_project_last_finalized_epoch(
                redis_conn=redis_conn,
                state_contract_obj=protocol_state_contract,
                rpc_helper=anchor_rpc_helper,
                project_id=project_id,
            )

        if not target_epoch:
            logger.error(f"No last finalized epoch found for project {project_id}")
            return None
        else:
            logger.info(f"Using epoch {target_epoch} for fetch against project {project_id}")
    # if block_number is provided, use that as the target epoch and seek around it if needed
    else:
        # TODO: assumes epoch is set to block number in data market contract, may need to add config flag for this and derive epoch from block number if false
        target_epoch = block_number
        seek = True
        
    # Fetch snapshot data for the determined epoch
    snapshot_response = await get_project_epoch_snapshot(
        redis_conn=redis_conn,
        state_contract_obj=protocol_state_contract,
        rpc_helper=anchor_rpc_helper,
        ipfs_reader=ipfs_reader,
        epoch_id=target_epoch,
        project_id=project_id,
        seek=seek
    )
    
    # Process exact match response
    if snapshot_response.exact_match:
        try:
            parsed_snapshot = message_model(**snapshot_response.exact_match.data)
            return target_epoch, parsed_snapshot
        except Exception as e:
            logger.error(f"Failed to parse snapshot data for project {project_id} against epoch {target_epoch}: {e}")
            return None
    else:
        # Handle case when exact match not found but nearby epochs available
        if snapshot_response.has_closest_epochs:
            logger.info(f"No exact match found for project {project_id} against epoch {target_epoch}, but nearby epochs found: {snapshot_response.closest_epochs}") 
            
            # Try previous epoch first
            previous_epoch = snapshot_response.closest_epochs.previous        
            if previous_epoch:
                logger.info(f"Fetching previous epoch {previous_epoch} CID for project {project_id} against actual sought epoch {target_epoch}")
                target_epoch = previous_epoch.epoch_id
                snapshot_response = await get_submission_data(
                    cid=previous_epoch.snapshot_cid,
                    ipfs_reader=ipfs_reader,
                )
                if snapshot_response:
                    parsed_snapshot = message_model(**snapshot_response)
                    return target_epoch, parsed_snapshot
                else:
                    logger.error(f"No snapshot data found for project {project_id} against nearby epoch {previous_epoch.epoch_id} with CID {previous_epoch.snapshot_cid}")
                    return None
                    
            # Fallback to next epoch if previous not available        
            next_epoch = snapshot_response.closest_epochs.next
            if next_epoch:
                logger.info(f"Fetching next epoch {next_epoch} CID for project {project_id} against actual sought epoch {target_epoch}")
                target_epoch = next_epoch.epoch_id
                snapshot_response = await get_submission_data(
                    cid=next_epoch.snapshot_cid,
                    ipfs_reader=ipfs_reader,
                )
                if snapshot_response:
                    parsed_snapshot = message_model(**snapshot_response)
                    return target_epoch, parsed_snapshot
                else:
                    logger.error(f"No snapshot data found for project {project_id} against nearby epoch {next_epoch.epoch_id} with CID {next_epoch.snapshot_cid}")
                    return None
        return None


async def get_uniswap_v3_token_pools_snapshot(
    redis_conn: aioredis.Redis,
    anchor_rpc_helper: RpcHelper,
    ipfs_reader: AsyncIPFSClient,
    protocol_state_contract,
    token_address: str,
):
    """
    Retrieves the token pools snapshot for a specific token on Uniswap V3.
    
    This function fetches snapshot data that contains information about all pools
    that include the specified token address.
    
    Args:
        redis_conn (aioredis.Redis): Redis connection for data access
        anchor_rpc_helper (RpcHelper): RPC helper for blockchain interactions
        ipfs_reader (AsyncIPFSClient): IPFS client for reading snapshot data
        protocol_state_contract: Smart contract object for protocol state
        token_address (str): Ethereum address of the token to get pools for
        
    Returns:
        Optional[UniswapTokenPoolsSnapshot]: Snapshot containing pool information
                                           for the token, or None if not found
    """
    token_address = Web3.to_checksum_address(token_address)
    project_id = f"tokenPools:{token_address}:{settings.namespace}"
    result = await get_uniswapv3_snapshot(
        redis_conn=redis_conn,
        anchor_rpc_helper=anchor_rpc_helper,
        ipfs_reader=ipfs_reader,
        protocol_state_contract=protocol_state_contract,
        project_id=project_id,
        message_model=UniswapTokenPoolsSnapshot,
    )
    if not result:
        logger.error(f"No snapshot data found for project {project_id}")
        return None
        
    snapshot_epoch, snapshot_data = result
    if snapshot_data:
        return snapshot_data
    else:
        return None


async def get_uniswap_v3_base_snapshots_for_token(
    redis_conn: aioredis.Redis,
    anchor_rpc_helper: RpcHelper,
    ipfs_reader: AsyncIPFSClient,
    protocol_state_contract,
    token_address: str,

):
    """
    Retrieves base snapshots for all pools containing a specific token.
    
    This function first gets the list of pools for a token, then fetches
    the base snapshot data for each of those pools.
    
    Args:
        redis_conn (aioredis.Redis): Redis connection for data access
        anchor_rpc_helper (RpcHelper): RPC helper for blockchain interactions
        ipfs_reader (AsyncIPFSClient): IPFS client for reading snapshot data
        protocol_state_contract: Smart contract object for protocol state
        token_address (str): Ethereum address of the token
        
    Returns:
        Optional[Dict[str, UniswapBaseSnapshot]]: Dictionary mapping pool addresses
                                                to their base snapshots, or None if
                                                no pools found for the token
    """
    token_pools = await get_uniswap_v3_token_pools_snapshot(
        redis_conn=redis_conn,
        anchor_rpc_helper=anchor_rpc_helper,
        ipfs_reader=ipfs_reader,
        protocol_state_contract=protocol_state_contract,
        token_address=token_address,
    )
    if not token_pools:
        logger.error(f"No token pools found for token {token_address}")
        return None
    data = {}
    for pool in token_pools.pools:
        base_snapshot = await get_uniswap_v3_base_snapshot(
            redis_conn=redis_conn,
            anchor_rpc_helper=anchor_rpc_helper,
            ipfs_reader=ipfs_reader,
            protocol_state_contract=protocol_state_contract,
            pool_address=pool,
        )
        if not base_snapshot:
            logger.error(f"No base snapshot found for pool {pool}")
            continue
        data[pool] = base_snapshot
    return data


async def get_uniswap_v3_base_snapshot(
    redis_conn: aioredis.Redis,
    anchor_rpc_helper: RpcHelper,
    ipfs_reader: AsyncIPFSClient,
    protocol_state_contract,
    pool_address: str,
    block_number: Optional[int] = None,
):
    """
    Retrieves the base snapshot for a specific Uniswap V3 pool.
    
    Base snapshots contain fundamental pool data including token information,
    liquidity, pricing data, and other core metrics.
    
    Args:
        redis_conn (aioredis.Redis): Redis connection for data access
        anchor_rpc_helper (RpcHelper): RPC helper for blockchain interactions
        ipfs_reader (AsyncIPFSClient): IPFS client for reading snapshot data
        protocol_state_contract: Smart contract object for protocol state
        pool_address (str): Ethereum address of the pool
        block_number (Optional[int]): Specific block to target, uses latest if None
        
    Returns:
        Optional[UniswapBaseSnapshot]: Base snapshot data for the pool,
                                     or None if not found
    """
    project_id = f"baseSnapshot:{pool_address}:{settings.namespace}"
    result = await get_uniswapv3_snapshot(
        redis_conn=redis_conn,
        anchor_rpc_helper=anchor_rpc_helper,
        ipfs_reader=ipfs_reader,
        protocol_state_contract=protocol_state_contract,
        project_id=project_id,
        message_model=UniswapBaseSnapshot,
        block_number=block_number,
    )
    if not result:
        logger.error(f"No snapshot data found for project {project_id}")
        return None
        
    snapshot_epoch, snapshot_data = result
    return snapshot_data


async def get_uniswap_v3_trades_snapshot(
    redis_conn: aioredis.Redis,
    anchor_rpc_helper: RpcHelper,
    ipfs_reader: AsyncIPFSClient,
    protocol_state_contract,
    pool_address: str,
    block_number: Optional[int] = None,
):
    """
    Retrieves the trades snapshot for a specific Uniswap V3 pool.
    
    Trade snapshots contain information about individual trades/swaps
    that occurred in the pool during the snapshot period.
    
    Args:
        redis_conn (aioredis.Redis): Redis connection for data access
        anchor_rpc_helper (RpcHelper): RPC helper for blockchain interactions
        ipfs_reader (AsyncIPFSClient): IPFS client for reading snapshot data
        protocol_state_contract: Smart contract object for protocol state
        pool_address (str): Ethereum address of the pool
        block_number (Optional[int]): Specific block to target, uses latest if None
        
    Returns:
        Optional[UniswapTradesSnapshot]: Trades snapshot data for the pool,
                                       or None if not found
    """
    project_id = f"tradesSnapshot:{pool_address}:{settings.namespace}"
    result = await get_uniswapv3_snapshot(
        redis_conn=redis_conn,
        anchor_rpc_helper=anchor_rpc_helper,
        ipfs_reader=ipfs_reader,
        protocol_state_contract=protocol_state_contract,
        project_id=project_id,
        message_model=UniswapTradesSnapshot,
        block_number=block_number,
    )
    if not result:
        logger.error(f"No snapshot data found for project {project_id}")
        return None
        
    snapshot_epoch, snapshot_data = result
    return snapshot_data



async def get_uniswap_v3_eth_price_snapshot(
    redis_conn: aioredis.Redis,
    anchor_rpc_helper: RpcHelper,
    ipfs_reader: AsyncIPFSClient,
    protocol_state_contract,
    block_number: Optional[int] = None,
):
    """
    Retrieves the ETH price snapshot from the Uniswap V3 ecosystem.
    
    This function fetches the latest ETH price data as captured by the
    snapshotter system from various Uniswap V3 pools.
    
    Args:
        redis_conn (aioredis.Redis): Redis connection for data access
        anchor_rpc_helper (RpcHelper): RPC helper for blockchain interactions
        ipfs_reader (AsyncIPFSClient): IPFS client for reading snapshot data
        protocol_state_contract: Smart contract object for protocol state
        block_number (Optional[int]): Specific block to target, uses latest if None
        
    Returns:
        Optional[UniswapEthPriceSnapshot]: ETH price snapshot data,
                                         or None if not found
    """
    project_id = f'price:ETH:{settings.namespace}'
    result = await get_uniswapv3_snapshot(
        redis_conn=redis_conn,
        anchor_rpc_helper=anchor_rpc_helper,
        ipfs_reader=ipfs_reader,
        protocol_state_contract=protocol_state_contract,
        project_id=project_id,
        message_model=UniswapEthPriceSnapshot,
        block_number=block_number,
    )
    if not result:
        logger.error(f"No snapshot data found for project {project_id}")
        return None
        
    snapshot_epoch, snapshot_data = result
    if snapshot_data:
        return snapshot_data
    else:
        return None


async def get_uniswap_v3_token_price_pool(
    redis_conn: aioredis.Redis,
    anchor_rpc_helper: RpcHelper,
    ipfs_reader: AsyncIPFSClient,
    protocol_state_contract,
    token_address: str,
    pool_address: str,
    block_number: Optional[int] = None,
):
    """
    Retrieves the USD price of a specific token from a specific pool.
    
    This function fetches the base snapshot for a pool and extracts the USD price
    for the specified token. It determines if the token is token0 or token1 in
    the pool and returns the appropriate price data.
    
    Args:
        redis_conn (aioredis.Redis): Redis connection for data access
        anchor_rpc_helper (RpcHelper): RPC helper for blockchain interactions
        ipfs_reader (AsyncIPFSClient): IPFS client for reading snapshot data
        protocol_state_contract: Smart contract object for protocol state
        token_address (str): Ethereum address of the token to get price for
        pool_address (str): Ethereum address of the pool to get price from
        block_number (Optional[int]): Specific block to target, uses latest if None
        
    Returns:
        Optional[float]: USD price of the token in the specified pool,
                        or None if token not found in pool or data unavailable
                        
    Note:
        Assumes snapshot_epoch corresponds to the block number when accessing
        price data from the snapshot.
    """
    base_project_id = f"baseSnapshot:{pool_address}:{settings.namespace}"

    result = await get_uniswapv3_snapshot(
        redis_conn=redis_conn,
        anchor_rpc_helper=anchor_rpc_helper,
        ipfs_reader=ipfs_reader,
        protocol_state_contract=protocol_state_contract,
        project_id=base_project_id,
        message_model=UniswapBaseSnapshot,
        block_number=block_number,
    )
    if not result:
        logger.error(f"No snapshot data found for project {base_project_id}")
        return None
        
    snapshot_epoch, snapshot_data = result
    if not snapshot_data:
        logger.error(f"No base snapshot data found for project {base_project_id} against epoch {snapshot_epoch}")
        return None

    # Determine which token in the pair and extract its price
    if Web3.to_checksum_address(token_address) == snapshot_data.token0:
        # NOTE: assumes snapshot_epoch is the block number
        token_price = snapshot_data.token0PricesUSD[snapshot_epoch]
    elif Web3.to_checksum_address(token_address) == snapshot_data.token1:
        token_price = snapshot_data.token1PricesUSD[snapshot_epoch]
    else:
        logger.error(f"Token address {token_address} not found in base snapshot data for project {base_project_id} against epoch {snapshot_epoch}")
        return None
    
    return token_price


async def get_uniswap_v3_token_prices_all_snapshot(
    redis_conn: aioredis.Redis,
    anchor_rpc_helper: RpcHelper,
    ipfs_reader: AsyncIPFSClient,
    protocol_state_contract,
    token_address: str,
    block_number: Optional[int] = None,
):
    """
    Get token prices from all pools for a given token address.
    Uses batch processing with asyncio tasks to fetch prices concurrently.
    Returns a dict mapping pool addresses to their respective token prices.
    """
    logger.info(f"Getting pool addresses for token {token_address}")
    token_pools_snapshot_result = await get_uniswap_v3_token_pools_snapshot(
        redis_conn=redis_conn,
        anchor_rpc_helper=anchor_rpc_helper,
        ipfs_reader=ipfs_reader,
        protocol_state_contract=protocol_state_contract,
        token_address=token_address,
    )
    if not token_pools_snapshot_result:
        logger.error(f"No token pools snapshot found for token {token_address}")
        return None
    
    if not token_pools_snapshot_result or not token_pools_snapshot_result.pools:
        logger.error(f"No token pools snapshot data found for token {token_address}")
        return None
    
    logger.info(f"Token pools snapshot result against token {token_address}: {token_pools_snapshot_result}")
    # Get list of pool addresses
    pool_addresses = list(token_pools_snapshot_result.pools.keys())
    if not pool_addresses:
        logger.error(f"No pools found for token {token_address}")
        return None

    # Process pools in batches of 20
    BATCH_SIZE = 20
    results = {}
    
    for i in range(0, len(pool_addresses), BATCH_SIZE):
        batch_pools = pool_addresses[i:i + BATCH_SIZE]
        logger.info(f"Processing batch of {len(batch_pools)} pools against token {token_address} for prices: {batch_pools}")
        # Create tasks for each pool in the batch
        tasks = [
            get_uniswap_v3_token_price_pool(
                redis_conn=redis_conn,
                anchor_rpc_helper=anchor_rpc_helper,
                ipfs_reader=ipfs_reader,
                protocol_state_contract=protocol_state_contract,
                token_address=token_address,
                pool_address=pool_address,
                block_number=block_number,
            )
            for pool_address in batch_pools
        ]
        
        try:
            # Execute batch of tasks concurrently
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Process results
            for pool_address, result in zip(batch_pools, batch_results):
                if isinstance(result, Exception):
                    logger.error(f"Error getting price for pool {pool_address}: {str(result)}")
                    results[pool_address] = None
                else:
                    results[pool_address] = result
                    
        except Exception as e:
            logger.error(f"Error processing batch of pools: {str(e)}")
            # Continue with next batch even if current batch fails
    
    return results


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


async def get_uniswap_trade_volume_agg(
    redis_conn: aioredis.Redis,
    anchor_rpc_helper: RpcHelper,
    ipfs_reader: AsyncIPFSClient,
    protocol_state_contract,
    time_interval: int,
    project_id: str,
):
    """
    Calculates aggregated trade volume for a project over a specified time interval.
    
    This function fetches all snapshots within the time interval and sums up
    the total trade volume across all epochs.
    
    Args:
        redis_conn (aioredis.Redis): Redis connection for data access
        anchor_rpc_helper (RpcHelper): RPC helper for blockchain interactions
        ipfs_reader (AsyncIPFSClient): IPFS client for reading snapshot data
        protocol_state_contract: Smart contract object for protocol state
        time_interval (int): Time interval in seconds to aggregate over
        project_id (str): Project identifier for the data
        
    Returns:
        Dict[str, Union[int, float]]: Dictionary containing totalTradeVolume
                                    and timeInterval values
    """
    current_epoch = await get_current_epoch_id(anchor_rpc_helper, protocol_state_contract)

    tail_epoch_id, _ = await get_tail_epoch_id(
        redis_conn, protocol_state_contract, anchor_rpc_helper, current_epoch, time_interval, project_id,
    )

    snapshots = await get_project_epoch_snapshot_bulk(
        redis_conn, protocol_state_contract, anchor_rpc_helper, ipfs_reader, tail_epoch_id, current_epoch, project_id,
    )
    total_trade_volume = 0
    for snapshot in snapshots:
        if snapshot:
            total_trade_volume += snapshot['totalTrade']
    return {
        'totalTradeVolume': total_trade_volume,
        'timeInterval': time_interval,
    }


async def get_active_pools(
    redis_conn: aioredis.Redis,
    anchor_rpc_helper: RpcHelper,
    ipfs_reader: AsyncIPFSClient,
    protocol_state_contract,
    time_interval: int,
):
    """
    Retrieves the most active pools over a specified time interval.
    
    This function aggregates pool activity data from snapshots and returns
    pools sorted by their activity frequency.
    
    Args:
        redis_conn (aioredis.Redis): Redis connection for data access
        anchor_rpc_helper (RpcHelper): RPC helper for blockchain interactions
        ipfs_reader (AsyncIPFSClient): IPFS client for reading snapshot data
        protocol_state_contract: Smart contract object for protocol state
        time_interval (int): Time interval in seconds to analyze
        
    Returns:
        List[Tuple[str, int]]: List of tuples (pool_address, frequency)
                              sorted by frequency in descending order
    """
    # check if data is already in redis
    active_pool_data = await redis_conn.get(f"active_pool_data:{settings.namespace}")
    if active_pool_data:
        return json.loads(active_pool_data)
    
    project_id = f"activePools:{settings.namespace}"
    current_epoch = await get_current_epoch_id(anchor_rpc_helper, protocol_state_contract)

    tail_epoch_id, _ = await get_tail_epoch_id(
        redis_conn, protocol_state_contract, anchor_rpc_helper, current_epoch, time_interval, project_id,
    )

    snapshots = await get_project_epoch_snapshot_bulk(
        redis_conn, protocol_state_contract, anchor_rpc_helper, ipfs_reader, tail_epoch_id, current_epoch, project_id,
    )
    active_pools = {}
    for snapshot in snapshots:
        if snapshot:
            for pool_address, frequency in snapshot['pools'].items():
                if pool_address not in active_pools:
                    active_pools[pool_address] = 0
                active_pools[pool_address] += frequency
    active_pool_data = [(pool_address, frequency) for pool_address, frequency in active_pools.items()]
    active_pool_data.sort(key=lambda x: x[1], reverse=True)
    # set in redis with 1 min expiry
    await redis_conn.set(f"active_pool_data:{settings.namespace}", json.dumps(active_pool_data), ex=300)
    return active_pool_data


async def get_active_tokens(
    redis_conn: aioredis.Redis,
    anchor_rpc_helper: RpcHelper,
    ipfs_reader: AsyncIPFSClient,
    protocol_state_contract,
    time_interval: int,
):
    """
    Retrieves the most active tokens over a specified time interval.
    
    This function aggregates token activity data from snapshots and returns
    tokens sorted by their activity frequency.
    
    Args:
        redis_conn (aioredis.Redis): Redis connection for data access
        anchor_rpc_helper (RpcHelper): RPC helper for blockchain interactions
        ipfs_reader (AsyncIPFSClient): IPFS client for reading snapshot data
        protocol_state_contract: Smart contract object for protocol state
        time_interval (int): Time interval in seconds to analyze
        
    Returns:
        List[Tuple[str, int]]: List of tuples (token_address, frequency)
                              sorted by frequency in descending order
    """
    # check if data is already in redis
    active_token_data = await redis_conn.get(f"active_token_data:{settings.namespace}")
    if active_token_data:
        return json.loads(active_token_data)
    
    project_id = f"activeTokens:{settings.namespace}"
    current_epoch = await get_current_epoch_id(anchor_rpc_helper, protocol_state_contract)

    tail_epoch_id, _ = await get_tail_epoch_id(
        redis_conn, protocol_state_contract, anchor_rpc_helper, current_epoch, time_interval, project_id,
    )
    
    snapshots = await get_project_epoch_snapshot_bulk(
        redis_conn, protocol_state_contract, anchor_rpc_helper, ipfs_reader, tail_epoch_id, current_epoch, project_id,
    )
    active_tokens = {}
    for snapshot in snapshots:
        if snapshot:
            for token_address, frequency in snapshot['tokens'].items():
                if token_address not in active_tokens:
                    active_tokens[token_address] = 0
                active_tokens[token_address] += frequency
    active_token_data = [(token_address, frequency) for token_address, frequency in active_tokens.items()]
    active_token_data.sort(key=lambda x: x[1], reverse=True)
    # set in redis with 1 min expiry
    await redis_conn.set(f"active_token_data:{settings.namespace}", json.dumps(active_token_data), ex=300
    return active_token_data


async def get_uniswap_trade_volume_agg_all_pools(
    redis_conn: aioredis.Redis,
    anchor_rpc_helper: RpcHelper,
    ipfs_reader: AsyncIPFSClient,
    protocol_state_contract,
    time_interval: int,
    token_address: str,
):
    """
    Calculates aggregated trade volume across all pools containing a specific token.
    
    This function first finds all pools that contain the specified token, then
    aggregates trade volume data from all those pools over the time interval.
    
    Args:
        redis_conn (aioredis.Redis): Redis connection for data access
        anchor_rpc_helper (RpcHelper): RPC helper for blockchain interactions
        ipfs_reader (AsyncIPFSClient): IPFS client for reading snapshot data
        protocol_state_contract: Smart contract object for protocol state
        time_interval (int): Time interval in seconds to aggregate over
        token_address (str): Ethereum address of the token to analyze
        
    Returns:
        Optional[Dict[str, Union[int, float]]]: Dictionary containing cumulative
                                              totalTradeVolume and timeInterval,
                                              or None if no pools found
    """
    token_address = Web3.to_checksum_address(token_address)
    token_pools = await get_uniswap_v3_token_pools_snapshot(
        redis_conn=redis_conn,
        anchor_rpc_helper=anchor_rpc_helper,
        ipfs_reader=ipfs_reader,
        protocol_state_contract=protocol_state_contract,
        token_address=token_address,
    )

    tasks = []
    if not token_pools:
        logger.error(f"No token pools found for token {token_address}")
        return None
    
    for pool in token_pools.pools:
        tasks.append(get_uniswap_trade_volume_agg(
            redis_conn=redis_conn,
            anchor_rpc_helper=anchor_rpc_helper,
            ipfs_reader=ipfs_reader,
            protocol_state_contract=protocol_state_contract,
            time_interval=time_interval,
            project_id=f"baseSnapshot:{pool}:{settings.namespace}",
        ))
    results = await asyncio.gather(*tasks)
    
    cumulative_trade = {
        'totalTradeVolume': 0,
        'timeInterval': time_interval,
    }
    for data in results:
        cumulative_trade['totalTradeVolume'] += data['totalTradeVolume']

    return cumulative_trade


async def get_uniswap_price_series_agg(
    redis_conn: aioredis.Redis,
    rpc_helper: RpcHelper,
    anchor_rpc_helper: RpcHelper,
    ipfs_reader: AsyncIPFSClient,
    protocol_state_contract,
    time_interval: int,
    project_id: str,
    token_address: str,
    step_seconds: int,
):
    """
    Generates a time series of token prices over a specified interval with regular spacing.
    
    This complex function retrieves price data for a token from snapshots, handles missing
    timestamps by fetching from RPC, and creates a time series with evenly spaced intervals.
    It includes sophisticated logic for handling gaps in data and fallback mechanisms.
    
    Args:
        redis_conn (aioredis.Redis): Redis connection for data access and caching
        rpc_helper (RpcHelper): RPC helper for fetching block timestamps from blockchain
        anchor_rpc_helper (RpcHelper): Additional RPC helper for protocol interactions
        ipfs_reader (AsyncIPFSClient): IPFS client for reading snapshot data
        protocol_state_contract: Smart contract object for protocol state
        time_interval (int): Total time interval in seconds to analyze
        project_id (str): Project identifier for the price data
        token_address (str): Ethereum address of the token to get prices for
        step_seconds (int): Time spacing in seconds between price data points
        
    Returns:
        Dict[str, Any]: Dictionary containing:
                       - priceSeries: List of price entries with blockNumber, price, timestamp
                       - timeInterval: The time interval used for the analysis
                       
    Raises:
        ValueError: If target token price key cannot be determined or no price data available
        Exception: If snapshot data cannot be retrieved for required epochs
        
    Note:
        This function implements complex logic for handling missing price data by using
        the last known price for gaps and includes fallback mechanisms for timestamp
        resolution when Redis cache misses occur.
    """
    # Get current epoch for determining data range
    [current_epoch_data] = await anchor_rpc_helper.web3_call(
        tasks=[
            ('currentEpoch', [Web3.to_checksum_address(settings.data_market)]),
        ],
        contract_addr=protocol_state_contract.address,
        abi=protocol_state_contract.abi,
    )

    current_epoch = current_epoch_data[2]

    # Calculate tail epoch for the time interval
    tail_epoch_id, _ = await get_tail_epoch_id(
        redis_conn, protocol_state_contract, anchor_rpc_helper, current_epoch, time_interval, project_id,
    )

    # Initialize block-to-timestamp mapping from Redis cache
    block_to_timestamp_map = {}
    if tail_epoch_id <= current_epoch:
        try:
            timestamp_data_with_scores = await redis_conn.zrangebyscore(
                block_number_to_timestamp_key(settings.namespace),
                min=tail_epoch_id,
                max=current_epoch,
                withscores=True
            )
            
            # Process cached timestamp data
            for json_timestamp_str, block_num_score in timestamp_data_with_scores:
                if json_timestamp_str:
                    try:
                        block_num = int(block_num_score) 
                        timestamp = json.loads(json_timestamp_str) 
                        if isinstance(timestamp, int):
                            block_to_timestamp_map[block_num] = timestamp
                        else:
                            logger.warning(
                                f"Decoded timestamp for block {block_num} is not an int: {timestamp} "
                                f"(type: {type(timestamp)}) from key {block_number_to_timestamp_key(settings.namespace)}"
                            )
                    except json.JSONDecodeError as jde:
                        logger.warning(
                            f"Failed to decode JSON for timestamp data: {json_timestamp_str}. "
                            f"Block score: {block_num_score}. Error: {jde}. Key: {block_number_to_timestamp_key(settings.namespace)}"
                        )
                    except (TypeError, ValueError) as e:
                        logger.warning(
                            f"TypeError or ValueError during processing of cached timestamp data: {json_timestamp_str}. "
                            f"Block score: {block_num_score}. Error: {e}. Key: {block_number_to_timestamp_key(settings.namespace)}"
                        )
        except Exception as e:
            logger.opt(exception=True).error(
                f"Error fetching block timestamps from Redis for key {block_number_to_timestamp_key(settings.namespace)} "
                f"in range {tail_epoch_id}-{current_epoch}: {e}"
            )


    # Fetch all snapshots for the epoch range
    snapshots = await get_project_epoch_snapshot_bulk(
        redis_conn, protocol_state_contract, anchor_rpc_helper, ipfs_reader, tail_epoch_id, current_epoch, project_id,
    )
    
    # Initialize price data processing variables
    price_data = []
    target_token_address = Web3.to_checksum_address(token_address)
    target_token_price_key = None
    snapshot_prices_map = {}
    tail_epoch_covered_by_bulk = False
    snapshot_at_tail = None

    # Determine which price key to use (token0PricesUSD or token1PricesUSD)
    if snapshots:
        for snapshot_data_for_key_check in snapshots:
            if snapshot_data_for_key_check: 
                token0 = snapshot_data_for_key_check.get('token0')
                token1 = snapshot_data_for_key_check.get('token1')
                if target_token_address == token0:
                    target_token_price_key = 'token0PricesUSD'
                    break
                elif target_token_address == token1:
                    target_token_price_key = 'token1PricesUSD'
                    break
        
        # Extract price data from snapshots if price key is determined
        if target_token_price_key:
            for snapshot in snapshots: 
                if not snapshot:
                    continue
                prices_for_current_snapshot = snapshot.get(target_token_price_key)
                if isinstance(prices_for_current_snapshot, dict):
                    for block_num_str, price_val in prices_for_current_snapshot.items():
                        try:
                            block_num = int(block_num_str)
                            price = float(price_val)
                            snapshot_prices_map[block_num] = price
                            if block_num == tail_epoch_id:
                                tail_epoch_covered_by_bulk = True
                        except ValueError:
                            logger.warning(
                                f"Could not parse block number '{block_num_str}' or price '{price_val}' "
                                f"from bulk snapshot for project {project_id}. Key: {target_token_price_key}"
                            )
                elif prices_for_current_snapshot is not None:
                    logger.warning(
                        f"Price data for {target_token_price_key} in bulk snapshot for project {project_id} "
                        f"is not a dictionary: {prices_for_current_snapshot}"
                    )

            # Handle case where tail epoch is not covered by bulk snapshots
            if not tail_epoch_covered_by_bulk:
                logger.info(
                    f"Tail epoch {tail_epoch_id} not covered by bulk or price key undetermined. "
                    f"Fetching snapshot_at_tail for project {project_id}."
                )
                snapshot_response = await get_project_epoch_snapshot(
                    redis_conn=redis_conn, 
                    state_contract_obj=protocol_state_contract, 
                    rpc_helper=anchor_rpc_helper, 
                    ipfs_reader=ipfs_reader, 
                    epoch_id=tail_epoch_id, 
                    project_id=project_id, 
                    seek=True,
                )

                logger.info(f"Snapshot response for project {project_id} at tail_epoch_id {tail_epoch_id}: {snapshot_response.model_dump_json()}")
                
                if snapshot_response.exact_match:
                    # This shouldn't happen, but just in case
                    snapshot_at_tail = snapshot_response.exact_match.data
                elif snapshot_response.has_closest_epochs:
                    previous_epoch = snapshot_response.closest_epochs.previous
                    if previous_epoch:
                        snapshot_at_tail = await get_submission_data(
                            cid=previous_epoch.snapshot_cid, 
                            ipfs_reader=ipfs_reader,
                        )
                        logger.info(f"Closest snapshot at tail for project {project_id} at tail_epoch_id {tail_epoch_id} is {previous_epoch.epoch_id}.")
                    else:
                        logger.error(f"No previous closest epoch found for project {project_id} at tail_epoch_id {tail_epoch_id}.")
                        raise Exception(f"No previous closest epoch found for project {project_id} at tail_epoch_id {tail_epoch_id}.")
                else:
                    logger.error(f"No snapshot data found for project {project_id} at tail_epoch_id {tail_epoch_id}.")
                    raise Exception(f"No snapshot data found for project {project_id} at tail_epoch_id {tail_epoch_id}.")
        else:
            msg = f"Unable to determine target token price key for project {project_id} and token {token_address}."
            logger.error(msg)
            raise ValueError(msg)

    # Initialize last known price from tail snapshot if needed
    last_known_price = None
    if snapshot_at_tail and target_token_price_key and not tail_epoch_covered_by_bulk:
        prices_from_tail_snapshot = snapshot_at_tail.get(target_token_price_key)
        if isinstance(prices_from_tail_snapshot, dict):
            latest_relevant_block_num_in_tail = -1
            for block_num_str in prices_from_tail_snapshot.keys():
                try:
                    block_num = int(block_num_str)
                    if block_num <= tail_epoch_id and block_num > latest_relevant_block_num_in_tail:
                        latest_relevant_block_num_in_tail = block_num
                except ValueError:
                    logger.warning(f"Could not parse block_num_str '{block_num_str}' from snapshot_at_tail for project {project_id}.")
                    continue
            
            # Extract price from the most relevant block
            if latest_relevant_block_num_in_tail == -1:
                logger.warning(f"No relevant block (<= tail_epoch_id) with a valid price found in snapshot_at_tail for project {project_id}.")
            else:
                price_value = prices_from_tail_snapshot.get(str(latest_relevant_block_num_in_tail))  # Keys are strings
                if price_value is not None:
                    try:
                        last_known_price = float(price_value)
                        logger.info(
                            f"Initialized last_known_price to {last_known_price} from block {latest_relevant_block_num_in_tail} "
                            f"in snapshot_at_tail for project {project_id} (tail_epoch_id {tail_epoch_id})."
                        )
                    except (ValueError, TypeError):
                        logger.warning(
                            f"Could not convert price '{price_value}' to float for block {latest_relevant_block_num_in_tail} "
                            f"from snapshot_at_tail for project {project_id}."
                        )
        
        # Validate that we have initial price data
        if last_known_price is None:
            msg = f"No last known price data available for project {project_id} and token {token_address}."
            logger.error(msg)
            raise ValueError(msg)

    price_data = []
    
    # Generate list of all block numbers in the range
    all_block_numbers_in_range = []
    if tail_epoch_id <= current_epoch:
        all_block_numbers_in_range = list(range(tail_epoch_id, current_epoch + 1))

    # Identify blocks missing timestamp data
    block_timestamps_to_fetch_rpc = []
    if all_block_numbers_in_range:
        for block_num in all_block_numbers_in_range:
            if block_num not in block_to_timestamp_map:
                block_timestamps_to_fetch_rpc.append(block_num)

    # Fetch missing timestamps via RPC if needed
    if block_timestamps_to_fetch_rpc:
        logger.info(
            f"Timestamps for {len(block_timestamps_to_fetch_rpc)} blocks (e.g., {block_timestamps_to_fetch_rpc[:5]}{'...' if len(block_timestamps_to_fetch_rpc) > 5 else ''}) "
            f"not found in initial Redis cache for project {project_id}. Attempting to fetch from RPC."
        )
        fetched_timestamps_from_rpc = await fetch_block_timestamps(
            redis_conn=redis_conn,
            rpc_helper=rpc_helper,
            block_numbers=block_timestamps_to_fetch_rpc,
        )
        if fetched_timestamps_from_rpc:
            block_to_timestamp_map.update(fetched_timestamps_from_rpc)
        else:
            logger.warning(f"fetch_block_timestamps returned no data for {len(block_timestamps_to_fetch_rpc)} blocks for project {project_id}.")

    # Build price data series for all blocks
    if all_block_numbers_in_range:
        for current_block_num in all_block_numbers_in_range:
            current_timestamp = block_to_timestamp_map.get(current_block_num, None)

            if current_timestamp is None:
                # Skip blocks without timestamp data
                # logger.warning(f"Unable to determine timestamp for block {current_block_num} in project {project_id}, even after RPC attempt. Skipping this block in price series.")
                continue

            # Use snapshot price if available, otherwise use last known price
            price_from_bulk = snapshot_prices_map.get(current_block_num)
            if price_from_bulk is not None:
                last_known_price = price_from_bulk 
                price_data.append({
                    'blockNumber': current_block_num,
                    'price': price_from_bulk,
                    'timestamp': current_timestamp,
                })
            elif last_known_price is not None: 
                price_data.append({
                    'blockNumber': current_block_num,
                    'price': last_known_price,
                    'timestamp': current_timestamp,
                })
            else:
                # Log warning for blocks without any price data
                logger.warning(
                    f"No price data available for block {current_block_num} (project {project_id}) "
                    f"and no preceding price established (last_known_price is None). Skipping this block."
                )
 
    # Create evenly spaced price data based on step_seconds
    spaced_price_data = []
    if price_data: 
        spaced_price_data.append(price_data[0]) 
        last_added_timestamp_for_spacing = price_data[0]['timestamp']

        for i in range(1, len(price_data)):
            entry = price_data[i]
            if entry['timestamp'] >= last_added_timestamp_for_spacing + step_seconds:
                spaced_price_data.append(entry)
                last_added_timestamp_for_spacing = entry['timestamp']

    return {
        'priceSeries': spaced_price_data,
        'timeInterval': time_interval,
    }


async def get_uniswap_v3_pool_trades(
    redis_conn: aioredis.Redis,
    anchor_rpc_helper: RpcHelper,
    rpc_helper: RpcHelper,
    ipfs_reader: AsyncIPFSClient,
    project_id: str,
    pool_address: str,
    start_timestamp: int,
    end_timestamp: int,
    protocol_state_contract,
) -> List[Dict]:
    """
    Retrieves detailed trade information for a Uniswap V3 pool within a time range.
    
    This function fetches pool metadata, determines the appropriate block range based
    on timestamps, retrieves trade snapshots, and processes individual trades with
    calculated prices and USD values.
    
    Args:
        redis_conn (aioredis.Redis): Redis connection for data access
        anchor_rpc_helper (RpcHelper): RPC helper for blockchain interactions
        rpc_helper (RpcHelper): Additional RPC helper for block data fetching
        ipfs_reader (AsyncIPFSClient): IPFS client for reading snapshot data
        project_id (str): Project identifier for the trades data
        pool_address (str): Ethereum address of the pool to analyze
        start_timestamp (int): Start timestamp for the trade query range
        end_timestamp (int): End timestamp for the trade query range
        protocol_state_contract: Smart contract object for protocol state
        
    Returns:
        List[Dict]: List of processed trade dictionaries containing:
                   - timestamp: Trade timestamp
                   - tokens: Token amounts for both tokens in the pair
                   - trade_amount_usd: USD value of the trade
                   - trade_type: Type of trade (e.g., "Swap")
                   - trade_price_usd: USD price of the non-base token
                   - token0_amount: Raw token0 amount
                   - token1_amount: Raw token1 amount
                   - transaction_hash: Transaction hash of the trade
                   
    Raises:
        Exception: If pool metadata cannot be found, block timestamp lookup fails,
                  or trade snapshot fetching encounters errors
                  
    Note:
        The function implements a fallback mechanism for timestamp-to-block conversion
        when initial Redis cache lookup fails.
    """
    
    # Get pool metadata for token information and decimals
    pool_metadata = await get_uniswap_v3_pool_metadata(
        pool_address=pool_address,
        redis_conn=redis_conn,
        anchor_rpc_helper=anchor_rpc_helper,
        ipfs_reader=ipfs_reader,
        protocol_state_contract=protocol_state_contract,
    )

    if not pool_metadata:
        logger.error(f"No pool metadata found for project {project_id} and pool {pool_address}.")
        raise Exception(f"No pool metadata found for project {project_id} and pool {pool_address}.")

    # Find the closest block to the start timestamp
    closest_start_block = await get_block_number_closest_to_timestamp(
        redis_conn=redis_conn,
        target_timestamp=start_timestamp,
    )

    # Implement fallback mechanism if block lookup fails
    if not closest_start_block:
        logger.warning(f"No closest start block found in Redis for project {project_id}, pool {pool_address}, target timestamp {start_timestamp}. Attempting fallback.")
        try:
            # Fallback: Estimate block, fetch a range, and retry
            source_chain_block_time = await get_source_chain_block_time(redis_conn, protocol_state_contract, anchor_rpc_helper)
            if not source_chain_block_time or source_chain_block_time <= 0:
                logger.error("Invalid source_chain_block_time for fallback. Cannot estimate target block.")
                raise Exception(f"Fallback failed: Invalid source_chain_block_time for project {project_id}.")

            latest_block_data = await rpc_helper.eth_get_block()
            if not latest_block_data or not isinstance(latest_block_data, dict) or \
               'number' not in latest_block_data or 'timestamp' not in latest_block_data:
                logger.error(f"Could not fetch valid latest block data for fallback estimation. Received: {latest_block_data}")
                raise Exception(f"Fallback failed: Could not fetch latest block with number and timestamp for project {project_id}.")
            
            current_block_number = int(latest_block_data['number'], 16)
            current_block_timestamp = int(latest_block_data['timestamp'], 16)

            # Estimate target block based on time difference
            timestamp_diff = current_block_timestamp - start_timestamp
            block_diff_estimate = int(timestamp_diff / source_chain_block_time)
            estimated_target_block = current_block_number - block_diff_estimate

            # Define a range around the estimate to fetch (e.g., +/- 50 blocks)
            # This range can be adjusted based on chain specifics and desired accuracy vs. RPC load.
            BLOCK_RANGE_FOR_FALLBACK = 50 
            fetch_start_block = max(0, estimated_target_block - BLOCK_RANGE_FOR_FALLBACK)
            fetch_end_block = estimated_target_block + BLOCK_RANGE_FOR_FALLBACK
            
            blocks_to_fetch = list(range(fetch_start_block, fetch_end_block + 1))
            if not blocks_to_fetch:
                logger.error("No blocks to fetch in fallback range.")
                raise Exception(f"Fallback failed: No blocks to fetch for project {project_id}.")

            logger.info(f"Fallback: Fetching block timestamps for range [{fetch_start_block}, {fetch_end_block}] around estimated target {estimated_target_block} for start_timestamp {start_timestamp}.")
            await fetch_block_timestamps(redis_conn, rpc_helper, blocks_to_fetch)

            # Retry block lookup after fallback
            closest_start_block = await get_block_number_closest_to_timestamp(
                redis_conn=redis_conn,
                target_timestamp=start_timestamp,
            )
            
            if not closest_start_block:
                logger.error(f"Fallback attempt for project {project_id}, pool {pool_address} also failed to find a closest start block after fetching range [{fetch_start_block}-{fetch_end_block}].")
                raise Exception(f"No closest start block found even after fallback for project {project_id}, pool {pool_address}.")
            else:
                logger.info(f"Fallback successful: Found closest_start_block {closest_start_block} after fetching block data.")

        except Exception as e:
            logger.error(f"Error during fallback mechanism for project {project_id}, pool {pool_address}: {e}", exc_info=True)
            raise Exception(f"No closest start block found for project {project_id} and pool {pool_address}, and fallback failed: {e}")

    
    logger.info(f"Closest start block for project {project_id} and pool {pool_address} is {closest_start_block}.")

    # Calculate time interval and tail epoch for data fetching
    time_interval = end_timestamp - start_timestamp

    tail_epoch_id, _ = await get_tail_epoch_id(
        redis_conn, protocol_state_contract, anchor_rpc_helper, closest_start_block, time_interval, project_id,
    )

    logger.info(f"Tail epoch ID for project {project_id} and pool {pool_address} is {tail_epoch_id}.")

    # Fetch trade snapshot data for the determined range
    try:
        trade_snapshots_raw = await get_project_epoch_snapshot_bulk(
            redis_conn, protocol_state_contract, anchor_rpc_helper, ipfs_reader, tail_epoch_id, closest_start_block, project_id,
        )
    except Exception as e:
        logger.error(f"Error fetching trade snapshots for project {project_id} and pool {pool_address}: {e}")
        raise Exception(f"Error fetching trade snapshots for project {project_id} and pool {pool_address}: {e}")

    # Determine base token (WETH is typically the base)
    if Web3.to_checksum_address(pool_metadata.token0.address) == WETH:
        base_token_num = 0
    else:
        base_token_num = 1

    token0_symbol = pool_metadata.token0.symbol
    token1_symbol = pool_metadata.token1.symbol

    processed_trades = []

    # Process each trade snapshot
    for trade_snapshot_raw in trade_snapshots_raw:
        if not trade_snapshot_raw:
            continue

        try:
            trade_snapshot = UniswapTradesSnapshot.model_validate(trade_snapshot_raw)
        except Exception as e:
            logger.error(f"Error validating trade snapshot for project {project_id} and pool {pool_address}: {e}")
            continue

        # Process individual trades within the snapshot
        for trade in trade_snapshot.trades:
            if trade.tradeType == TradeType.SWAP:
                # Extract trade data
                block_timestamp = trade.data['block_timestamp']
                token0_amount = trade.data['amount0']
                token1_amount = trade.data['amount1']
                transaction_hash = trade.log['transactionHash']
                
                # Adjust amounts for token decimals
                token0_amount_adjusted = abs(token0_amount) / 10 ** pool_metadata.token0.decimals
                token1_amount_adjusted = abs(token1_amount) / 10 ** pool_metadata.token1.decimals
                trade_amount_usd = trade.data['calculated_trade_amount_usd']
                trade_type = "Swap"
                  
                # Calculate price of non-base token in terms of base token
                price_of_non_base_token_in_weth = 0.0
                if base_token_num == 0:
                    if token1_amount_adjusted > 1e-18:  # Avoid division by zero or near-zero
                        price_of_non_base_token_in_weth = token0_amount_adjusted / token1_amount_adjusted
                    else:
                        logger.warning(f"Non-base token (token1: {token1_symbol}) amount is effectively zero for trade. Pool: {pool_address}, ts: {block_timestamp}")
                else:
                    if token0_amount_adjusted > 1e-18:
                        price_of_non_base_token_in_weth = token1_amount_adjusted / token0_amount_adjusted
                    else:
                        logger.warning(f"Non-base token (token0: {token0_symbol}) amount is effectively zero for trade. Pool: {pool_address}, ts: {block_timestamp}")

                # Calculate USD price using ETH price from trade data
                eth_price_usd = trade.data.get('calculated_eth_price', 0.0)
                logger.info(f"ETH price for block {trade.log['blockNumber']} is {eth_price_usd}")
                price_of_non_base_token_usd = price_of_non_base_token_in_weth * eth_price_usd
                
                # Create processed trade entry
                processed_trade_entry = {
                    'timestamp': block_timestamp,
                    'tokens': {
                        token0_symbol: token0_amount_adjusted,
                        token1_symbol: token1_amount_adjusted,
                    },
                    'trade_amount_usd': trade_amount_usd,
                    'trade_type': trade_type,
                    'trade_price_usd': price_of_non_base_token_usd,
                    'token0_amount': token0_amount,
                    'token1_amount': token1_amount,
                    'transaction_hash': transaction_hash,
                }
                processed_trades.append(processed_trade_entry)

    return processed_trades

    
