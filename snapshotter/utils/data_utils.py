import asyncio
import json
from typing import List, Optional
import time
import tenacity
from pydantic import BaseModel
from redis import asyncio as aioredis
from rpc_helper.rpc import RpcHelper
from tenacity import retry
from tenacity import retry_if_exception_type
from tenacity import stop_after_attempt
from tenacity import wait_random_exponential
from web3 import Web3
from ipfs_client.main import AsyncIPFSClient

from computes.utils.models.message_models import UniswapBaseSnapshot
from snapshotter.utils.models.data_models import UniswapPoolMetadata, UniswapTokenPoolsSnapshot, UniswapEthPriceSnapshot
from snapshotter.settings.config import settings
from snapshotter.utils.default_logger import default_logger
from snapshotter.utils.redis.redis_keys import cid_not_found_key
from snapshotter.utils.redis.redis_keys import project_first_epoch_hmap
from snapshotter.utils.redis.redis_keys import project_last_finalized_epoch_hmap
from snapshotter.utils.redis.redis_keys import project_data_hmap
from snapshotter.utils.redis.redis_keys import source_chain_block_time_key
from snapshotter.utils.redis.redis_keys import source_chain_epoch_size_key
from snapshotter.utils.redis.redis_keys import source_chain_id_key
from snapshotter.utils.redis.redis_keys import project_data_expiry_zset
from snapshotter.settings.config import projects_config

logger = default_logger.bind(module='data_helper')
BATCH_SIZE = 50
PROJECT_DATA_ENTRY_EXPIRY = 60 * 60 * 24 * 7  # 7 days in seconds


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

    if epoch_id_min < project_first_epoch:
        logger.warning(
            f'Min. Epoch ID: {epoch_id_min} is less than the project first epoch {project_first_epoch}.',
            'Cannot fetch CIDs for epochs before project first epoch.',
        )
        return None

    epoch_ids_set = set(range(epoch_id_min, epoch_id_max + 1))

    # Check Redis cache for existing CIDs
    epoch_ids_to_fetch = list(range(epoch_id_min, epoch_id_max + 1))
    logger.info(f'Fetching CIDs for epochs {epoch_ids_to_fetch} for project {project_id}')
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
    missing_epochs = list(epoch_ids_set.difference(existing_epochs))

    # batch_web3_contract_calls
    if missing_epochs:
        if project_config.keep_previous_snapshot_data:
            logger.info(f'Fetching CIDs for epochs {missing_epochs} for project {project_id}')
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

        expiry_key = f"{project_id}|{epoch_id}"
        pipeline.zadd(
            name=project_data_expiry_zset(),
            mapping={expiry_key: expiry_time},
        )

        # Process previousSnapshots if available
        try:
            snapshot_data = await fetch_file_from_ipfs(redis_conn, ipfs_reader, cid)
            if snapshot_data and "previousSnapshots" in snapshot_data:
                data_to_cache = {}
                expiry_keys = []
                all_previous_snapshot_keys = snapshot_data["previousSnapshots"].keys()
                min_previous_snapshot_key = min(all_previous_snapshot_keys)
                max_previous_snapshot_key = max(all_previous_snapshot_keys)
                all_previous_snapshot_keys = set(range(min_previous_snapshot_key, max_previous_snapshot_key + 1))
                # Process each previous snapshot
                for (epoch_id, snapshot_cid) in snapshot_data["previousSnapshots"]:
                    epoch_id = int(epoch_id)
                    data_to_cache[epoch_id] = json.dumps({
                        "snapshot_cid": snapshot_cid,
                        "status": status + 1
                    })
                    all_previous_snapshot_keys.remove(epoch_id)
                    expiry_keys.append(f"{project_id}|{epoch_id}")

                    # Add to pipeline if we have data to cache
                    if data_to_cache:
                        pipeline.hset(
                            project_hmap_key,
                            mapping=data_to_cache,
                        )

                    if expiry_keys:
                        expiry_data = {key: expiry_time for key in expiry_keys}
                        pipeline.zadd(
                            name=project_data_expiry_zset(),
                            mapping=expiry_data,
                        )
                for epoch_id in all_previous_snapshot_keys:
                    pipeline.hset(
                        project_hmap_key,
                        epoch_id,
                        json.dumps({"snapshot_cid": null_cid, "status": -1}),
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
        pipeline = redis_conn.pipeline()

        pipeline.hset(
            project_data_hmap(project_id=project_id),
            epoch_id,
            json.dumps({"snapshot_cid": null_cid, "status": -1}),
        )

        expiry_key = f"{project_id}|{epoch_id}"
        pipeline.zadd(
            name=project_data_expiry_zset(),
            mapping={expiry_key: expiry_time},
        )

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

        missing_epochs = set(epoch_ids)
        cid_data_with_epochs = []
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
                missing_epoch_list = list(missing_epochs)
                redis_cache_data = await redis_conn.hmget(project_hmap_key, missing_epoch_list)
                data = []
                for data_raw in redis_cache_data:
                    if data_raw:
                        data.append(json.loads(data_raw))
                    else:
                        data.append(dict())

                for snapshot_data, epoch_id in zip(data, missing_epoch_list):
                    if "snapshot_cid" in snapshot_data:
                        cid_data_with_epochs.append((snapshot_data["snapshot_cid"], epoch_id))
                        missing_epochs.remove(epoch_id)

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
    try:
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
            else:
                # Only cache null if we're sure there's no CID (timestamp > 0 but no CID)
                data_to_cache[epoch_id] = json.dumps({"snapshot_cid": null_cid, "status": -1})
            expiry_keys.append(f"{project_id}|{epoch_id}")

            # Add to result list regardless of whether we're caching
            if cid:
                cids_with_epochs.append((cid, epoch_id))
            else:
                cids_with_epochs.append((null_cid, epoch_id))

        # Use pipeline for Redis operations if we have keys to update
        if data_to_cache:
            pipeline = redis_conn.pipeline()

            pipeline.hset(
                project_hmap_key,
                mapping=data_to_cache,
            )

            # Add to expiry tracking sorted set with TTL
            if expiry_keys:
                expiry_data = {key: expiry_time for key in expiry_keys}
                pipeline.zadd(
                    name=project_data_expiry_zset(),
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


async def fetch_file_from_ipfs(redis_conn: aioredis.Redis, ipfs_reader, cid):
    """
    Fetches a file from IPFS using the given IPFS reader and CID.

    Uses _fetch_file_from_ipfs under the hood, if it is unable to fetch file from IPFS, it will mark the cid as not found in redis.
    """
    if await redis_conn.get(cid_not_found_key(cid)):
        return dict()
    try:
        data = await _fetch_file_from_ipfs(ipfs_reader, cid)
        return json.loads(data)
    except Exception as e:
        logger.opt(exception=True).error(f'Error while fetching data from IPFS | CID {cid} | Error: {e}')
        await redis_conn.set(cid_not_found_key(cid), 'true', ex=86400)
        return dict()


async def get_submission_data(redis_conn: aioredis.Redis, cid, ipfs_reader, cleanup_previous_snapshots: bool = True) -> dict:
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

    data = await fetch_file_from_ipfs(redis_conn, ipfs_reader, cid)
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
    all_snapshot_data = []

    # Process submissions in batches
    for i in range(0, len(cids), BATCH_SIZE):
        batch_cids = cids[i:i + BATCH_SIZE]
        batch_snapshot_data = await asyncio.gather(
            *[
                get_submission_data(redis_conn, cid, ipfs_reader)
                for cid in batch_cids
            ],
        )

        if ensure_complete:
            missing_cids = [
                cid for cid, data in zip(batch_cids, batch_snapshot_data)
                if data == dict()
            ]
            if missing_cids:
                logger.error(f'Incomplete ipfs data for CIDs: {missing_cids}')
                return []

        all_snapshot_data.extend(batch_snapshot_data)

    return all_snapshot_data


async def get_project_epoch_snapshot(
    redis_conn: aioredis.Redis, state_contract_obj, rpc_helper, ipfs_reader, epoch_id, project_id,
) -> dict:
    """
    Retrieves the epoch snapshot for a given project.

    This function first gets the finalized CID for the given epoch and project,
    then fetches the corresponding submission data.

    Args:
        redis_conn (aioredis.Redis): Redis connection object.
        state_contract_obj: State contract object.
        rpc_helper: RPC helper object.
        ipfs_reader: IPFS reader object.
        epoch_id (int): Epoch ID.
        project_id (str): Project ID.

    Returns:
        dict: The epoch snapshot data.
    """
    cid = await get_project_finalized_cid(redis_conn, state_contract_obj, rpc_helper, ipfs_reader, epoch_id, project_id)
    if cid:
        data = await get_submission_data(redis_conn, cid, ipfs_reader)
        return data
    else:
        return dict()


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
):
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
        dict: The latest snapshot data for the given project.
    """
    last_finalized_epoch = await get_project_last_finalized_epoch(redis_conn, state_contract_obj, rpc_helper, project_id)
    if not last_finalized_epoch:
        return dict()
    return await get_project_epoch_snapshot(redis_conn, state_contract_obj, rpc_helper, ipfs_reader, last_finalized_epoch, project_id)


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
):
    """
    Returns a list of snapshot data containing equally spaced observations starting with the start_epoch id
    for the given project_id, and including epochs spaced step_seconds apart until the maximum observations has been reached.

    Args:
        observations: Total number of data points to gather
        step_seconds: Time in seconds between each obsveration
        project_last_finalized_epoch: Epoch ID of the last finalized epoch for'project_id'
        redis_conn (aioredis.Redis): Redis connection object.
        state_contract_obj: State contract object.
        rpc_helper: RPC helper object.
        ipfs_reader: IPFS reader object.
        project_id: ID of the project to fetch snapshot data for.


    Returns:
        A list of snapshot data objects for the given project_id with a maximum length of the observations param.
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
    )

### UNISWAP V3 SPECIFIC LOGIC ###
# TODO: consider packaging this as a separate plugin like computes since it uses compute specific logic and cache access
 
async def get_uniswap_v3_pool_metadata(
        pool_address: str, 
        redis_conn: aioredis.Redis, 
        anchor_rpc_helper: RpcHelper,
        ipfs_reader: AsyncIPFSClient,
        protocol_state_contract,
        
    ) -> Optional[UniswapPoolMetadata]:
        # check redis cache first
        project_id: str = 'metadata:{poolAddress}:{Namespace}'
        cache_key = f'pool_metadata:{pool_address}'
        cached_data = await redis_conn.get(cache_key)
        if cached_data:
            logger.info(f"Found cached metadata for pool {pool_address}")
            return UniswapPoolMetadata(**json.loads(cached_data))

        try:
            last_finalized_epoch = await get_project_last_finalized_epoch(
                redis_conn, protocol_state_contract, anchor_rpc_helper, project_id.format(poolAddress=pool_address, Namespace=settings.namespace)
            )
        except Exception as e:
            logger.opt(exception=e).error(f"Error getting last finalized epoch for pool {pool_address} while processing metadata")
            last_finalized_epoch = None

        if not last_finalized_epoch:
            logger.error(f"No last finalized epoch found for pool {pool_address} while processing metadata")
            return None

        # get finalized cid
        finalized_cid = await get_project_finalized_cid(
            redis_conn=redis_conn,
            state_contract_obj=protocol_state_contract,
            rpc_helper=anchor_rpc_helper,
            ipfs_reader=ipfs_reader,
            epoch_id=last_finalized_epoch,
            project_id=project_id.format(poolAddress=pool_address, Namespace=settings.namespace),
        )
        if not finalized_cid:
            logger.error(f"No finalized cid found for pool {pool_address} against epoch {last_finalized_epoch} while processing metadata")
            return None
        data = await get_project_epoch_snapshot(
            redis_conn, protocol_state_contract, anchor_rpc_helper, ipfs_reader, last_finalized_epoch, project_id.format(poolAddress=pool_address, Namespace=settings.namespace)
        )
        if not data:
            logger.error(f"No snapshot data found for pool {pool_address} against epoch {last_finalized_epoch} while processing metadata")
            return None
        return UniswapPoolMetadata(**data)


async def get_uniswapv3_snapshot(
    redis_conn: aioredis.Redis,
    anchor_rpc_helper: RpcHelper,
    ipfs_reader: AsyncIPFSClient,
    protocol_state_contract,
    project_id: str,
    message_model: BaseModel,
    block_number: Optional[int] = None,
):
    # if block_number is not provided, get the last finalized epoch and use that
    if not block_number:
        target_epoch = await get_project_last_finalized_epoch(
            redis_conn, protocol_state_contract, anchor_rpc_helper, project_id,
        )
        if not target_epoch:
            logger.error(f"No last finalized epoch found for project {project_id}")
            return None
    else:
        # TODO: assumes epoch is set to block number in data market contract, may need to add config flag for this and derive epoch from block number if false
        target_epoch = block_number
    
    snapshot = await get_project_epoch_snapshot(
        redis_conn, protocol_state_contract, anchor_rpc_helper, ipfs_reader, target_epoch, project_id,
    )
    if snapshot:
        parsed_snapshot = message_model(**snapshot)
        return target_epoch, parsed_snapshot
    else:
        logger.error(f"No snapshot data found for project {project_id} against epoch {target_epoch}")
        return None


async def get_uniswap_v3_token_pools_snapshot(
    redis_conn: aioredis.Redis,
    anchor_rpc_helper: RpcHelper,
    ipfs_reader: AsyncIPFSClient,
    protocol_state_contract,
    token_address: str,
):
    """
    Get the snapshot of token pools for a Uniswap pair.
    """
    token_address = Web3.to_checksum_address(token_address)
    project_id = f"tokenPools:{token_address}:{settings.namespace}"
    result = await get_uniswapv3_snapshot(
        redis_conn,
        anchor_rpc_helper,
        ipfs_reader,
        protocol_state_contract,
        project_id,
        UniswapTokenPoolsSnapshot,
    )
    if not result:
        logger.error(f"No snapshot data found for project {project_id}")
        return None
        
    snapshot_epoch, snapshot_data = result
    if snapshot_data:
        return snapshot_data
    else:
        return None


async def get_uniswap_v3_eth_price_snapshot(
    redis_conn: aioredis.Redis,
    anchor_rpc_helper: RpcHelper,
    ipfs_reader: AsyncIPFSClient,
    protocol_state_contract,
    block_number: Optional[int] = None,
):
    project_id = f'price:ETH:{settings.namespace}'
    result = await get_uniswapv3_snapshot(
        redis_conn,
        anchor_rpc_helper,
        ipfs_reader,
        protocol_state_contract,
        project_id,
        UniswapEthPriceSnapshot,
        block_number,
    )
    if not result:
        logger.error(f"No snapshot data found for project {project_id}")
        return None
        
    snapshot_epoch, snapshot_data = result
    if snapshot_data:
        return snapshot_data
    else:
        return None


async def get_uniswap_v3_token_price_pool_snapshot(
    redis_conn: aioredis.Redis,
    anchor_rpc_helper: RpcHelper,
    ipfs_reader: AsyncIPFSClient,
    protocol_state_contract,
    token_address: str,
    pool_address: str,
    block_number: Optional[int] = None,
):
    base_project_id = f"baseSnapshot:{pool_address.lower()}:{settings.namespace}"

    result = await get_uniswapv3_snapshot(
        redis_conn,
        anchor_rpc_helper,
        ipfs_reader,
        protocol_state_contract,
        base_project_id,
        UniswapBaseSnapshot,
        block_number,
    )
    if not result:
        logger.error(f"No snapshot data found for project {base_project_id}")
        return None
        
    snapshot_epoch, snapshot_data = result
    if not snapshot_data:
        logger.error(f"No base snapshot data found for project {base_project_id} against epoch {snapshot_epoch}")
        return None

    if Web3.to_checksum_address(token_address) == snapshot_data.token0:
        # NOTE: assumes snapshot_epoch is the block number
        token_price = snapshot_data.token0PricesUSD[snapshot_epoch]
    elif Web3.to_checksum_address(token_address) == snapshot_data.token1:
        token_price = snapshot_data.token0PricesUSD[snapshot_epoch]
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
    token_pools_snapshot_result = await get_uniswap_v3_token_pools_snapshot(
        redis_conn,
        anchor_rpc_helper,
        ipfs_reader,
        protocol_state_contract,
        token_address,
    )
    if not token_pools_snapshot_result:
        logger.error(f"No token pools snapshot found for token {token_address}")
        return None
    
    snapshot_data = token_pools_snapshot_result
    if not snapshot_data or not snapshot_data.pools:
        logger.error(f"No token pools snapshot data found for token {token_address}")
        return None
    
    # Get list of pool addresses
    pool_addresses = list(snapshot_data.pools.keys())
    if not pool_addresses:
        logger.error(f"No pools found for token {token_address}")
        return None

    # Process pools in batches of 20
    BATCH_SIZE = 20
    results = {}
    
    for i in range(0, len(pool_addresses), BATCH_SIZE):
        batch_pools = pool_addresses[i:i + BATCH_SIZE]
        
        # Create tasks for each pool in the batch
        tasks = [
            get_uniswap_v3_token_price_pool_snapshot(
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



async def get_uniswap_trade_volume_agg(
    redis_conn: aioredis.Redis,
    anchor_rpc_helper: RpcHelper,
    ipfs_reader: AsyncIPFSClient,
    protocol_state_contract,
    time_interval: int,
    project_id: str,
):
    [current_epoch_data] = await anchor_rpc_helper.web3_call(
        tasks=[
            ('currentEpoch', [Web3.to_checksum_address(settings.data_market)]),
        ],
        contract_addr=protocol_state_contract.address,
        abi=protocol_state_contract.abi,
    )

    current_epoch = current_epoch_data[2]

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