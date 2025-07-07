import asyncio
import json

from redis import asyncio as aioredis
from rpc_helper.rpc import RpcHelper

from snapshotter.settings.config import settings
from snapshotter.utils.default_logger import default_logger
from snapshotter.utils.redis.redis_keys import cached_block_details_at_height
from snapshotter.utils.redis.redis_keys import source_chain_epoch_size_key


snapshot_util_logger = default_logger.bind(module='Snapshotter|SnapshotUtilLogger')


async def get_block_details_in_block_range(
    from_block,
    to_block,
    redis_conn: aioredis.Redis,
    rpc_helper: RpcHelper,
):
    """
    Fetches block details for a given range of block numbers.

    Args:
        from_block (int): The starting block number.
        to_block (int): The ending block number.
        redis_conn (aioredis.Redis): The Redis connection object.
        rpc_helper (RpcHelper): The RPC helper object.

    Returns:
        dict: A dictionary containing block details for each block number in the given range.

    Raises:
        Exception: If there's an error fetching the block details.
    """
    try:
        # Check if block details are already cached in Redis
        cached_details = await redis_conn.zrangebyscore(
            name=cached_block_details_at_height,
            min=int(from_block),
            max=int(to_block),
        )

        # If all block details are cached, return them
        if cached_details and len(cached_details) == to_block - (from_block - 1):
            cached_details = {
                int(json.loads(block_detail.decode('utf-8'))['number'], 16):
                json.loads(block_detail.decode('utf-8'))
                for block_detail in cached_details
            }
            # convert timestamp to int and number to int
            for block_num, block_detail in cached_details.items():
                block_detail['timestamp'] = int(block_detail['timestamp'], 16)
                block_detail['number'] = int(block_detail['number'], 16)
            return cached_details

        # Fetch block details from RPC if not cached
        rpc_batch_block_details = await rpc_helper.batch_eth_get_block(from_block, to_block)

        rpc_batch_block_details = rpc_batch_block_details if rpc_batch_block_details else []

        block_details_dict = dict()
        redis_cache_mapping = dict()

        # Process and format block details
        for block_num, block_details in enumerate(rpc_batch_block_details, start=from_block):
            block_details = block_details.get('result')
            block_details['timestamp'] = int(block_details['timestamp'], 16)
            block_details['number'] = int(block_details['number'], 16)
            block_details_dict[block_num] = block_details
            redis_cache_mapping[json.dumps(block_details)] = int(block_num)

        # Cache new block details and prune old ones
        source_chain_epoch_size = int(await redis_conn.get(source_chain_epoch_size_key()))
        # removed: block details cache pruning since that is handled by responsbile periphery services
        await redis_conn.zadd(
            name=cached_block_details_at_height,
            mapping=redis_cache_mapping,
        )
        return block_details_dict

    except Exception as e:
        snapshot_util_logger.opt(exception=settings.logs.debug_mode, lazy=True).trace(
            'Unable to fetch block details, error_msg:{err}',
            err=lambda: str(e),
        )

        raise e
