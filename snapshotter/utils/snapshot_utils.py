import asyncio
import json

from redis import asyncio as aioredis

from snapshotter.settings.config import settings
from snapshotter.utils.default_logger import default_logger
from snapshotter.utils.redis.redis_keys import cached_block_details_at_height
from snapshotter.utils.redis.redis_keys import source_chain_epoch_size_key
from snapshotter.utils.rpc import RpcHelper


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
                json.loads(block_detail.decode('utf-8'))['number']:
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
            formatted_details = {
                'timestamp': int(block_details.get('timestamp', None), 16),
                'number': int(block_details.get('number', None), 16),
                'transactions': block_details.get('transactions', []),
            }

            block_details_dict[block_num] = formatted_details
            redis_cache_mapping[json.dumps(formatted_details)] = int(block_num)

        # Cache new block details and prune old ones
        source_chain_epoch_size = int(await redis_conn.get(source_chain_epoch_size_key()))
        await asyncio.gather(
            redis_conn.zadd(
                name=cached_block_details_at_height,
                mapping=redis_cache_mapping,
            ),
            redis_conn.zremrangebyscore(
                name=cached_block_details_at_height,
                min=0,
                max=int(from_block) - source_chain_epoch_size * 3,
            ),
        )
        return block_details_dict

    except Exception as e:
        snapshot_util_logger.opt(exception=settings.logs.debug_mode, lazy=True).trace(
            'Unable to fetch block details, error_msg:{err}',
            err=lambda: str(e),
        )

        raise e
