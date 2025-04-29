import asyncio
import json

from eth_utils.address import to_checksum_address
from rpc_helper.rpc import RpcHelper
from web3 import HTTPProvider
from web3 import Web3

from snapshotter.settings.config import settings
from snapshotter.utils.default_logger import default_logger
from snapshotter.utils.redis.redis_conn import RedisPoolCache

test_logger = default_logger.bind(module='test_web3_async_call')


async def test_web3_async_call():
    """
    Test asynchronous Web3 calls using a storage contract.

    This function demonstrates the process of interacting with an Ethereum smart contract
    using Web3.py in an asynchronous manner. It includes setting up the contract,
    initializing necessary components, and making an asynchronous call to the contract.
    """
    # Load the ABI (Application Binary Interface) for the storage contract
    with open('snapshotter/tests/static/abi/storage_contract.json') as f:
        contract_abi = json.load(f)

    # Initialize and populate the Redis pool cache
    aioredis_pool = RedisPoolCache()
    await aioredis_pool.populate()
    writer_redis_pool = aioredis_pool._aioredis_pool

    # Set up the RPC helper with the anchor chain RPC
    rpc_helper = RpcHelper(settings.anchor_chain_rpc)
    await rpc_helper.init()

    # Create a synchronous Web3 client
    sync_w3_client = Web3(HTTPProvider(settings.anchor_chain_rpc.full_nodes[0].url))

    # Initialize the contract object
    contract_obj = sync_w3_client.eth.contract(
        address=to_checksum_address('0x31b554545279DBB438FC66c55A449263a6b56dB5'),
        abi=contract_abi,
    )

    # Prepare the task for asynchronous execution
    tasks = [
        contract_obj.functions.retrieve(),
    ]

    # Execute the Web3 call asynchronously
    result = await rpc_helper.web3_call(tasks)
    test_logger.debug('Retrieve: {}', result)


if __name__ == '__main__':
    try:
        # Run the test function in the event loop
        asyncio.get_event_loop().run_until_complete(test_web3_async_call())
    except Exception as e:
        # Log any exceptions that occur during execution
        test_logger.opt(exception=settings.logs.debug_mode).error('exception: {}', e)
