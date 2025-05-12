"""
This module contains the adapted API endpoints that parse specific computes made by the Uniswap V3 compute modules
TODO: consider packaging this as a separate plugin like computes since it uses compute specific logic and cache access
"""

from fastapi import FastAPI
from fastapi import Request
from fastapi import Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi_pagination import add_pagination
from fastapi_pagination import Page
from ipfs_client.main import AsyncIPFSClientSingleton
from pydantic import Field
from rpc_helper.rpc import RpcHelper
from typing import Optional
from web3 import Web3

from snapshotter.settings.config import settings
from snapshotter.utils.data_utils import get_project_epoch_snapshot, get_uniswap_v3_token_pools_snapshot
from snapshotter.utils.data_utils import get_project_finalized_cid
from snapshotter.utils.data_utils import get_project_time_series_data
from snapshotter.utils.data_utils import get_uniswap_v3_eth_price_snapshot
from snapshotter.utils.default_logger import default_logger
from snapshotter.utils.file_utils import read_json_file
from snapshotter.utils.models.data_models import TaskStatusRequest
from snapshotter.utils.redis.redis_conn import RedisPoolCache
from snapshotter.utils.data_utils import get_uniswap_v3_pool_metadata


rest_logger = default_logger.bind(module='UniswapV3API')


# Load protocol state contract ABI and address
protocol_state_contract_abi = read_json_file(
    settings.protocol_state.abi,
    rest_logger,
)
protocol_state_contract_address = settings.protocol_state.address

# Setup CORS origins
origins = ['*']
app = FastAPI()

# Configure pagination for epoch processing status reports
Page = Page.with_custom_options(
    size=Field(10, ge=1, le=30),
)
add_pagination(app)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*'],
)


@app.on_event('startup')
async def startup_boilerplate():
    """
    Initialize various state variables and caches required for the application to function properly.
    This function is called when the FastAPI application starts up.
    """
    app.state.core_settings = settings
    app.state.local_user_cache = dict()
    app.state.anchor_rpc_helper = RpcHelper(rpc_settings=settings.anchor_chain_rpc)
    await app.state.anchor_rpc_helper.init()
    app.state.protocol_state_contract = app.state.anchor_rpc_helper.get_current_node()['web3_client'].eth.contract(
        address=Web3.to_checksum_address(
            protocol_state_contract_address,
        ),
        abi=protocol_state_contract_abi,
    )

    # Initialize IPFS client if URL is set
    if not settings.ipfs.url:
        rest_logger.warning('IPFS url not set, /data API endpoint will be unusable!')
    else:
        app.state.ipfs_singleton = AsyncIPFSClientSingleton(settings.ipfs)
        await app.state.ipfs_singleton.init_sessions()
        app.state.ipfs_reader_client = app.state.ipfs_singleton._ipfs_read_client
    app.state.epoch_size = 0
    app.state._aioredis_pool = RedisPoolCache()
    await app.state._aioredis_pool.populate()
    app.state.redis_conn = app.state._aioredis_pool._aioredis_pool


@app.get('/pool/{pool_address}/metadata')
async def get_pool_metadata(
    pool_address: str,
    request: Request,
    response: Response,
):
    """
    Get the metadata for a specific pool.
    """
    pool_address = Web3.to_checksum_address(pool_address)
    # TODO: integrate pool metadata fetch logic from compute module
    try:
        pool_metadata = await get_uniswap_v3_pool_metadata(
            redis_conn=app.state.redis_conn,
            protocol_state_contract=app.state.protocol_state_contract,
            anchor_rpc_helper=app.state.anchor_rpc_helper,
            ipfs_reader=app.state.ipfs_reader_client,
            pool_address=pool_address,
        )
        if not pool_metadata:
            response.status_code = 404
            return {"error": "Pool metadata not found"}
        else:
            response.status_code = 200
            return pool_metadata
    except Exception as e:
        rest_logger.error(f"Error getting pool metadata for {pool_address}: {e}")
        response.status_code = 500
        return {"error": "Pool metadata not found"}


@app.get('/token/{token_address}/pools')
async def get_token_pools(
    token_address: str,
    request: Request,
    response: Response,
):
    """
    Get the token pools for a specific token.
    """
    token_address = Web3.to_checksum_address(token_address)
    # TODO: integrate token pools fetch logic from compute module
    try:
        token_pools_snapshot = await get_uniswap_v3_token_pools_snapshot(
            redis_conn=app.state.redis_conn,
            protocol_state_contract=app.state.protocol_state_contract,
            anchor_rpc_helper=app.state.anchor_rpc_helper,
            ipfs_reader=app.state.ipfs_reader_client,
            token_address=token_address,
        )
        if not token_pools_snapshot:
            response.status_code = 404
            return {"error": "Token pools not found"}
        else:
            response.status_code = 200
            return token_pools_snapshot
    except Exception as e:
        rest_logger.error(f"Error getting token pools for {token_address}: {e}")
        response.status_code = 500
        return {"error": "Token pools not found"}


@app.get('/ethPrice/{block_number}')
@app.get('/ethPrice')
async def get_ethprice(
    request: Request,
    response: Response,
    block_number: Optional[int] = None,
):
    """
    Get ETH price snapshot for a specific block number or latest finalized epoch.
    
    Args:
        request: FastAPI request object
        response: FastAPI response object
        block_number: Optional block number to get ETH price for. If not provided, uses latest finalized epoch.
    """
    eth_price_snapshot = await get_uniswap_v3_eth_price_snapshot(
        redis_conn=app.state.redis_conn,
        protocol_state_contract=app.state.protocol_state_contract,
        anchor_rpc_helper=app.state.anchor_rpc_helper,
        ipfs_reader=app.state.ipfs_reader_client,
        block_number=block_number,
    )
    if not eth_price_snapshot:
        response.status_code = 404
        return {"error": "ETH price snapshot not found"}
    else:
        response.status_code = 200
        return eth_price_snapshot