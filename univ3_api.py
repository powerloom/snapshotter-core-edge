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
from fastapi import Query

from snapshotter.settings.config import settings
from snapshotter.utils.data_utils import get_project_epoch_snapshot, get_uniswap_v3_token_pools_snapshot, get_uniswap_v3_token_price_pool, get_uniswap_v3_token_prices_all_snapshot, get_uniswap_trade_volume_agg
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
    try:
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
    except Exception as e:
        rest_logger.error(f"Error getting ETH price snapshot: {e}")
        response.status_code = 500
        return {"error": "ETH price snapshot not found"}


@app.get('/token/price/{token_address}/{pool_address}')
@app.get('/token/price/{token_address}/{pool_address}/{block_number}')
async def get_token_price_pool(
    request: Request,
    response: Response,
    token_address: str,
    pool_address: Optional[str] = None,
    block_number: Optional[int] = None,
):
    try:
        token_price = await get_uniswap_v3_token_price_pool(
            redis_conn=app.state.redis_conn,
            protocol_state_contract=app.state.protocol_state_contract,
            anchor_rpc_helper=app.state.anchor_rpc_helper,
            ipfs_reader=app.state.ipfs_reader_client,
            token_address=token_address,
            pool_address=pool_address,
            block_number=block_number,
        )
        if not token_price:
            response.status_code = 404
            return {"error": "Token price snapshot not found"}
        else:
            response.status_code = 200
            return token_price
    except Exception as e:
        rest_logger.error(f"Error getting token price snapshot for {token_address} in pool {pool_address} at block {block_number}: {e}")
        response.status_code = 500
        return {"error": "Token price snapshot not found"}
    

@app.get('/token/price/{token_address}')
@app.get('/token/price/{token_address}/{block_number}')
async def get_token_price_all(
    request: Request,
    response: Response,
    token_address: str,
    block_number: Optional[int] = None,
):
    try:
        token_prices = await get_uniswap_v3_token_prices_all_snapshot(
            redis_conn=app.state.redis_conn,
            protocol_state_contract=app.state.protocol_state_contract,
            anchor_rpc_helper=app.state.anchor_rpc_helper,
            ipfs_reader=app.state.ipfs_reader_client,
            token_address=token_address,
            block_number=block_number,
        )
        if not token_prices:
            response.status_code = 404
            return {"error": "Token price snapshot not found"}
        else:
            response.status_code = 200
            return token_prices
    except Exception as e:
        rest_logger.error(f"Error getting token price snapshot for {token_address} at block {block_number}: {e}")
        response.status_code = 500
        return {"error": "Token price snapshot not found"}


@app.get('/tradeVolume/{pool_address}/{time_interval}')
async def get_trade_volume_agg(
    request: Request,
    response: Response,
    pool_address: str,
    time_interval: int,
):
    pool_address = Web3.to_checksum_address(pool_address)
    project_id = f"baseSnapshot:{pool_address.lower()}:{settings.namespace}"
    try:
        trade_volume_agg = await get_uniswap_trade_volume_agg(
            redis_conn=app.state.redis_conn,
            protocol_state_contract=app.state.protocol_state_contract,
            anchor_rpc_helper=app.state.anchor_rpc_helper,
            ipfs_reader=app.state.ipfs_reader_client,
            project_id=project_id,
            time_interval=time_interval,
        )
    except Exception as e:
        rest_logger.opt(exception=True).error(f"Error getting trade volume agg for {pool_address}: {e}")
        response.status_code = 500
        return {"error": "Trade volume agg not found"}
    if not trade_volume_agg:
        response.status_code = 404
        return {"error": "Trade volume agg not found"}
    else:
        response.status_code = 200
        return trade_volume_agg
    

@app.get('/dailyActiveTokens', 
    summary="Get daily active tokens with pagination",
    description="Retrieves a paginated list of active tokens for the current day, sorted by frequency. Use page and size parameters to control pagination.",
    response_description="Returns a paginated list of active tokens with their frequencies"
)
async def get_daily_active_tokens(
    request: Request,
    response: Response,
    page: int = Query(
        default=1,
        ge=1,
        description="Page number to retrieve (starts at 1)",
        example=1
    ),
    size: int = Query(
        default=50,
        ge=1,
        le=100,
        description="Number of items per page (max 100)",
        example=50
    ),
):
    """
    Get a paginated list of active tokens for the current day.
    
    Parameters:
    - page: The page number to retrieve (starts at 1)
    - size: Number of items per page (default: 50, max: 100)
    
    Returns:
    - List of active tokens with their frequencies
    - Pagination metadata including total count and pages
    """
    current_day = await app.state.redis_conn.get("current_day")
    if not current_day:
        response.status_code = 404
        return {"error": "Current day not found"}
    
    try:
        # Decode current_day if it's bytes
        if isinstance(current_day, bytes):
            current_day = current_day.decode('utf-8')
        
        redis_key = f"active_tokens:day_{current_day}"
        
        # Calculate start and end indices for pagination
        start_idx = (page - 1) * size
        end_idx = start_idx + size - 1
        
        # Get total count of tokens
        total_tokens = await app.state.redis_conn.zcard(redis_key)
        
        # Get paginated tokens from the sorted set
        active_tokens = await app.state.redis_conn.zrange(
            redis_key,
            start_idx,
            end_idx,
            withscores=True,
            desc=True  # Get highest frequency tokens first
        )
        
        # Format the response
        tokens_data = [
            {
                "token_address": token.decode('utf-8'),
                "frequency": int(score)
            }
            for token, score in active_tokens
        ]
        
        response.status_code = 200
        return {
            "day": int(current_day),
            "active_tokens": tokens_data,
            "pagination": {
                "page": page,
                "size": size,
                "total": total_tokens,
                "total_pages": (total_tokens + size - 1) // size
            }
        }
    except Exception as e:
        rest_logger.error(f"Error getting daily active tokens: {e}")
        response.status_code = 500
        return {"error": "Failed to retrieve daily active tokens"}
    

@app.get('/dailyActivePools', 
    summary="Get daily active pools with pagination",
    description="Retrieves a paginated list of active pools for the current day, sorted by frequency. Use page and size parameters to control pagination.",
    response_description="Returns a paginated list of active pools with their frequencies"
)
async def get_daily_active_pools(
    request: Request,
    response: Response,
    page: int = Query(
        default=1,
        ge=1,
        description="Page number to retrieve (starts at 1)",
        example=1
    ),
    size: int = Query(
        default=50,
        ge=1,
        le=100,
        description="Number of items per page (max 100)",
        example=50
    ),
):
    """
    Get a paginated list of active pools for the current day.
    
    Parameters:
    - page: The page number to retrieve (starts at 1)
    - size: Number of items per page (default: 50, max: 100)
    
    Returns:
    - List of active pools with their frequencies
    - Pagination metadata including total count and pages
    """
    current_day = await app.state.redis_conn.get("current_day")
    if not current_day:
        response.status_code = 404
        return {"error": "Current day not found"}
    
    try:
        # Decode current_day if it's bytes
        if isinstance(current_day, bytes):
            current_day = current_day.decode('utf-8')
        
        redis_key = f"active_pools:day_{current_day}"
        
        # Calculate start and end indices for pagination
        start_idx = (page - 1) * size
        end_idx = start_idx + size - 1
        
        # Get total count of tokens
        total_pools = await app.state.redis_conn.zcard(redis_key)
        
        # Get paginated tokens from the sorted set
        active_pools = await app.state.redis_conn.zrange(
            redis_key,
            start_idx,
            end_idx,
            withscores=True,
            desc=True  # Get highest frequency tokens first
        )
        
        # Format the response
        pools_data = [
            {
                "pool_address": pool.decode('utf-8'),
                "frequency": int(score)
            }
            for pool, score in active_pools
        ]
        
        response.status_code = 200
        return {
            "day": int(current_day),
            "active_pools": pools_data,
            "pagination": {
                "page": page,
                "size": size,
                "total": total_pools,
                "total_pages": (total_pools + size - 1) // size
            }
        }
    except Exception as e:
        rest_logger.error(f"Error getting daily active pools: {e}")
        response.status_code = 500
        return {"error": "Failed to retrieve daily active pools"}


    
