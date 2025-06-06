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
import asyncio

from computes.utils.models.message_models import UniswapBaseSnapshot
from snapshotter.settings.config import settings
from snapshotter.utils.data_utils import (
    get_uniswap_trade_volume_agg,
    get_uniswap_v3_base_snapshot,
    get_uniswap_v3_eth_price_snapshot,
    get_uniswap_price_series_agg,
    get_uniswap_v3_token_pools_snapshot, 
    get_uniswap_v3_token_price_pool, 
    get_uniswap_v3_token_prices_all_snapshot, 
    get_uniswap_v3_trades_snapshot,
    get_uniswapv3_snapshot,
    get_uniswap_v3_pool_metadata,
    get_uniswap_v3_pool_trades,
    get_uniswap_v3_base_snapshots_for_token,
    get_uniswap_trade_volume_agg_all_pools,
    get_active_pools,
    get_active_tokens,
)
from snapshotter.utils.default_logger import default_logger
from snapshotter.utils.file_utils import read_json_file
from snapshotter.utils.redis.redis_conn import RedisPoolCache
from computes.settings.config import settings as compute_settings

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
    app.state.rpc_helper = RpcHelper(rpc_settings=settings.rpc)
    await app.state.rpc_helper.init()
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
    

@app.get('/snapshot/base_all_pools/{token_address}')
async def get_token_base_snapshots(
    request: Request,
    response: Response,
    token_address: str,
):
    token_address = Web3.to_checksum_address(token_address)
    tokens_to_ignore = [compute_settings.contract_addresses.WETH]
    if token_address in tokens_to_ignore:
        response.status_code = 400
        return {"error": "Invalid token address"}
    
    try:
        base_snapshots = await get_uniswap_v3_base_snapshots_for_token(
            redis_conn=app.state.redis_conn,
            anchor_rpc_helper=app.state.anchor_rpc_helper,
            ipfs_reader=app.state.ipfs_reader_client,
            protocol_state_contract=app.state.protocol_state_contract,
            token_address=token_address,
        )
        if not base_snapshots:
            response.status_code = 404
            return {"error": "Base snapshots not found"}
        else:
            response.status_code = 200
            return base_snapshots
    except Exception as e:
        rest_logger.error(f"Error getting base snapshots for {token_address}: {e}")
        response.status_code = 500
        return {"error": "Base snapshots not found"}


@app.get('/snapshot/base/{pool_address}')
@app.get('/snapshot/base/{pool_address}/{block_number}')
async def get_base_snapshot(
    request: Request,
    response: Response,
    pool_address: str,
    block_number: Optional[int] = None,
):
    pool_address = Web3.to_checksum_address(pool_address)
    try:
        base_snapshot = await get_uniswap_v3_base_snapshot(
            redis_conn=app.state.redis_conn,
            protocol_state_contract=app.state.protocol_state_contract,
            anchor_rpc_helper=app.state.anchor_rpc_helper,
            ipfs_reader=app.state.ipfs_reader_client,
            pool_address=pool_address,
            block_number=block_number,
        )
        if not base_snapshot:
            response.status_code = 404
            return {"error": "Base snapshot not found"}
        else:
            response.status_code = 200
            return base_snapshot
    except Exception as e:
        rest_logger.error(f"Error getting base snapshot for {pool_address} at block {block_number}: {e}")
        response.status_code = 500
        return {"error": "Base snapshot not found"}
    

@app.get('/snapshot/trades/{pool_address}')
@app.get('/snapshot/trades/{pool_address}/{block_number}')
async def get_trades_snapshot(
    request: Request,
    response: Response,
    pool_address: str,
    block_number: Optional[int] = None,
):
    pool_address = Web3.to_checksum_address(pool_address)
    try:
        trades_snapshot = await get_uniswap_v3_trades_snapshot(
            redis_conn=app.state.redis_conn,
            protocol_state_contract=app.state.protocol_state_contract,
            anchor_rpc_helper=app.state.anchor_rpc_helper,
            ipfs_reader=app.state.ipfs_reader_client,
            pool_address=pool_address,
            block_number=block_number,
        )
        if not trades_snapshot:
            response.status_code = 404
            return {"error": "Trades snapshot not found"}
        else:
            response.status_code = 200
            return trades_snapshot
    except Exception as e:
        rest_logger.error(f"Error getting trades snapshot for {pool_address} at block {block_number}: {e}")
        response.status_code = 500
        return {"error": "Trades snapshot not found"}
    

@app.get('/tokenPrices/all/{token_address}')
@app.get('/tokenPrices/all/{token_address}/{block_number}')
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


@app.get('/tradeVolumeAllPools/{token_address}/{time_interval}')
async def get_trade_volume_agg_all_pools(
    request: Request,
    response: Response,
    token_address: str,
    time_interval: int,
):  
    token_address = Web3.to_checksum_address(token_address)
    tokens_to_ignore = [compute_settings.contract_addresses.WETH]
    if token_address in tokens_to_ignore:
        response.status_code = 400
        return {"error": "Invalid token address"}
    
    try:
        trade_volume_agg = await get_uniswap_trade_volume_agg_all_pools(
            redis_conn=app.state.redis_conn,
            anchor_rpc_helper=app.state.anchor_rpc_helper,
            ipfs_reader=app.state.ipfs_reader_client,
            protocol_state_contract=app.state.protocol_state_contract,
            time_interval=time_interval,
            token_address=token_address,
        )
    except Exception as e:
        rest_logger.opt(exception=True).error(f"Error getting trade volume agg for {token_address}: {e}")
        response.status_code = 500
        return {"error": "Trade volume agg not found"}
    if not trade_volume_agg:
        response.status_code = 404
        return {"error": "Trade volume agg not found"}
    else:
        response.status_code = 200
        return trade_volume_agg


@app.get('/tradeVolume/{pool_address}/{time_interval}')
async def get_trade_volume_agg(
    request: Request,
    response: Response,
    pool_address: str,
    time_interval: int,
):
    pool_address = Web3.to_checksum_address(pool_address)
    project_id = f"baseSnapshot:{pool_address}:{settings.namespace}"
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
    

@app.get('/poolTrades/{pool_address}/{start_timestamp}/{end_timestamp}')
async def get_pool_trades(
    request: Request,
    response: Response,
    pool_address: str,
    start_timestamp: int,
    end_timestamp: int,
):
    pool_address = Web3.to_checksum_address(pool_address)
    project_id = f"tradesSnapshot:{pool_address}:{settings.namespace}"
    try:
        pool_trades = await get_uniswap_v3_pool_trades(
            redis_conn=app.state.redis_conn,
            anchor_rpc_helper=app.state.anchor_rpc_helper,
            rpc_helper=app.state.rpc_helper,
            ipfs_reader=app.state.ipfs_reader_client,
            project_id=project_id,
            pool_address=pool_address,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            protocol_state_contract=app.state.protocol_state_contract,
        )
    except Exception as e:
        rest_logger.opt(exception=True).error(f"Error getting pool trades for {pool_address}: {e}")
        response.status_code = 500
        return {"error": "Pool trades not found"}
    
    if not pool_trades:
        response.status_code = 404
        return {"error": "Pool trades not found"}
    else:
        response.status_code = 200
        return pool_trades


@app.get('/timeSeries/{token_address}/{pool_address}/{time_interval}/{step_seconds}')
async def get_token_price_series(
    request: Request,
    response: Response,
    token_address: str,
    pool_address: str,
    time_interval: int,
    step_seconds: int,
):
    token_address = Web3.to_checksum_address(token_address)
    pool_address = Web3.to_checksum_address(pool_address)
    project_id = f"baseSnapshot:{pool_address}:{settings.namespace}"
    try:
        token_price_series = await get_uniswap_price_series_agg(
            redis_conn=app.state.redis_conn,
            protocol_state_contract=app.state.protocol_state_contract,
            rpc_helper=app.state.rpc_helper,
            anchor_rpc_helper=app.state.anchor_rpc_helper,
            ipfs_reader=app.state.ipfs_reader_client,
            token_address=token_address,
            time_interval=time_interval,
            project_id=project_id,
            step_seconds=step_seconds,
        )
        if not token_price_series:
            response.status_code = 404
            return {"error": "Token price series not found"}
        else:
            response.status_code = 200
            return token_price_series
    except Exception as e:
        rest_logger.error(f"Error getting token price series for {token_address}: {e}")
        response.status_code = 500
        return {"error": "Token price series not found"}

@app.get(
    '/dailyActiveTokens',
    summary="Get daily active tokens with pagination",
    description=(
        "Retrieves a paginated list of active tokens for the current day, "
        "sorted by frequency. Use page and size parameters to control pagination."
    ),
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
    metadata: bool = Query(
        default=False,
        description="Include token metadata in the response",
        example=False
    ),
    time_interval: int = Query(
        default=86400,
        description="Time interval in seconds",
        example=86400
    ),
):
    """
    Get a paginated list of active tokens for the current day.
    
    Parameters:
    - page: The page number to retrieve (starts at 1)
    - size: Number of items per page (default: 50, max: 100)
    - metadata: Include token metadata in the response (default: False)
    
    Returns:
    - List of active tokens with their frequencies and optional metadata
    - Pagination metadata including total count and pages
    """
    
    try:
        active_tokens = await get_active_tokens(
            redis_conn=app.state.redis_conn,
            protocol_state_contract=app.state.protocol_state_contract,
            anchor_rpc_helper=app.state.anchor_rpc_helper,
            ipfs_reader=app.state.ipfs_reader_client,
            time_interval=time_interval,
        )
        
        # Calculate start and end indices for pagination
        start_idx = (page - 1) * size
        end_idx = start_idx + size - 1
        
        # Get total count of tokens
        total_tokens = len(active_tokens)
        
        # Get paginated tokens from the sorted set
        active_tokens = active_tokens[start_idx:end_idx+1]
        
        # Format the response
        tokens_data = []
        for token, score in active_tokens:
            token_data = {
                "token_address": token,
                "frequency": score
            }
            tokens_data.append(token_data)
        
        # Add metadata if requested (parallelized in batches)
        if metadata:
            
            # Process tokens in batches of 50
            batch_size = 50
            for i in range(0, len(tokens_data), batch_size):
                batch = tokens_data[i:i + batch_size]
                
                # Create metadata fetch tasks for this batch
                metadata_tasks = []
                for token_data in batch:
                    task = get_uniswap_v3_token_pools_snapshot(
                        redis_conn=app.state.redis_conn,
                        protocol_state_contract=app.state.protocol_state_contract,
                        anchor_rpc_helper=app.state.anchor_rpc_helper,
                        ipfs_reader=app.state.ipfs_reader_client,
                        token_address=Web3.to_checksum_address(token_data["token_address"]),
                    )
                    metadata_tasks.append(task)
                
                # Fetch metadata for all tokens in this batch in parallel
                try:
                    metadata_results = await asyncio.gather(*metadata_tasks, return_exceptions=True)
                    
                    # Assign metadata results back to token data
                    for j, metadata_result in enumerate(metadata_results):
                        if isinstance(metadata_result, Exception):
                            rest_logger.error(
                                f"Exception fetching metadata for token {batch[j]['token_address']}: {metadata_result}"
                            )
                            batch[j]["metadata"] = None
                        elif not metadata_result:
                            batch[j]["metadata"] = None
                        else:
                            if metadata_result.pools:
                                # Get any pool metadata from the pools dict
                                token_pool_metadata = next(iter(metadata_result.pools.values()))
                                if token_pool_metadata.token0.address == batch[j]["token_address"]:
                                    batch[j]["metadata"] = token_pool_metadata.token0
                                elif token_pool_metadata.token1.address == batch[j]["token_address"]:
                                    batch[j]["metadata"] = token_pool_metadata.token1
                                else:
                                    batch[j]["metadata"] = None
                            else:
                                batch[j]["metadata"] = None
                except Exception as e:
                    rest_logger.opt(exception=True).error(f"Error in batch metadata fetch: {e}")
                    # Set metadata to None for all tokens in this batch
                    for token_data in batch:
                        token_data["metadata"] = None
        
        response.status_code = 200
        return {
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
    metadata: bool = Query(
        default=False,
        description="Include pool metadata in the response",
        example=False
    ),
    time_interval: int = Query(
        default=86400,
        description="Time interval in seconds",
        example=86400
    ),
):
    """
    Get a paginated list of active pools for the current day.
    
    Parameters:
    - page: The page number to retrieve (starts at 1)
    - size: Number of items per page (default: 50, max: 100)
    - metadata: Include pool metadata in the response (default: False)
    
    Returns:
    - List of active pools with their frequencies and optional metadata
    - Pagination metadata including total count and pages
    """
    pools = await get_active_pools(
        redis_conn=app.state.redis_conn,
        protocol_state_contract=app.state.protocol_state_contract,
        anchor_rpc_helper=app.state.anchor_rpc_helper,
        ipfs_reader=app.state.ipfs_reader_client,
        time_interval=time_interval,
    )
    
    try:
        # Calculate start and end indices for pagination
        start_idx = (page - 1) * size
        end_idx = start_idx + size - 1

        total_pools = len(pools)
        active_pools = pools[start_idx:end_idx+1]
        
        # Format the response
        pools_data = []
        for pool, score in active_pools:
            pool_data = {
                "pool_address": pool,
                "frequency": score
            }
            pools_data.append(pool_data)
        
        # Add metadata if requested (parallelized in batches)
        if metadata:
            
            # Process pools in batches of 50
            batch_size = 50
            for i in range(0, len(pools_data), batch_size):
                batch = pools_data[i:i + batch_size]
                
                # Create metadata fetch tasks for this batch
                metadata_tasks = []
                for pool_data in batch:
                    task = get_uniswap_v3_pool_metadata(
                        redis_conn=app.state.redis_conn,
                        protocol_state_contract=app.state.protocol_state_contract,
                        anchor_rpc_helper=app.state.anchor_rpc_helper,
                        ipfs_reader=app.state.ipfs_reader_client,
                        pool_address=Web3.to_checksum_address(pool_data["pool_address"]),
                    )
                    metadata_tasks.append(task)
                
                # Fetch metadata for all pools in this batch in parallel
                try:
                    metadata_results = await asyncio.gather(*metadata_tasks, return_exceptions=True)
                    
                    # Assign metadata results back to pool data
                    for j, metadata_result in enumerate(metadata_results):
                        if isinstance(metadata_result, Exception):
                            rest_logger.error(
                                f"Exception fetching metadata for pool {batch[j]['pool_address']}: {metadata_result}"
                            )
                            batch[j]["metadata"] = None
                        else:
                            batch[j]["metadata"] = metadata_result
                except Exception as e:
                    rest_logger.error(f"Error in batch metadata fetch: {e}")
                    # Set metadata to None for all pools in this batch
                    for pool_data in batch:
                        pool_data["metadata"] = None
        
        response.status_code = 200
        return {
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


@app.get('/poolData/{pool_address}/{block_number}', summary='Returns the base snapshot for a given pool address and block number')
@app.get('/poolData/{pool_address}', summary='Returns the base snapshot for a given pool address and last finalized epoch/block number')
async def get_pool_data(
    request: Request,
    response: Response,
    pool_address: str,
    block_number: Optional[int] = None,
):
    pool_address = Web3.to_checksum_address(pool_address)
    result = await get_uniswapv3_snapshot(
        redis_conn=app.state.redis_conn,
        anchor_rpc_helper=app.state.anchor_rpc_helper,
        ipfs_reader=app.state.ipfs_reader_client,
        protocol_state_contract=app.state.protocol_state_contract,
        project_id=f"baseSnapshot:{pool_address.lower()}:{settings.namespace}",
        message_model=UniswapBaseSnapshot,
        block_number=block_number,
    )
    if not result:
        response.status_code = 404
        return {"error": "Base snapshot not found"}
    else:
        base_snapshot = result[1]
        response.status_code = 200
        return base_snapshot
    