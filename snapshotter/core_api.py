"""
This module contains the core API endpoints for the Snapshotter service.

It includes functionality for health checks, epoch information retrieval,
project data fetching, and task status checking.
"""
import asyncio
import time
from fastapi import FastAPI
from fastapi import Request
from fastapi import Response
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from fastapi_pagination import add_pagination
from fastapi_pagination import Page
from fastapi_pagination.customization import CustomizedPage, UseParamsFields
from ipfs_client.main import AsyncIPFSClientSingleton
from pydantic import Field
from rpc_helper.rpc import RpcHelper
from typing import TypeVar
from web3 import Web3
import json

from snapshotter.settings.config import settings
from snapshotter.utils.data_utils import get_project_epoch_snapshot
from snapshotter.utils.data_utils import get_project_finalized_cid
from snapshotter.utils.data_utils import get_project_time_series_data
from snapshotter.utils.default_logger import default_logger
from snapshotter.utils.file_utils import read_json_file
from snapshotter.utils.models.data_models import TaskStatusRequest
from snapshotter.utils.redis.redis_conn import RedisPoolCache
from snapshotter.utils.redis.redis_keys import last_submitted_snapshot_raw_data_key
from computes.api.router import router as compute_router


rest_logger = default_logger.bind(module='CoreAPI')


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """Middleware to log all incoming requests"""
    async def dispatch(self, request: Request, call_next):
        start_time = time.time()
        path = request.url.path
        method = request.method
        client_ip = request.client.host if request.client else "unknown"
        query_params = str(request.query_params) if request.query_params else ""
        
        rest_logger.info(
            f"[REQUEST] {method} {path}{'?' + query_params if query_params else ''} from {client_ip}"
        )
        
        try:
            # Add timeout wrapper to detect hanging requests
            rest_logger.debug(f"[REQUEST] Calling next middleware/handler for {method} {path}")
            response = await call_next(request)
            process_time = time.time() - start_time
            rest_logger.info(
                f"[REQUEST] {method} {path} - Status: {response.status_code} - "
                f"Time: {process_time:.2f}s"
            )
            return response
        except asyncio.TimeoutError as e:
            process_time = time.time() - start_time
            rest_logger.error(
                f"[REQUEST] {method} {path} - TIMEOUT after {process_time:.2f}s: {e}",
                exc_info=True
            )
            raise
        except Exception as e:
            process_time = time.time() - start_time
            rest_logger.error(
                f"[REQUEST] {method} {path} - ERROR after {process_time:.2f}s: {e}",
                exc_info=True
            )
            raise


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
T = TypeVar("T")
Page = CustomizedPage[
    Page[T],
    UseParamsFields(size=Field(10, ge=1, le=30)),
]
add_pagination(app)

# Add request logging middleware (first, so it logs all requests)
app.add_middleware(RequestLoggingMiddleware)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*'],
)

# Test endpoint to verify routing
@app.get("/api/test")
async def test_endpoint(request: Request):
    """Test endpoint to verify routing works"""
    rest_logger.info("[TEST] Test endpoint called")
    return {"status": "ok", "path": request.url.path}

# Include the Uniswap V3 API router
app.include_router(compute_router)


@app.on_event('startup')
async def startup_boilerplate():
    """
    Initialize various state variables and caches required for the application to function properly.
    This function is called when the FastAPI application starts up.
    """
    rest_logger.info("=" * 80)
    rest_logger.info("Core API starting up...")
    rest_logger.info(f"Core API host: {settings.core_api.host}, port: {settings.core_api.port}")
    rest_logger.info("=" * 80)
    
    app.state.core_settings = settings
    app.state.local_user_cache = dict()
    # Initialize both anchor and main RPC helpers
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
    
    rest_logger.info("=" * 80)
    rest_logger.info("Core API startup complete - Ready to accept requests")
    rest_logger.info(f"Redis connection: {app.state.redis_conn is not None}")
    rest_logger.info(f"IPFS reader: {app.state.ipfs_reader_client is not None}")
    rest_logger.info("=" * 80)


@app.get('/health')
async def health_check(
    request: Request,
    response: Response,
):
    """
    Endpoint to check the health of the Snapshotter service.

    Parameters:
    request (Request): The incoming request object.
    response (Response): The outgoing response object.

    Returns:
    dict: A dictionary containing the status of the service.
    """
    return {'status': 'OK'}


@app.get('/current_epoch')
async def get_current_epoch(
    request: Request,
    response: Response,
):
    """
    Get the current epoch data from the protocol state contract.

    Args:
        request (Request): The incoming request object.
        response (Response): The outgoing response object.

    Returns:
        dict: A dictionary containing the current epoch data.
    """
    try:
        [current_epoch_data] = await request.app.state.anchor_rpc_helper.web3_call(
            tasks=[
                ('currentEpoch', [Web3.to_checksum_address(settings.data_market)]),
            ],
            contract_addr=protocol_state_contract_address,
            abi=protocol_state_contract_abi,
        )
        current_epoch = {
            'begin': current_epoch_data[0],
            'end': current_epoch_data[1],
            'epochId': current_epoch_data[2],
        }

    except Exception as e:
        rest_logger.exception(
            'Exception in get_current_epoch',
            e=e,
        )
        response.status_code = 500
        return {
            'status': 'error',
            'message': f'Unable to get current epoch, error: {e}',
        }

    return current_epoch


@app.get('/epoch/{epoch_id}')
async def get_epoch_info(
    request: Request,
    response: Response,
    epoch_id: int,
):
    """
    Get epoch information for a given epoch ID.

    Args:
        request (Request): The incoming request object.
        response (Response): The outgoing response object.
        epoch_id (int): The epoch ID for which to retrieve information.

    Returns:
        dict: A dictionary containing epoch information including timestamp, block number, and epoch end.
    """
    try:
        [epoch_info_data] = await request.app.state.anchor_rpc_helper.web3_call(
            tasks=[
                ('epochInfo', [Web3.to_checksum_address(settings.data_market), epoch_id]),
            ],
            contract_addr=protocol_state_contract_address,
            abi=protocol_state_contract_abi,
        )
        epoch_info = {
            'timestamp': epoch_info_data[0],
            'blocknumber': epoch_info_data[1],
            'epochEnd': epoch_info_data[2],
        }

    except Exception as e:
        rest_logger.exception(
            'Exception in get_current_epoch',
            e=e,
        )
        response.status_code = 500
        return {
            'status': 'error',
            'message': f'Unable to get current epoch, error: {e}',
        }

    return epoch_info


@app.get('/last_finalized_epoch/{project_id}')
async def get_project_last_finalized_epoch_info(
    request: Request,
    response: Response,
    project_id: str,
):
    """
    Get the last finalized epoch information for a given project.

    Args:
        request (Request): The incoming request object.
        response (Response): The outgoing response object.
        project_id (str): The ID of the project to get the last finalized epoch information for.

    Returns:
        dict: A dictionary containing the last finalized epoch information for the given project.
    """

    try:
        # Find the last finalized epoch from the contract
        [project_last_finalized_epoch] = await request.app.state.anchor_rpc_helper.web3_call(
            tasks=[
                ('lastSequencerFinalizedSnapshot', [Web3.to_checksum_address(settings.data_market), project_id]),
            ],
            contract_addr=protocol_state_contract_address,
            abi=protocol_state_contract_abi,
        )

        # Get epoch info for the last finalized epoch
        [epoch_info_data] = await request.app.state.anchor_rpc_helper.web3_call(
            tasks=[
                ('epochInfo', [Web3.to_checksum_address(settings.data_market), project_last_finalized_epoch]),
            ],
            contract_addr=protocol_state_contract_address,
            abi=protocol_state_contract_abi,
        )
        epoch_info = {
            'epochId': project_last_finalized_epoch,
            'timestamp': epoch_info_data[0],
            'blocknumber': epoch_info_data[1],
            'epochEnd': epoch_info_data[2],
        }

    except Exception as e:
        rest_logger.exception(
            'Exception in get_project_last_finalized_epoch_info',
            e=e,
        )
        response.status_code = 500
        return {
            'status': 'error',
            'message': f'Unable to get last finalized epoch for project {project_id}, error: {e}',
        }

    return epoch_info


@app.get('/data/{epoch_id}/{project_id}/')
async def get_data_for_project_id_epoch_id(
    request: Request,
    response: Response,
    project_id: str,
    epoch_id: int,
):
    """
    Get data for a given project and epoch ID.

    Args:
        request (Request): The incoming request.
        response (Response): The outgoing response.
        project_id (str): The ID of the project.
        epoch_id (int): The ID of the epoch.

    Returns:
        dict: The data for the given project and epoch ID.
    """
    if not settings.ipfs.url:
        response.status_code = 500
        return {
            'status': 'error',
            'message': f'IPFS url not set, /data API endpoint is unusable, please use /cid endpoint instead!',
        }
    try:
        snapshot_response = await get_project_epoch_snapshot(
            request.app.state.redis_conn,
            request.app.state.protocol_state_contract,
            request.app.state.anchor_rpc_helper,
            request.app.state.ipfs_reader_client,
            epoch_id,
            project_id,
        )
    except Exception as e:
        rest_logger.exception(
            'Exception in get_data_for_project_id_epoch_id',
            e=e,
        )
        response.status_code = 500
        return {
            'status': 'error',
            'message': f'Unable to get data for project_id: {project_id},'
            f' epoch_id: {epoch_id}, error: {e}',
        }

    if not snapshot_response.has_data:
        response.status_code = 404
        return {
            'status': 'error',
            'message': f'No data found for project_id: {project_id},'
            f' epoch_id: {epoch_id}',
        }
    
    # If we have an exact match, return its data
    if snapshot_response.exact_match:
        return snapshot_response.exact_match.data
    
    # If we have closest epochs info, return that
    if snapshot_response.closest_epochs:
        return {
            'status': 'closest_epochs',
            'previous': snapshot_response.closest_epochs.previous.dict() if snapshot_response.closest_epochs.previous else None,
            'next': snapshot_response.closest_epochs.next.dict() if snapshot_response.closest_epochs.next else None
        }
    
    # This should never happen since we checked has_data above
    response.status_code = 500
    return {
        'status': 'error',
        'message': f'Internal error: response has no data despite has_data check'
    }


@app.get('/cid/{epoch_id}/{project_id}/')
async def get_finalized_cid_for_project_id_epoch_id(
    request: Request,
    response: Response,
    project_id: str,
    epoch_id: int,
):
    """
    Get finalized cid for a given project_id and epoch_id.

    Args:
        request (Request): The incoming request.
        response (Response): The outgoing response.
        project_id (str): The project id.
        epoch_id (int): The epoch id.

    Returns:
        dict: The finalized cid for the given project_id and epoch_id.
    """

    try:
        data = await get_project_finalized_cid(
            request.app.state.redis_conn,
            request.app.state.protocol_state_contract,
            request.app.state.anchor_rpc_helper,
            request.app.state.ipfs_reader_client,
            epoch_id,
            project_id,
        )
    except Exception as e:
        rest_logger.exception(
            'Exception in get_finalized_cid_for_project_id_epoch_id',
            e=e,
        )
        response.status_code = 500
        return {
            'status': 'error',
            'message': f'Unable to get finalized cid for project_id: {project_id},'
            f' epoch_id: {epoch_id}, error: {e}',
        }

    if not data:
        response.status_code = 404
        return {
            'status': 'error',
            'message': f'No finalized cid found for project_id: {project_id},'
            f' epoch_id: {epoch_id}',
        }

    return data


@app.post('/task_status')
async def get_task_status_post(
    request: Request,
    response: Response,
    task_status_request: TaskStatusRequest,
):
    """
    Endpoint to get the status of a task for a given wallet address.

    Args:
        request (Request): The incoming request object.
        response (Response): The outgoing response object.
        task_status_request (TaskStatusRequest): The request body containing the task type and wallet address.

    Returns:
        dict: A dictionary containing the status of the task and a message.
    """
    # Check if wallet address is a valid EVM address
    try:
        Web3.to_checksum_address(task_status_request.wallet_address)
    except:
        response.status_code = 400
        return {
            'status': 'error',
            'message': f'Invalid wallet address: {task_status_request.wallet_address}',
        }

    # Construct project ID
    project_id = f'{task_status_request.task_type}:{task_status_request.wallet_address.lower()}:{settings.namespace}'

    try:
        # Get the last finalized epoch for the project
        [last_finalized_epoch] = await request.app.state.anchor_rpc_helper.web3_call(
            tasks=[
                ('lastFinalizedSnapshot', [Web3.to_checksum_address(settings.data_market), project_id]),
            ],
            contract_addr=protocol_state_contract_address,
            abi=protocol_state_contract_abi,
        )

    except Exception as e:
        rest_logger.exception(
            'Exception in get_current_epoch',
            e=e,
        )
        response.status_code = 500
        return {
            'status': 'error',
            'message': f'Unable to get last_finalized_epoch, error: {e}',
        }
    else:
        # Determine if the task is completed based on the last finalized epoch
        if last_finalized_epoch > 0:
            return {
                'completed': True,
                'message': f'Task {task_status_request.task_type} for wallet {task_status_request.wallet_address} was completed in epoch {last_finalized_epoch}',
            }
        else:
            return {
                'completed': False,
                'message': f'Task {task_status_request.task_type} for wallet {task_status_request.wallet_address} is not completed yet',
            }


# get data points at each step begining at start_time until epoch_id is reached for project_id
@app.get('/time_series/{epoch_id}/{start_time}/{step_seconds}/{project_id}')
async def get_time_series_data_for_project_id(
    request: Request,
    response: Response,
    epoch_id: int,
    start_time: int,
    step_seconds: int,
    project_id: str,
):
    """
    Get data points at each step begining at start_time until current_epoch for project_id.

    Args:
        request (Request): The incoming request.
        response (Response): The outgoing response.
        epoch_id (int): The epoch ID to end the series at.
        start_time (int): Unix timestamp in seconds of when to begin data
        step_seconds (int): Length of time in seconds between data points to gather
        project_id (str): The ID of the project to get the data points for.

    Returns:
        dict: The data for the given project and epoch ID.
    """

    try:

        [epoch_info_data] = await request.app.state.anchor_rpc_helper.web3_call(
            [request.app.state.protocol_state_contract.functions.epochInfo(epoch_id)],
            redis_conn=request.app.state.redis_pool,
        )

        end_timestamp = epoch_info_data[0]

        observations = int((end_timestamp - start_time) / step_seconds)

        if observations <= 0:
            rest_logger.exception(
                f'Invalid start time in get_time_series_data_for_project_id for project_id: {project_id}',
            )
            response.status_code = 500
            return {
                'status': 'error',
                'message': f'Unable to get time series data for project_id: {project_id},'
                f' start_time: {start_time}, step: {step_seconds}, error: Start timestamp is after current epoch timestamp',
            }
        elif observations > 200:
            rest_logger.exception(
                f'Requested too many observations in get_time_series_data_for_project_id for project_id: {project_id}',
            )
            response.status_code = 500
            return {
                'status': 'error',
                'message': f'Unable to get time series data for project_id: {project_id},'
                f' start_time: {start_time}, step: {step_seconds}, error: Too many observations requested, use smaller step size or increase start time',
            }

        data_list = await get_project_time_series_data(
            start_time,
            end_timestamp,
            step_seconds,
            epoch_id,
            request.app.state.redis_pool,
            request.app.state.protocol_state_contract,
            request.app.state.anchor_rpc_helper,
            request.app.state.ipfs_reader_client,
            project_id,
        )

    except Exception as e:
        rest_logger.exception(
            'Exception in get_time_series_data_for_project_id',
            e=e,
        )
        response.status_code = 500
        return {
            'status': 'error',
            'message': f'Unable to get time series data for project_id: {project_id},'
            f' start_time: {start_time}, step: {step_seconds}, error: {e}',
        }

    if not any(data for data in data_list):
        response.status_code = 404
        return {
            'status': 'error',
            'message': f'No time series data found for project_id: {project_id},'
            f' start_time: {start_time}, step: {step_seconds}',
        }

    return data_list


@app.get('/latest_epoch_info')
async def get_latest_epoch_info(
    request: Request,
    response: Response,
):

    project_id = f'activePools:{settings.data_market}:{settings.namespace}'

    last_submitted_snapshot_data_raw = await request.app.state.redis_conn.get(last_submitted_snapshot_raw_data_key(project_id))
    if last_submitted_snapshot_data_raw:
        last_submitted_snapshot_data = json.loads(last_submitted_snapshot_data_raw)
        snapshot_cid = last_submitted_snapshot_data['snapshotCid']
        epoch_id = int(last_submitted_snapshot_data['epochId'])
        snapshot = json.loads(last_submitted_snapshot_data['snapshot'])

        data = {
            'snapshot_cid': snapshot_cid,
            'pools': snapshot['pools'],
            'epoch_id': epoch_id,
        }
        return data
    return None

@app.get('/get_previous_epoch_info/{epoch_id}')
async def get_previous_epoch_info(
    request: Request,
    response: Response,
    epoch_id: int,
):
    """
    Get previous epoch info for a given epoch_id.
    """
    # onchain logic
    project_id = f'activePools:{settings.data_market}:{settings.namespace}'

    snapshot_response = await get_project_epoch_snapshot(
        request.app.state.redis_conn,
        request.app.state.protocol_state_contract,
        request.app.state.anchor_rpc_helper,
        request.app.state.ipfs_reader_client,
        epoch_id,
        project_id,
        seek=False,
        cleanup_previous_snapshots=True,
    )

    if snapshot_response.exact_match:
        data = {
            'snapshot_cid': snapshot_response.exact_match.snapshot_cid,
            'epoch_id': snapshot_response.exact_match.epoch_id,
            'pools': snapshot_response.exact_match.data['pools'],
        }
        return data
    return None
    # cache logic.
    # TODO: fill up CID from cached set appropriate for active pools
        # fetch from Redis for now 
    key = f"active_pools_per_block:{epoch_id}:{settings.namespace}"
    active_pools = {}
    # Retrieve all pools and their activity scores for this block
    block_active_pools = await request.app.state.redis_conn.zrange(key, 0, -1, withscores=True)
    # retry 2 times if no data is found with 500ms delay between retries
    for i in range(2):
        rest_logger.info(f"Retrying to get active pools for epoch {epoch_id}, attempt {i+1}")
        if block_active_pools:
            break
        await asyncio.sleep(0.5)
        block_active_pools = await request.app.state.redis_conn.zrange(key, 0, -1, withscores=True)
    # Process each pool's activity data
    for pool_address, score in block_active_pools:
        # Decode and normalize pool address
        pool_address = pool_address.decode('utf-8')
        pool_address = Web3.to_checksum_address(pool_address)
        
        # Accumulate activity score for this pool
        if pool_address not in active_pools:
            active_pools[pool_address] = 0
        active_pools[pool_address] += int(score)
  
    return {
        'snapshot_cid': 'dummyCid',
        'epoch_id': epoch_id,
        'pools': active_pools.keys() if active_pools else [],
    }


@app.get('/previous_snapshots_data/{pool_address}/{epoch_id}')
async def get_previous_snapshots_data(
    request: Request,
    response: Response,
    pool_address: str,
    epoch_id: int,
):
    """
    Get previous snapshots data for a given project_id and epoch_id.
    """

    project_id = f'baseSnapshot:{pool_address}:{settings.namespace}'

    # get submitted snapshot data from redis

    snapshot_response = await get_project_epoch_snapshot(
        request.app.state.redis_conn,
        request.app.state.protocol_state_contract,
        request.app.state.anchor_rpc_helper,
        request.app.state.ipfs_reader_client,
        epoch_id,
        project_id,
        seek=False,
        cleanup_previous_snapshots=False,
    )

    if snapshot_response.exact_match:
        previous_snapshots = snapshot_response.exact_match.data['previousSnapshots']
        return previous_snapshots
    return []