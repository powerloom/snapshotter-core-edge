import json
from typing import List
from unittest.mock import AsyncMock
from unittest.mock import patch

import pytest
from fakeredis import FakeAsyncRedis
from pytest_asyncio import fixture as async_fixture
from rpc_helper.rpc import RpcHelper
from web3 import Web3

from snapshotter.settings.config import settings
from snapshotter.utils.data_utils import get_project_epoch_snapshot_bulk
from snapshotter.utils.data_utils import get_project_finalized_cid
from snapshotter.utils.data_utils import get_project_finalized_cids_bulk
from snapshotter.utils.data_utils import get_submission_data_bulk
from snapshotter.utils.file_utils import read_json_file
from snapshotter.utils.redis.redis_keys import project_data_hmap

"""
Data Utils Test Suite

This test suite tests the data utility functions in the snapshotter module,
particularly focusing on CID retrieval and caching mechanisms.

The tests use:
- Real contracts from settings configuration
- Configured RPC endpoints from settings  
- Mock Redis instance (FakeAsyncRedis) for simulating Redis operations
- Mock IPFS reader for testing data retrieval

To run the tests:
poetry run python -m pytest snapshotter/tests/data_utils_test.py
"""


@async_fixture(scope='module')
async def mock_redis():
    """Create a FakeAsyncRedis instance for testing."""
    fake_redis = FakeAsyncRedis()
    yield fake_redis
    await fake_redis.close()


@async_fixture(scope='module')
async def rpc_helper():
    """Initialize RpcHelper with settings configuration."""
    helper = RpcHelper(settings.anchor_chain_rpc)
    await helper.init()
    yield helper


@async_fixture(scope='module')
async def protocol_state_contract(rpc_helper):
    """Get the protocol state contract from settings."""
    protocol_state_contract_abi = read_json_file('snapshotter/static/abis/ProtocolContract.json')
    
    # Create contract instance using the RPC helper's web3 client
    w3_client = rpc_helper.get_current_node()['web3_client']
    contract = w3_client.eth.contract(
        address=Web3.to_checksum_address(settings.protocol_state.address),
        abi=protocol_state_contract_abi,
    )
    return contract


@async_fixture(scope='module')
async def project_id():
    yield 'test_project_id'


@async_fixture(scope='module')
async def epoch_ids():
    """Use realistic epoch IDs."""
    # Use a small range of recent epochs for testing (must be > epoch_offset of 22400000)
    yield list(range(22500000, 22500010))


@async_fixture(scope='module')
async def ipfs_reader():
    """Initialize a mock IPFS reader for testing."""
    reader = AsyncMock()
    reader.cat = AsyncMock(return_value=json.dumps({'key': 'value'}).encode('utf-8'))
    yield reader


@pytest.mark.asyncio(loop_scope='module')
async def test_get_project_finalized_cid_success(protocol_state_contract, mock_redis, rpc_helper, project_id: str):
    """
    Test `get_project_finalized_cid` function when CID is found in Redis cache.
    """
    epoch_id = 22500001
    expected_cid = f'QmTestCID{project_id}{epoch_id}'

    # Mock Redis data
    await mock_redis.hset(
        project_data_hmap(project_id=project_id), 
        epoch_id, 
        json.dumps({"snapshot_cid": expected_cid, "status": 1})
    )

    cid = await get_project_finalized_cid(
        redis_conn=mock_redis,
        state_contract_obj=protocol_state_contract,
        rpc_helper=rpc_helper,
        ipfs_reader=AsyncMock(),
        epoch_id=epoch_id,
        project_id=project_id,
    )

    assert cid == expected_cid

    # clean slate redis
    await mock_redis.flushall()


@pytest.mark.asyncio(loop_scope='module')
async def test_get_project_finalized_cid_not_found_returns_none(protocol_state_contract, mock_redis, rpc_helper, project_id):
    """
    Test `get_project_finalized_cid` function when CID is not found anywhere.
    
    Since we're using real contracts, we expect this to return None for non-existent data
    rather than trying to create test data on the blockchain.
    """
    epoch_id = 99999999  # Use a very high epoch that likely doesn't exist (must be > epoch_offset)
    
    raw_data = await mock_redis.hget(
        project_data_hmap(project_id=project_id),
        epoch_id,
    )
    data = json.loads(raw_data) if raw_data else None
    assert not data, 'Data should not be cached in Redis'

    cid = await get_project_finalized_cid(
        redis_conn=mock_redis,
        state_contract_obj=protocol_state_contract,
        rpc_helper=rpc_helper,
        ipfs_reader=AsyncMock(),
        epoch_id=epoch_id,
        project_id=project_id,
    )

    # For non-existent data, should return None
    assert cid is None

    # clean slate redis
    await mock_redis.flushall()


@pytest.mark.asyncio(loop_scope='module')
async def test_get_project_finalized_cids_bulk_cached(
    protocol_state_contract,
    mock_redis,
    rpc_helper,
    project_id: str,
    epoch_ids: List[int],
    ipfs_reader,
):
    """
    Test `get_project_finalized_cids_bulk` function with cached data.
    """
    expected_cids = [f'QmTestCID{project_id}{epoch_id}' for epoch_id in epoch_ids]

    # Populate Redis with cached CIDs
    for cid, epoch_id in zip(expected_cids, epoch_ids):
        await mock_redis.hset(
            project_data_hmap(project_id=project_id), 
            epoch_id, 
            json.dumps({"snapshot_cid": cid, "status": 1})
        )

    # Mock the dependent functions and get_project_config to avoid RPC calls
    mock_project_config = AsyncMock()
    mock_project_config.keep_previous_snapshot_data = False
    
    with patch('snapshotter.utils.data_utils.get_project_config', return_value=mock_project_config), \
         patch('snapshotter.utils.data_utils.get_project_first_epoch', AsyncMock(return_value=min(epoch_ids) - 1000)), \
         patch('snapshotter.utils.data_utils.get_last_submitted_snapshot_data', AsyncMock(return_value=None)), \
         patch('snapshotter.utils.data_utils.get_project_last_finalized_epoch', AsyncMock(return_value=max(epoch_ids) + 1000)), \
         patch('snapshotter.utils.data_utils.redis_bitmap.get_bits_in_range', AsyncMock(return_value=[])):

        cids, _ = await get_project_finalized_cids_bulk(
            redis_conn=mock_redis,
            state_contract_obj=protocol_state_contract,
            rpc_helper=rpc_helper,
            ipfs_reader=ipfs_reader,
            epoch_id_min=min(epoch_ids),
            epoch_id_max=max(epoch_ids),
            project_id=project_id,
        )

        assert cids == expected_cids

    # Clean slate Redis
    await mock_redis.flushall()


@pytest.mark.asyncio(loop_scope='module')
async def test_get_submission_data_bulk_ensure_complete_true(
    mock_redis,
    ipfs_reader,
    epoch_ids: List[int],
):
    """
    Test `get_submission_data_bulk` function with ensure_complete=True.
    Ensures that if any CID fetch fails, the function returns an empty list.
    """
    # Arrange
    cids = [f'invalid_cid{epoch_id}' for epoch_id in epoch_ids]
    # Each CID will be retried 3 times due to @retry decorator, so we need 3 exceptions per CID
    side_effects = []
    for _ in cids:
        side_effects.extend([Exception('Invalid IPFS Data')] * 3)
    ipfs_reader.cat = AsyncMock(side_effect=side_effects)

    # Act
    result = await get_submission_data_bulk(
        redis_conn=mock_redis,
        cids=cids,
        ipfs_reader=ipfs_reader,
        project_id=None,
        ensure_complete=True,
    )

    # Assert - when ensure_complete=True and any fetch fails, should return empty list
    assert result == []
    # Verify all CIDs were attempted to be fetched (with retry attempts)
    # Each failing CID gets 3 retry attempts due to the @retry decorator
    expected_calls = len(cids) * 3  # All CIDs fail, each gets 3 attempts
    assert ipfs_reader.cat.call_count == expected_calls

    # clean slate
    await mock_redis.flushall()
    ipfs_reader.cat.reset_mock()


@pytest.mark.asyncio(loop_scope='module')
async def test_get_submission_data_bulk_ensure_complete_false(
    mock_redis,
    epoch_ids: List[int],
):
    """
    Test `get_submission_data_bulk` function with ensure_complete=False.
    Ensures that partial failures result in empty dicts in the returned list.
    """
    # Arrange
    cids = [f'valid_cid{epoch_id}' for epoch_id in epoch_ids]
    invalid_cid = 'invalid_cid'
    cids[-1] = invalid_cid
    ipfs_reader = AsyncMock()
    
    # Set up side effects: successful calls return data on first attempt,
    # failing call gets 3 retry attempts
    side_effects = []
    for i, cid in enumerate(cids):
        if i == len(cids) - 1:  # Last CID fails
            side_effects.extend([Exception('Invalid IPFS Data')] * 3)
        else:  # Successful CIDs
            side_effects.append(json.dumps({'key': 'value'}).encode('utf-8'))
    
    ipfs_reader.cat = AsyncMock(side_effect=side_effects)

    result = await get_submission_data_bulk(
        redis_conn=mock_redis,
        cids=cids,
        ipfs_reader=ipfs_reader,
        project_id=None,
        ensure_complete=False,
    )

    # Assert - when ensure_complete=False, successful fetches return data, failed ones return empty dict
    expected_result = [{'key': 'value'} for _ in range(len(cids) - 1)]
    expected_result.append({})
    assert result == expected_result
    # Verify all CIDs were attempted to be fetched (with retry attempts)
    # Successful CIDs get 1 attempt, failing CID gets 3 attempts due to @retry decorator
    successful_calls = len(cids) - 1  # 9 successful CIDs, 1 call each
    failed_calls = 3  # 1 failing CID, 3 retry attempts
    expected_calls = successful_calls + failed_calls
    assert ipfs_reader.cat.call_count == expected_calls

    # clean slate
    await mock_redis.flushall()
    ipfs_reader.cat.reset_mock()


@pytest.mark.asyncio(loop_scope='module')
async def test_get_project_epoch_snapshot_bulk_ensure_complete_true(
    mock_redis,
    rpc_helper,
    epoch_ids: List[int],
    project_id: str,
    protocol_state_contract,
    ipfs_reader,
):
    """
    Test `get_project_epoch_snapshot_bulk` function with ensure_complete=True.
    Ensures that if any submission data fetch fails, the entire function returns an empty list.
    """
    # Arrange
    epoch_id_min = min(epoch_ids)
    epoch_id_max = max(epoch_ids)
    expected_cids = [f'QmTestCID{project_id}{epoch_id}' for epoch_id in epoch_ids]

    # Each CID will be retried 3 times due to @retry decorator
    side_effects = []
    for _ in expected_cids:
        side_effects.extend([Exception('Invalid IPFS Data')] * 3)
    ipfs_reader.cat = AsyncMock(side_effect=side_effects)

    # Mock get_project_finalized_cids_bulk to return predefined CIDs
    with patch('snapshotter.utils.data_utils.get_project_finalized_cids_bulk', AsyncMock(return_value=(expected_cids, epoch_id_min))):
        result = await get_project_epoch_snapshot_bulk(
            redis_conn=mock_redis,
            state_contract_obj=protocol_state_contract,
            rpc_helper=rpc_helper,
            ipfs_reader=ipfs_reader,
            epoch_id_min=epoch_id_min,
            epoch_id_max=epoch_id_max,
            project_id=project_id,
            ensure_complete=True,
        )

    assert result == []

    # clean slate
    await mock_redis.flushall()
    ipfs_reader.cat.reset_mock()


@pytest.mark.asyncio(loop_scope='module')
async def test_get_project_epoch_snapshot_bulk_ensure_complete_false(
    mock_redis,
    rpc_helper,
    epoch_ids: List[int],
    project_id: str,
    protocol_state_contract,
):
    """
    Test `get_project_epoch_snapshot_bulk` function with ensure_complete=False.
    Ensures that partial submission data fetch failures do not result in an empty list.
    """
    # Arrange
    epoch_id_min = min(epoch_ids)
    epoch_id_max = max(epoch_ids)
    expected_cids = [f'QmTestCID{project_id}{epoch_id}' for epoch_id in epoch_ids]

    # Set up side effects: most calls succeed on first attempt, last one fails with retries
    side_effects = []
    for i, cid in enumerate(expected_cids):
        if i == len(expected_cids) - 1:  # Last CID fails
            side_effects.extend([Exception('Invalid IPFS Data')] * 3)
        else:  # Successful CIDs
            side_effects.append(json.dumps({'key': 'value'}).encode('utf-8'))
    
    ipfs_reader = AsyncMock()
    ipfs_reader.cat = AsyncMock(side_effect=side_effects)

    expected_submission_data = [{'key': 'value'} for _ in range(len(epoch_ids) - 1)]
    expected_submission_data.append({})

    # Mock get_project_finalized_cids_bulk to return predefined CIDs
    with patch('snapshotter.utils.data_utils.get_project_finalized_cids_bulk', AsyncMock(return_value=(expected_cids, epoch_id_min))):
        result = await get_project_epoch_snapshot_bulk(
            redis_conn=mock_redis,
            state_contract_obj=protocol_state_contract,
            rpc_helper=rpc_helper,
            ipfs_reader=ipfs_reader,
            epoch_id_min=epoch_id_min,
            epoch_id_max=epoch_id_max,
            project_id=project_id,
            ensure_complete=False,
        )

    assert result == expected_submission_data

    # clean slate
    await mock_redis.flushall()
    ipfs_reader.cat.reset_mock()
