import asyncio
import json

import pytest
from pytest_asyncio import fixture as async_fixture
from rpc_helper.rpc import RpcHelper
from rpc_helper.rpc import get_contract_abi_dict
from rpc_helper.rpc import get_event_sig_and_abi
from rpc_helper.utils.models.settings_model import RPCConfigFull
from rpc_helper.utils.models.settings_model import RPCNodeConfig
from web3 import AsyncHTTPProvider
from web3 import AsyncWeb3
from web3.contract import AsyncContract

from snapshotter.settings.config import settings

"""
RPC Helper Test Suite

This test suite is designed to test the RpcHelper class, which provides various methods for interacting with Ethereum nodes via RPC calls.

Requirements:
- A local development Hardhat node is required to run these tests.

To run the tests:
1. Ensure you have a local Hardhat node running (typically on http://127.0.0.1:8545).
2. Run the tests using Poetry with the following command:
   poetry run python -m pytest snapshotter/tests/rpc_helper_test.py

What these tests cover:
- Initialization of RpcHelper
- Basic RPC calls (e.g., getting current block number, transaction receipts)
- Contract interactions (function calls)
- Batch RPC calls (e.g., getting balances or calling functions across a range of blocks)
- Event log retrieval
- Rate limiting behavior

Note: These tests use a custom RPC configuration (TEST_RPC_CONFIG) that points to the local Hardhat node. Ensure your local node matches this configuration.
"""

# Custom RPC config for testing with Hardhat

TEST_RPC_CONFIG = RPCConfigFull(
    full_nodes=[
        RPCNodeConfig(
            url='http://127.0.0.1:8545',
        ),
    ],
    connection_limits=settings.rpc.connection_limits,
    retry=settings.rpc.retry,
    semaphore_value=settings.rpc.semaphore_value,
    force_archive_blocks=settings.rpc.force_archive_blocks,
    request_time_out=settings.rpc.request_time_out,
    polling_interval=settings.rpc.polling_interval,
)

@async_fixture(scope='module')
async def web3():
    w3 = AsyncWeb3(AsyncHTTPProvider('http://127.0.0.1:8545'))
    yield w3

@async_fixture(scope='module')
async def snapshot(web3: AsyncWeb3):
    # Take a snapshot of the current state
    snapshot_id = await web3.provider.make_request('evm_snapshot', [])
    print(f'Snapshot created with ID: {snapshot_id}')

    yield snapshot_id['result']

    # Revert to the snapshot after all tests are done
    revert_result = await web3.provider.make_request('evm_revert', [snapshot_id['result']])
    print(f'Snapshot revert result: {revert_result}')

    if not revert_result['result']:
        raise Exception('Snapshot revert failed')

@async_fixture(scope='module')
async def rpc_helper(snapshot):
    helper = RpcHelper(rpc_settings=TEST_RPC_CONFIG)
    await helper.init()
    yield helper

@async_fixture(scope='module')
async def rpc_helper_override(snapshot):
    override_config = TEST_RPC_CONFIG
    override_config.full_nodes[0].rate_limit = RATE_LIMIT_OVERRIDE
    override_helper = RpcHelper(rpc_settings=override_config)
    await override_helper.init()
    yield override_helper

@async_fixture(scope='module')
async def protocol_contract(web3: AsyncWeb3, snapshot):
    # Load Implementation ABI and Bytecode
    with open('snapshotter/static/abis/ProtocolContract.json', 'r') as abi_file:
        implementation_abi = json.load(abi_file)

    with open('snapshotter/tests/static/bytecode/protocol_state.json', 'r') as bytecode_file:
        implementation_bytecode_json = json.load(bytecode_file)
        implementation_bytecode = implementation_bytecode_json['bytecode']

    # Deploy the Implementation Contract
    implementation_contract = web3.eth.contract(abi=implementation_abi, bytecode=implementation_bytecode)
    tx_hash_impl = await implementation_contract.constructor().transact()
    tx_receipt_impl = await web3.eth.wait_for_transaction_receipt(tx_hash_impl)
    implementation_address = tx_receipt_impl['contractAddress']

    accounts = await web3.eth.accounts

    # Load UUPSUpgradeable ABI and Bytecode
    with open('snapshotter/tests/static/abi/UUPSUpgradeable.json', 'r') as proxy_abi_file:
        proxy_abi = json.load(proxy_abi_file)

    with open('snapshotter/tests/static/bytecode/uups_upgradeable.json', 'r') as proxy_bytecode_file:
        proxy_bytecode_json = json.load(proxy_bytecode_file)
        proxy_bytecode = proxy_bytecode_json['bytecode']

    # Initialize Proxy Constructor Parameters
    # The initializer must be encoded using the implementation's initializer function
    initializer = implementation_contract.encodeABI(fn_name='initialize', args=[web3.to_checksum_address(accounts[0])])

    # Deploy the Proxy Contract
    proxy_contract = web3.eth.contract(abi=proxy_abi, bytecode=proxy_bytecode)
    tx_hash_proxy = await proxy_contract.constructor(
        implementation_address,
        initializer,
    ).transact({'from': accounts[0]})
    tx_receipt_proxy = await web3.eth.wait_for_transaction_receipt(tx_hash_proxy)
    proxy_address = tx_receipt_proxy['contractAddress']

    proxy_instance = web3.eth.contract(address=proxy_address, abi=implementation_abi)

    # verify initialization
    owner = await proxy_instance.functions.owner().call()
    assert owner == web3.to_checksum_address(accounts[0]), 'Initialization failed.'

    yield proxy_instance

@pytest.mark.asyncio(loop_scope='module')
async def test_get_current_block_number(rpc_helper, web3):
    result = await rpc_helper.get_current_block_number()
    assert isinstance(result, int)
    assert result == await web3.eth.block_number

@pytest.mark.asyncio(loop_scope='module')
async def test_get_transaction_receipt(rpc_helper, web3: AsyncWeb3, protocol_contract: AsyncContract):
    accounts = await web3.eth.accounts
    tx_hash = await protocol_contract.functions.updateSnapshotterState(accounts[0]).transact({'from': accounts[0]})
    await web3.eth.wait_for_transaction_receipt(tx_hash)

    result: dict = await rpc_helper.get_transaction_receipt(tx_hash.hex())
    assert result['transactionHash'] == tx_hash
    assert 'blockNumber' in result
    assert 'gasUsed' in result

@pytest.mark.asyncio(loop_scope='module')
async def test_web3_call(rpc_helper, protocol_contract):
    result: dict = await rpc_helper.web3_call([('owner', [])], protocol_contract.address, protocol_contract.abi)
    assert result[0] == await protocol_contract.functions.owner().call()

@pytest.mark.asyncio(loop_scope='module')
async def test_batch_eth_get_balance_on_block_range(rpc_helper: RpcHelper, web3: AsyncWeb3):
    accounts = await web3.eth.accounts
    account = accounts[0]
    start_block = await web3.eth.block_number

    for _ in range(3):
        tx_hash = await web3.eth.send_transaction({
            'from': account,
            'to': accounts[1],
            'value': web3.to_wei(1, 'ether'),
        })
        await web3.eth.wait_for_transaction_receipt(tx_hash)

    end_block = await web3.eth.block_number
    balances = await rpc_helper.batch_eth_get_balance_on_block_range(account, start_block, end_block)

    assert len(balances) == end_block - start_block + 1
    assert all(isinstance(balance, int) for balance in balances)
    assert balances[0] > balances[-1]

@pytest.mark.asyncio(loop_scope='module')
async def test_batch_eth_call_on_block_range(rpc_helper: RpcHelper, web3: AsyncWeb3, protocol_contract: AsyncContract):
    accounts = await web3.eth.accounts
    start_block = await web3.eth.block_number

    for i in range(1, 4):
        tx_hash = await protocol_contract.functions.updateSnapshotterState(
            accounts[i],
        ).transact({'from': accounts[0]})
        await web3.eth.wait_for_transaction_receipt(tx_hash)

    end_block = await web3.eth.block_number

    abi_dict = get_contract_abi_dict(protocol_contract.abi)

    results: list[tuple[int]] = await rpc_helper.batch_eth_call_on_block_range(abi_dict, 'snapshotterState', protocol_contract.address, start_block, end_block)

    assert len(results) == end_block - start_block + 1
    assert web3.to_checksum_address(results[-1][0]) == web3.to_checksum_address(accounts[3])

@pytest.mark.asyncio(loop_scope='module')
async def test_get_events_logs(rpc_helper: RpcHelper, web3: AsyncWeb3, protocol_contract: AsyncContract):
    accounts = await web3.eth.accounts
    start_block = await web3.eth.block_number

    for i in range(1, 4):
        tx_hash = await protocol_contract.functions.transferOwnership(
            accounts[i],
        ).transact({'from': accounts[i-1]})
        await web3.eth.wait_for_transaction_receipt(tx_hash)

    end_block = await web3.eth.block_number

    EVENT_SIGS = {
        'OwnershipTransferred': 'OwnershipTransferred(address,address)',
    }

    EVENT_ABI = {
        'OwnershipTransferred': protocol_contract.events.OwnershipTransferred._get_event_abi(),
    }

    event_sig, event_abi = get_event_sig_and_abi(EVENT_SIGS, EVENT_ABI)

    events: list[dict[str, dict]] = await rpc_helper.get_events_logs(
        protocol_contract.address,
        end_block,
        start_block,
        event_sig,
        event_abi,
    )

    assert len(events) == 3
    for i, event in enumerate(events):
        assert event['event'] == 'OwnershipTransferred'
        assert event['args']['previousOwner'] == accounts[i]
        assert event['args']['newOwner'] == accounts[i+1]

@pytest.mark.asyncio(loop_scope='module')
async def test_rate_limiting(rpc_helper_override: RpcHelper):
    samples = 10
    start_time = asyncio.get_event_loop().time()
    tasks = [rpc_helper_override.get_current_block_number() for _ in range(samples)]
    await asyncio.gather(*tasks)
    end_time = asyncio.get_event_loop().time()

    elapsed_time = end_time - start_time
    expected_time = (samples - 1) / RATE_LIMIT_OVERRIDE.requests_per_second

    assert elapsed_time >= expected_time, f'Rate limiting not working as expected. Elapsed time: {elapsed_time}, Expected time: {expected_time}'
