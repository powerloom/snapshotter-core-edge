import pytest
import os # For fetching environment variables to compare against
from web3 import Web3
from web3.contract.contract import Contract

# Import client classes and data models for type checking and instantiation
from ipfs_client.settings.data_models import IPFSConfig
from rpc_helper.utils.models.settings_model import RPCConfigBase
from snapshotter.utils.models.settings_model import Redis as RedisConfigPydanticModel
from rpc_helper.rpc import RpcHelper
from ipfs_client.main import AsyncIPFSClient
from redis.asyncio import Redis as AsyncIORedis


# Helper to get expected values from environment (populated by .env.test)
def get_test_env_var(var_name, default=None):
    return os.getenv(var_name, default)

def test_app_settings_loaded_successfully(app_config):
    """Checks if the application's settings object was imported."""
    assert app_config is not None, \
        "Failed to import 'app_config' from 'snapshotter.settings.config'. "\
        "Ensure this path is correct and conftest.py has prepared settings."
    print("\nPASSED: test_app_settings_loaded_successfully")

def test_general_settings_are_correct(app_config):
    """Tests if general settings in app_config match .env.test values."""
    assert app_config is not None, "app_config not loaded."
    
    expected_namespace = get_test_env_var("TEST_NAMESPACE", "test_namespace_placeholder")
    expected_psc_address = get_test_env_var("TEST_PROTOCOL_STATE_CONTRACT_ADDRESS", "0xTestProtocolStateContractPlaceholder")
    
    assert hasattr(app_config, 'namespace'), "app_config missing 'namespace' attribute."
    assert app_config.namespace == expected_namespace, \
        f"Namespace mismatch: Expected {expected_namespace}, Got {app_config.namespace}"
    
    assert hasattr(app_config, 'protocol_state'), "app_config missing 'protocol_state' object."
    assert hasattr(app_config.protocol_state, 'address'), "app_config.protocol_state missing 'address' attribute."
    assert app_config.protocol_state.address == expected_psc_address, \
        f"PSC address mismatch: Expected {expected_psc_address}, Got {app_config.protocol_state.address}"
    # test whether a valid EVM address
    assert Web3.is_address(app_config.protocol_state.address), "PSC address is not a valid EVM address"

    print("\nPASSED: test_general_settings_are_correct")
    print(f"  Namespace from app_config: {app_config.namespace}")
    print(f"  Protocol State Contract Address from app_config: {app_config.protocol_state.address}")

def test_rpc_settings_are_correct(app_config):
    """Tests if RPC settings in app_config match .env.test values."""
    assert app_config is not None, "app_config not loaded."
    assert hasattr(app_config, 'rpc'), "app_config missing 'rpc' attribute."
    
    expected_rpc_url = get_test_env_var("TEST_RPC_URL_FULL_NODE_1", "http://localhost:8545/test_rpc")
    
    assert isinstance(app_config.rpc, RPCConfigBase), f"app_config.rpc is not an RPCConfigBase instance, type: {type(app_config.rpc)}"
    assert len(app_config.rpc.full_nodes) > 0, "Expected at least one full_node in app_config.rpc"
    assert app_config.rpc.full_nodes[0].url == expected_rpc_url
    
    # Example: Check timeout if it's defined in your settings and .env.test
    # expected_rpc_timeout = int(get_test_env_var("TEST_RPC_REQUEST_TIMEOUT", "30"))
    # assert app_config.rpc.request_time_out == expected_rpc_timeout
    
    print("\nPASSED: test_rpc_settings_are_correct")
    print(f"  RPC Full Node URL from app_config: {app_config.rpc.full_nodes[0].url}")

def test_rpc_helper_instantiation(app_config):
    """Tests if RpcHelper can be instantiated with app_config.rpc."""
    assert app_config is not None and hasattr(app_config, 'rpc'), "RPC config not available."
    try:
        rpc_helper = RpcHelper(rpc_settings=app_config.rpc)
        assert isinstance(rpc_helper, RpcHelper)
        print("\nPASSED: test_rpc_helper_instantiation (RpcHelper created successfully)")
    except Exception as e:
        pytest.fail(f"Failed to instantiate RpcHelper with app_config.rpc: {e}")

def test_anchor_rpc_settings_are_correct(app_config):
    """Tests if Anchor RPC settings in app_config match .env.test values."""
    assert app_config is not None, "app_config not loaded."
    assert hasattr(app_config, 'anchor_chain_rpc'), "app_config missing 'anchor_chain_rpc' attribute."
    
    expected_anchor_rpc_url = get_test_env_var("TEST_ANCHOR_RPC_URL_FULL_NODE_1", "http://localhost:8546/test_anchor_rpc")
    
    assert isinstance(app_config.anchor_chain_rpc, RPCConfigBase)
    assert len(app_config.anchor_chain_rpc.full_nodes) > 0
    assert app_config.anchor_chain_rpc.full_nodes[0].url == expected_anchor_rpc_url
    
    print("\nPASSED: test_anchor_rpc_settings_are_correct")
    print(f"  Anchor RPC Full Node URL from app_config: {app_config.anchor_chain_rpc.full_nodes[0].url}")

def test_anchor_rpc_helper_instantiation(app_config):
    """Tests if RpcHelper can be instantiated with app_config.anchor_chain_rpc for Anchor chain."""
    assert app_config is not None and hasattr(app_config, 'anchor_chain_rpc'), "Anchor RPC config not available."
    try:
        anchor_rpc_helper = RpcHelper(rpc_settings=app_config.anchor_chain_rpc)
        assert isinstance(anchor_rpc_helper, RpcHelper)
        print("\nPASSED: test_anchor_rpc_helper_instantiation (Anchor RpcHelper created successfully)")
    except Exception as e:
        pytest.fail(f"Failed to instantiate RpcHelper for anchor chain: {e}")

def test_ipfs_settings_are_correct(app_config):
    """Tests if IPFS settings in app_config match .env.test values."""
    assert app_config is not None, "app_config not loaded."
    assert hasattr(app_config, 'ipfs'), "app_config missing 'ipfs' attribute."

    print(f"app config: {app_config}")
    
    expected_ipfs_url = get_test_env_var("TEST_IPFS_URL", "/ip4/127.0.0.1/tcp/5001")
    
    assert isinstance(app_config.ipfs, IPFSConfig) # Assuming app_config.ipfs is an IPFSConfig model
    assert app_config.ipfs.url == expected_ipfs_url
    
    print("\nPASSED: test_ipfs_settings_are_correct")
    print(f"  IPFS URL from app_config: {app_config.ipfs.url}")

def test_ipfs_client_instantiation(app_config):
    """Tests if AsyncIPFSClient can be instantiated with app_config.ipfs."""
    assert app_config is not None and hasattr(app_config, 'ipfs'), "IPFS config not available."
    assert app_config.ipfs.url, "IPFS URL is missing in config for client instantiation."
    try:
        # AsyncIPFSClient constructor might take settings directly or individual params
        # Adjust based on its actual signature. Assuming it takes the IPFSConfig model.
        ipfs_client = AsyncIPFSClient(
            addr=app_config.ipfs.url, # addr might be redundant if settings object contains it and is used
            settings=app_config.ipfs
        )
        assert isinstance(ipfs_client, AsyncIPFSClient)
        print("\nPASSED: test_ipfs_client_instantiation (AsyncIPFSClient created successfully)")
    except Exception as e:
        pytest.fail(f"Failed to instantiate AsyncIPFSClient with app_config.ipfs: {e}")

def test_redis_settings_are_correct(app_config):
    """Tests if Redis settings in app_config match .env.test values."""
    assert app_config is not None, "app_config not loaded."
    assert hasattr(app_config, 'redis'), "app_config missing 'redis' object."
    redis_conf = app_config.redis

    assert hasattr(redis_conf, 'host'), "app_config.redis_config missing 'host' attribute."
    assert hasattr(redis_conf, 'port'), "app_config.redis_config missing 'port' attribute."

    expected_redis_host = get_test_env_var("TEST_REDIS_HOST", "localhost")
    expected_redis_port = int(get_test_env_var("TEST_REDIS_PORT", "6379")) # conftest default is str
    
    assert isinstance(redis_conf, RedisConfigPydanticModel) 
    assert redis_conf.host == expected_redis_host
    assert redis_conf.port == expected_redis_port
    
    print("\nPASSED: test_redis_settings_are_correct")
    print(f"  Redis Host from app_config.redis_config: {redis_conf.host}, Port: {redis_conf.port}")

@pytest.mark.asyncio
async def test_redis_connection(app_config):
    """Tests if a Redis connection can be established and pinged using app_config."""
    assert app_config is not None and hasattr(app_config, 'redis'), "Redis config object not available in app_config."
    redis_conf = app_config.redis

    try:
        redis_client = AsyncIORedis(
            host=redis_conf.host,
            port=redis_conf.port,
            password=getattr(redis_conf, 'password', None), # Safely get optional password
            db=getattr(redis_conf, 'db', 0) # Safely get optional db
        )
        pong = await redis_client.ping()
        assert pong, "Redis PING failed."
        await redis_client.close() # Or use a context manager if your client supports it
        print("\nPASSED: test_redis_connection (Redis PING successful)")
    except Exception as e:
        pytest.fail(f"Failed to connect to Redis or PING: {e}")

def test_web3_instance_creation(app_config):
    """Tests if a Web3 instance can be created using RPC settings from app_config."""
    assert app_config is not None and hasattr(app_config, 'rpc'), "RPC config not available."
    assert len(app_config.rpc.full_nodes) > 0 and app_config.rpc.full_nodes[0].url, "RPC full_node URL missing."
    
    try:
        w3 = Web3(Web3.HTTPProvider(app_config.rpc.full_nodes[0].url))
        assert isinstance(w3, Web3)
        assert w3.is_connected(), "Web3 instance not connected to provider."
        print("\nPASSED: test_web3_instance_creation (Web3 instance created and connected)")
    except Exception as e:
        pytest.fail(f"Failed to create or connect Web3 instance: {e}")

def test_protocol_state_contract_instantiation(app_config):
    """
    Tests if a Web3 contract instance for ProtocolState can be created
    using address from app_config and a manually specified/loaded ABI.
    """
    assert app_config is not None
    assert hasattr(app_config, 'protocol_state') and \
           hasattr(app_config.protocol_state, 'address'), "PSC address missing in app_config.protocol_state."
    assert hasattr(app_config, 'rpc') and \
           len(app_config.rpc.full_nodes) > 0 and \
           app_config.rpc.full_nodes[0].url, "RPC URL for Web3 missing."

    psc_address = app_config.protocol_state.address
    # ABI loading:
    # The conftest.py currently does not inject the ABI path into settings.json.
    # For this test to pass, you'd either:
    # 1. Modify conftest.py to also replace a placeholder for ABI path in settings.json.
    # 2. Hardcode/construct the ABI path here, or load it from a known test static location.
    # For now, let's assume the ABI path is known for testing or directly in app_config.
    
    # Example: Get ABI path from config if it were populated.
    # abi_path_in_config = getattr(app_config.protocol_state_contract, 'abi_path', None)
    # if not abi_path_in_config:
    #     pytest.skip("ABI path for ProtocolState not found in app_config, cannot test contract instantiation.")

    # Using the ABI path from the *original* conftest for this example
    # THIS MIGHT NEED ADJUSTMENT - ENSURE THIS ABI FILE EXISTS
    abi_path_relative = "snapshotter/static/abis/ProtocolContract.json"
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    abi_full_path = os.path.join(project_root, abi_path_relative)

    if not os.path.exists(abi_full_path):
        pytest.fail(f"ABI file for ProtocolState not found at: {abi_full_path}. Test cannot proceed.")

    import json
    try:
        with open(abi_full_path, 'r') as f:
            abi = json.load(f)
    except Exception as e:
        pytest.fail(f"Failed to load ABI from {abi_full_path}: {e}")

    try:
        w3 = Web3(Web3.HTTPProvider(app_config.rpc.full_nodes[0].url))
        assert w3.is_connected()
        
        contract_instance = w3.eth.contract(address=Web3.to_checksum_address(psc_address), abi=abi)
        assert isinstance(contract_instance, Contract)
        assert contract_instance.address == Web3.to_checksum_address(psc_address)
        print("\nPASSED: test_protocol_state_contract_instantiation")
        print(f"  PSC Address from app_config used: {psc_address}")
    except Exception as e:
        pytest.fail(f"Failed to instantiate ProtocolState contract: {e}")
