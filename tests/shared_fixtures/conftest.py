import sys
import os
import pytest
import json
from typing import Dict, AsyncGenerator
from web3 import Web3
from web3.contract.contract import Contract
from rpc_helper.rpc import RpcHelper
from ipfs_client.main import AsyncIPFSClient
from redis import asyncio as aioredis
import certifi
from httpx import AsyncHTTPTransport, Limits, Timeout, AsyncClient

# Add project root to sys.path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# Create test_logs directory for tests
test_logs_dir = os.path.join(PROJECT_ROOT, 'test_logs')
os.makedirs(test_logs_dir, exist_ok=True)

# Monkey patch the loguru logger to use test_logs instead of logs
original_add = None

def patch_loguru_for_tests():
    """Patch loguru to redirect file logging to test_logs directory"""
    global original_add
    from loguru import logger
    
    if original_add is None:
        original_add = logger.add
    
    def patched_add(sink, **kwargs):
        # If sink is a file path that starts with 'logs/', redirect to test_logs/
        if isinstance(sink, str) and sink.startswith('logs/'):
            sink = sink.replace('logs/', 'test_logs/')
        return original_add(sink, **kwargs)
    
    logger.add = patched_add

# Apply the patch before any logger initialization
patch_loguru_for_tests()

print("--- conftest.py ---")
print(f"PROJECT_ROOT added to sys.path: {PROJECT_ROOT}")
print(f"Current Working Directory: {os.getcwd()}")

# --- Configuration for Test Setup ---
CONFIG_FILES_TO_MANAGE = [
    "settings.json",
    "projects.json",
    "auth_settings.json",
    "aggregator.json",
]
APP_CONFIG_DIR_NAME = "test_config"  # Relative to PROJECT_ROOT
BACKUP_DIR_NAME = "tmp_config_backup_pytest"  # Created in tests/ directory
ENV_TEST_FILE_NAME = ".env.test"  # Relative to PROJECT_ROOT

# Global set to track which original files were actually backed up
_backed_up_files = set()

# --- Helper Functions ---

@pytest.fixture(scope="session")
def app_config():
    """
    Provides the application settings object and monkeypatches the RpcHelper
    to fix a hardcoded SSL certificate path and disable rate-limiting checks.
    This is the earliest point in our test setup that runs before any
    RpcHelper instances are created.
    """
    # Create test-specific settings from the test_config directory created by root conftest.py
    import os
    import json
    from snapshotter.utils.models.settings_model import Settings
    
    # Load test settings from test_config directory (created by root conftest.py)
    test_config_dir = os.path.join(os.getcwd(), APP_CONFIG_DIR_NAME)
    test_settings_path = os.path.join(test_config_dir, "settings.json")
    
    if not os.path.exists(test_settings_path):
        pytest.fail(f"Test settings file not found at {test_settings_path}. Make sure the root conftest.py setup completed successfully.")
    
    with open(test_settings_path, 'r') as f:
        test_settings_dict = json.load(f)
    
    # Create test settings object
    settings = Settings(**test_settings_dict)
    
    # Disable file logging for tests to avoid permission issues
    settings.logs.write_to_files = False
    
    # --- MONKEYPATCH RpcHelper for SSL ---
    # The RpcHelper library has a hardcoded, Linux-specific SSL certificate path,
    # causing OSError on other platforms like macOS. We replace the problematic
    # method with a corrected version that uses the cross-platform `certifi` library.

    original_init_http_clients = RpcHelper._init_http_clients

    async def corrected_init_http_clients(self):
        """A corrected version of _init_http_clients that uses certifi."""
        if self._client is not None:
            return

        # This is the corrected part: `verify=certifi.where()`
        self._async_transport = AsyncHTTPTransport(
            limits=Limits(
                max_connections=self._rpc_settings.connection_limits.max_connections,
                max_keepalive_connections=self._rpc_settings.connection_limits.max_keepalive_connections,
                keepalive_expiry=self._rpc_settings.connection_limits.keepalive_expiry,
            ),
            verify=certifi.where(),
        )
        self._client = AsyncClient(
            timeout=Timeout(timeout=15.0),
            follow_redirects=False,
            transport=self._async_transport,
        )

    # Apply the SSL patch
    RpcHelper._init_http_clients = corrected_init_http_clients


    # --- MONKEYPATCH RpcHelper for Rate Limiter ---
    # The RpcHelper attempts to contact a 'rate-limiter' service which is not
    # available in a local test environment. We patch it to prevent errors.
    original_check_rate_limit = RpcHelper.check_rate_limit

    async def mock_check_rate_limit(self, key):
        """A mocked version of check_rate_limit that always returns True."""
        return True

    # Apply the rate limiter patch
    RpcHelper.check_rate_limit = mock_check_rate_limit

    yield settings
    
    # Restore the original methods after the test session
    RpcHelper._init_http_clients = original_init_http_clients
    RpcHelper.check_rate_limit = original_check_rate_limit


# --- Centralized Test Fixtures ---

@pytest.fixture(scope="module")
async def rpc_helper(app_config) -> AsyncGenerator[RpcHelper, None]:
    """Fixture for RPC helper, initialized from test settings."""
    helper = RpcHelper(rpc_settings=app_config.rpc)
    await helper.init()
    yield helper
    # The RpcHelper class does not have a shutdown method.
    # The underlying httpx client will be closed when the event loop closes.

@pytest.fixture(scope="module")
async def anchor_rpc_helper(app_config) -> AsyncGenerator[RpcHelper, None]:
    """Fixture for anchor chain RPC helper, initialized from test settings."""
    helper = RpcHelper(rpc_settings=app_config.anchor_chain_rpc)
    await helper.init()
    yield helper
    # The RpcHelper class does not have a shutdown method.

@pytest.fixture(scope="module")
async def redis_conn(app_config) -> AsyncGenerator[aioredis.Redis, None]:
    """Fixture for a real Redis connection, configured from test settings."""
    redis = aioredis.from_url(
        f"redis://{app_config.redis.host}:{app_config.redis.port}",
        password=getattr(app_config.redis, 'password', None),
        db=getattr(app_config.redis, 'db', 0)
    )
    yield redis
    await redis.close()

@pytest.fixture(scope="module")
async def ipfs_reader(app_config) -> AsyncGenerator[AsyncIPFSClient, None]:
    """
    Fixture for IPFS client, initialized from test settings.
    This is now an async fixture to allow for proper session management.
    """
    client = AsyncIPFSClient(
        addr=app_config.ipfs.url,
        settings=app_config.ipfs
    )
    await client.init_session()
    yield client
    # The AsyncIPFSClient holds an httpx.AsyncClient that needs to be closed.
    if hasattr(client, '_client') and client._client:
        await client._client.aclose()

@pytest.fixture(scope="module")
def w3_instance(app_config) -> Web3:
    """Fixture for Web3 instance, initialized from test settings."""
    return Web3(Web3.HTTPProvider(app_config.rpc.full_nodes[0].url))

@pytest.fixture(scope="module")
def protocol_state_contract(w3_instance: Web3, app_config) -> Contract:
    """Fixture for protocol state contract, initialized from test settings."""
    # This path is relative to the project root, where pytest is run.
    abi_path = "snapshotter/static/abis/ProtocolContract.json"
    
    project_root = os.getcwd() # Assumes pytest is run from the project root.
    abs_path = os.path.join(project_root, abi_path)

    try:
        with open(abs_path) as f:
            abi = json.load(f)
    except FileNotFoundError:
        pytest.fail(f"ProtocolState ABI file not found at: {abs_path}", pytrace=False)

    return w3_instance.eth.contract(
        address=Web3.to_checksum_address(app_config.protocol_state.address),
        abi=abi
    )

@pytest.fixture(scope="module")
def load_abi_fn():
    """
    Returns a function to load an ABI from a path relative to the project root.
    """
    project_root = os.getcwd() # Assumes pytest is run from the project root.
    
    def _load(abi_path_relative_to_root: str) -> Dict:
        abi_path = os.path.join(project_root, abi_path_relative_to_root)
        try:
            with open(abi_path) as f:
                return json.load(f)
        except FileNotFoundError:
            pytest.fail(f"ABI file not found at calculated path: {abi_path}", pytrace=False)
            
    return _load
