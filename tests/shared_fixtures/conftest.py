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
APP_CONFIG_DIR_NAME = "config"  # Relative to PROJECT_ROOT
BACKUP_DIR_NAME = "tmp_config_backup_pytest"  # Created in tests/ directory
ENV_TEST_FILE_NAME = ".env.test"  # Relative to PROJECT_ROOT

# Global set to track which original files were actually backed up
_backed_up_files = set()

# --- Helper Functions ---

def _get_env_value(key, default=""):
    """Retrieves an environment variable, using a provided default if not set."""
    return os.getenv(key, default)

def _apply_replacements_to_content(content, replacement_rules):
    """
    Applies a list of replacement rules to the given string content.
    Each rule is a tuple: (literal_to_find, env_var_key, default_env_val, formatting_function)
    formatting_function takes the environment value and returns the string for replacement.
    """
    for literal_to_find, env_key, default_val, format_fn in replacement_rules:
        env_value = _get_env_value(env_key, default_val)
        replacement_string = format_fn(env_value)
        content = content.replace(literal_to_find, replacement_string)
    return content

REPLACEMENTS_FOR_SETTINGS_JSON = [
    # Simple string replacements
    ("relevant-namespace", "TEST_NAMESPACE", "test_namespace_placeholder", lambda v: v),
    ("account-address", "TEST_SIGNER_ACCOUNT_ADDRESS", "0xTestSignerAccountAddressPlaceholder", lambda v: v),
    ("slot-id", "TEST_SLOT_ID", "1", lambda v: str(v)),
    ("https://rpc-url", "TEST_RPC_URL_FULL_NODE_1", "http://localhost:8545/test_rpc", lambda v: v),
    ("https://prost-rpc-url", "TEST_ANCHOR_RPC_URL_FULL_NODE_1", "http://localhost:8546/test_anchor_rpc", lambda v: v),
    ("ipfs-writer-url", "TEST_IPFS_URL", "/ip4/127.0.0.1/tcp/5001", lambda v: v),
    ("ipfs-writer-key", "TEST_IPFS_API_KEY", "", lambda v: v),
    ("ipfs-writer-secret", "TEST_IPFS_API_SECRET", "", lambda v: v),
    ("ipfs-reader-url", "TEST_IPFS_URL", "/ip4/127.0.0.1/tcp/5001/test_ipfs", lambda v: v),
    ("ipfs-reader-key", "TEST_IPFS_API_KEY", "", lambda v: v),
    ("ipfs-reader-secret", "TEST_IPFS_API_SECRET", "", lambda v: v),
    ("protocol-state-contract", "TEST_PROTOCOL_STATE_CONTRACT_ADDRESS", "0xTestProtocolStateContractPlaceholder", lambda v: v),
    ("data-market-contract", "TEST_DATA_MARKET_CONTRACT_ADDRESS", "", lambda v: v),
    ("signer-account-private-key", "TEST_SIGNER_ACCOUNT_PRIVATE_KEY", "0xTestPrivateKeyPlaceholder", lambda v: v),
    ("local-collector-port", "TEST_LOCAL_COLLECTOR_PORT", "50051", lambda v: v),
    ("https://telegram-reporting-url", "TEST_TELEGRAM_REPORTING_URL", "", lambda v: v),
    ("telegram-chat-id", "TEST_TELEGRAM_CHAT_ID", "", lambda v: v),
    ("redis-host", "TEST_REDIS_HOST", "localhost", lambda v: v),
    ("ipfs-s3-endpoint-url", "TEST_IPFS_S3_ENDPOINT_URL", "", lambda v: v),
    ("ipfs-s3-bucket-name", "TEST_IPFS_S3_BUCKET_NAME", "", lambda v: v),
    ("ipfs-s3-access-key", "TEST_IPFS_S3_ACCESS_KEY", "", lambda v: v),
    ("ipfs-s3-secret-key", "TEST_IPFS_S3_SECRET_KEY", "", lambda v: v),

    # Replacements requiring specific formatting (mimicking sed's behavior for JSON types)
    # Example in settings.json: "port": "redis-port" -> "port": 6379
    ('"redis-port"', "TEST_REDIS_PORT", "6379", lambda v: str(v)),
    # Example: "password": "redis-password" -> "password": "actual_password" or "password": null
    ('"redis-password"', "TEST_REDIS_PASSWORD", "", lambda v: f'"{v}"' if v else "null"),
    # Example: "enabled": "ipfs-s3-config-enabled" -> "enabled": true
    ('"ipfs-s3-config-enabled"', "TEST_IPFS_S3_CONFIG_ENABLED", "false", lambda v: str(v).lower()),
    ('"ipfs-unpinning-enabled"', "TEST_IPFS_UNPINNING_ENABLED", "false", lambda v: str(v).lower()),
    # Assuming ipfs-unpin-after is a numeric value but replaced as a string in the template initially
    ('"ipfs-unpin-after"', "TEST_IPFS_UNPINNING_AFTER", "720", lambda v: str(v)),
]

REPLACEMENTS_FOR_AUTH_SETTINGS_JSON = [
    ("redis-host", "TEST_REDIS_HOST", "localhost", lambda v: v),
    ('"redis-port"', "TEST_REDIS_PORT", "63790", lambda v: str(v)),
    ('"redis-password"', "TEST_REDIS_PASSWORD", "", lambda v: f'"{v}"' if v else "null"),
]

def _populate_config_file(file_path, replacement_rules):
    """Reads a config file, applies replacements, and writes it back."""
    print(f"  Populating {os.path.basename(file_path)} with test data...")
    try:
        with open(file_path, 'r') as f:
            content = f.read()
    except FileNotFoundError:
        print(f"    Error: {file_path} not found (should have been copied from example). Skipping population.")
        return False

    # Special handling for IPFS API key/secret if IPFS_URL is not set
    # This mimics the logic in snapshotter_autofill.sh where if IPFS_URL is empty, key/secret are cleared.
    # This assumes the placeholders for key/secret are distinct and known.
    if "settings.json" in file_path: # Only apply this logic for settings.json
        if not _get_env_value("TEST_IPFS_URL"):
            print(f"    TEST_IPFS_URL is not set. Ensuring IPFS key/secret placeholders are replaced with empty strings.")
            # Find original placeholders for ipfs-writer-key and ipfs-writer-secret
            # This part is tricky with simple string replacement if the original placeholders are not unique
            # or if they were already replaced by an empty string from TEST_IPFS_API_KEY being empty.
            # For now, this relies on TEST_IPFS_API_KEY/SECRET being empty in .env.test if URL is also empty.
            # A more robust way would be to have unique placeholders in *.example.json for these.
            pass # The general replacement logic with empty defaults should handle this.

    content = _apply_replacements_to_content(content, replacement_rules)

    try:
        with open(file_path, 'w') as f:
            f.write(content)
        print(f"    Finished populating {os.path.basename(file_path)}.")
        return True
    except Exception as e:
        print(f"    Error writing populated file {file_path}: {e}")
        return False

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
    test_config_dir = os.path.join(os.getcwd(), "test_config")
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
