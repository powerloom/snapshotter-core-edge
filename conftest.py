import sys
import os
import shutil
import pytest
from dotenv import load_dotenv
import json
import certifi

# --- Helper Functions for logging ---
def _log_info(message):
    """Helper to print messages to the console, as logging may not be configured."""
    print(message)

# Add project root to sys.path. Since this conftest.py is at the root, __file__ gives us the correct path.
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

_log_info("--- Root conftest.py loaded ---")
_log_info(f"PROJECT_ROOT added to sys.path: {PROJECT_ROOT}")
_log_info(f"Current Working Directory: {os.getcwd()}")

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
    ("data-market-contract", "TEST_DATA_MARKET_CONTRACT_ADDRESS", "0xTestDataMarketContractPlaceholder", lambda v: v),
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
    ('"redis-port"', "TEST_REDIS_PORT", "6379", lambda v: str(v)),
    ('"redis-password"', "TEST_REDIS_PASSWORD", "", lambda v: f'"{v}"' if v else "null"),
    ('"core-api-port"', "TEST_CORE_API_PORT", "8002", lambda v: str(v)),
    ('"redis-db"', "TEST_REDIS_DB", "0", lambda v: str(v)),
    ('"block-shift-for-bitmap-index"', "TEST_BLOCK_SHIFT_FOR_BITMAP_INDEX", "22400000", lambda v: str(v)),
    ('"ipfs-s3-config-enabled"', "TEST_IPFS_S3_CONFIG_ENABLED", "false", lambda v: str(v).lower()),
    ('"ipfs-unpinning-enabled"', "TEST_IPFS_UNPINNING_ENABLED", "false", lambda v: str(v).lower()),
    ('"ipfs-unpin-after"', "TEST_IPFS_UNPINNING_AFTER", "720", lambda v: str(v)),
]

REPLACEMENTS_FOR_AUTH_SETTINGS_JSON = [
    ("redis-host", "TEST_REDIS_HOST", "localhost", lambda v: v),
    ('"redis-port"', "TEST_REDIS_PORT", "6379", lambda v: str(v)),
    ('"redis-password"', "TEST_REDIS_PASSWORD", "", lambda v: f'"{v}"' if v else "null"),
]

def _populate_config_file(file_path, replacement_rules):
    """Reads a config file, applies replacements, and writes it back."""
    _log_info(f"  Populating {os.path.basename(file_path)} with test data...")
    try:
        with open(file_path, 'r') as f:
            content = f.read()
    except FileNotFoundError:
        _log_info(f"    Error: {file_path} not found (should have been copied from example). Skipping population.")
        return False

    if "settings.json" in file_path:
        if not _get_env_value("TEST_IPFS_URL"):
            _log_info(f"    TEST_IPFS_URL is not set. Ensuring IPFS key/secret placeholders are replaced with empty strings.")
            pass 

    content = _apply_replacements_to_content(content, replacement_rules)

    try:
        with open(file_path, 'w') as f:
            f.write(content)
        _log_info(f"    Finished populating {os.path.basename(file_path)}.")
        return True
    except Exception as e:
        _log_info(f"    Error writing populated file {file_path}: {e}")
        return False

# --- Pytest Hooks ---

@pytest.hookimpl(tryfirst=True)
def pytest_sessionstart(session):
    """
    Pytest hook that runs at the beginning of a test session.
    - Sets up the test environment by creating temporary config files.
    - Backs up any existing config files.
    - Populates the temporary configs with values from .env.test.
    """
    # Force httpx to use certifi's CA bundle by setting the SSL_CERT_FILE env var.
    # This is crucial for cross-platform compatibility (especially macOS).
    os.environ['SSL_CERT_FILE'] = certifi.where()

    _log_info("--- Pytest Session Start: Preparing test configurations ---")

    env_test_path = os.path.join(PROJECT_ROOT, ENV_TEST_FILE_NAME)
    if not os.path.exists(env_test_path):
        pytest.exit(
            f"CRITICAL: Test environment file '{env_test_path}' not found. "
            f"Please create it from 'env.test.example' and populate it. Aborting."
        )

    _log_info(f"Loading environment variables from: {env_test_path}")
    load_dotenv(dotenv_path=env_test_path, override=True)

    app_config_dir_abs = os.path.join(PROJECT_ROOT, APP_CONFIG_DIR_NAME)
    backup_dir_abs = os.path.join(PROJECT_ROOT, "tests", BACKUP_DIR_NAME)

    _log_info(f"Application config directory: {app_config_dir_abs}")
    _log_info(f"Backup directory for original configs: {backup_dir_abs}")

    if not os.path.isdir(app_config_dir_abs):
        _log_info(f"Warning: Application config directory '{app_config_dir_abs}' does not exist. Creating it.")
        os.makedirs(app_config_dir_abs, exist_ok=True)

    os.makedirs(backup_dir_abs, exist_ok=True)
    _backed_up_files.clear()

    for file_base_name in CONFIG_FILES_TO_MANAGE:
        original_file_path = os.path.join(app_config_dir_abs, file_base_name)
        # Look for example files in the main config directory, not the test directory
        main_config_dir = os.path.join(PROJECT_ROOT, "config")
        example_file_path = os.path.join(main_config_dir, file_base_name.replace(".json", ".example.json"))
        backup_file_path = os.path.join(backup_dir_abs, file_base_name)

        # Since we're using a test config directory, we don't need to backup anything
        # Just copy the example files to create the test config files
        
        if os.path.exists(example_file_path):
            _log_info(f"  Copying '{example_file_path}' to '{original_file_path}' for test configuration")
            try:
                shutil.copy2(example_file_path, original_file_path)
            except Exception as e:
                pytest.exit(f"Failed to copy {example_file_path} to {original_file_path}: {e}. Aborting.")
        else:
             _log_info(f"  Warning: Example file '{example_file_path}' not found.")
             # Try to create a minimal config file for testing
             if file_base_name == "settings.json":
                 _log_info(f"  Creating minimal settings.json for testing")
                 minimal_settings = {
                     "namespace": "test_namespace",
                     "signer_private_key": "0x0000000000000000000000000000000000000000000000000000000000000001",
                     "instance_id": "test_instance",
                     "slot_id": 1,
                     "logs": {"debug_mode": False, "write_to_files": False},
                     "redis": {"host": "localhost", "port": 6379, "db": 0},
                     "rpc": {"full_nodes": [{"url": "http://localhost:8545"}]},
                     "anchor_chain_rpc": {"full_nodes": [{"url": "http://localhost:8546"}]}
                 }
                 with open(original_file_path, 'w') as f:
                     json.dump(minimal_settings, f, indent=2)

    settings_json_target_path = os.path.join(app_config_dir_abs, "settings.json")
    if os.path.exists(settings_json_target_path):
        _populate_config_file(settings_json_target_path, REPLACEMENTS_FOR_SETTINGS_JSON)
    else:
        _log_info(f"Warning: Cannot populate '{settings_json_target_path}' as it does not exist.")

    auth_settings_json_target_path = os.path.join(app_config_dir_abs, "auth_settings.json")
    if os.path.exists(auth_settings_json_target_path):
         _populate_config_file(auth_settings_json_target_path, REPLACEMENTS_FOR_AUTH_SETTINGS_JSON)
    else:
        _log_info(f"Warning: Cannot populate '{auth_settings_json_target_path}' as it does not exist.")

    _log_info("--- Test configurations prepared ---")

@pytest.hookimpl(trylast=True)
def pytest_sessionfinish(session, exitstatus):
    """
    Pytest hook that runs at the end of a test session.
    - Cleans up test configuration files.
    """
    _log_info("\n--- Pytest Session Finish: Cleaning up test configurations ---")
    app_config_dir_abs = os.path.join(PROJECT_ROOT, APP_CONFIG_DIR_NAME)
    backup_dir_abs = os.path.join(PROJECT_ROOT, "tests", BACKUP_DIR_NAME)

    # Since we're using a separate test config directory, just remove it
    if os.path.exists(app_config_dir_abs):
        _log_info(f"  Removing test config directory: {app_config_dir_abs}")
        try:
            shutil.rmtree(app_config_dir_abs)
        except Exception as e:
            _log_info(f"    Error removing test config directory {app_config_dir_abs}: {e}")
    
    # Also clean up the backup directory if it exists and is empty
    if os.path.exists(backup_dir_abs):
        _log_info(f"  Cleaning up backup directory: {backup_dir_abs}")
        try:
            if not os.listdir(backup_dir_abs):
                 os.rmdir(backup_dir_abs)
            else:
                 shutil.rmtree(backup_dir_abs)
        except Exception as e:
            _log_info(f"    Error removing backup directory {backup_dir_abs}: {e}. Please check manually.")
    
    _backed_up_files.clear()
    _log_info("--- Test configurations cleaned up ---") 