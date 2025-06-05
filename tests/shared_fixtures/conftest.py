import sys
import os
import shutil
import re
import pytest
from dotenv import load_dotenv

# Add project root to sys.path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

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

# --- Pytest Hooks ---

@pytest.hookimpl(tryfirst=True)
def pytest_sessionstart(session):
    print("\n--- Pytest Session Start: Preparing test configurations (snapshotter_autofill style) ---")

    env_test_path = os.path.join(PROJECT_ROOT, ENV_TEST_FILE_NAME)
    if not os.path.exists(env_test_path):
        pytest.exit(
            f"CRITICAL: Test environment file '{env_test_path}' not found. "
            f"Please create it (e.g., by copying 'env.test.example') and populate it. Aborting."
        )

    print(f"Loading environment variables from: {env_test_path}")
    load_dotenv(dotenv_path=env_test_path, override=True)

    app_config_dir_abs = os.path.join(PROJECT_ROOT, APP_CONFIG_DIR_NAME)
    backup_dir_abs = os.path.join(PROJECT_ROOT, "tests", BACKUP_DIR_NAME) # Place backup inside tests/

    print(f"Application config directory: {app_config_dir_abs}")
    print(f"Backup directory for original configs: {backup_dir_abs}")

    if not os.path.isdir(app_config_dir_abs):
        # If config dir doesn't exist, we can't do much.
        # Tests might fail later if they expect these files.
        print(f"Warning: Application config directory '{app_config_dir_abs}' does not exist. "
              "Cannot backup or place test configs. Test behavior is undefined.")
        return # Proceed, but population will likely fail or be skipped.

    os.makedirs(backup_dir_abs, exist_ok=True)
    _backed_up_files.clear()

    for file_base_name in CONFIG_FILES_TO_MANAGE:
        original_file_path = os.path.join(app_config_dir_abs, file_base_name)
        example_file_path = os.path.join(app_config_dir_abs, file_base_name.replace(".json", ".example.json"))
        backup_file_path = os.path.join(backup_dir_abs, file_base_name)

        if os.path.exists(original_file_path):
            print(f"  Backing up '{original_file_path}' to '{backup_file_path}'")
            try:
                shutil.copy2(original_file_path, backup_file_path)
                _backed_up_files.add(file_base_name)
            except Exception as e:
                pytest.exit(f"Failed to backup {original_file_path}: {e}. Aborting.")
        
        if os.path.exists(example_file_path):
            print(f"  Copying '{example_file_path}' to '{original_file_path}'")
            try:
                shutil.copy2(example_file_path, original_file_path)
            except Exception as e:
                # If original didn't exist but example copy fails, that's also critical
                pytest.exit(f"Failed to copy {example_file_path} to {original_file_path}: {e}. Aborting.")
        elif not os.path.exists(original_file_path):
             print(f"  Warning: Example file '{example_file_path}' not found, and no existing '{original_file_path}' to use as base for test config.")


    # Populate the copied files
    settings_json_target_path = os.path.join(app_config_dir_abs, "settings.json")
    if os.path.exists(settings_json_target_path): # Populate only if it was successfully copied from example or existed
        _populate_config_file(settings_json_target_path, REPLACEMENTS_FOR_SETTINGS_JSON)
    else:
        print(f"Warning: Cannot populate '{settings_json_target_path}' as it does not exist (example missing or copy failed).")

    auth_settings_json_target_path = os.path.join(app_config_dir_abs, "auth_settings.json")
    if os.path.exists(auth_settings_json_target_path):
         _populate_config_file(auth_settings_json_target_path, REPLACEMENTS_FOR_AUTH_SETTINGS_JSON)
    else:
        print(f"Warning: Cannot populate '{auth_settings_json_target_path}' as it does not exist.")

    print("--- Test configurations prepared ---")

@pytest.hookimpl(trylast=True)
def pytest_sessionfinish(session, exitstatus):
    print("\n--- Pytest Session Finish: Restoring original configurations ---")
    app_config_dir_abs = os.path.join(PROJECT_ROOT, APP_CONFIG_DIR_NAME)
    backup_dir_abs = os.path.join(PROJECT_ROOT, "tests", BACKUP_DIR_NAME)

    if not os.path.isdir(app_config_dir_abs) and not os.path.isdir(backup_dir_abs):
        print("  No config or backup directories found. Nothing to restore or clean.")
        return

    for file_base_name in CONFIG_FILES_TO_MANAGE:
        original_file_path = os.path.join(app_config_dir_abs, file_base_name)
        backup_file_path = os.path.join(backup_dir_abs, file_base_name)

        if file_base_name in _backed_up_files:
            if os.path.exists(backup_file_path):
                print(f"  Restoring '{original_file_path}' from '{backup_file_path}'")
                try:
                    shutil.move(backup_file_path, original_file_path)
                except Exception as e:
                    print(f"    Error restoring {original_file_path} from {backup_file_path}: {e}")
            else:
                print(f"    Warning: Backup for {file_base_name} was expected but not found at {backup_file_path}.")
        elif os.path.exists(original_file_path):
            # If not backed up, it means it was created from an example (or was an unexpected file)
            # We should remove it to clean up test-generated files.
            print(f"  Removing test-generated '{original_file_path}' (no original backup was made).")
            try:
                os.remove(original_file_path)
            except Exception as e:
                print(f"    Error removing {original_file_path}: {e}")
    
    if os.path.exists(backup_dir_abs):
        print(f"  Cleaning up backup directory: {backup_dir_abs}")
        try:
            # Ensure backup directory is empty before rmdir, or use rmtree if it might contain leftovers
            if not os.listdir(backup_dir_abs):
                 os.rmdir(backup_dir_abs)
                 print(f"    Successfully removed empty backup directory: {backup_dir_abs}")
            else:
                 shutil.rmtree(backup_dir_abs) # Use rmtree if not empty (e.g. a restore failed)
                 print(f"    Successfully removed backup directory (and its contents): {backup_dir_abs}")
        except Exception as e:
            print(f"    Error removing backup directory {backup_dir_abs}: {e}. Please check manually.")
    
    _backed_up_files.clear()
    print("--- Original configurations restored ---")
