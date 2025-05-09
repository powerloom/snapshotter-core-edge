#!/bin/bash

source .env


if [ -z "$OVERRIDE_DEFAULTS" ]; then
    echo "setting default values..."
    export PROST_RPC_URL="https://rpc-v2.powerloom.network"
    export PROTOCOL_STATE_CONTRACT="0x000AA7d3a6a2556496f363B59e56D9aA1881548F"
    export DATA_MARKET_CONTRACT="0x21cb57C1f2352ad215a463DD867b838749CD3b8f"
    export PROST_CHAIN_ID="7869"
fi

# cleanup redis and ipfs data
rm -rf redis-data
rm -rf ipfs-data
rm -rf ipfs-export
rm -rf logs
mkdir logs

echo "testing before build..."

if [ -z "$SOURCE_RPC_URL" ]; then
    echo "RPC URL not found, please set this in your .env!"
    exit 1
fi

if [ -z "$SIGNER_ACCOUNT_ADDRESS" ]; then
    echo "SIGNER_ACCOUNT_ADDRESS not found, please set this in your .env!"
    exit 1
fi

if [ -z "$SIGNER_ACCOUNT_PRIVATE_KEY" ]; then
    echo "SIGNER_ACCOUNT_PRIVATE_KEY not found, please set this in your .env!"
    exit 1
fi
echo "Found SOURCE RPC URL ${SOURCE_RPC_URL}"
echo "Found SIGNER ACCOUNT ADDRESS ${SIGNER_ACCOUNT_ADDRESS}"

[ -n "$PROST_RPC_URL" ] && echo "Found PROST_RPC_URL ${PROST_RPC_URL}"
[ -n "$PROST_CHAIN_ID" ] && echo "Found PROST_CHAIN_ID ${PROST_CHAIN_ID}"
[ -n "$IPFS_URL" ] && echo "Found IPFS_URL ${IPFS_URL}"
[ -n "$PROTOCOL_STATE_CONTRACT" ] && echo "Found PROTOCOL_STATE_CONTRACT ${PROTOCOL_STATE_CONTRACT}"

if [ -z "$CORE_API_PORT" ]; then
    export CORE_API_PORT=8002
    echo "CORE_API_PORT not found in .env, setting to default value ${CORE_API_PORT}"
else
    echo "Found CORE_API_PORT ${CORE_API_PORT}"
fi

if [ -z "$LOCAL_COLLECTOR_PORT" ]; then
    export LOCAL_COLLECTOR_PORT=50051
    echo "LOCAL_COLLECTOR_PORT not found in .env, setting to default value ${LOCAL_COLLECTOR_PORT}"
else
    echo "Found LOCAL_COLLECTOR_PORT ${LOCAL_COLLECTOR_PORT}"
fi

PROFILES=""
# removing all old and new profiles to avoid conflicts
PROFILES="$PROFILES --profile old"
PROFILES="$PROFILES --profile new"
PROFILES="$PROFILES --profile ipfs"
PROFILES="$PROFILES --profile local-collector"


if command -v docker-compose &> /dev/null; then
    COMPOSE_CMD="docker-compose"
else
    echo 'docker compose not found, trying to see if compose exists within docker'
    COMPOSE_CMD="docker compose"
fi

$COMPOSE_CMD -f docker-compose.yaml $PROFILES down --volumes --remove-orphans
