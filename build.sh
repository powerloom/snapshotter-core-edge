#!/bin/bash

# check if .env exists
if [ ! -f .env ]; then
    echo "🟡 .env file not found, creating one..."
    cp env.example .env

    # Prompt for required values that were previously handled here
    read -p "Enter SOURCE_RPC_URL: " SOURCE_RPC_URL_INPUT
    read -p "Enter SIGNER_ACCOUNT_ADDRESS: " SIGNER_ACCOUNT_ADDRESS_INPUT
    read -s -p "Enter SIGNER_ACCOUNT_PRIVATE_KEY: " SIGNER_ACCOUNT_PRIVATE_KEY_INPUT
    echo # Add a newline after the silent private key input
    read -p "Enter Your SLOT_ID (NFT_ID): " SLOT_ID_INPUT
    read -p "Enter Your TELEGRAM_CHAT_ID (Optional, leave blank to skip.): " TELEGRAM_CHAT_ID_INPUT

    # Update env file with collected values
    sed -i".backup" "s#<source-rpc-url>#$SOURCE_RPC_URL_INPUT#" ".env"
    sed -i".backup" "s#<signer-account-address>#$SIGNER_ACCOUNT_ADDRESS_INPUT#" ".env"
    sed -i".backup" "s#<signer-account-private-key>#$SIGNER_ACCOUNT_PRIVATE_KEY_INPUT#" ".env"
    sed -i".backup" "s#<slot-id>#$SLOT_ID_INPUT#" ".env"
    # Handle potentially empty TELEGRAM_CHAT_ID
    if [ -z "$TELEGRAM_CHAT_ID_INPUT" ]; then
        # If empty, remove the placeholder line or just the placeholder depending on desired outcome
        # This example removes just the placeholder, leaving the variable empty
        sed -i".backup" "s#<telegram-chat-id>##" ".env"
    else
        sed -i".backup" "s#<telegram-chat-id>#$TELEGRAM_CHAT_ID_INPUT#" ".env"
    fi

    echo "🟢 .env file created and populated with required inputs."
fi

source .env

if [ -z "$OVERRIDE_DEFAULTS" ]; then
    echo "setting default values..."
    export PROST_RPC_URL="https://rpc-v2.powerloom.network"
    export PROTOCOL_STATE_CONTRACT="0x000AA7d3a6a2556496f363B59e56D9aA1881548F"
    export DATA_MARKET_CONTRACT="0x21cb57C1f2352ad215a463DD867b838749CD3b8f"
    export PROST_CHAIN_ID="7869"
fi


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
[ -n "$WETH_ADDRESS" ] && echo "Found WETH_ADDRESS ${WETH_ADDRESS}"
[ -n "$IPFS_S3_CONFIG_ENABLED" ] && echo "Found IPFS_S3_CONFIG_ENABLED ${IPFS_S3_CONFIG_ENABLED}"
[ -n "$IPFS_S3_CONFIG_ENDPOINT_URL" ] && echo "Found IPFS_S3_CONFIG_ENDPOINT_URL ${IPFS_S3_CONFIG_ENDPOINT_URL}"
[ -n "$IPFS_S3_CONFIG_BUCKET_NAME" ] && echo "Found IPFS_S3_CONFIG_BUCKET_NAME ${IPFS_S3_CONFIG_BUCKET_NAME}"
[ -n "$IPFS_S3_CONFIG_ACCESS_KEY" ] && echo "Found IPFS_S3_CONFIG_ACCESS_KEY ${IPFS_S3_CONFIG_ACCESS_KEY}"
[ -n "$IPFS_S3_CONFIG_SECRET_KEY" ] && echo "Found IPFS_S3_CONFIG_SECRET_KEY ${IPFS_S3_CONFIG_SECRET_KEY}"
[ -n "$IPFS_UNPINNING_ENABLED" ] && echo "Found IPFS_UNPINNING_ENABLED ${IPFS_UNPINNING_ENABLED}"
[ -n "$IPFS_UNPINNING_AFTER" ] && echo "Found IPFS_UNPINNING_AFTER ${IPFS_UNPINNING_AFTER}"
[ -n "$GUNICORN_WORKERS" ] && echo "Found GUNICORN_WORKERS ${GUNICORN_WORKERS}"

if [ -z "$IPFS_S3_CONFIG_ENABLED" ]; then
    export IPFS_S3_CONFIG_ENABLED=false
    echo "IPFS_S3_CONFIG_ENABLED not found in .env, setting to default value ${IPFS_S3_CONFIG_ENABLED}"
fi

# check if all other IPFS_S3_CONFIG_* variables are set
if [ "$IPFS_S3_CONFIG_ENABLED" = "true" ]; then
    if [ -z "$IPFS_S3_CONFIG_ENDPOINT_URL" ]; then
        echo "IPFS_S3_CONFIG_ENDPOINT_URL not found in .env, please set this in your .env!"
        exit 1
    fi

    if [ -z "$IPFS_S3_CONFIG_BUCKET_NAME" ]; then
    echo "IPFS_S3_CONFIG_BUCKET_NAME not found in .env, please set this in your .env!"
        exit 1
    fi

    if [ -z "$IPFS_S3_CONFIG_ACCESS_KEY" ]; then
        echo "IPFS_S3_CONFIG_ACCESS_KEY not found in .env, please set this in your .env!"
        exit 1
    fi

    if [ -z "$IPFS_S3_CONFIG_SECRET_KEY" ]; then
        echo "IPFS_S3_CONFIG_SECRET_KEY not found in .env, please set this in your .env!"
        exit 1
    fi

fi



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

if [ "$MAX_STREAM_POOL_SIZE" ]; then
    echo "Found MAX_STREAM_POOL_SIZE ${MAX_STREAM_POOL_SIZE}";
else
    export MAX_STREAM_POOL_SIZE=1024
    echo "MAX_STREAM_POOL_SIZE not found in .env, setting to default value ${MAX_STREAM_POOL_SIZE}";
fi

if [ "$SNAPSHOT_WORKER_REPLICAS" ]; then
    echo "Found SNAPSHOT_WORKER_REPLICAS ${SNAPSHOT_WORKER_REPLICAS}";
else
    export SNAPSHOT_WORKER_REPLICAS=6
    echo "SNAPSHOT_WORKER_REPLICAS not found in .env, setting to default value ${SNAPSHOT_WORKER_REPLICAS}";
fi

if [ "$AGGREGATION_WORKER_REPLICAS" ]; then
    echo "Found AGGREGATION_WORKER_REPLICAS ${AGGREGATION_WORKER_REPLICAS}";
else
    export AGGREGATION_WORKER_REPLICAS=6
    echo "AGGREGATION_WORKER_REPLICAS not found in .env, setting to default value ${AGGREGATION_WORKER_REPLICAS}";
fi

if [ "$DEFAULT_RATE_LIMIT" ]; then
    echo "Found DEFAULT_RATE_LIMIT ${DEFAULT_RATE_LIMIT}";
else
    export DEFAULT_RATE_LIMIT=10
    echo "DEFAULT_RATE_LIMIT not found in .env, setting to default value ${DEFAULT_RATE_LIMIT}";
fi



if [ "$STREAM_POOL_HEALTH_CHECK_INTERVAL" ]; then
    echo "Found STREAM_POOL_HEALTH_CHECK_INTERVAL ${STREAM_POOL_HEALTH_CHECK_INTERVAL}";
else
    export STREAM_POOL_HEALTH_CHECK_INTERVAL=600
    echo "STREAM_POOL_HEALTH_CHECK_INTERVAL not found in .env, setting to default value ${STREAM_POOL_HEALTH_CHECK_INTERVAL}";
fi

if [ "$LOCAL_COLLECTOR_PRIVATE_KEY" ]; then
    echo "Found LOCAL_COLLECTOR_PRIVATE_KEY... proceeding with build...";
else
    echo "LOCAL_COLLECTOR_PRIVATE_KEY not found in .env, please set this in your .env!";
    exit 1;
fi

if [ "$GOSSIPSUB_SNAPSHOT_SUBMISSION_PREFIX" ]; then
    echo "Found GOSSIPSUB_SNAPSHOT_SUBMISSION_PREFIX ${GOSSIPSUB_SNAPSHOT_SUBMISSION_PREFIX}"
else
    echo "GOSSIPSUB_SNAPSHOT_SUBMISSION_PREFIX not found in .env, please set this in your .env!";
    exit 1;
fi

if [ "$RENDEZVOUS_POINT" ]; then
    echo "Found RENDEZVOUS_POINT ${RENDEZVOUS_POINT}"
else
    echo "RENDEZVOUS_POINT not found in .env, please set this in your .env!";
    exit 1;
fi

if [ "$BOOTSTRAP_NODE_ADDRS" ]; then
    echo "Found BOOTSTRAP_NODE_ADDRS ${BOOTSTRAP_NODE_ADDRS}"
else
    echo "BOOTSTRAP_NODE_ADDRS not found in .env, please set this in your .env!";
    exit 1;
fi

# Get the first command line argument
ARG1=${1:-yes_collector}

if [ "$DEVMODE" = "true" ]; then
    echo "Building local collector..."
    rm -rf snapshotter-lite-local-collector
    git clone https://github.com/powerloom/snapshotter-lite-local-collector.git --single-branch --branch feat/non-blocking-grpc-sem-acq-full-node
    (cd ./snapshotter-lite-local-collector/ && chmod +x build-docker.sh && ./build-docker.sh)

    echo "Building snapshotter..."
    docker build -t snapshotter-core .

    echo "Building rate-limiter..."
    cd rate-limiter
    docker build -t rate-limiter .
    cd ..

    export SNAPSHOTTER_COLLECTOR_IMAGE="snapshotter-lite-local-collector"
    export SNAPSHOTTER_IMAGE="snapshotter-core"
    export RATE_LIMITER_IMAGE="rate-limiter"
else
    #fetch current git branch name
    GIT_BRANCH=$(git rev-parse --abbrev-ref HEAD)

    echo "Current branch is ${GIT_BRANCH}"

    #if on main git branch, set image_tag to latest or use the branch name
    export IMAGE_TAG=$([ "$GIT_BRANCH" = "dockerify" ] && echo "dockerify" || echo "latest")

    echo "Building image with tag ${IMAGE_TAG}"

    export SNAPSHOTTER_COLLECTOR_IMAGE="ghcr.io/powerloom/snapshotter-lite-local-collector:${IMAGE_TAG}"
    export SNAPSHOTTER_IMAGE="ghcr.io/powerloom/snapshotter-core:${IMAGE_TAG}"
fi

# check if python3 is installed (python3 is preferred, fallback to python)
if command -v python3 &> /dev/null; then
    PYTHON_CMD="python3"
elif command -v python &> /dev/null; then
    PYTHON_CMD="python"
else
    echo "python3 or python could not be found, please install it"
    exit 1
fi

# generate the docker-compose.yaml file
echo "Generating docker-compose.yaml file..."
$PYTHON_CMD scripts/generate_docker_compose.py

PROFILES=""
[ "$IPFS_URL" = "/dns/ipfs/tcp/5001" ] && PROFILES="$PROFILES --profile ipfs"
[ "$ARG1" = "yes_collector" ] && PROFILES="$PROFILES --profile local-collector"
[ "$REDIS_HOST" = "redis" ] && PROFILES="$PROFILES --profile redis"
if [ "$USE_NEW_SETUP" = "true" ]; then
    PROFILES="$PROFILES --profile new"
else
    PROFILES="$PROFILES --profile old"
fi

if command -v docker-compose &> /dev/null; then
    COMPOSE_CMD="docker-compose"
else
    echo 'docker compose not found, trying to see if compose exists within docker'
    COMPOSE_CMD="docker compose"
fi

if [ "$DEVMODE" = "false" ]; then
    $COMPOSE_CMD -f docker-compose.yaml $PROFILES pull
fi

$COMPOSE_CMD -f docker-compose.yaml $PROFILES up -V --remove-orphans --build
