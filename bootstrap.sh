#!/bin/bash
source .env

rm -rf ipfs-data;
rm -rf ipfs-export;
rm -rf redis_data;

# cleanup logs
rm -rf logs;
mkdir logs;
cd logs;
mkdir snapshotter;
mkdir local-collector;
cd ..;

rm -rf snapshotter-periphery-blockfetcher;
rm -rf snapshotter-periphery-txprocessor;
rm -rf snapshotter-periphery-epochsyncer;
rm -rf rate-limiter;

git clone https://github.com/Powerloom/rate-limiter.git;
cd rate-limiter;
git checkout $RATE_LIMITER_BRANCH;
cd ..;

git clone https://github.com/Powerloom/snapshotter-periphery-blockfetcher.git;
cd snapshotter-periphery-blockfetcher;
git checkout $SNAPSHOTTER_PERIPHERY_BLOCKFETCHER_BRANCH;
cd ..;

git clone https://github.com/Powerloom/snapshotter-periphery-txprocessor.git;
cd snapshotter-periphery-txprocessor;
git checkout $SNAPSHOTTER_PERIPHERY_TXPROCESSOR_BRANCH;
cd ..;

git clone https://github.com/Powerloom/snapshotter-periphery-epochsyncer.git;
cd snapshotter-periphery-epochsyncer;
git checkout $SNAPSHOTTER_PERIPHERY_EPOCHSYNCER_BRANCH;
cd ..;


if [ "$SNAPSHOT_CONFIG_REPO" ]; then
    echo "Found SNAPSHOT_CONFIG_REPO ${SNAPSHOT_CONFIG_REPO}";
    rm -rf config;
    git clone $SNAPSHOT_CONFIG_REPO config;
    cd config;
    if [ "$SNAPSHOT_CONFIG_REPO_BRANCH" ]; then
        echo "Found SNAPSHOT_CONFIG_REPO_BRANCH ${SNAPSHOT_CONFIG_REPO_BRANCH}";
        git checkout $SNAPSHOT_CONFIG_REPO_BRANCH;
    fi
    cd ../;
fi

if [ "$SNAPSHOTTER_COMPUTE_REPO" ]; then
    echo "Found SNAPSHOTTER_COMPUTE_REPO ${SNAPSHOTTER_COMPUTE_REPO}";
    rm -rf computes;
    git clone $SNAPSHOTTER_COMPUTE_REPO computes;
    cd computes;
    if [ "$SNAPSHOTTER_COMPUTE_REPO_BRANCH" ]; then
        echo "Found SNAPSHOTTER_COMPUTE_REPO_BRANCH ${SNAPSHOTTER_COMPUTE_REPO_BRANCH}";
        git checkout $SNAPSHOTTER_COMPUTE_REPO_BRANCH;
    fi
    cd ../;
fi

echo "bootstrapping complete!";
