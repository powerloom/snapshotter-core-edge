#!/bin/bash


service_name=$1
extra_args=$2

if [ "$service_name" == "system_event_detector" ]; then
    poetry run python -m snapshotter.snapshotter_id_ping
    ret_status=$?

    if [ $ret_status -ne 0 ]; then
        echo "Snapshotter identity check failed on protocol smart contract"
        exit 1
    fi
fi
# sleep for 30 seconds to allow other services to start
sleep 30

echo "Starting ${service_name} with extra args: ${extra_args}"

# Handle unified_cache as a special case
if [ "$service_name" == "unified_cache" ]; then
    poetry run python snapshotter/unified_cache.py $extra_args
else
    poetry run python -m snapshotter.$service_name $extra_args
fi
