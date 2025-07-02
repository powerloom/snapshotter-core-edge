from snapshotter.settings.config import settings

# Snapshot Worker Queues
SNAPSHOT_QUEUE_NAME = f'powerloom-snapshotter_{settings.namespace}_{settings.instance_id}'
SNAPSHOT_HEALTH_QUEUE_NAME = f'powerloom-snapshotter-health_{settings.namespace}_{settings.instance_id}'

# Aggregation Worker Queues
AGGREGATION_QUEUE_NAME = f'powerloom-aggregator_{settings.namespace}_{settings.instance_id}'
AGGREGATION_HEALTH_QUEUE_NAME = f'powerloom-aggregator-health_{settings.namespace}_{settings.instance_id}'

# Processor Distributor Queues (Listens on Event Detector Queue)
EVENT_DETECTOR_QUEUE_NAME = f'powerloom-event-detector_{settings.namespace}_{settings.instance_id}'
DISTRIBUTOR_HEALTH_QUEUE_NAME = f'powerloom-distributor-health_{settings.namespace}_{settings.instance_id}'

# Cacher Worker Queues
CACHER_QUEUE_NAME = f'powerloom-cacher_{settings.namespace}_{settings.instance_id}'
CACHER_HEALTH_QUEUE_NAME = f'powerloom-cacher-health_{settings.namespace}_{settings.instance_id}'

# New Worker Queues
TRADE_VOLUME_WORKER_QUEUE_NAME = f'powerloom-trade-volume-worker_{settings.namespace}_{settings.instance_id}'
METADATA_WORKER_QUEUE_NAME = f'powerloom-metadata-worker_{settings.namespace}_{settings.instance_id}'
TIMESERIES_WORKER_QUEUE_NAME = f'powerloom-timeseries-worker_{settings.namespace}_{settings.instance_id}'
CROSS_PROJECT_WORKER_QUEUE_NAME = f'powerloom-cross-project-worker_{settings.namespace}_{settings.instance_id}'