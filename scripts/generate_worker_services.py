#!/usr/bin/env python3
import json
import os
import sys
from pathlib import Path

def generate_worker_services():
    # Read the projects configuration
    config_path = Path('config/projects.example.json')
    if not config_path.exists():
        print(f"Error: {config_path} not found", file=sys.stderr)
        sys.exit(1)

    aggregator_config_path = Path('config/aggregator.example.json')
    if not aggregator_config_path.exists():
        print(f"Error: {aggregator_config_path} not found", file=sys.stderr)
        sys.exit(1)

    with open(config_path) as f:
        projects_config = json.load(f)

    with open(aggregator_config_path) as f:
        aggregator_config = json.load(f)

    # Generate worker services for each project type
    worker_services = []
    for project in projects_config['config']:
        project_name = project['project_name'].split(':')[0]  # Get the base project type
        service_name = f"snapshot-worker-{project_name.lower()}"
        
        service_config = f"""  {service_name}:
    <<: *snapshotter-base
    command: bash -c "bash init_processes.sh utils.snapshot_worker {project_name}"
    depends_on:
      - processor-distributor
"""
        worker_services.append(service_config)

    for aggregator in aggregator_config['config']:
        project_name = aggregator['project_name'].split(':')[0]  # Get the base project type
        service_name = f"aggregation-worker-{project_name.lower()}"

        service_config = f"""  {service_name}:
    <<: *snapshotter-base
    command: bash -c "bash init_processes.sh utils.aggregation_worker {project_name}"
    depends_on:
      - processor-distributor
"""
        worker_services.append(service_config)
        

    return '\n'.join(worker_services)

if __name__ == '__main__':
    print(generate_worker_services()) 