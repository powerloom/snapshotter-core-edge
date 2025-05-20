#!/usr/bin/env python3
import sys
from pathlib import Path
from generate_worker_services import generate_worker_services
import os


def generate_docker_compose():
    # Read the base docker-compose.yaml
    compose_template_path = Path('docker-compose.yaml.template')
    if not compose_template_path.exists():
        print(f"Error: {compose_template_path} not found", file=sys.stderr)
        sys.exit(1)

    with open(compose_template_path) as f:
        compose_content = f.read()

    # Generate worker services
    worker_services = generate_worker_services()

    # Replace the placeholder with generated worker services
    start_marker = "# Generated worker services start"
    end_marker = "# Generated worker services end"
    
    start_idx = compose_content.find(start_marker)
    end_idx = compose_content.find(end_marker)
    
    if start_idx == -1 or end_idx == -1:
        print("Error: Could not find markers in docker-compose.yaml", file=sys.stderr)
        sys.exit(1)
    
    # Insert the worker services between the markers
    new_content = (
        compose_content[:start_idx + len(start_marker)] +
        "\n" + worker_services + "\n" +
        compose_content[end_idx:]
    )

    compose_path = Path('docker-compose.yaml')
    if compose_path.exists():
        # delete the file
        os.remove(compose_path)

    # Write the new content back to docker-compose.yaml
    with open(compose_path, 'w') as f:
        f.write(new_content)


if __name__ == '__main__':
    generate_docker_compose() 