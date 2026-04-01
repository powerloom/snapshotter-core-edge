#!/bin/bash

# Run the fetch script with unbuffered output and pipe through the parser
TEMPO_PRIVATE_KEY="${TEMPO_PRIVATE_KEY}" TEMP_CHAIN_ID="${TEMP_CHAIN_ID}" python -u fetch_paid.py | python parse_output.py
