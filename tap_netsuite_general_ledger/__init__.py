#!/usr/bin/env python3
"""
NetSuite GL Detail Singer Tap
Extracts GL detail data from NetSuite via RESTlet API
"""

import argparse
import json
import sys
from typing import Dict, Any

import singer
from singer import utils

from .client import NetSuiteClient
from .discover import discover_streams
from .sync import sync_stream


LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    "netsuite_account",
    "netsuite_consumer_key",
    "netsuite_consumer_secret",
    "netsuite_token_id",
    "netsuite_token_secret"
]

OPTIONAL_CONFIG_KEYS = [
    "netsuite_script_id",
    "netsuite_deploy_id",
    "netsuite_search_id",
    "period_ids",
    "period_names"
]


def do_discover(config: Dict[str, Any]) -> None:
    """Discovery mode - output catalog of available streams"""
    LOGGER.info("Starting discovery mode")
    catalog = discover_streams(config)
    json.dump(catalog.to_dict(), sys.stdout, indent=2)


def do_sync(config: Dict[str, Any], state: Dict[str, Any],
            catalog: Dict[str, Any]) -> None:
    """Sync mode - extract data from NetSuite"""
    LOGGER.info("Starting sync mode")

    # Parse catalog
    catalog_obj = singer.Catalog.from_dict(catalog)

    # Get selected streams
    selected_streams = []
    for stream in catalog_obj.streams:
        # Check if stream is selected in the metadata
        stream_metadata = stream.metadata
        if isinstance(stream_metadata, dict):
            if stream_metadata.get('selected', False):
                selected_streams.append(stream)
        else:
            # If no metadata or selected not specified, default to selected
            selected_streams.append(stream)

    if not selected_streams:
        LOGGER.info("No streams selected for sync")
        return

    # Initialize client
    client = NetSuiteClient(config)

    # Sync each selected stream
    for stream in selected_streams:
        LOGGER.info(f"Syncing stream: {stream.tap_stream_id}")
        state = sync_stream(client, stream, state, config)
        
        # Handle broken pipe when writing state
        try:
            singer.write_state(state)
        except BrokenPipeError:
            LOGGER.warning(
                "Broken pipe detected when writing state - exiting gracefully"
            )
            break
        except Exception as e:
            LOGGER.error(f"Error writing state: {str(e)}")
            # Continue with next stream if state write fails
            continue


def main() -> None:
    """Main entry point for the tap"""
    # Parse command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-c', '--config',
        help='Config file',
        required=True
    )
    parser.add_argument(
        '-s', '--state',
        help='State file'
    )
    parser.add_argument(
        '-p', '--properties',
        help='Property selection file (deprecated, use catalog)'
    )
    parser.add_argument(
        '--catalog',
        help='Catalog file'
    )
    parser.add_argument(
        '-d', '--discover',
        action='store_true',
        help='Do schema discovery'
    )
    args = parser.parse_args()

    # Load config
    with open(args.config) as f:
        config = json.load(f)

    # Validate config
    utils.check_config(config, REQUIRED_CONFIG_KEYS)

    # Load state if provided
    state = {}
    if args.state:
        with open(args.state) as f:
            state = json.load(f)

    if args.discover:
        do_discover(config)
    else:
        # Load catalog
        if args.catalog:
            with open(args.catalog) as f:
                catalog = json.load(f)
        elif args.properties:
            # Legacy properties file support
            LOGGER.warning("Properties file is deprecated, please use catalog")
            # Convert to basic catalog using discovery
            discovered_catalog = discover_streams(config)
            catalog = {
                "streams": [{
                    "tap_stream_id": "netsuite_general_ledger_detail",
                    "schema": discovered_catalog.streams[0].schema.to_dict(),
                    "metadata": []
                }]
            }
        else:
            LOGGER.error("Either --catalog or --discover must be provided")
            sys.exit(1)

        do_sync(config, state, catalog)


if __name__ == "__main__":
    main()
