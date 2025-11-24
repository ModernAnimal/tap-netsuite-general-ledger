"""
Schema discovery for NetSuite GL Detail tap
"""

import json
import os
from typing import Dict, Any

from singer import Schema, CatalogEntry, Catalog

# Stream configuration: maps stream_id to its key properties
STREAM_CONFIGS = {
    'netsuite_general_ledger_detail': {
        'key_properties': ['internal_id', 'trans_acct_line_id'],
        'replication_method': 'FULL_TABLE'
    },
    'netsuite_account': {
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE'
    },
    'netsuite_vendor': {
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE'
    },
    'netsuite_classification': {
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE'
    },
    'netsuite_department': {
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE'
    },
    'netsuite_location': {
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE'
    },
    'netsuite_customer': {
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE'
    },
    'netsuite_employee': {
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE'
    }
}


def load_schemas() -> Dict[str, Dict[str, Any]]:
    """Load all schema files from the schemas directory

    Returns:
        Dictionary mapping stream_id to schema dict
    """
    schemas = {}
    schema_dir = os.path.join(os.path.dirname(__file__), "schemas")

    # Load all JSON files in the schemas directory
    for filename in os.listdir(schema_dir):
        if filename.endswith('.json'):
            stream_id = filename.replace('.json', '')
            schema_path = os.path.join(schema_dir, filename)

            with open(schema_path, 'r') as f:
                schemas[stream_id] = json.load(f)

    return schemas


def discover_streams(config: Dict[str, Any]) -> Catalog:
    """Discover available streams by loading schemas from JSON files"""

    # Load all schemas from JSON files
    raw_schemas = load_schemas()

    streams = []

    for stream_id, schema_dict in raw_schemas.items():
        # Get stream configuration
        stream_config = STREAM_CONFIGS.get(stream_id, {})
        key_properties = stream_config.get('key_properties', ['id'])
        replication_method = stream_config.get(
            'replication_method', 'FULL_TABLE'
        )

        # Create schema object
        schema = Schema.from_dict(schema_dict)

        # Create metadata
        metadata = {
            'selected': True,
            'replication-method': replication_method,
            'forced-replication-method': replication_method
        }

        # Create catalog entry
        entry = CatalogEntry(
            tap_stream_id=stream_id,
            stream=stream_id,
            schema=schema,
            key_properties=key_properties,
            metadata=metadata
        )

        streams.append(entry)

    return Catalog(streams)
