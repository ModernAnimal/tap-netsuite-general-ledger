"""
Schema discovery for NetSuite GL Detail tap
"""

import json
import os
from typing import Dict, Any

from singer import Schema, CatalogEntry, Catalog


def get_gl_detail_schema() -> Dict[str, Any]:
    """Get the schema for GL detail records from SuiteQL"""
    return {
        "type": "object",
        "properties": {
            "posting_period": {
                "type": ["null", "string"],
                "description": "Posting period name (display format)"
            },
            "posting_period_id": {
                "type": ["null", "integer"],
                "description": "Posting period internal ID"
            },
            "internalid": {
                "type": ["null", "integer"],
                "description": "Internal ID of the transaction"
            },
            "tran_id": {
                "type": ["null", "string"],
                "description": "Transaction ID"
            },
            "transaction_type": {
                "type": ["null", "string"],
                "description": "Transaction type (display format)"
            },
            "tran_date": {
                "type": ["null", "string"],
                "format": "date",
                "description": "Transaction date"
            },
            "transAcctLineID": {
                "type": ["null", "integer"],
                "description": "Transaction line ID"
            },
            "acctID": {
                "type": ["null", "integer"],
                "description": "Account internal ID"
            },
            "account_number": {
                "type": ["null", "string"],
                "description": "Account number"
            },
            "account_name": {
                "type": ["null", "string"],
                "description": "Account name"
            },
            "net_amount": {
                "type": ["null", "number"],
                "description": "Net amount"
            },
            "debit": {
                "type": ["null", "number"],
                "description": "Debit amount"
            },
            "credit": {
                "type": ["null", "number"],
                "description": "Credit amount"
            },
            "memo": {
                "type": ["null", "string"],
                "description": "Transaction line memo"
            },
            "account_type": {
                "type": ["null", "string"],
                "description": "Account type"
            },
            "department_name": {
                "type": ["null", "string"],
                "description": "Department name"
            },
            "class_name": {
                "type": ["null", "string"],
                "description": "Class name"
            },
            "location_name": {
                "type": ["null", "string"],
                "description": "Location name"
            },
            "entity_id": {
                "type": ["null", "integer"],
                "description": "Entity internal ID"
            },
            "entity_name": {
                "type": ["null", "string"],
                "description": "Entity name (display format)"
            },
            "subsidiary_id": {
                "type": ["null", "integer"],
                "description": "Subsidiary internal ID"
            },
            "subsidiary_name": {
                "type": ["null", "string"],
                "description": "Subsidiary name (display format)"
            },
            "currency": {
                "type": ["null", "string"],
                "description": "Currency (display format)"
            },
            "lastmodified": {
                "type": ["null", "string"],
                "format": "date-time",
                "description": "Last modified date"
            },
            "account_internalid": {
                "type": ["null", "integer"],
                "description": (
                    "Account internal ID (duplicate for compatibility)"
                )
            }
        }
    }


def get_gl_detail_metadata() -> Dict[str, Any]:
    """Get metadata for GL detail stream"""
    return {
        "selected": True,
        "replication-method": "FULL_TABLE",
        "forced-replication-method": "FULL_TABLE"
    }


def discover_streams(config: Dict[str, Any]) -> Catalog:
    """Discover available streams"""

    # Create the GL detail stream
    gl_detail_schema = Schema.from_dict(get_gl_detail_schema())
    gl_detail_metadata = get_gl_detail_metadata()

    gl_detail_entry = CatalogEntry(
        tap_stream_id="netsuite_general_ledger_detail",
        stream="netsuite_general_ledger_detail",
        schema=gl_detail_schema,
        key_properties=["internalid", "transAcctLineID"],
        metadata=gl_detail_metadata
    )

    return Catalog([gl_detail_entry])


def save_schema_to_file() -> None:
    """Save schema to JSON file for reference"""
    schema = get_gl_detail_schema()
    schema_dir = os.path.join(os.path.dirname(__file__), "schemas")
    os.makedirs(schema_dir, exist_ok=True)

    schema_file = os.path.join(
        schema_dir, "netsuite_general_ledger_detail.json"
    )
    with open(schema_file, 'w') as f:
        json.dump(schema, f, indent=2)

    print(f"Schema saved to {schema_file}")


if __name__ == "__main__":
    save_schema_to_file()
