"""
Schema discovery for NetSuite GL Detail tap
"""

import json
import os
from typing import Dict, Any

import singer
from singer import Schema, CatalogEntry, Catalog


def get_gl_detail_schema() -> Dict[str, Any]:
    """Get the schema for GL detail records"""
    return {
        "type": "object",
        "properties": {
            "internal_id": {
                "type": ["null", "string"],
                "description": "Internal ID of the transaction"
            },
            "document_number": {
                "type": ["null", "string"],
                "description": "Document number"
            },
            "type": {
                "type": ["null", "string"],
                "description": "Transaction type"
            },
            "journal_name": {
                "type": ["null", "string"],
                "description": "Journal name"
            },
            "date": {
                "type": ["null", "string"],
                "format": "date-time",
                "description": "Transaction date"
            },
            "period": {
                "type": ["null", "string"],
                "description": "Posting period"
            },
            "subsidiary": {
                "type": ["null", "string"],
                "description": "Subsidiary"
            },
            "account": {
                "type": ["null", "string"],
                "description": "Account"
            },
            "amount_debit": {
                "type": ["null", "number"],
                "description": "Debit amount"
            },
            "amount_credit": {
                "type": ["null", "number"],
                "description": "Credit amount"
            },
            "amount_net": {
                "type": ["null", "number"],
                "description": "Net amount"
            },
            "amount_transaction_total": {
                "type": ["null", "number"],
                "description": "Transaction total amount"
            },
            "class": {
                "type": ["null", "string"],
                "description": "Class"
            },
            "location": {
                "type": ["null", "string"],
                "description": "Location"
            },
            "department": {
                "type": ["null", "string"],
                "description": "Department"
            },
            "line": {
                "type": ["null", "string"],
                "description": "Line number"
            },
            "name_line": {
                "type": ["null", "string"],
                "description": "Line name"
            },
            "memo_main": {
                "type": ["null", "string"],
                "description": "Main memo"
            },
            "memo_line": {
                "type": ["null", "string"],
                "description": "Line memo"
            },
            "status": {
                "type": ["null", "string"],
                "description": "Status"
            },
            "approval_status": {
                "type": ["null", "string"],
                "description": "Approval status"
            },
            "date_created": {
                "type": ["null", "string"],
                "format": "date-time",
                "description": "Date created"
            },
            "created_by": {
                "type": ["null", "string"],
                "description": "Created by"
            },
            "name": {
                "type": ["null", "string"],
                "description": "Name"
            },
            "posting": {
                "type": ["null", "string"],
                "description": "Posting"
            },
            "company_name": {
                "type": ["null", "string"],
                "description": "Company name"
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
        key_properties=[],
        metadata=gl_detail_metadata
    )

    return Catalog([gl_detail_entry])


def save_schema_to_file() -> None:
    """Save schema to JSON file for reference"""
    schema = get_gl_detail_schema()
    schema_dir = os.path.join(os.path.dirname(__file__), "schemas")
    os.makedirs(schema_dir, exist_ok=True)

    schema_file = os.path.join(schema_dir, "netsuite_general_ledger_detail.json")
    with open(schema_file, 'w') as f:
        json.dump(schema, f, indent=2)

    print(f"Schema saved to {schema_file}")


if __name__ == "__main__":
    save_schema_to_file()
