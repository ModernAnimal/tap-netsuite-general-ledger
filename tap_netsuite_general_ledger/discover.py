"""
Schema discovery for NetSuite GL Detail tap
"""

import json
import os
from typing import Dict, Any

from singer import Schema, CatalogEntry, Catalog


def get_gl_detail_schema() -> Dict[str, Any]:
    """Get the schema for GL detail records from SuiteQL
    
    All fields are defined as strings for simplicity.
    Type casting will be handled by the target (Redshift).
    """
    return {
        "type": "object",
        "properties": {
            "posting_period": {
                "type": ["null", "string"],
                "description": "Posting period name (display format)"
            },
            "posting_period_id": {
                "type": ["null", "string"],
                "description": "Posting period internal ID"
            },
            "created_date": {
                "type": ["null", "string"],
                "description": "Date created"
            },
            "trans_acct_line_last_modified": {
                "type": ["null", "string"],
                "description": "Transaction accounting line last modified date"
            },
            "transaction_last_modified": {
                "type": ["null", "string"],
                "description": "Transaction last modified date"
            },
            "account_last_modified": {
                "type": ["null", "string"],
                "description": "Account last modified date"
            },
            "posting": {
                "type": ["null", "string"],
                "description": "Posting flag"
            },
            "approval": {
                "type": ["null", "string"],
                "description": "Approval status"
            },
            "transaction_date": {
                "type": ["null", "string"],
                "description": "Transaction date"
            },
            "transaction_id": {
                "type": ["null", "string"],
                "description": "Transaction ID"
            },
            "trans_acct_line_id": {
                "type": ["null", "string"],
                "description": "Transaction accounting line ID"
            },
            "internal_id": {
                "type": ["null", "string"],
                "description": "Internal ID of the transaction"
            },
            "entity_name": {
                "type": ["null", "string"],
                "description": "Entity name"
            },
            "trans_memo": {
                "type": ["null", "string"],
                "description": "Transaction memo"
            },
            "trans_line_memo": {
                "type": ["null", "string"],
                "description": "Transaction line memo"
            },
            "transaction_type": {
                "type": ["null", "string"],
                "description": "Transaction type"
            },
            "acct_id": {
                "type": ["null", "string"],
                "description": "Account ID"
            },
            "account_group": {
                "type": ["null", "string"],
                "description": "Account group (parent account)"
            },
            "department": {
                "type": ["null", "string"],
                "description": "Department ID"
            },
            "class": {
                "type": ["null", "string"],
                "description": "Class ID"
            },
            "location": {
                "type": ["null", "string"],
                "description": "Location ID"
            },
            "debit": {
                "type": ["null", "string"],
                "description": "Debit amount"
            },
            "credit": {
                "type": ["null", "string"],
                "description": "Credit amount"
            },
            "net_amount": {
                "type": ["null", "string"],
                "description": "Net amount"
            },
            "subsidiary": {
                "type": ["null", "string"],
                "description": "Subsidiary"
            },
            "document_number": {
                "type": ["null", "string"],
                "description": "Document number"
            },
            "status": {
                "type": ["null", "string"],
                "description": "Status"
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
        key_properties=["internal_id", "transaction_id", "trans_acct_line_id"],
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
