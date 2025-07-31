"""
Sync functionality for NetSuite GL Detail tap
"""

import asyncio
from datetime import datetime, timezone
from typing import Dict, Any
from decimal import InvalidOperation

import singer
from singer import CatalogEntry

from .client import NetSuiteClient

LOGGER = singer.get_logger()


# Field mapping from NetSuite API to our schema
FIELD_MAPPING = {
    'internal_id': 'internalid',
    'document_number': 'tranid',
    'type': 'type',
    'journal_name': 'journalname',
    'date': 'trandate',
    'period': 'postingperiod',
    'subsidiary': 'subsidiarynohierarchy',
    'account': 'account',
    'amount_debit': 'debitamount',
    'amount_credit': 'creditamount',
    'amount_net': 'netamount',
    'amount_transaction_total': 'total',
    'class': 'class',
    'location': 'location',
    'department': 'department',
    'line': 'line',
    'name_line': 'entity',
    'memo_main': 'memo',
    'memo_line': 'memoline',
    'status': 'status',
    'approval_status': 'approvalstatus',
    'date_created': 'datecreated',
    'created_by': 'createdby',
    'name': 'name',
    'posting': 'posting',
    'company_name': 'companyname'
}


def convert_to_number(value: str) -> Any:
    """Convert string value to number if possible"""
    if not value or value.strip() == '':
        return None

    try:
        # Remove common formatting characters
        cleaned = value.replace(',', '').replace('$', '').strip()

        # Try to convert to decimal
        if '.' in cleaned or 'e' in cleaned.lower():
            return float(cleaned)
        else:
            return int(cleaned)
    except (ValueError, InvalidOperation):
        # If conversion fails, return as string
        return value


def format_date(date_str: str) -> str:
    """Format date string to ISO format"""
    if not date_str:
        return None

    try:
        # Common NetSuite date formats
        formats = [
            '%m/%d/%Y %I:%M %p',  # 5/17/2025 4:04 am
            '%m/%d/%Y',
            '%m/%d/%y',
            '%Y-%m-%d',
            '%m-%d-%Y',
            '%d/%m/%Y'
        ]

        for fmt in formats:
            try:
                dt = datetime.strptime(date_str, fmt)
                return dt.isoformat() + 'Z'
            except ValueError:
                continue

        # If no format matches, return as-is
        LOGGER.warning(f"Could not parse date: {date_str}")
        return date_str

    except Exception as e:
        LOGGER.warning(f"Date formatting error: {str(e)}")
        return date_str


def transform_record(
    record: Dict[str, Any], client: NetSuiteClient
) -> Dict[str, Any]:
    """Transform NetSuite record to our schema format"""
    transformed = {}

    # Handle record metadata
    transformed['internal_id'] = record.get('id', '')
    transformed['type'] = record.get('recordType', '')

    # Transform field values using mapping
    for schema_field, netsuite_field in FIELD_MAPPING.items():
        if schema_field in ['internal_id', 'type']:
            continue  # Already handled above

        raw_value = client.extract_field_value(record, netsuite_field)

        # Apply type-specific transformations
        amount_fields = [
            'amount_debit', 'amount_credit', 'amount_net',
            'amount_transaction_total'
        ]
        if schema_field in amount_fields:
            transformed[schema_field] = convert_to_number(raw_value)
        elif schema_field in ['date', 'date_created']:
            transformed[schema_field] = format_date(raw_value)
        else:
            transformed[schema_field] = raw_value if raw_value else None

    return transformed


def get_sync_params(config: Dict[str, Any]) -> Dict[str, Any]:
    """Extract sync parameters from config"""
    params = {}

    # Handle period_ids (convert single values to lists for compatibility)
    if config.get('period_ids'):
        period_ids = config['period_ids']
        if isinstance(period_ids, list):
            params['period_ids'] = period_ids
        else:
            params['period_ids'] = [period_ids]
    elif config.get('period_id'):
        # Legacy support: convert single period_id to list
        params['period_ids'] = [config['period_id']]

    # Handle period_names (convert single values to lists for compatibility)
    elif config.get('period_names'):
        period_names = config['period_names']
        if isinstance(period_names, list):
            params['period_names'] = period_names
        else:
            params['period_names'] = [period_names]
    elif config.get('period_name'):
        # Legacy support: convert single period_name to list
        params['period_names'] = [config['period_name']]

    return params


def sync_stream(
    client: NetSuiteClient,
    stream: CatalogEntry,
    state: Dict[str, Any],
    config: Dict[str, Any]
) -> Dict[str, Any]:
    """Sync a single stream using FULL_TABLE replication"""

    stream_name = stream.tap_stream_id
    LOGGER.info(f"Starting full refresh sync for stream: {stream_name}")

    # Write schema
    singer.write_schema(
        stream_name, stream.schema.to_dict(), stream.key_properties
    )

    # Get sync parameters
    sync_params = get_sync_params(config)
    LOGGER.info(f"Sync parameters: {sync_params}")

    # For full refresh, we always truncate and reload
    LOGGER.info(f"Performing FULL_TABLE refresh for {stream_name}")

    # Fetch ALL data first
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        records = loop.run_until_complete(
            client.fetch_gl_data(**sync_params)
        )
    finally:
        loop.close()

    if not records:
        LOGGER.warning("No records found for the specified date range")
        return state

    LOGGER.info(f"Fetched {len(records)} records, now processing...")

    # Process ALL records in one continuous stream
    record_count = 0
    for record in records:
        try:
            transformed = transform_record(record, client)
            singer.write_record(stream_name, transformed)
            record_count += 1

            # Log progress every 10000 records
            if record_count % 10000 == 0:
                LOGGER.info(f"Processed {record_count} records")

        except Exception as e:
            LOGGER.error(f"Error processing record: {str(e)}")
            continue

    LOGGER.info(f"Completed sync: {record_count} records processed")

    # Update state
    state[stream_name] = {
        'last_sync': datetime.now(timezone.utc).isoformat(),
        'record_count': record_count,
        'replication_method': 'FULL_TABLE',
        'sync_parameters': sync_params
    }

    return state
