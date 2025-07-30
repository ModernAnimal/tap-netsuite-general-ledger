"""
Sync functionality for NetSuite GL Detail tap
"""

import asyncio
from datetime import datetime
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


def transform_record(record: Dict[str, Any], client: NetSuiteClient) -> Dict[str, Any]:
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
        if schema_field in ['amount_debit', 'amount_credit', 'amount_net', 'amount_transaction_total']:
            transformed[schema_field] = convert_to_number(raw_value)
        elif schema_field in ['date', 'date_created']:
            transformed[schema_field] = format_date(raw_value)
        else:
            transformed[schema_field] = raw_value if raw_value else None

    return transformed


def get_sync_params(config: Dict[str, Any]) -> Dict[str, Any]:
    """Extract sync parameters from config"""
    params = {}

    if config.get('period_id'):
        params['period_id'] = config['period_id']
    elif config.get('period_name'):
        params['period_name'] = config['period_name']

    if config.get('date_from'):
        params['date_from'] = config['date_from']
    if config.get('date_to'):
        params['date_to'] = config['date_to']

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
    singer.write_schema(stream_name, stream.schema.to_dict(), stream.key_properties)

    # Get sync parameters
    sync_params = get_sync_params(config)
    LOGGER.info(f"Sync parameters: {sync_params}")

    # For full refresh, we always truncate and reload
    # This ensures any deletions in NetSuite are reflected in the target
    LOGGER.info(f"Performing FULL_TABLE refresh for {stream_name}")

    # Note: Singer targets handle table truncation automatically for FULL_TABLE
    # replication method when they receive the schema message

    # Fetch data asynchronously
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

    LOGGER.info(f"Processing {len(records)} records for full refresh")

    # Transform and write records
    record_count = 0
    for record in records:
        try:
            transformed = transform_record(record, client)
            singer.write_record(stream_name, transformed)
            record_count += 1

            # Log progress every 1000 records
            if record_count % 1000 == 0:
                LOGGER.info(f"Processed {record_count} records")

        except Exception as e:
            LOGGER.error(f"Error transforming record: {str(e)}")
            LOGGER.error(f"Record data: {record}")
            continue

    LOGGER.info(
        f"Completed full refresh for {stream_name}: {record_count} records"
    )

    # Update state with completion time and date range
    sync_date_range = ""
    if sync_params.get('date_from') and sync_params.get('date_to'):
        date_from = sync_params['date_from']
        date_to = sync_params['date_to']
        sync_date_range = f" (date range: {date_from} to {date_to})"
    elif sync_params.get('period_name'):
        sync_date_range = f" (period: {sync_params['period_name']})"
    elif sync_params.get('period_id'):
        sync_date_range = f" (period ID: {sync_params['period_id']})"

    state[stream_name] = {
        'last_sync': datetime.utcnow().isoformat(),
        'record_count': record_count,
        'replication_method': 'FULL_TABLE',
        'sync_parameters': sync_params,
        'sync_note': f"Full refresh completed{sync_date_range}"
    }

    return state
