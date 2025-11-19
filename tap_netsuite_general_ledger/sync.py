"""
Sync functionality for NetSuite GL Detail tap
"""

import asyncio
from datetime import datetime, timezone
from typing import Dict, Any, List
from decimal import InvalidOperation

import singer
from singer import CatalogEntry

from .client import NetSuiteClient

LOGGER = singer.get_logger()


def convert_to_number(value: Any) -> Any:
    """Convert value to number if possible"""
    if value is None or value == '':
        return None

    # If already a number, return as-is
    if isinstance(value, (int, float)):
        return value

    # Try to convert string to number
    if isinstance(value, str):
        try:
            # Remove common formatting characters
            cleaned = value.replace(',', '').replace('$', '').strip()

            # Try to convert to decimal
            if '.' in cleaned or 'e' in cleaned.lower():
                return float(cleaned)
            else:
                return int(cleaned)
        except (ValueError, InvalidOperation):
            return None

    return None


def convert_to_integer(value: Any) -> Any:
    """Convert value to integer if possible"""
    if value is None or value == '':
        return None

    # If already an integer, return as-is
    if isinstance(value, int):
        return value

    # Try to convert to integer
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def format_date(date_str: Any) -> str:
    """Format date string to ISO format (YYYY-MM-DD)"""
    if not date_str or date_str == '':
        return None

    # If already a string, try to parse and format it
    if isinstance(date_str, str):
        # Check if already in ISO format
        if len(date_str) >= 10 and date_str[4] == '-' and date_str[7] == '-':
            # Already in YYYY-MM-DD format (or YYYY-MM-DD HH:MM:SS)
            return date_str[:10]
        
        try:
            # Common NetSuite date formats
            formats = [
                '%Y-%m-%d',            # 2025-03-17
                '%m/%d/%Y',            # 3/17/2025
                '%m/%d/%y',            # 3/17/25
                '%m-%d-%Y',
                '%d/%m/%Y'
            ]

            for fmt in formats:
                try:
                    dt = datetime.strptime(date_str, fmt)
                    return dt.strftime('%Y-%m-%d')
                except ValueError:
                    continue

            # If no format matches, log and return None
            LOGGER.warning(f"Unable to parse date: {date_str}")
            return None

        except Exception as e:
            LOGGER.warning(
                f"Date formatting error: {str(e)} for value: {date_str}"
            )
            return None

    return None


def format_datetime(datetime_str: Any) -> str:
    """Format datetime string to ISO format with timezone"""
    if not datetime_str or datetime_str == '':
        return None

    if isinstance(datetime_str, str):
        # Check if already in ISO format with Z
        if 'T' in datetime_str:
            # Already in ISO format (e.g., 2025-03-17T14:30:00Z)
            if datetime_str.endswith('Z'):
                return datetime_str
            else:
                return datetime_str + 'Z'
        
        try:
            # Common NetSuite datetime formats
            formats = [
                '%Y-%m-%d %H:%M:%S',   # 2025-03-17 14:30:00
                '%m/%d/%Y %I:%M %p',   # 5/17/2025 4:04 am
                '%m/%d/%Y %H:%M:%S',   # 5/17/2025 16:04:00
                '%Y-%m-%dT%H:%M:%S',   # 2025-03-17T14:30:00
            ]

            for fmt in formats:
                try:
                    dt = datetime.strptime(datetime_str, fmt)
                    return dt.isoformat() + 'Z'
                except ValueError:
                    continue

            # If no format matches, log and return None
            LOGGER.warning(f"Unable to parse datetime: {datetime_str}")
            return None

        except Exception as e:
            LOGGER.warning(
                f"Datetime formatting error: {str(e)} "
                f"for value: {datetime_str}"
            )
            return None

    return None


def transform_record(
    record: Dict[str, Any],
    client: NetSuiteClient
) -> Dict[str, Any]:
    """Transform NetSuite SuiteQL record to schema format

    SuiteQL returns flat JSON, so we just need to handle type conversions
    """
    transformed = {}

    # String fields - use as-is or convert to string
    string_fields = [
        'posting_period', 'tran_id', 'transaction_type',
        'account_number', 'account_name', 'memo', 'account_type',
        'department_name', 'class_name', 'location_name',
        'entity_name', 'subsidiary_name', 'currency'
    ]
    for field in string_fields:
        value = record.get(field)
        # Only convert to string if not None; keep None as None
        if value is not None and value != '':
            transformed[field] = str(value)
        else:
            transformed[field] = None

    # Integer fields
    integer_fields = [
        'posting_period_id', 'internalid', 'transAcctLineID',
        'acctID', 'entity_id', 'subsidiary_id', 'account_internalid'
    ]
    for field in integer_fields:
        transformed[field] = convert_to_integer(record.get(field))

    # Numeric fields (float)
    numeric_fields = ['debit', 'credit', 'net_amount']
    for field in numeric_fields:
        transformed[field] = convert_to_number(record.get(field))

    # Date fields
    transformed['tran_date'] = format_date(record.get('tran_date'))

    # Datetime fields
    transformed['lastmodified'] = format_datetime(record.get('lastmodified'))

    return transformed


def get_sync_params(config: Dict[str, Any]) -> Dict[str, Any]:
    """Extract sync parameters from config"""
    params = {}

    # Check for last_modified_date for incremental sync
    if config.get('last_modified_date'):
        params['last_modified_date'] = config['last_modified_date']

    return params


def sync_stream(
    client: NetSuiteClient,
    stream: CatalogEntry,
    state: Dict[str, Any],
    config: Dict[str, Any]
) -> Dict[str, Any]:
    """Sync a single stream with FULL_TABLE or incremental replication"""

    stream_name = stream.tap_stream_id

    # Check if incremental sync is enabled
    if client.last_modified_date:
        LOGGER.info(
            f"Starting incremental sync for stream: {stream_name} "
            f"(last_modified >= {client.last_modified_date})"
        )
    else:
        LOGGER.info(f"Starting full refresh sync for stream: {stream_name}")

    # Write schema
    singer.write_schema(
        stream_name, stream.schema.to_dict(), stream.key_properties
    )

    # Get batch size (default: 100000)
    batch_size = config.get('batch_size', 100000)

    return _sync_stream_with_memory_optimization(
        client, stream, state, config, batch_size
    )


def _sync_stream_with_memory_optimization(
    client: NetSuiteClient,
    stream: CatalogEntry,
    state: Dict[str, Any],
    config: Dict[str, Any],
    batch_size: int
) -> Dict[str, Any]:
    """Sync a single stream using memory-optimized batching"""

    stream_name = stream.tap_stream_id
    LOGGER.info(
        f"Using memory-optimized processing with batch size: {batch_size}"
    )

    # Track overall sync state
    sync_state = {
        'total_processed': 0,
        'start_time': datetime.now(timezone.utc),
        'state': state.copy() if state else {},
        'stream_name': stream_name
    }

    if 'bookmarks' not in sync_state['state']:
        sync_state['state']['bookmarks'] = {}

    # Write initial state
    replication_method = (
        'INCREMENTAL' if client.last_modified_date else 'FULL_TABLE'
    )
    sync_state['state']['bookmarks'][stream_name] = {
        'replication_method': replication_method,
        'sync_started': sync_state['start_time'].isoformat(),
    }

    if client.last_modified_date:
        sync_state['state']['bookmarks'][stream_name]['last_modified_date'] = (
            client.last_modified_date
        )

    try:
        singer.write_state(sync_state['state'])
    except BrokenPipeError:
        LOGGER.warning("Broken pipe detected when writing initial state")
        return sync_state['state']
    except Exception as state_error:
        LOGGER.error(f"Error writing initial state: {str(state_error)}")

    async def process_batch(
        batch: List[Dict[str, Any]],
        batch_num: int,
        total_batches: int,
        period_label: str
    ):
        """Process a single batch of records"""
        batch_start_count = sync_state['total_processed']

        LOGGER.info(f"Processing batch {batch_num} ({len(batch)} records)")

        for record in batch:
            try:
                transformed = transform_record(record, client)

                try:
                    singer.write_record(stream_name, transformed)
                    sync_state['total_processed'] += 1
                except BrokenPipeError:
                    LOGGER.error(
                        f"Broken pipe error - target process terminated after "
                        f"{sync_state['total_processed']} records. "
                        f"Check target logs for errors."
                    )
                    raise
                except Exception as write_error:
                    LOGGER.error(f"Error writing record: {str(write_error)}")
                    continue

            except Exception as e:
                LOGGER.error(f"Error processing record: {str(e)}")
                continue

        batch_processed = sync_state['total_processed'] - batch_start_count
        LOGGER.info(
            f"Completed batch {batch_num}: {batch_processed} records processed"
        )

        # Write state after each batch for checkpointing
        try:
            sync_state['state']['bookmarks'][stream_name] = {
                'last_sync': datetime.now(timezone.utc).isoformat(),
                'record_count': sync_state['total_processed'],
                'replication_method': replication_method,
                'current_batch': batch_num
            }

            if client.last_modified_date:
                sync_state['state']['bookmarks'][stream_name][
                    'last_modified_date'
                ] = client.last_modified_date

            singer.write_state(sync_state['state'])
        except BrokenPipeError:
            LOGGER.warning(
                f"Broken pipe detected when writing state after batch "
                f"{batch_num} - exiting gracefully"
            )
            raise
        except Exception as state_error:
            LOGGER.error(
                f"Error writing state after batch: {str(state_error)}"
            )

    # Run the streaming fetch
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        loop.run_until_complete(
            client.fetch_gl_data_streaming(
                batch_callback=process_batch,
                batch_size=batch_size
            )
        )
    except BrokenPipeError:
        LOGGER.warning(
            "Broken pipe detected during streaming - exiting gracefully"
        )
        return sync_state['state']
    except Exception as e:
        LOGGER.error(f"Error during streaming sync: {str(e)}")
        raise
    finally:
        loop.close()

    LOGGER.info(
        f"Completed streaming sync: {sync_state['total_processed']} "
        f"records processed"
    )

    # Final state update
    sync_state['state']['bookmarks'][stream_name] = {
        'last_sync': datetime.now(timezone.utc).isoformat(),
        'record_count': sync_state['total_processed'],
        'replication_method': replication_method,
        'sync_completed': True
    }

    if client.last_modified_date:
        sync_state['state']['bookmarks'][stream_name]['last_modified_date'] = (
            client.last_modified_date
        )

    try:
        singer.write_state(sync_state['state'])
    except BrokenPipeError:
        LOGGER.warning("Broken pipe detected when writing final state")

    return sync_state['state']
