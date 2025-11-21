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
    """Convert value to number if possible, preserving zero values"""
    # Only return None for actual None or empty string
    if value is None or value == '':
        return None

    # If already a number, return as-is (including 0 and 0.0)
    if isinstance(value, (int, float)):
        return value

    # Try to convert string to number
    if isinstance(value, str):
        try:
            # Remove common formatting characters
            cleaned = value.replace(',', '').replace('$', '').strip()

            # After cleaning, check if empty
            if cleaned == '':
                return None

            # Try to convert to number
            if '.' in cleaned or 'e' in cleaned.lower():
                return float(cleaned)
            else:
                return int(cleaned)
        except (ValueError, InvalidOperation, AttributeError):
            # If conversion fails, return the original value as string
            # This preserves the data rather than losing it
            return str(value)

    # For any other type, convert to string to preserve data
    return str(value)


def convert_to_integer(value: Any) -> Any:
    """Convert value to integer if possible, preserving zero values"""
    # Only return None for actual None or empty string
    if value is None or value == '':
        return None

    # If already an integer, return as-is (including 0)
    if isinstance(value, int):
        return value

    # If it's a float, convert to int (including 0.0 -> 0)
    if isinstance(value, float):
        return int(value)

    # Try to convert string to integer
    if isinstance(value, str):
        try:
            # Strip whitespace first
            cleaned = value.strip()

            # After cleaning, check if empty
            if cleaned == '':
                return None

            # Try direct conversion
            return int(cleaned)
        except (ValueError, TypeError, AttributeError):
            # Try to convert to float first, then to int
            try:
                return int(float(cleaned))
            except (ValueError, TypeError, AttributeError):
                # If conversion fails, return original value as string
                # This preserves the data rather than losing it
                return str(value)

    # For any other type, convert to string to preserve data
    return str(value)


def format_date(date_str: Any) -> str:
    """Format date string to ISO format (YYYY-MM-DD)
    
    Preserves non-null values by returning original if parsing fails
    """
    # Only return None for actual None or empty string
    if date_str is None or date_str == '':
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

            # If no format matches, log and return original value
            # This preserves data rather than losing it
            LOGGER.warning(
                f"Unable to parse date: {date_str}, "
                f"returning original value"
            )
            return str(date_str)

        except Exception as e:
            LOGGER.warning(
                f"Date formatting error: {str(e)} for value: {date_str}, "
                f"returning original value"
            )
            return str(date_str)

    # For non-string types, convert to string to preserve data
    return str(date_str)


def format_datetime(datetime_str: Any) -> str:
    """Format datetime string to ISO format with timezone
    
    Preserves non-null values by returning original if parsing fails
    """
    # Only return None for actual None or empty string
    if datetime_str is None or datetime_str == '':
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

            # If no format matches, log and return original value
            # This preserves data rather than losing it
            LOGGER.warning(
                f"Unable to parse datetime: {datetime_str}, "
                f"returning original value"
            )
            return str(datetime_str)

        except Exception as e:
            LOGGER.warning(
                f"Datetime formatting error: {str(e)} "
                f"for value: {datetime_str}, returning original value"
            )
            return str(datetime_str)

    # For non-string types, convert to string to preserve data
    return str(datetime_str)


def transform_record(
    record: Dict[str, Any],
    client: NetSuiteClient
) -> Dict[str, Any]:
    """Transform NetSuite SuiteQL record to schema format

    Converts values to appropriate data types based on field names.
    """
    transformed = {}

    # Define field type mappings
    integer_fields = {
        'internal_id', 'posting_period_id', 'trans_acct_line_id',
        'acct_id', 'account_group', 'department', 'class', 'location'
    }

    number_fields = {
        'debit', 'credit', 'net_amount'
    }

    date_fields = {
        'transaction_date', 'account_last_modified',
        'trans_acct_line_last_modified', 'transaction_last_modified'
    }

    datetime_fields = {
        'created_date'
    }

    for field, value in record.items():
        # Handle null/empty values
        if value is None or value == '':
            transformed[field] = None
            continue

        # Convert based on field type
        if field in integer_fields:
            transformed[field] = convert_to_integer(value)
        elif field in number_fields:
            transformed[field] = convert_to_number(value)
        elif field in date_fields:
            transformed[field] = format_date(value)
        elif field in datetime_fields:
            transformed[field] = format_datetime(value)
        else:
            # Keep as string
            transformed[field] = str(value)

    # Validate that key fields are not None or empty
    # Note: Use explicit None check because 0 is a valid integer value
    if transformed.get('trans_acct_line_id') is None:
        LOGGER.warning(
            f"Skipping record with NULL/empty trans_acct_line_id: "
            f"internal_id={transformed.get('internal_id')}, "
            f"raw_value={record.get('trans_acct_line_id')}"
        )
        return None

    if transformed.get('internal_id') is None:
        LOGGER.warning(
            f"Skipping record with NULL/empty internal_id: "
            f"trans_acct_line_id={transformed.get('trans_acct_line_id')}"
        )
        return None

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

                # Skip records where transformation returned None
                # (indicates missing required fields)
                if transformed is None:
                    continue

                try:
                    singer.write_record(stream_name, transformed)
                    sync_state['total_processed'] += 1
                except BrokenPipeError:
                    LOGGER.error(
                        f"Broken pipe error - target process terminated "
                        f"after {sync_state['total_processed']} records "
                        f"(batch {batch_num}, record {len(batch)}). "
                        f"This usually indicates the target crashed. "
                        f"Check target logs for errors."
                    )
                    raise
                except IOError as io_error:
                    if io_error.errno == 32:  # EPIPE
                        LOGGER.error(
                            f"Broken pipe (IOError) - target terminated "
                            f"after {sync_state['total_processed']} records. "
                            f"Check target logs."
                        )
                        raise BrokenPipeError from io_error
                    else:
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
