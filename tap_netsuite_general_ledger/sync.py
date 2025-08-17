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
    'name_line': 'entity',
    'memo_main': 'memo',
    'memo_line': 'memoline',
    'status': 'status',
    'approval_status': 'approvalstatus',
    'date_created': 'datecreated',
    'created_by': 'createdby',
    'name': 'name',
    'posting': 'posting',
    'company_name': 'companyname',
    'transaction_line_id': 'line'
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
    record: Dict[str, Any],
    client: NetSuiteClient
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
    """Sync a single stream with FULL_TABLE replication and memory opts"""

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

    # Get batch size (default: 100000 to match target defaults)
    batch_size = config.get('batch_size', 100000)

    return _sync_stream_with_memory_optimization(
        client, stream, state, config, sync_params, batch_size
    )


def _sync_stream_with_memory_optimization(
    client: NetSuiteClient,
    stream: CatalogEntry,
    state: Dict[str, Any],
    config: Dict[str, Any],
    sync_params: Dict[str, Any],
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
        'stream_name': stream_name,
        'sync_params': sync_params
    }

    if 'bookmarks' not in sync_state['state']:
        sync_state['state']['bookmarks'] = {}

    # Write initial state
    sync_state['state']['bookmarks'][stream_name] = {
        'replication_method': 'FULL_TABLE',
        'sync_started': sync_state['start_time'].isoformat(),
        'sync_parameters': sync_params
    }

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

        LOGGER.info(
            f"Processing batch from {period_label} ({len(batch)} records)"
        )

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
            f"Completed batch from "
            f"{period_label}: {batch_processed} records processed"
        )

        # Write state after each batch for checkpointing
        try:
            sync_state['state']['bookmarks'][stream_name] = {
                'last_sync': datetime.now(timezone.utc).isoformat(),
                'record_count': sync_state['total_processed'],
                'replication_method': 'FULL_TABLE',
                'sync_parameters': sync_params,
                'current_batch': batch_num,
                'total_batches': total_batches,
                'period_label': period_label
            }
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

    # Process each period separately with full memory cleanup between periods
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        # Process periods one at a time to minimize peak memory usage
        if sync_params.get('period_ids'):
            for period_id in sync_params['period_ids']:
                LOGGER.info(f"Processing period ID: {period_id}")
                processed = loop.run_until_complete(
                    client.fetch_gl_data_streaming(
                        batch_callback=process_batch,
                        batch_size=batch_size,
                        period_ids=[period_id]  # Process one period at a time
                    )
                )
                # Force garbage collection between periods
                import gc
                gc.collect()
                LOGGER.info(
                    f"Completed period ID {period_id}: {processed} records"
                )

        elif sync_params.get('period_names'):
            for period_name in sync_params['period_names']:
                LOGGER.info(f"Processing period name: {period_name}")
                processed = loop.run_until_complete(
                    client.fetch_gl_data_streaming(
                        batch_callback=process_batch,
                        batch_size=batch_size,
                        # Process one period at a time
                        period_names=[period_name]
                    )
                )
                # Force garbage collection between periods
                import gc
                gc.collect()
                LOGGER.info(
                    f"Completed period {period_name}: {processed} records"
                )
        else:
            # Single period or default
            processed = loop.run_until_complete(
                client.fetch_gl_data_streaming(
                    batch_callback=process_batch,
                    batch_size=batch_size,
                    **sync_params
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
        'replication_method': 'FULL_TABLE',
        'sync_parameters': sync_params,
        'sync_completed': True
    }

    try:
        singer.write_state(sync_state['state'])
    except BrokenPipeError:
        LOGGER.warning("Broken pipe detected when writing final state")

    return sync_state['state']
