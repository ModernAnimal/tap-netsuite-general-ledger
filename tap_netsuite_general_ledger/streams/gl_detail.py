"""
GL Detail stream class for complex general ledger data
"""

import asyncio
import json
from datetime import datetime, timezone
from typing import Dict, Any, List

import singer
from singer import CatalogEntry

from .base import BaseStream

LOGGER = singer.get_logger()


class GLDetailStream(BaseStream):
    """Stream class for NetSuite General Ledger Detail with chunking"""

    def get_stream_id(self) -> str:
        """Return the stream ID"""
        return 'netsuite_general_ledger_detail'

    def get_key_properties(self) -> List[str]:
        """Return the key properties"""
        return ['internal_id', 'trans_acct_line_id']

    def transform_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Transform NetSuite SuiteQL record with lightweight type conversion

        Only converts numeric fields to int/float. Everything else stays as
        string. This is much faster than heavy transformation logic.

        Args:
            record: Raw record from NetSuite

        Returns:
            Transformed record or None if invalid
        """
        # Fields that should be integers
        int_fields = {
            'internal_id', 'acct_id', 'posting_period_id',
            'trans_acct_line_id', 'account_group', 'department', 'class',
            'location'
        }

        # Fields that should be floats
        float_fields = {'debit', 'credit', 'net_amount'}

        # All expected fields from the schema (to ensure they exist even if
        # NULL)
        all_expected_fields = {
            'posting_period', 'posting_period_id', 'created_date',
            'trans_acct_line_last_modified', 'transaction_last_modified',
            'account_last_modified', 'posting', 'approval',
            'transaction_date', 'transaction_id', 'trans_acct_line_id',
            'internal_id', 'entity_name', 'trans_memo', 'trans_line_memo',
            'transaction_type', 'acct_id', 'account_group', 'department',
            'class', 'location', 'debit', 'credit', 'net_amount',
            'subsidiary', 'document_number', 'status'
        }

        transformed = {}

        # First, initialize all expected fields to None
        for field in all_expected_fields:
            transformed[field] = None

        # Then process the actual record data
        for field, value in record.items():
            # Skip the 'links' field entirely (not needed)
            if field == 'links':
                continue

            # Convert numeric fields
            if field in int_fields:
                transformed[field] = self.safe_int(value)
            elif field in float_fields:
                transformed[field] = self.safe_float(value)
            else:
                # Everything else stays as string (or None if empty)
                transformed[field] = None if value == '' else value

        # Validate required fields
        if transformed.get('trans_acct_line_id') is None:
            LOGGER.warning(
                f"Skipping record with NULL/empty trans_acct_line_id: "
                f"internal_id={transformed.get('internal_id')}"
            )
            return None

        if transformed.get('internal_id') is None:
            LOGGER.warning(
                f"Skipping record with NULL/empty internal_id: "
                f"trans_acct_line_id={transformed.get('trans_acct_line_id')}"
            )
            return None

        return transformed

    def sync(
        self,
        catalog_entry: CatalogEntry,
        state: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Sync GL detail stream with page-level streaming

        This uses the existing complex logic for GL detail with chunking
        and incremental sync support.

        Args:
            catalog_entry: Catalog entry for this stream
            state: Current state dict

        Returns:
            Updated state dict
        """
        # Check if incremental sync is enabled
        if self.client.last_modified_date:
            LOGGER.info(
                f"Starting incremental sync for stream: {self.tap_stream_id} "
                f"(last_modified >= {self.client.last_modified_date})"
            )
        else:
            LOGGER.info(
                f"Starting full refresh sync for stream: {self.tap_stream_id}"
            )

        # Write schema
        self.write_schema()

        # Initialize state
        if state is None:
            state = {}
        if 'bookmarks' not in state:
            state['bookmarks'] = {}

        # Sync using page-level streaming
        return self._sync_page_by_page(state)

    def _sync_page_by_page(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Sync by processing pages as they arrive

        This is the original page-by-page sync logic for GL detail.
        Kept unchanged to preserve existing functionality.

        Args:
            state: Current state dict

        Returns:
            Updated state dict
        """
        replication_method = (
            'INCREMENTAL' if self.client.last_modified_date else 'FULL_TABLE'
        )

        # Initialize tracking
        total_processed = 0
        page_num = 0
        start_time = datetime.now(timezone.utc)

        # Write initial state
        state['bookmarks'][self.tap_stream_id] = {
            'replication_method': replication_method,
            'sync_started': start_time.isoformat(),
        }
        if self.client.last_modified_date:
            state['bookmarks'][self.tap_stream_id]['last_modified_date'] = (
                self.client.last_modified_date
            )

        self.write_state(state)

        # Create event loop for async operations
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            # Process each page as it arrives
            async def process_pages():
                nonlocal total_processed, page_num

                async for page in self.client.fetch_gl_data_pages():
                    page_num += 1
                    page_start_count = total_processed

                    LOGGER.info(
                        f"Processing page {page_num} ({len(page)} records)"
                    )

                    # Process each record in the page
                    for idx, record in enumerate(page):
                        try:
                            transformed = self.transform_record(record)

                            # Skip records with missing required fields
                            if transformed is None:
                                continue

                            try:
                                self.write_record(transformed)
                                total_processed += 1
                            except BrokenPipeError:
                                LOGGER.error(
                                    f"Broken pipe - target terminated after "
                                    f"{total_processed} records "
                                    f"(page {page_num}, record {idx + 1})"
                                )
                                LOGGER.error(
                                    f"Problem record - "
                                    f"internal_id: "
                                    f"{record.get('internal_id')}, "
                                    f"trans_acct_line_id: "
                                    f"{record.get('trans_acct_line_id')}, "
                                    f"transaction_id: "
                                    f"{record.get('transaction_id')}"
                                )
                                # Log record data for debugging
                                record_str = json.dumps(record)
                                LOGGER.error(f"Record data: {record_str}")
                                raise

                        except BrokenPipeError:
                            raise
                        except Exception as e:
                            LOGGER.warning(
                                f"Error processing record {idx + 1} "
                                f"in page {page_num}: {str(e)}"
                            )
                            continue

                    page_processed = total_processed - page_start_count
                    LOGGER.info(
                        f"Completed page {page_num}: "
                        f"{page_processed} records processed "
                        f"(Total: {total_processed})"
                    )

                    # Write state after each page for checkpointing
                    state['bookmarks'][self.tap_stream_id] = {
                        'last_sync': datetime.now(
                            timezone.utc
                        ).isoformat(),
                        'record_count': total_processed,
                        'replication_method': replication_method,
                        'current_page': page_num
                    }
                    if self.client.last_modified_date:
                        state['bookmarks'][self.tap_stream_id][
                            'last_modified_date'
                        ] = self.client.last_modified_date

                    self.write_state(state)

            # Run the async page processor
            loop.run_until_complete(process_pages())

        except BrokenPipeError:
            LOGGER.warning("Broken pipe during sync - exiting gracefully")
            return state
        except Exception as e:
            LOGGER.error(f"Error during sync: {str(e)}")
            raise
        finally:
            loop.close()

        LOGGER.info(
            f"Completed sync: {total_processed} records processed "
            f"across {page_num} pages"
        )

        # Final state update
        state['bookmarks'][self.tap_stream_id] = {
            'last_sync': datetime.now(timezone.utc).isoformat(),
            'record_count': total_processed,
            'replication_method': replication_method,
            'sync_completed': True
        }
        if self.client.last_modified_date:
            state['bookmarks'][self.tap_stream_id]['last_modified_date'] = (
                self.client.last_modified_date
            )

        self.write_state(state)

        return state
