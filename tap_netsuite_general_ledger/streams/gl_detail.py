"""
GL Detail stream class for complex general ledger data
"""

import asyncio
from datetime import datetime, timezone
from typing import Dict, Any, List

import singer
from singer import CatalogEntry

from .base import BaseStream

LOGGER = singer.get_logger()


class GLDetailStream(BaseStream):
    """Stream class for NetSuite General Ledger Detail with chunking"""

    # Pre-compiled field sets for optimized transformation
    INT_FIELDS = frozenset({
        'internal_id', 'acct_id', 'posting_period_id',
        'trans_acct_line_id', 'account_group', 'department', 'class',
        'location'
    })

    FLOAT_FIELDS = frozenset({'debit', 'credit', 'net_amount'})

    ALL_EXPECTED_FIELDS = frozenset({
        'posting_period', 'posting_period_id', 'created_date',
        'trans_acct_line_last_modified', 'transaction_last_modified',
        'account_last_modified', 'posting', 'approval',
        'transaction_date', 'transaction_id', 'trans_acct_line_id',
        'internal_id', 'entity_name', 'trans_memo', 'trans_line_memo',
        'transaction_type', 'acct_id', 'account_group', 'department',
        'class', 'location', 'debit', 'credit', 'net_amount',
        'subsidiary', 'document_number', 'status'
    })

    def get_stream_id(self) -> str:
        """Return the stream ID"""
        return 'netsuite_general_ledger_detail'

    def get_key_properties(self) -> List[str]:
        """Return the key properties"""
        return ['internal_id', 'trans_acct_line_id']

    def build_query(
        self,
        min_internal_id: int = 0,
        last_modified_date: str = None,
        posting_period_name: str = None
    ) -> str:
        """Build the SuiteQL query to fetch GL data

        Args:
            min_internal_id: Minimum internal ID to fetch (for chunking
                beyond offset limit)
            last_modified_date: Optional date filter for incremental sync
            posting_period_name: Optional posting period name to filter by
                (e.g., "Jan 2025")

        Returns:
            SuiteQL query string
        """
        # Base query with all fields
        # Note: TransactionAccountingLine (tal) has debit/credit/account
        #       TransactionLine (tl) has department/class/location/memo
        query = """
        SELECT
            t.ID AS internal_id,
            t.Trandate AS transaction_date,
            coalesce(t.TranID, 'NULL') AS transaction_id,
            tal.TransactionLine AS trans_acct_line_id,
            BUILTIN.DF(t.PostingPeriod) AS posting_period,
            t.PostingPeriod AS posting_period_id,
            t.createdDateTime AS created_date,
            tal.lastmodifieddate AS trans_acct_line_last_modified,
            t.lastmodifieddate AS transaction_last_modified,
            a.lastmodifieddate AS account_last_modified,
            t.Posting AS posting,
            BUILTIN.DF(t.approvalStatus) AS approval,
            BUILTIN.DF(t.Entity) AS entity_name,
            t.memo AS trans_memo,
            tl.memo AS trans_line_memo,
            BUILTIN.DF(t.Type) AS transaction_type,
            tal.Account AS acct_id,
            a.parent AS account_group,
            tl.Department AS department,
            tl.Class AS class,
            tl.Location AS location,
            tal.Debit AS debit,
            tal.Credit AS credit,
            tal.Amount AS net_amount,
            BUILTIN.DF(tl.Subsidiary) AS subsidiary,
            t.Number AS document_number,
            BUILTIN.DF(t.Status) AS status
        FROM
            Transaction t
        INNER JOIN TransactionAccountingLine tal ON (
            tal.Transaction = t.ID
        )
        INNER JOIN Account a ON (
            a.ID = tal.Account
        )
        LEFT JOIN TransactionLine tl ON (
            tl.transaction = t.ID
            AND tl.id = tal.TransactionLine
        )
        WHERE
            ( t.Posting = 'T' )
            AND ( tal.Posting = 'T' )
            AND (
                ( tal.Debit IS NOT NULL )
                OR ( tal.Credit IS NOT NULL )
            )
        """

        # Add posting period filter if specified
        if posting_period_name is not None:
            query += (
                f" AND BUILTIN.DF(t.PostingPeriod) = '{posting_period_name}'"
            )

        # Add ID filter if chunking (to handle offset limit)
        if min_internal_id > 0:
            query += f" AND t.ID > {min_internal_id}"

        # Add incremental filter if last_modified_date is set
        if last_modified_date:
            query += (
                f"""
                    AND (
                        t.lastModifiedDate >=
                        TO_DATE('{last_modified_date}', 'YYYY-MM-DD')
                        OR tal.lastModifiedDate >=
                        TO_DATE('{last_modified_date}', 'YYYY-MM-DD')
                        OR a.lastModifiedDate >=
                        TO_DATE('{last_modified_date}', 'YYYY-MM-DD')
                    )
                """
            )

        # Order by transaction ID and line ID for consistent pagination
        query += " ORDER BY t.id, t.TranDate, t.TranID, tal.TransactionLine"

        return query

    def transform_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Transform NetSuite SuiteQL record with optimized type conversion

        Uses pre-compiled field sets and dict comprehension for maximum
        performance. This is significantly faster than the original
        implementation when processing millions of records.

        Args:
            record: Raw record from NetSuite

        Returns:
            Transformed record or None if invalid
        """
        # Fast path: use dict comprehension with inline type checking
        transformed = {
            field: (
                self.safe_int(record.get(field))
                if field in self.INT_FIELDS
                else self.safe_float(record.get(field))
                if field in self.FLOAT_FIELDS
                else (None if record.get(field) == '' else record.get(field))
            )
            for field in self.ALL_EXPECTED_FIELDS
            if field != 'links'  # Skip links field
        }

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
        and incremental sync support. If posting_period_ids are configured,
        will loop through each period sequentially.

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

        # Get posting periods from config
        posting_periods = self.config.get('posting_periods', [])
        
        # If empty or not provided, do a single sync without period filter
        if not posting_periods:
            LOGGER.info(
                "No posting_periods configured - syncing all periods"
            )
            total_records = self._sync_page_by_page(state, None)
            
            # Write final state
            state['bookmarks'][self.tap_stream_id] = {
                'last_sync': datetime.now(timezone.utc).isoformat(),
                'record_count': total_records,
                'replication_method': (
                    'INCREMENTAL'
                    if self.client.last_modified_date
                    else 'FULL_TABLE'
                ),
                'sync_completed': True
            }
            if self.client.last_modified_date:
                state['bookmarks'][self.tap_stream_id][
                    'last_modified_date'
                ] = self.client.last_modified_date
            
            self.write_state(state)
            return state

        # Otherwise, loop through each posting period
        LOGGER.info(
            f"Syncing {len(posting_periods)} posting periods: "
            f"{posting_periods}"
        )

        # Get previously completed periods from state
        stream_state = state['bookmarks'].get(self.tap_stream_id, {})
        completed_periods = set(
            stream_state.get('completed_posting_periods', [])
        )

        # Track per-period stats
        period_stats = stream_state.get('posting_period_stats', {})
        total_records_all_periods = 0

        # Process each posting period sequentially
        for period_name in posting_periods:
            # Skip if already completed
            if period_name in completed_periods:
                LOGGER.info(
                    f"Posting period '{period_name}' already completed - "
                    f"skipping"
                )
                # Add to total from previous run
                total_records_all_periods += period_stats.get(
                    period_name, {}
                ).get('record_count', 0)
                continue

            LOGGER.info(
                f"Syncing posting period '{period_name}' "
                f"({posting_periods.index(period_name) + 1}/"
                f"{len(posting_periods)})"
            )

            # Sync this posting period
            period_start_time = datetime.now(timezone.utc)
            period_records = self._sync_page_by_page(state, period_name)
            period_end_time = datetime.now(timezone.utc)

            # Update per-period stats
            period_stats[period_name] = {
                'record_count': period_records,
                'sync_started': period_start_time.isoformat(),
                'sync_completed': period_end_time.isoformat(),
                'duration_seconds': (
                    period_end_time - period_start_time
                ).total_seconds()
            }

            # Mark period as completed
            completed_periods.add(period_name)
            total_records_all_periods += period_records

            LOGGER.info(
                f"Completed posting period '{period_name}': "
                f"{period_records} records"
            )

            # Update state after each period for checkpointing
            state['bookmarks'][self.tap_stream_id] = {
                'last_sync': datetime.now(timezone.utc).isoformat(),
                'total_record_count': total_records_all_periods,
                'completed_posting_periods': sorted(list(completed_periods)),
                'posting_period_stats': period_stats,
                'replication_method': (
                    'INCREMENTAL'
                    if self.client.last_modified_date
                    else 'FULL_TABLE'
                ),
                'sync_completed': False
            }
            if self.client.last_modified_date:
                state['bookmarks'][self.tap_stream_id][
                    'last_modified_date'
                ] = self.client.last_modified_date

            self.write_state(state)

        # Final state update - mark sync as complete
        state['bookmarks'][self.tap_stream_id]['sync_completed'] = True
        state['bookmarks'][self.tap_stream_id]['sync_finished'] = (
            datetime.now(timezone.utc).isoformat()
        )

        LOGGER.info(
            f"Completed all posting periods: "
            f"{total_records_all_periods} total records"
        )

        self.write_state(state)
        return state

    def _sync_page_by_page(
        self,
        state: Dict[str, Any],
        posting_period_name: str = None
    ) -> Dict[str, Any]:
        """Sync by processing pages as they arrive

        This is the original page-by-page sync logic for GL detail.
        Kept unchanged to preserve existing functionality.

        Args:
            state: Current state dict
            posting_period_name: Optional posting period name to filter by
                (e.g., "Jan 2025")

        Returns:
            Total number of records processed (int)
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
        if posting_period_name is not None:
            state['bookmarks'][self.tap_stream_id][
                'current_posting_period'
            ] = posting_period_name
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

                # Batch accumulator for efficient writing
                batch = []
                batch_size = self.client.record_batch_size

                # Pass query builder to client with posting period
                def query_builder(min_id, last_mod):
                    return self.build_query(
                        min_id, last_mod, posting_period_name
                    )

                async for page in self.client.fetch_gl_data_pages(
                    query_builder
                ):
                    page_num += 1
                    page_start_count = total_processed

                    LOGGER.info(
                        f"Processing page {page_num} ({len(page)} records)"
                    )

                    # Transform all records in the page
                    for idx, record in enumerate(page):
                        try:
                            transformed = self.transform_record(record)

                            # Skip records with missing required fields
                            if transformed is None:
                                continue

                            # Add to batch
                            batch.append(transformed)

                            # Write batch when it reaches batch_size
                            if len(batch) >= batch_size:
                                try:
                                    self.write_records_batch(batch)
                                    total_processed += len(batch)
                                    batch = []  # Clear batch
                                except BrokenPipeError:
                                    LOGGER.error(
                                        f"Broken pipe - target terminated "
                                        f"after {total_processed} records "
                                        f"(page {page_num}, batch at "
                                        f"record {idx + 1})"
                                    )
                                    raise

                        except BrokenPipeError:
                            raise
                        except Exception as e:
                            LOGGER.warning(
                                f"Error processing record {idx + 1} "
                                f"in page {page_num}: {str(e)}"
                            )
                            continue

                    # Write any remaining records in the batch at page boundary
                    if batch:
                        try:
                            self.write_records_batch(batch)
                            total_processed += len(batch)
                            batch = []
                        except BrokenPipeError:
                            LOGGER.error(
                                f"Broken pipe when flushing batch at "
                                f"page {page_num}"
                            )
                            raise

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
                    if posting_period_name is not None:
                        state['bookmarks'][self.tap_stream_id][
                            'current_posting_period'
                        ] = posting_period_name
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

        # Return total processed for aggregation
        return total_processed
