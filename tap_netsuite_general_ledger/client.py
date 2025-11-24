"""
NetSuite Client for Singer Tap
Handles authentication and API requests to NetSuite SuiteQL API

This client is responsible for transport-level concerns only:
- OAuth 1.0a HMAC-SHA256 authentication
- HTTP request/response handling
- Pagination and chunking logic
- Error handling and retries

The client does NOT define queries or business logic - that belongs
in the stream classes.
"""

import asyncio
import time
import hmac
import hashlib
import base64
import secrets
from urllib.parse import quote
from collections import OrderedDict
from typing import Dict, Any, List

import aiohttp
import singer

LOGGER = singer.get_logger()


class NetSuiteClient:
    """NetSuite API client with OAuth 1.0a authentication for SuiteQL"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config

        # Required configuration
        self.account = config["netsuite_account"]
        self.consumer_key = config["netsuite_consumer_key"]
        self.consumer_secret = config["netsuite_consumer_secret"]
        self.token_id = config["netsuite_token_id"]
        self.token_secret = config["netsuite_token_secret"]

        # Optional configuration
        self.page_size = config.get("page_size", 1000)
        self.last_modified_date = config.get("last_modified_date")
        self.concurrent_requests = config.get("concurrent_requests", 5)

        # Build SuiteQL URL
        self.base_url = f"https://{self.account}.suitetalk.api.netsuite.com"
        self.suiteql_url = f"{self.base_url}/services/rest/query/v1/suiteql"
        
        # HTTP session for connection reuse
        self.session = None

        LOGGER.info(
            f"Initialized NetSuite SuiteQL client for account: {self.account}"
        )
        LOGGER.info(
            f"Concurrency: {self.concurrent_requests} concurrent requests"
        )
        if self.last_modified_date:
            LOGGER.info(
                f"Incremental sync mode: "
                f"last_modified_date = {self.last_modified_date}"
            )

    async def _ensure_session(self):
        """Ensure aiohttp session exists for connection reuse"""
        if self.session is None:
            connector = aiohttp.TCPConnector(
                limit=self.concurrent_requests * 2,
                limit_per_host=self.concurrent_requests
            )
            timeout = aiohttp.ClientTimeout(total=600)  # 10 minutes
            self.session = aiohttp.ClientSession(
                connector=connector,
                timeout=timeout
            )
            LOGGER.info("Created persistent HTTP session for connection reuse")

    async def _close_session(self):
        """Close the aiohttp session"""
        if self.session is not None:
            await self.session.close()
            self.session = None
            LOGGER.info("Closed HTTP session")

    def generate_oauth_header(
        self,
        url: str,
        query_params: Dict[str, Any] = None
    ) -> str:
        """Generate OAuth 1.0a authorization header for SuiteQL

        Args:
            url: Base URL without query parameters
            query_params: Dictionary of query parameters (e.g., limit, offset)
        """
        oauth_nonce = secrets.token_hex(16)
        oauth_timestamp = str(int(time.time()))

        oauth_params = OrderedDict([
            ('oauth_consumer_key', self.consumer_key),
            ('oauth_nonce', oauth_nonce),
            ('oauth_signature_method', 'HMAC-SHA256'),
            ('oauth_timestamp', oauth_timestamp),
            ('oauth_token', self.token_id),
            ('oauth_version', '1.0')
        ])

        # Combine OAuth params with query params for signature
        all_params = OrderedDict()
        if query_params:
            all_params.update(query_params)
        all_params.update(oauth_params)

        # Build parameter string for signature (sorted)
        param_pairs = []
        for k, v in sorted(all_params.items()):
            encoded_key = quote(str(k), safe='')
            encoded_value = quote(str(v), safe='')
            param_pairs.append(f"{encoded_key}={encoded_value}")
        param_string = '&'.join(param_pairs)

        # Build signature base string
        signature_base = (
            f"POST&{quote(url, safe='')}"
            f"&{quote(param_string, safe='')}"
        )

        # Create signing key
        signing_key = f"{self.consumer_secret}&{self.token_secret}"

        # Generate signature
        signature = base64.b64encode(
            hmac.new(
                signing_key.encode('utf-8'),
                signature_base.encode('utf-8'),
                hashlib.sha256
            ).digest()
        ).decode('utf-8')

        oauth_params['oauth_signature'] = signature

        # Build authorization header
        header_parts = [f'OAuth realm="{self.account}"']
        for key in [
            'oauth_consumer_key', 'oauth_nonce', 'oauth_signature',
            'oauth_signature_method', 'oauth_timestamp', 'oauth_token',
            'oauth_version'
        ]:
            value = quote(str(oauth_params[key]), safe="")
            header_parts.append(f'{key}="{value}"')

        return ', '.join(header_parts)

    async def fetch_gl_data_pages(self, query_builder_fn):
        """Fetch GL data from NetSuite SuiteQL with concurrent page fetching

        Yields pages of records as they are fetched. Handles NetSuite's
        offset limit of 99,000 by using ID-based chunking when necessary.

        Uses concurrent fetching within each chunk to improve performance,
        but maintains order for Singer protocol compliance.

        Args:
            query_builder_fn: Function that takes (min_internal_id,
                last_modified_date) and returns a SuiteQL query string

        Yields:
            List[Dict[str, Any]]: A page of records (up to page_size)
        """
        await self._ensure_session()

        LOGGER.info("Starting SuiteQL data fetch with concurrent requests")
        LOGGER.info(
            f"Concurrency level: {self.concurrent_requests} simultaneous pages"
        )
        if self.last_modified_date:
            LOGGER.info(f"Using incremental sync: {self.last_modified_date}")
        else:
            LOGGER.info("Using full refresh mode")

        last_internal_id = 0
        chunk_num = 1
        max_offset = 99000  # NetSuite's maximum offset limit
        total_fetched = 0

        try:
            # Fetch data in chunks when needed (due to offset limit)
            while True:
                LOGGER.info(
                    f"Fetching chunk {chunk_num} "
                    f"(starting from internal ID > {last_internal_id})"
                )

                # Build query with ID filter if needed
                query = query_builder_fn(
                    last_internal_id,
                    self.last_modified_date
                )

                # Fetch pages concurrently within this chunk
                async for page in self._fetch_chunk_concurrent(
                    query,
                    max_offset
                ):
                    if not page:
                        continue

                    total_fetched += len(page)
                    last_internal_id = int(page[-1].get('internal_id', 0))

                    LOGGER.info(
                        f"Fetched {len(page)} records "
                        f"(Total so far: {total_fetched})"
                    )

                    # Yield this page immediately for processing
                    yield page

                    # Check if this was the last page of results
                    if len(page) < self.page_size:
                        LOGGER.info("Reached last page of chunk")
                        # Signal end of chunk
                        chunk_complete = True
                        break
                else:
                    # Loop completed without break (no incomplete page)
                    chunk_complete = False

                # If we got incomplete page, this was the final chunk
                if chunk_complete:
                    break

                # Continue to next chunk
                chunk_num += 1
                LOGGER.info(
                    f"Continuing to next chunk "
                    f"(ID filter: internal_id > {last_internal_id})"
                )

            LOGGER.info(f"Total records fetched: {total_fetched}")

        finally:
            await self._close_session()

    async def _fetch_chunk_concurrent(
        self,
        query: str,
        max_offset: int
    ):
        """Fetch a chunk of pages concurrently with ordered results

        This method fetches multiple pages in parallel but yields them
        in order to maintain Singer protocol compliance.

        Args:
            query: SuiteQL query to execute
            max_offset: Maximum offset allowed for this chunk

        Yields:
            List[Dict[str, Any]]: Pages in order
        """
        # Create semaphore to limit concurrent requests
        semaphore = asyncio.Semaphore(self.concurrent_requests)

        async def fetch_page_with_semaphore(offset: int, page_num: int):
            """Fetch a single page with semaphore control"""
            async with semaphore:
                LOGGER.debug(
                    f"Fetching page {page_num} "
                    f"(offset: {offset}, limit: {self.page_size})"
                )
                try:
                    records = await self._fetch_page(
                        query,
                        offset,
                        self.page_size
                    )
                    return (offset, page_num, records)
                except Exception as e:
                    error_msg = str(e)
                    # 404 or "no records" errors are expected at end of data
                    if "404" in error_msg or "failed: 404" in error_msg:
                        LOGGER.debug(
                            f"Page {page_num} (offset {offset}) not found - "
                            f"likely beyond end of data"
                        )
                        # Return empty result to indicate end
                        return (offset, page_num, [])
                    else:
                        # Other errors should be logged and re-raised
                        LOGGER.error(
                            f"Error fetching page {page_num} "
                            f"(offset {offset}): {error_msg}"
                        )
                        raise

        # Calculate all offsets for pages in this chunk
        offsets = []
        offset = 0
        page_num = 1
        while offset <= max_offset:
            offsets.append((offset, page_num))
            offset += self.page_size
            page_num += 1

        # Fetch pages in batches to avoid overwhelming NetSuite
        batch_size = self.concurrent_requests * 3

        for batch_start in range(0, len(offsets), batch_size):
            batch_offsets = offsets[batch_start:batch_start + batch_size]

            LOGGER.info(
                f"Fetching batch of {len(batch_offsets)} pages "
                f"(pages {batch_offsets[0][1]}-{batch_offsets[-1][1]})"
            )

            # Create tasks for this batch
            tasks = [
                fetch_page_with_semaphore(offset, pnum)
                for offset, pnum in batch_offsets
            ]

            # Gather results (will complete in parallel)
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Log any exceptions that occurred
            exceptions = [r for r in results if isinstance(r, Exception)]
            if exceptions:
                LOGGER.debug(
                    f"Encountered {len(exceptions)} errors in batch "
                    f"(likely beyond end of data)"
                )

            # Sort successful results by offset to maintain order
            sorted_results = sorted(
                [r for r in results if not isinstance(r, Exception)],
                key=lambda x: x[0]
            )

            # If no successful results, we're done
            if not sorted_results:
                LOGGER.info("No more records in this chunk")
                return

            # Yield pages in order
            for offset, page_num, records in sorted_results:
                if not records:
                    # Empty page - we've reached the end
                    LOGGER.info("No more records in this chunk")
                    return

                yield records

                # If incomplete page, we're done with this chunk
                if len(records) < self.page_size:
                    LOGGER.info("Reached last page of chunk")
                    return

    async def _fetch_page(
        self,
        query: str,
        offset: int,
        limit: int
    ) -> List[Dict[str, Any]]:
        """Fetch a single page of data from SuiteQL API
        
        Uses the persistent session for connection reuse.
        """
        await self._ensure_session()

        # Query parameters for pagination
        query_params = {
            'limit': str(limit),
            'offset': str(offset)
        }

        # Build URL with pagination parameters
        url = f"{self.suiteql_url}?limit={limit}&offset={offset}"

        # Prepare request headers with OAuth signature including query params
        headers = {
            'Authorization': self.generate_oauth_header(
                self.suiteql_url,
                query_params
            ),
            'Content-Type': 'application/json',
            'Prefer': 'transient'
        }

        # Prepare request payload
        payload = {"q": query}

        try:
            async with self.session.post(
                url,
                headers=headers,
                json=payload
            ) as response:

                if response.status == 200:
                    data = await response.json()
                    items = data.get('items', [])
                    return items
                elif response.status == 404:
                    # 404 when offset is beyond available data
                    LOGGER.debug(
                        f"404 response for offset {offset} - "
                        f"no more records available"
                    )
                    return []
                else:
                    error_text = await response.text()
                    LOGGER.error(
                        f"SuiteQL API error: {response.status} - "
                        f"{error_text}"
                    )
                    raise Exception(
                        f"SuiteQL API request failed: {response.status}"
                    )
        except asyncio.TimeoutError:
            LOGGER.error("Request timeout")
            raise
        except Exception as e:
            LOGGER.error(f"Error fetching page: {str(e)}")
            raise

    async def fetch_dimension_table(
        self,
        stream_name: str,
        query: str
    ) -> List[Dict[str, Any]]:
        """Fetch all data for a dimension table

        Simpler than GL detail - just paginate through all records.
        No need for chunking since dimension tables are typically smaller.

        Args:
            stream_name: Name of the stream/table to fetch
            query: SuiteQL query to execute

        Returns:
            List of all records for the table
        """
        await self._ensure_session()

        LOGGER.info(f"Fetching dimension table: {stream_name}")

        all_records = []
        offset = 0
        page_num = 1

        try:
            while True:
                LOGGER.info(
                    f"Fetching page {page_num} "
                    f"(offset: {offset}, limit: {self.page_size})..."
                )

                records = await self._fetch_page(query, offset, self.page_size)

                if not records:
                    LOGGER.info("No more records to fetch")
                    break

                all_records.extend(records)
                LOGGER.info(
                    f"Fetched {len(records)} records "
                    f"(Total so far: {len(all_records)})"
                )

                # Check if this was the last page
                if len(records) < self.page_size:
                    LOGGER.info("Last page reached")
                    break

                offset += self.page_size
                page_num += 1

            LOGGER.info(
                f"Total records fetched for {stream_name}: "
                f"{len(all_records)}"
            )
            return all_records

        finally:
            await self._close_session()
