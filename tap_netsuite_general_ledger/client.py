"""
NetSuite Client for Singer Tap
Handles authentication and API requests to NetSuite SuiteQL API
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

        # Build SuiteQL URL
        self.base_url = f"https://{self.account}.suitetalk.api.netsuite.com"
        self.suiteql_url = f"{self.base_url}/services/rest/query/v1/suiteql"

        LOGGER.info(
            f"Initialized NetSuite SuiteQL client for account: {self.account}"
        )
        if self.last_modified_date:
            LOGGER.info(
                f"Incremental sync mode: "
                f"last_modified_date = {self.last_modified_date}"
            )

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

    def build_gl_query(self, min_internal_id: int = 0) -> str:
        """Build the SuiteQL query to fetch GL data

        Args:
            min_internal_id: Minimum internal ID to fetch (for chunking
                beyond offset limit)

        Returns:
            SuiteQL query string
        """
        # Base query with all fields from original demo
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
            AND t.ID = 207037
        """

        # Add ID filter if chunking (to handle offset limit)
        if min_internal_id > 0:
            query += f" AND t.ID > {min_internal_id}"

        # Add incremental filter if last_modified_date is set
        if self.last_modified_date:
            query += (
                f"""
                    AND (
                        t.lastModifiedDate >=
                        TO_DATE('{self.last_modified_date}', 'YYYY-MM-DD')
                        OR tal.lastModifiedDate >=
                        TO_DATE('{self.last_modified_date}', 'YYYY-MM-DD')
                        OR a.lastModifiedDate >=
                        TO_DATE('{self.last_modified_date}', 'YYYY-MM-DD')
                    )
                """
            )

        # Order by transaction ID and line ID for consistent pagination
        query += "ORDER BY t.id, t.TranDate, t.TranID, tal.TransactionLine"

        return query

    async def fetch_gl_data_pages(self):
        """Fetch GL data from NetSuite SuiteQL page by page

        Yields pages of records as they are fetched. Handles NetSuite's
        offset limit of 99,000 by using ID-based chunking when necessary.

        Yields:
            List[Dict[str, Any]]: A page of records (up to page_size)
        """

        LOGGER.info("Starting SuiteQL data fetch")
        if self.last_modified_date:
            LOGGER.info(f"Using incremental sync: {self.last_modified_date}")
        else:
            LOGGER.info("Using full refresh mode")

        last_internal_id = 0
        chunk_num = 1
        max_offset = 99000  # NetSuite's maximum offset limit
        total_fetched = 0

        # Fetch data in chunks when needed (due to offset limit)
        while True:
            LOGGER.info(
                f"Fetching chunk {chunk_num} "
                f"(starting from internal ID > {last_internal_id})"
            )

            # Build query with ID filter if needed
            query = self.build_gl_query(min_internal_id=last_internal_id)

            offset = 0
            page_num = 1
            chunk_has_records = False

            # Fetch pages within this chunk (up to offset limit)
            while offset <= max_offset:
                LOGGER.info(
                    f"Fetching page {page_num} "
                    f"(offset: {offset}, limit: {self.page_size})..."
                )

                records = await self._fetch_page(query, offset, self.page_size)

                if not records:
                    LOGGER.info("No more records in this chunk")
                    break

                chunk_has_records = True
                total_fetched += len(records)
                last_internal_id = int(records[-1].get('internal_id', 0))

                LOGGER.info(
                    f"Fetched {len(records)} records "
                    f"(Total so far: {total_fetched})"
                )

                # Yield this page immediately for processing
                yield records

                # Check if this was the last page of results
                if len(records) < self.page_size:
                    LOGGER.info("Reached last page of chunk")
                    break

                offset += self.page_size
                page_num += 1

                # Check if we're approaching the offset limit
                if offset > max_offset:
                    LOGGER.info(
                        f"Approaching offset limit ({offset} > {max_offset})"
                    )
                    break

            # If no records in this chunk, we're done
            if not chunk_has_records:
                break

            # If we hit offset limit and got full page, continue next chunk
            if offset > max_offset and len(records) == self.page_size:
                chunk_num += 1
                LOGGER.info(
                    f"Continuing to next chunk "
                    f"(ID filter: internal_id > {last_internal_id})"
                )
            else:
                # This was the final chunk
                break

        LOGGER.info(f"Total records fetched: {total_fetched}")

    async def _fetch_page(
        self,
        query: str,
        offset: int,
        limit: int
    ) -> List[Dict[str, Any]]:
        """Fetch a single page of data from SuiteQL API"""

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

        # Make request with timeout
        timeout = aiohttp.ClientTimeout(total=600)  # 10 minutes

        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(
                    url,
                    headers=headers,
                    json=payload
                ) as response:

                    if response.status == 200:
                        data = await response.json()
                        items = data.get('items', [])
                        return items
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
