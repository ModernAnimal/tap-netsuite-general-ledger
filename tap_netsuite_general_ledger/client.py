"""
NetSuite Client for Singer Tap
Handles authentication and API requests to NetSuite RESTlet
"""

import asyncio
import time
import hmac
import hashlib
import base64
import secrets
from urllib.parse import quote
from collections import OrderedDict
from typing import Dict, Any, List, Optional

import aiohttp
import singer

LOGGER = singer.get_logger()


class NetSuiteClient:
    """NetSuite API client with OAuth 1.0a authentication"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config

        # Required configuration
        self.account = config["netsuite_account"]
        self.consumer_key = config["netsuite_consumer_key"]
        self.consumer_secret = config["netsuite_consumer_secret"]
        self.token_id = config["netsuite_token_id"]
        self.token_secret = config["netsuite_token_secret"]

        # Optional configuration
        self.script_id = config.get("netsuite_script_id")
        self.deploy_id = config.get("netsuite_deploy_id")
        self.search_id = config.get("netsuite_search_id")

        # Build base URL
        self.base_url = (
            f"https://{self.account}.restlets.api.netsuite.com"
            f"/app/site/hosting/restlet.nl"
        )

        LOGGER.info(f"Initialized NetSuite client for account: {self.account}")

    def generate_oauth_header(self) -> str:
        """Generate OAuth 1.0a authorization header"""
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

        url_params = OrderedDict([
            ('deploy', self.deploy_id),
            ('script', self.script_id)
        ])

        # Combine all parameters for signature
        signature_params = OrderedDict()
        signature_params.update(url_params)
        signature_params.update(oauth_params)

        # Build parameter string
        param_pairs = []
        for k, v in sorted(signature_params.items()):
            encoded_key = quote(str(k), safe='')
            encoded_value = quote(str(v), safe='')
            param_pairs.append(f"{encoded_key}={encoded_value}")
        param_string = '&'.join(param_pairs)

        # Build signature base string
        signature_base = (
            f"POST&{quote(self.base_url, safe='')}"
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

    async def fetch_gl_data_streaming(
        self,
        batch_callback,
        batch_size: int = 100000,
        period_ids: Optional[List[str]] = None,
        period_names: Optional[List[str]] = None
    ) -> int:
        """Fetch GL detail data from NetSuite with true streaming processing

        Args:
            batch_callback: Async function to call with each batch of records
            batch_size: Size of each batch to process (default: 100000)
            period_ids: List of period IDs to fetch
            period_names: List of period names to fetch

        Returns:
            Total number of records processed
        """

        # Validate that we have at least one period specified
        if not period_ids and not period_names:
            LOGGER.warning("No period specified, fetching default period")

        total_processed = 0

        if period_ids:
            for pid in period_ids:
                processed = await self._fetch_single_period_streaming(
                    batch_callback, batch_size, period_id=pid
                )
                total_processed += processed
        elif period_names:
            for pname in period_names:
                processed = await self._fetch_single_period_streaming(
                    batch_callback, batch_size, period_name=pname
                )
                total_processed += processed
        else:
            # No period specified, fetch all
            processed = await self._fetch_single_period_streaming(
                batch_callback, batch_size
            )
            total_processed += processed

        LOGGER.info(
            f"Total records processed across all periods: {total_processed}"
        )
        return total_processed

    async def _fetch_single_period_streaming(
        self,
        batch_callback,
        batch_size: int,
        period_id: Optional[str] = None,
        period_name: Optional[str] = None
    ) -> int:
        """Fetch GL data for a single period with streaming processing"""

        # Prepare request headers
        headers = {
            'Authorization': self.generate_oauth_header(),
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

        # Build request payload
        request_data = {'searchID': self.search_id}

        if period_id:
            request_data['periodId'] = period_id
            period_label = f"period ID: {period_id}"
        elif period_name:
            request_data['periodName'] = period_name
            period_label = f"period name: {period_name}"
        else:
            period_label = "default period"

        # Build request URL
        url = (f"{self.base_url}?script={self.script_id}"
               f"&deploy={self.deploy_id}")

        LOGGER.info(
            f"Making streaming request to NetSuite API - {period_label}"
        )

        # Make request with streaming JSON processing
        timeout = aiohttp.ClientTimeout(total=600)  # 10 minutes
        async with aiohttp.ClientSession(timeout=timeout) as session:
            try:
                async with session.post(
                    url,
                    headers=headers,
                    json=request_data
                ) as response:
                    if response.status == 200:
                        # Process the response with streaming JSON parsing
                        return await self._process_streaming_response(
                            response, batch_callback, batch_size, period_label
                        )
                    else:
                        error_text = await response.text()
                        LOGGER.error(f"HTTP {response.status}: {error_text}")
                        raise Exception(
                            f"HTTP {response.status}: {error_text}"
                        )
            except asyncio.TimeoutError:
                LOGGER.error("Request timed out after 10 minutes")
                raise Exception("Request timed out")
            except Exception as e:
                LOGGER.error(f"Request failed: {str(e)}")
                raise

    async def _process_streaming_response(
        self,
        response,
        batch_callback,
        batch_size: int,
        period_label: str
    ) -> int:
        """Process streaming JSON response to minimize memory usage"""

        # Unfortunately, aiohttp doesn't support streaming JSON parsing
        # The NetSuite API returns all data in one JSON response
        # But we can process it immediately with aggressive memory cleanup

        try:
            result = await response.json()
            LOGGER.info("NetSuite API request successful")

            if result.get('success') and 'results' in result:
                records = result['results']
                total_records = len(records)

                LOGGER.info(
                    f"Successfully retrieved {total_records} records for "
                    f"{period_label}"
                )

                # Process records immediately with memory cleanup
                return await self._process_records_in_streaming_batches(
                    records, batch_callback, batch_size, period_label
                )
            else:
                error_msg = result.get('error', 'Unknown error')
                LOGGER.error(f"NetSuite API error: {error_msg}")
                raise Exception(f"NetSuite API error: {error_msg}")

        except Exception as e:
            LOGGER.error(f"Error processing streaming response: {str(e)}")
            raise

    async def _process_records_in_streaming_batches(
        self,
        records: List[Dict[str, Any]],
        batch_callback,
        batch_size: int,
        period_label: str
    ) -> int:
        """Process records in batches with immediate memory cleanup"""
        total_records = len(records)
        processed_count = 0

        LOGGER.info(
            f"Processing {total_records} records in streaming batches of "
            f"{batch_size} for {period_label}"
        )

        # Process records in batches with aggressive memory management
        for i in range(0, total_records, batch_size):
            # Extract batch without copying - use slice view
            batch_end = min(i + batch_size, total_records)
            batch = records[i:batch_end]

            batch_num = (i // batch_size) + 1
            total_batches = (total_records + batch_size - 1) // batch_size

            LOGGER.info(
                f"Processing streaming batch {batch_num}/{total_batches} "
                f"({len(batch)} records) for {period_label}"
            )

            try:
                # Process the batch
                await batch_callback(
                    batch, batch_num, total_batches, period_label
                )
                processed_count += len(batch)

                # Explicitly clear the batch reference
                del batch

                # Clear processed records from the original list to free memory
                # This is the key optimization - we remove processed records
                for j in range(len(records) - 1, i - 1, -1):
                    if j < batch_end:
                        records.pop(j)

                # Trigger garbage collection periodically
                if batch_num % 5 == 0:  # Every 5 batches
                    import gc
                    gc.collect()

            except Exception as e:
                LOGGER.error(
                    f"Error processing streaming batch {batch_num} for "
                    f"{period_label}: {str(e)}"
                )
                raise

        # Final cleanup
        records.clear()
        import gc
        gc.collect()

        LOGGER.info(
            f"Completed streaming processing {processed_count} records for "
            f"{period_label}"
        )
        return processed_count

    async def _fetch_single_period(
        self,
        period_id: Optional[str] = None,
        period_name: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Fetch GL detail data for a single period from NetSuite"""

        # Prepare request headers
        headers = {
            'Authorization': self.generate_oauth_header(),
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

        # Build request payload
        request_data = {'searchID': self.search_id}

        if period_id:
            request_data['periodId'] = period_id
        elif period_name:
            request_data['periodName'] = period_name

        # Build request URL
        url = (f"{self.base_url}?script={self.script_id}"
               f"&deploy={self.deploy_id}")

        LOGGER.info(
            f"Making request to NetSuite API - {period_name or period_id}"
        )

        # Make request with timeout
        timeout = aiohttp.ClientTimeout(total=600)  # 10 minutes
        async with aiohttp.ClientSession(timeout=timeout) as session:
            try:
                async with session.post(
                    url,
                    headers=headers,
                    json=request_data
                ) as response:
                    if response.status == 200:
                        result = await response.json()
                        # Don't log full API response - it can be massive
                        LOGGER.info("NetSuite API request successful")
                        if result.get('success') and 'results' in result:
                            records = result['results']
                            LOGGER.info(
                                f"Successfully retrieved {len(records)} "
                                f"records"
                            )
                            return records
                        else:
                            error_msg = result.get('error', 'Unknown error')
                            LOGGER.error(f"NetSuite API error: {error_msg}")
                            raise Exception(f"NetSuite API error: {error_msg}")
                    else:
                        error_text = await response.text()
                        LOGGER.error(f"HTTP {response.status}: {error_text}")
                        raise Exception(
                            f"HTTP {response.status}: {error_text}"
                        )
            except asyncio.TimeoutError:
                LOGGER.error("Request timed out after 10 minutes")
                raise Exception("Request timed out")
            except Exception as e:
                LOGGER.error(f"Request failed: {str(e)}")
                raise

    def extract_field_value(
        self, record: Dict[str, Any], field_name: str
    ) -> str:
        """Extract field value from NetSuite record structure"""
        if 'values' not in record:
            return ''

        values = record['values']
        if field_name not in values:
            return ''

        field_data = values[field_name]

        if isinstance(field_data, list) and len(field_data) > 0:
            # Handle array format
            item = field_data[0]
            if isinstance(item, dict):
                return item.get('text', item.get('value', ''))
            else:
                return str(item) if item is not None else ''
        elif isinstance(field_data, dict):
            # Handle object format
            return field_data.get('text', field_data.get('value', ''))
        else:
            # Handle primitive format
            return str(field_data) if field_data is not None else ''
