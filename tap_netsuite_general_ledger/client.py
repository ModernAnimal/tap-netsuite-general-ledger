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
        self.account_chunk_size = config.get("account_chunk_size", 25)

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
        """Fetch GL data from NetSuite with chunking and streaming processing

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
        """Fetch GL data for a single period with account chunking"""

        if period_id:
            period_label = f"period ID: {period_id}"
        elif period_name:
            period_label = f"period name: {period_name}"
        else:
            period_label = "default period"

        LOGGER.info(f"Starting chunked fetch for {period_label}")
        
        # Try to get accounts for chunking
        try:
            accounts = await self._get_accounts()
            if accounts and len(accounts) > self.account_chunk_size:
                LOGGER.info(
                    f"Using account-based chunking: {len(accounts)} accounts "
                    f"in chunks of {self.account_chunk_size}"
                )
                return await self._fetch_with_account_chunks(
                    accounts, batch_callback, batch_size,
                    period_id, period_name, period_label
                )
            else:
                LOGGER.info(
                    f"Using single request "
                    f"(accounts: {len(accounts) if accounts else 'unknown'})"
                )
        except Exception as e:
            LOGGER.warning(f"Could not get accounts for chunking: {e}")
            LOGGER.info("Falling back to single request")

        # Fallback to single request
        return await self._fetch_single_request(
            batch_callback, batch_size, period_id, period_name, period_label
        )

    async def _get_accounts(self) -> Optional[List[Dict[str, Any]]]:
        """Get list of all accounts from NetSuite for chunking"""
        
        request_data = {'action': 'getAccounts'}
        
        try:
            result = await self._make_api_request(request_data)
            if result.get('success') and 'results' in result:
                accounts = result['results']
                LOGGER.info(f"Retrieved {len(accounts)} accounts for chunking")
                return accounts
            else:
                LOGGER.warning(
                    f"Failed to get accounts: "
                    f"{result.get('error', 'Unknown error')}"
                )
                return None
        except Exception as e:
            LOGGER.warning(f"Error getting accounts: {e}")
            return None

    async def _fetch_with_account_chunks(
        self,
        accounts: List[Dict[str, Any]],
        batch_callback,
        batch_size: int,
        period_id: Optional[str],
        period_name: Optional[str],
        period_label: str
    ) -> int:
        """Fetch GL data using account-based chunking"""
        
        # Create account chunks
        account_chunks = []
        for i in range(0, len(accounts), self.account_chunk_size):
            chunk = accounts[i:i + self.account_chunk_size]
            account_ids = [acc['id'] for acc in chunk if acc.get('id')]
            
            if account_ids:  # Only add chunks with valid account IDs
                end_account = min(i+self.account_chunk_size, len(accounts))
                account_chunks.append({
                    'ids': account_ids,
                    'description': f"Accounts {i+1}-{end_account}"
                })

        if not account_chunks:
            LOGGER.warning(
                "No valid account chunks created, using single request"
            )
            return await self._fetch_single_request(
                batch_callback, batch_size, period_id,
                period_name, period_label
            )

        LOGGER.info(
            f"Split {len(accounts)} accounts into {len(account_chunks)} "
            f"chunks of {self.account_chunk_size}"
        )

        # Process each chunk with pagination support
        total_processed = 0
        
        for chunk_num, chunk in enumerate(account_chunks, 1):
            LOGGER.info(
                f"Processing chunk {chunk_num}/{len(account_chunks)}: "
                f"{chunk['description']}"
            )
            
            try:
                chunk_records = await self._fetch_chunk_data(
                    chunk['ids'], period_id, period_name, period_label
                )
                
                if chunk_records:
                    # Process records in batches
                    processed = await (
                        self._process_records_in_streaming_batches(
                            chunk_records, batch_callback, batch_size,
                            f"{period_label} - {chunk['description']}"
                        )
                    )
                    total_processed += processed
                    
                    LOGGER.info(
                        f"Chunk {chunk_num} completed: {processed} "
                        f"records processed"
                    )
                else:
                    LOGGER.info(f"Chunk {chunk_num}: No records found")
                
                # Small delay between chunks to avoid overwhelming the API
                await asyncio.sleep(1)  # Match smart export delay
                
            except Exception as e:
                LOGGER.error(f"Chunk {chunk_num} failed: {str(e)}")
                # Continue with other chunks
                continue

        LOGGER.info(
            f"Account chunking completed: {total_processed} total records"
        )
        return total_processed

    async def _fetch_chunk_data(
        self,
        account_ids: List[str],
        period_id: Optional[str],
        period_name: Optional[str],
        period_label: str
    ) -> List[Dict[str, Any]]:
        """Fetch data for a specific chunk of accounts with pagination"""
        
        # Build base request payload
        request_data = {'searchID': self.search_id}
        
        if period_id:
            request_data['periodId'] = period_id
        elif period_name:
            request_data['periodName'] = period_name
            
        # Add account filter and pagination settings
        request_data['accountIds'] = account_ids
        request_data['maxResults'] = 50000  # Match smart export limit
        request_data['startIndex'] = 0
        
        # Handle pagination for large chunks
        chunk_records = []
        start_index = 0
        page_num = 1
        
        while True:
            try:
                request_data['startIndex'] = start_index
                
                LOGGER.debug(
                    f"Fetching page {page_num} for accounts "
                    f"(startIndex: {start_index})"
                )
                
                result = await self._make_api_request(request_data)
                
                if result.get('success') and 'results' in result:
                    page_records = result['results']
                    chunk_records.extend(page_records)
                    
                    LOGGER.info(
                        f"   Page {page_num}: {len(page_records)} records"
                    )
                    
                    # Check if there are more results
                    if (result.get('hasMoreResults') and
                            result.get('nextStartIndex')):
                        start_index = result['nextStartIndex']
                        page_num += 1
                        await asyncio.sleep(0.5)  # Small delay between pages
                    else:
                        break
                else:
                    error_msg = result.get('error', 'Unknown error')
                    LOGGER.warning(f"Page {page_num} failed: {error_msg}")
                    break
                    
            except Exception as e:
                LOGGER.error(f"Error fetching page {page_num}: {e}")
                break
        
        if chunk_records:
            LOGGER.info(
                f"Retrieved {len(chunk_records)} total records "
                f"({page_num} pages) for chunk"
            )
        else:
            LOGGER.warning("No records retrieved for chunk")
        
        return chunk_records

    async def _fetch_single_request(
        self,
        batch_callback,
        batch_size: int,
        period_id: Optional[str],
        period_name: Optional[str],
        period_label: str
    ) -> int:
        """Fetch data using a single API request (fallback)"""
        
        # Build request payload
        request_data = {'searchID': self.search_id}
        
        if period_id:
            request_data['periodId'] = period_id
        elif period_name:
            request_data['periodName'] = period_name

        LOGGER.info(f"Making single API request for {period_label}")

        try:
            result = await self._make_api_request(request_data)
            
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
            LOGGER.error(f"Error in single request: {str(e)}")
            raise

    async def _make_api_request(
        self, request_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Make a request to the NetSuite API"""
        
        # Prepare request headers
        headers = {
            'Authorization': self.generate_oauth_header(),
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

        # Build request URL
        url = (f"{self.base_url}?script={self.script_id}"
               f"&deploy={self.deploy_id}")

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
                        LOGGER.debug("NetSuite API request successful")
                        return result
                    else:
                        error_text = await response.text()
                        LOGGER.error(f"HTTP {response.status}: {error_text}")
                        return {
                            'success': False,
                            'error': f"HTTP {response.status}: {error_text}"
                        }
            except asyncio.TimeoutError:
                LOGGER.error("Request timed out after 10 minutes")
                return {'success': False, 'error': 'Request timed out'}
            except Exception as e:
                LOGGER.error(f"Request failed: {str(e)}")
                return {'success': False, 'error': str(e)}

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
