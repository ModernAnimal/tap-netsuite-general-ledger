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

    async def fetch_gl_data(
        self,
        period_ids: Optional[List[str]] = None,
        period_names: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Fetch GL detail data from NetSuite

        Args:
            period_ids: List of period IDs to fetch
            period_names: List of period names to fetch
        """

        # Validate that we have at least one period specified
        if not period_ids and not period_names:
            LOGGER.warning("No period specified, fetching all data")

        # If we have multiple periods, fetch them in batches and combine
        all_records = []

        if period_ids:
            for pid in period_ids:
                records = await self._fetch_single_period(period_id=pid)
                all_records.extend(records)
                LOGGER.info(
                    f"Fetched {len(records)} records for period ID: {pid}"
                )
        elif period_names:
            for pname in period_names:
                records = await self._fetch_single_period(period_name=pname)
                all_records.extend(records)
                LOGGER.info(
                    f"Fetched {len(records)} records for period name: {pname}"
                )
        else:
            # No period specified, fetch all
            all_records = await self._fetch_single_period()

        total_count = len(all_records)
        LOGGER.info(
            f"Total records retrieved across all periods: {total_count}"
        )
        return all_records

    async def fetch_gl_data_streaming(
        self,
        period_ids: Optional[List[str]] = None,
        period_names: Optional[List[str]] = None
    ):
        """Generator that yields GL detail records one at a time to reduce
        memory usage
        
        This is better for large datasets (400k+ records) as it doesn't load
        everything into memory at once.
        """
        
        # Validate that we have at least one period specified
        if not period_ids and not period_names:
            LOGGER.warning("No period specified, fetching all data")

        total_count = 0

        if period_ids:
            for pid in period_ids:
                LOGGER.info(f"Fetching records for period ID: {pid}")
                records = await self._fetch_single_period(period_id=pid)
                period_count = len(records)
                total_count += period_count
                LOGGER.info(
                    f"Retrieved {period_count} records for period ID: {pid}"
                )
                
                # Yield records one by one to keep memory usage low
                for record in records:
                    yield record
                    
        elif period_names:
            for pname in period_names:
                LOGGER.info(f"Fetching records for period name: {pname}")
                records = await self._fetch_single_period(period_name=pname)
                period_count = len(records)
                total_count += period_count
                LOGGER.info(
                    f"Retrieved {period_count} records for period name: "
                    f"{pname}"
                )
                
                # Yield records one by one to keep memory usage low
                for record in records:
                    yield record
        else:
            # No period specified, fetch all
            records = await self._fetch_single_period()
            total_count = len(records)
            
            for record in records:
                yield record

        LOGGER.info(
            f"Total records streamed across all periods: {total_count}"
        )

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

        LOGGER.info(f"Fetching GL data with filters: {request_data}")
        LOGGER.info(f"Making request to URL: {url}")
        LOGGER.info(f"Request headers: {dict(headers)}")
        LOGGER.info(f"Request payload: {request_data}")

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
                        LOGGER.info(f"NetSuite API response: {result}")
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
