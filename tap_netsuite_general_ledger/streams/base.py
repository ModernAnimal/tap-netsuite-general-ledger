"""
Base stream class with common functionality
"""

import json
import os
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

import singer
from singer import CatalogEntry

LOGGER = singer.get_logger()


class BaseStream(ABC):
    """Base class for all NetSuite streams"""

    def __init__(self, client, config: Dict[str, Any]):
        """Initialize the stream

        Args:
            client: NetSuiteClient instance
            config: Configuration dictionary
        """
        self.client = client
        self.config = config
        self.tap_stream_id = self.get_stream_id()
        self.schema = self.load_schema()
        self.key_properties = self.get_key_properties()
        self.replication_method = self.get_replication_method()

    @abstractmethod
    def get_stream_id(self) -> str:
        """Return the stream ID"""
        pass

    @abstractmethod
    def get_key_properties(self) -> List[str]:
        """Return the key properties for this stream"""
        pass

    def get_replication_method(self) -> str:
        """Return the replication method (default: FULL_TABLE)"""
        return 'FULL_TABLE'

    def load_schema(self) -> Dict[str, Any]:
        """Load the schema for this stream from JSON file"""
        schema_dir = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "schemas"
        )
        schema_file = os.path.join(schema_dir, f"{self.get_stream_id()}.json")

        with open(schema_file, 'r') as f:
            return json.load(f)

    def write_schema(self):
        """Write the schema to stdout"""
        singer.write_schema(
            self.tap_stream_id,
            self.schema,
            self.key_properties
        )

    def write_record(self, record: Dict[str, Any]):
        """Write a record to stdout"""
        try:
            singer.write_record(self.tap_stream_id, record)
        except BrokenPipeError:
            LOGGER.error(
                f"Broken pipe when writing record for {self.tap_stream_id}"
            )
            raise

    def write_records_batch(self, records: List[Dict[str, Any]]):
        """Write multiple records to stdout in a batch

        This is more efficient than calling write_record repeatedly
        as it reduces the overhead of multiple function calls and
        I/O operations.

        Args:
            records: List of records to write
        """
        try:
            for record in records:
                singer.write_record(self.tap_stream_id, record)
        except BrokenPipeError:
            LOGGER.error(
                f"Broken pipe when writing batch for {self.tap_stream_id}"
            )
            raise

    def write_state(self, state: Dict[str, Any]):
        """Write state to stdout"""
        try:
            singer.write_state(state)
        except BrokenPipeError:
            LOGGER.warning(
                f"Broken pipe when writing state for {self.tap_stream_id}"
            )

    def update_bookmark(
        self,
        state: Dict[str, Any],
        **kwargs
    ) -> Dict[str, Any]:
        """Update the bookmark in state

        Args:
            state: Current state dict
            **kwargs: Additional fields to add to bookmark

        Returns:
            Updated state dict
        """
        if state is None:
            state = {}
        if 'bookmarks' not in state:
            state['bookmarks'] = {}

        bookmark = {
            'last_sync': datetime.now(timezone.utc).isoformat(),
            'replication_method': self.replication_method,
        }
        bookmark.update(kwargs)

        state['bookmarks'][self.tap_stream_id] = bookmark
        return state

    @abstractmethod
    def sync(
        self,
        catalog_entry: CatalogEntry,
        state: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Sync this stream

        Args:
            catalog_entry: Catalog entry for this stream
            state: Current state dict

        Returns:
            Updated state dict
        """
        pass

    def safe_int(self, value: Any) -> Optional[int]:
        """Convert value to int, return None if empty/None"""
        if value is None or value == '':
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    def safe_float(self, value: Any) -> Optional[float]:
        """Convert value to float, return None if empty/None"""
        if value is None or value == '':
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
