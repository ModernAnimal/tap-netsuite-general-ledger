"""
Sync orchestration for NetSuite tap
Routes streams to appropriate stream classes
"""

from typing import Dict, Any

import singer
from singer import CatalogEntry

from .streams import DimensionStream, GLDetailStream

LOGGER = singer.get_logger()

# List of dimension table stream IDs
DIMENSION_STREAMS = {
    'netsuite_account', 'netsuite_vendor', 'netsuite_classification',
    'netsuite_department', 'netsuite_location', 'netsuite_customer',
    'netsuite_employee'
}


def sync_stream(
    client,
    stream: CatalogEntry,
    state: Dict[str, Any],
    config: Dict[str, Any]
) -> Dict[str, Any]:
    """Sync a single stream by routing to appropriate stream class

    Args:
        client: NetSuiteClient instance
        stream: CatalogEntry for the stream
        state: Current state dict
        config: Configuration dict

    Returns:
        Updated state dict
    """
    stream_id = stream.tap_stream_id

    LOGGER.info(f"Syncing stream: {stream_id}")

    try:
        # Route to appropriate stream class
        if stream_id in DIMENSION_STREAMS:
            # Use DimensionStream class for dimension tables
            stream_instance = DimensionStream(client, config, stream_id)
        elif stream_id in (
            'netsuite_general_ledger_detail', 'netsuite_gl_detail'
        ):
            # Use GLDetailStream class for GL detail
            # (supports both old and new names)
            stream_instance = GLDetailStream(client, config)
        else:
            raise ValueError(f"Unknown stream: {stream_id}")

        # Sync the stream
        state = stream_instance.sync(stream, state)

        # Write final state after stream completion
        try:
            singer.write_state(state)
        except BrokenPipeError:
            LOGGER.warning(
                "Broken pipe detected when writing final state - "
                "exiting gracefully"
            )
        except Exception as e:
            LOGGER.error(f"Error writing final state: {str(e)}")

        return state

    except Exception as stream_error:
        LOGGER.error(
            f"Error syncing stream {stream_id}: {str(stream_error)}"
        )
        raise
