"""
Dimension stream class for simple dimension tables
"""

import asyncio
from typing import Dict, Any, List

import singer
from singer import CatalogEntry

from .base import BaseStream

LOGGER = singer.get_logger()


# Dimension table query configurations
DIMENSION_TABLE_CONFIGS = {
    'netsuite_account': {
        'query': """
            SELECT
                Account.id,
                Account.category1099Misc,
                Account.balance,
                Account.sBankName,
                Account.sBankRoutingNumber,
                Account.custrecord_bdc_lastupdatedbyimp_acc,
                Account.deferralAcct,
                Account.description,
                Account.accountSearchDisplayName,
                Account.displayNameWithHierarchy,
                Account.eliminate,
                Account.externalId,
                Account.fullName,
                Account.isInactive,
                Account.includeChildren,
                Account.inventory,
                Account.lastModifiedDate,
                Account.custrecord_legacy_no,
                CUSTOMRECORD1032.name as revenue_classification,
                Account.accountSearchDisplayNameCopy,
                Account.acctNumber,
                Account.class,
                Account.department,
                Account.location,
                Account.revalue,
                Account.custrecord_fam_account_showinfixedasset,
                Account.sSpecAcct,
                Account.parent,
                Account.subsidiary,
                Account.isSummary,
                Account.billableExpensesAcct,
                Account.acctType,
                Account.reconcileWithMatching
            FROM Account
            LEFT JOIN CUSTOMRECORD1032
                on Account.custrecord6 = CUSTOMRECORD1032.id
            ORDER BY Account.id
        """,
        'key_properties': ['id']
    },
    'netsuite_vendor': {
        'query': """
            SELECT vc.name as category, v.id, v.accountnumber, v.altname,
                   v.balance, v.balanceprimary, v.comments, v.companyname,
                   v.creditlimit, v.currency, v.custentity_2663_payment_method,
                   v.datecreated, v.email, v.emailpreference,
                   v.emailtransactions, v.entityid, v.expenseaccount,
                   v.externalid, v.fax, v.faxtransactions, v.giveaccess,
                   v.incoterm, v.is1099eligible, v.isinactive,
                   v.isjobresourcevend, v.isperson, v.laborcost,
                   v.lastmodifieddate, v.legalname, v.payablesaccount, v.phone,
                   v.printoncheckas, v.printtransactions,
                   v.purchaseorderamount, v.purchaseorderquantity,
                   v.purchaseorderquantitydiff, v.receiptamount,
                   v.receiptquantity, v.receiptquantitydiff,
                   v.representingsubsidiary, v.subsidiary, v.terms,
                   v.unbilledorders, v.unbilledordersprimary, v.url,
                   v.workcalendar
            FROM Vendor v
            LEFT JOIN VendorCategory vc on v.category = vc.id
            ORDER BY v.id
        """,
        'key_properties': ['id']
    },
    'netsuite_classification': {
        'query': """
            SELECT *
            FROM classification
            ORDER BY classification.id
        """,
        'key_properties': ['id']
    },
    'netsuite_department': {
        'query': """
            SELECT *
            FROM Department
            ORDER BY Department.id
        """,
        'key_properties': ['id']
    },
    'netsuite_location': {
        'query': """
            SELECT
                l.id,
                l.cseg1,
                l.custrecord1 as TaxRate,
                l.custrecord2 as OpeningDate,
                l.custrecord3 as ClosingDate,
                l.custrecord4 as Lease_RefID,
                l.fullname,
                l.isinactive,
                l.custrecord_bdc_lastupdatedbyimp_loc,
                l.lastmodifieddate,
                l.mainaddress,
                l.makeinventoryavailable,
                l.name,
                l.subsidiary,
                l.locationtype,
                l.externalid,
                l.usebins,
                a.addr1,
                a.addr2,
                a.city,
                a.state,
                a.zip,
                a.country,
                a.addrphone,
                a.attention
            FROM Location l
            LEFT JOIN LocationMainAddress a ON l.mainaddress = a.nkey
            ORDER BY l.id
        """,
        'key_properties': ['id']
    },
    'netsuite_customer': {
        'query': """
            SELECT id, entityid, companyname
            FROM customer
            ORDER BY id
        """,
        'key_properties': ['id']
    },
    'netsuite_employee': {
        'query': """
            SELECT id, entityid, firstname || ' ' || lastname as companyname
            FROM employee
            ORDER BY id
        """,
        'key_properties': ['id']
    }
}


class DimensionStream(BaseStream):
    """Stream class for dimension tables with full refresh"""

    def __init__(self, client, config: Dict[str, Any], stream_id: str):
        """Initialize dimension stream

        Args:
            client: NetSuiteClient instance
            config: Configuration dictionary
            stream_id: ID of the dimension stream
        """
        if stream_id not in DIMENSION_TABLE_CONFIGS:
            raise ValueError(
                f"Unknown dimension stream: {stream_id}. "
                f"Available: {list(DIMENSION_TABLE_CONFIGS.keys())}"
            )

        self.stream_id = stream_id
        self.stream_config = DIMENSION_TABLE_CONFIGS[stream_id]
        super().__init__(client, config)

    def get_stream_id(self) -> str:
        """Return the stream ID"""
        return self.stream_id

    def get_key_properties(self) -> List[str]:
        """Return the key properties"""
        return self.stream_config['key_properties']

    def get_query(self) -> str:
        """Return the SuiteQL query for this dimension table"""
        return self.stream_config['query']

    def transform_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Transform a dimension table record with simple type conversion

        Args:
            record: Raw record from NetSuite

        Returns:
            Transformed record
        """
        # Integer fields that should be converted
        int_fields = {
            'id', 'parent', 'currency', 'subsidiary', 'expenseaccount',
            'payablesaccount', 'terms', 'incoterm',
            'representingsubsidiary', 'custentity_2663_payment_method',
            'mainaddress', 'locationtype', 'cseg1'
        }

        # Numeric fields that should be converted to float
        numeric_fields = {
            'balance', 'balanceprimary', 'creditlimit', 'laborcost',
            'purchaseorderamount', 'purchaseorderquantity',
            'purchaseorderquantitydiff', 'receiptamount', 'receiptquantity',
            'receiptquantitydiff', 'unbilledorders', 'unbilledordersprimary',
            'taxrate'
        }

        transformed = {}

        for field, value in record.items():
            # Skip the 'links' field
            if field == 'links':
                continue

            # Convert numeric fields
            if field in int_fields:
                transformed[field] = self.safe_int(value)
            elif field in numeric_fields:
                transformed[field] = self.safe_float(value)
            else:
                # Everything else stays as string (or None if empty)
                transformed[field] = None if value == '' else value

        return transformed

    def sync(
        self,
        catalog_entry: CatalogEntry,
        state: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Sync this dimension stream with full refresh

        Args:
            catalog_entry: Catalog entry for this stream
            state: Current state dict

        Returns:
            Updated state dict
        """
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

        # Create event loop for async operations
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        total_processed = 0

        try:
            # Fetch and process all records
            async def fetch_and_process():
                nonlocal total_processed

                LOGGER.info(
                    f"Fetching all records for {self.tap_stream_id}..."
                )
                records = await self.client.fetch_dimension_table(
                    self.tap_stream_id,
                    self.get_query()
                )

                LOGGER.info(
                    f"Processing {len(records)} records "
                    f"for {self.tap_stream_id}..."
                )

                # Process each record
                for idx, record in enumerate(records):
                    try:
                        transformed = self.transform_record(record)
                        self.write_record(transformed)
                        total_processed += 1

                    except BrokenPipeError:
                        raise
                    except Exception as e:
                        LOGGER.warning(
                            f"Error processing record {idx + 1} "
                            f"in {self.tap_stream_id}: {str(e)}"
                        )
                        continue

                LOGGER.info(
                    f"Completed {self.tap_stream_id}: "
                    f"{total_processed} records processed"
                )

            # Run the async processor
            loop.run_until_complete(fetch_and_process())

        except BrokenPipeError:
            LOGGER.warning("Broken pipe during sync - exiting gracefully")
            return state
        except Exception as e:
            LOGGER.error(
                f"Error during sync of {self.tap_stream_id}: {str(e)}"
            )
            raise
        finally:
            loop.close()

        # Update state
        state = self.update_bookmark(
            state,
            record_count=total_processed,
            sync_completed=True
        )

        self.write_state(state)
        return state
