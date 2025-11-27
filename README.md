# tap-netsuite-general-ledger

A [Singer](https://www.singer.io/) tap for extracting NetSuite General Ledger Detail using the SuiteQL REST API.

## Overview

This tap extracts General Ledger Detail data from NetSuite via the **SuiteQL REST API** using OAuth 1.0a authentication. It supports both full refresh and incremental sync modes based on the `last_modified_date` configuration.

**Key Features:**
- 🚀 Uses NetSuite's modern SuiteQL REST API (not RESTlet)
- 🔐 OAuth 1.0a HMAC-SHA256 authentication
- 📊 Extracts posted GL transactions with full accounting dimensions
- 🔄 Supports incremental syncs via `last_modified_date` filter
- 💾 Memory-optimized streaming for large datasets (>100k records)
- 🎯 All fields returned as strings for flexible downstream type casting
- 🔑 Composite key support: `internal_id`, `transaction_id`, `trans_acct_line_id`

## Installation

Install from source:

```bash
git clone https://github.com/ModernAnimal/tap-netsuite-general-ledger.git
cd tap-netsuite-general-ledger
pip install -e .
```

Or install directly:

```bash
pip install tap-netsuite-general-ledger
```

## Configuration

### Required Settings

| Setting | Type | Description |
|---------|------|-------------|
| `netsuite_account` | string | Your NetSuite account ID |
| `netsuite_consumer_key` | string | OAuth consumer key from Integration record |
| `netsuite_consumer_secret` | string | OAuth consumer secret from Integration record |
| `netsuite_token_id` | string | OAuth token ID from Access Token |
| `netsuite_token_secret` | string | OAuth token secret from Access Token |

### Optional Settings

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `last_modified_date` | string | `null` | Date for incremental sync (format: `YYYY-MM-DD`). Omit for full refresh. |
| `posting_periods` | array | `[]` | List of posting period names to sync (e.g., `["Jan 2025", "Feb 2025"]`). If empty or omitted, syncs all periods. |
| `page_size` | integer | `1000` | Records per API request (max: 1000 per NetSuite limits). State is written after each page. |
| `concurrent_requests` | integer | `5` | Number of concurrent page requests to fetch in parallel. Increase for faster syncs (test with 5-10). |
| `record_batch_size` | integer | `1000` | Number of records to accumulate before writing to output. Larger batches reduce I/O overhead. |

### Sample Configuration

**Full Refresh (all posted transactions):**
```json
{
  "netsuite_account": "your_account_id",
  "netsuite_consumer_key": "your_consumer_key",
  "netsuite_consumer_secret": "your_consumer_secret",
  "netsuite_token_id": "your_token_id",
  "netsuite_token_secret": "your_token_secret",
  "page_size": 1000,
  "concurrent_requests": 5,
  "record_batch_size": 1000
}
```

**Incremental Sync (modified records only):**
```json
{
  "netsuite_account": "your_account_id",
  "netsuite_consumer_key": "your_consumer_key",
  "netsuite_consumer_secret": "your_consumer_secret",
  "netsuite_token_id": "your_token_id",
  "netsuite_token_secret": "your_token_secret",
  "last_modified_date": "2025-11-17",
  "page_size": 1000,
  "concurrent_requests": 5,
  "record_batch_size": 1000
}
```

**Specific Posting Periods:**
```json
{
  "netsuite_account": "your_account_id",
  "netsuite_consumer_key": "your_consumer_key",
  "netsuite_consumer_secret": "your_consumer_secret",
  "netsuite_token_id": "your_token_id",
  "netsuite_token_secret": "your_token_secret",
  "posting_periods": ["Jan 2025", "Feb 2025", "Mar 2025"],
  "page_size": 1000,
  "concurrent_requests": 5,
  "record_batch_size": 1000
}
```

## Usage

### Discovery Mode

Generate a catalog of available streams:

```bash
tap-netsuite-general-ledger --config sample_config.json --discover > catalog.json
```

### Sync Mode

Extract data using the catalog:

```bash
tap-netsuite-general-ledger --config sample_config.json --catalog catalog.json
```

### With a Singer Target

Pipe output directly to a target (e.g., Redshift):

```bash
tap-netsuite-general-ledger --config sample_config.json --catalog catalog.json | \
  target-redshift --config target_config.json
```

### With State (for incremental syncs)

```bash
tap-netsuite-general-ledger \
  --config sample_config.json \
  --catalog catalog.json \
  --state state.json > output.json
```

## Sync Modes

### Full Refresh
When `last_modified_date` is **not** provided in config, the tap fetches **all** GL transactions that meet the posting criteria (posted transactions with debit or credit amounts).

**Use Case:** Initial data loads, historical backfills, complete refreshes

### Incremental Sync
When `last_modified_date` is provided, the tap only fetches records where `t.lastModifiedDate >= last_modified_date`. 

**Use Case:** 
- Regular scheduled syncs (e.g., daily updates)
- Reducing data transfer volume
- Capturing recent changes only

**Example:** Set `last_modified_date` to the date of your last successful sync to only fetch new/modified records.

### Posting Period Filtering

The `posting_periods` configuration allows you to sync **specific accounting periods** instead of all periods. This is useful for:

- **Closed period syncs**: Extract only finalized months (e.g., `["Jan 2025", "Feb 2025"]`)
- **Year-end processing**: Sync specific fiscal periods
- **Selective backfills**: Re-sync particular months without affecting others
- **Testing**: Extract sample data from a single period

**How It Works:**

1. **Specify periods by name**: Use the human-readable format displayed in NetSuite (e.g., "Jan 2025", "Feb 2025")
2. **Sequential processing**: Each period is processed completely before moving to the next
3. **State tracking**: Completed periods are saved in state for resumability
4. **Per-period stats**: State includes record counts and timing for each period

**Configuration Examples:**

```json
{
  "posting_periods": ["Jan 2025", "Feb 2025", "Mar 2025"]
}
```

**Behavior:**
- **If specified**: Only transactions with matching posting periods are extracted
- **If empty array `[]`**: All periods are extracted (no filter applied)
- **If omitted**: All periods are extracted (no filter applied)

**State Output:**

When using `posting_periods`, the state includes detailed per-period tracking:

```json
{
  "bookmarks": {
    "netsuite_general_ledger_detail": {
      "last_sync": "2025-11-26T10:30:00Z",
      "total_record_count": 15000,
      "completed_posting_periods": ["Jan 2025", "Feb 2025", "Mar 2025"],
      "posting_period_stats": {
        "Jan 2025": {
          "record_count": 5000,
          "sync_started": "2025-11-26T10:00:00Z",
          "sync_completed": "2025-11-26T10:10:00Z",
          "duration_seconds": 600
        },
        "Feb 2025": {
          "record_count": 5500,
          "sync_started": "2025-11-26T10:10:00Z",
          "sync_completed": "2025-11-26T10:20:00Z",
          "duration_seconds": 600
        },
        "Mar 2025": {
          "record_count": 4500,
          "sync_started": "2025-11-26T10:20:00Z",
          "sync_completed": "2025-11-26T10:30:00Z",
          "duration_seconds": 600
        }
      },
      "sync_completed": true
    }
  }
}
```

**Recovery from Failures:**

If a sync is interrupted midway through processing multiple periods:
1. Already-completed periods are **skipped** on the next run
2. The sync resumes from the next uncompleted period
3. Record counts from completed periods are preserved

**Example:** If syncing `["Jan 2025", "Feb 2025", "Mar 2025"]` fails after completing Jan and Feb, rerunning with the same config will skip Jan and Feb and start with Mar 2025.

**Combining with Incremental Sync:**

You can use both `posting_periods` and `last_modified_date` together:

```json
{
  "posting_periods": ["Jan 2025", "Feb 2025"],
  "last_modified_date": "2025-02-15"
}
```

This will sync **only** Jan 2025 and Feb 2025 periods, **but only** records modified on or after 2025-02-15.

## Stream Details

### netsuite_general_ledger_detail

**Key Properties (Composite Primary Key):** 
- `internal_id` (Transaction ID)
- `transaction_id` (Transaction TranID)
- `trans_acct_line_id` (Transaction Accounting Line ID)

**Replication Method:** `FULL_TABLE` or `INCREMENTAL` (based on `last_modified_date` configuration)

**Record Count:** Varies by NetSuite instance (can range from thousands to millions)

### Field Definitions

Fields are typed appropriately for optimal downstream processing:
- **Integers**: IDs and foreign keys (`internal_id`, `acct_id`, `location`, etc.)
- **Numbers**: Monetary amounts (`debit`, `credit`, `net_amount`)
- **Dates**: Date fields in YYYY-MM-DD format (`transaction_date`, `account_last_modified`, etc.)
- **Strings**: All other fields (names, memos, status, etc.)

| Field Name | Type | Description | Source Table |
|------------|------|-------------|--------------|
| `internal_id` | integer | Transaction internal ID | `Transaction.ID` |
| `transaction_date` | string (date) | Transaction date (YYYY-MM-DD) | `Transaction.TranDate` |
| `transaction_id` | string | Transaction number/ID | `Transaction.TranID` |
| `trans_acct_line_id` | integer | Accounting line ID | `TransactionAccountingLine.TransactionLine` |
| `posting_period` | string | Posting period name | `BUILTIN.DF(Transaction.PostingPeriod)` |
| `posting_period_id` | integer | Posting period internal ID | `Transaction.PostingPeriod` |
| `created_date` | string (datetime) | Record creation datetime (ISO 8601) | `Transaction.createdDateTime` |
| `trans_acct_line_last_modified` | string (date) | Transaction accounting line last modified (YYYY-MM-DD) | `TransactionAccountingLine.lastmodifieddate` |
| `transaction_last_modified` | string (date) | Transaction last modified (YYYY-MM-DD) | `Transaction.lastmodifieddate` |
| `account_last_modified` | string (date) | Account last modified (YYYY-MM-DD) | `Account.lastmodifieddate` |
| `posting` | string | Posting flag (T/F) | `Transaction.Posting` |
| `approval` | string | Approval status | `BUILTIN.DF(Transaction.approvalStatus)` |
| `entity_name` | string | Entity/customer/vendor name | `BUILTIN.DF(Transaction.Entity)` |
| `trans_memo` | string | Transaction-level memo | `Transaction.memo` |
| `trans_line_memo` | string | Line-level memo | `TransactionLine.memo` |
| `transaction_type` | string | Transaction type (e.g., Journal Entry) | `BUILTIN.DF(Transaction.Type)` |
| `acct_id` | integer | Account ID | `TransactionAccountingLine.Account` |
| `account_group` | integer | Parent account ID | `Account.parent` |
| `department` | integer | Department ID | `TransactionLine.Department` |
| `class` | integer | Class ID | `TransactionLine.Class` |
| `location` | integer | Location ID | `TransactionLine.Location` |
| `debit` | number | Debit amount | `TransactionAccountingLine.Debit` |
| `credit` | number | Credit amount | `TransactionAccountingLine.Credit` |
| `net_amount` | number | Net amount (debit - credit) | `TransactionAccountingLine.Amount` |
| `subsidiary` | string | Subsidiary name | `BUILTIN.DF(TransactionLine.Subsidiary)` |
| `document_number` | string | Document number | `Transaction.Number` |
| `status` | string | Transaction status | `BUILTIN.DF(Transaction.Status)` |

**Note:** The tap uses NetSuite's `BUILTIN.DF()` function to get display format names for foreign key fields instead of numeric IDs where applicable.

### netsuite_account

**Key Properties:** `id`

Chart of accounts dimension table with account hierarchy and metadata.

| Field Name | Type | Description | Source Table |
|------------|------|-------------|--------------|
| `id` | integer | Account ID | `Account.id` |
| `acctname` | string | Account name | `Account.acctname` |
| `acctnumber` | string | Account number | `Account.acctnumber` |
| `accttype` | string | Account type | `Account.accttype` |
| `balance` | number | Account balance | `Account.balance` |
| `cashflowrate` | string | Cash flow rate | `Account.cashflowrate` |
| `category1099misc` | string | 1099 MISC category | `Account.category1099misc` |
| `currency` | string | Currency | `Account.currency` |
| `description` | string | Account description | `Account.description` |
| `displaynamewithhierarchy` | string | Display name with hierarchy | `Account.displaynamewithhierarchy` |
| `eliminate` | string | Eliminate flag | `Account.eliminate` |
| `exchangerate` | string | Exchange rate | `Account.exchangerate` |
| `externalid` | string | External ID | `Account.externalid` |
| `fullname` | string | Full name | `Account.fullname` |
| `generalrate` | string | General rate | `Account.generalrate` |
| `includechildren` | string | Include children flag | `Account.includechildren` |
| `inventory` | string | Inventory flag | `Account.inventory` |
| `isinactive` | string | Is inactive flag | `Account.isinactive` |
| `issummary` | string | Is summary flag | `Account.issummary` |
| `lastmodifieddate` | string | Last modified date | `Account.lastmodifieddate` |
| `legalname` | string | Legal name | `Account.legalname` |
| `localizations` | string | Localizations | `Account.localizations` |
| `openbalance` | string | Opening balance | `Account.openbalance` |
| `parent` | string | Parent account ID | `Account.parent` |
| `reconcilewithmatching` | string | Reconcile with matching | `Account.reconcilewithmatching` |
| `revalue` | string | Revalue flag | `Account.revalue` |
| `subsidiary` | string | Subsidiary | `Account.subsidiary` |

### netsuite_vendor

**Key Properties:** `id`

Vendor master data with category, financials, and contact information.

| Field Name | Type | Description | Source Table |
|------------|------|-------------|--------------|
| `id` | integer | Vendor ID | `Vendor.id` |
| `category` | string | Vendor category name | `VendorCategory.name` |
| `accountnumber` | string | Account number | `Vendor.accountnumber` |
| `altname` | string | Alternative name | `Vendor.altname` |
| `balance` | number | Balance | `Vendor.balance` |
| `balanceprimary` | number | Primary balance | `Vendor.balanceprimary` |
| `comments` | string | Comments | `Vendor.comments` |
| `companyname` | string | Company name | `Vendor.companyname` |
| `creditlimit` | number | Credit limit | `Vendor.creditlimit` |
| `currency` | integer | Currency ID | `Vendor.currency` |
| `custentity_2663_payment_method` | integer | Payment method custom field | `Vendor.custentity_2663_payment_method` |
| `datecreated` | string | Date created | `Vendor.datecreated` |
| `email` | string | Email address | `Vendor.email` |
| `emailpreference` | string | Email preference | `Vendor.emailpreference` |
| `emailtransactions` | string | Email transactions flag | `Vendor.emailtransactions` |
| `entityid` | string | Entity ID | `Vendor.entityid` |
| `expenseaccount` | integer | Expense account ID | `Vendor.expenseaccount` |
| `externalid` | string | External ID | `Vendor.externalid` |
| `fax` | string | Fax number | `Vendor.fax` |
| `faxtransactions` | string | Fax transactions flag | `Vendor.faxtransactions` |
| `giveaccess` | string | Give access flag | `Vendor.giveaccess` |
| `incoterm` | integer | Incoterm ID | `Vendor.incoterm` |
| `is1099eligible` | string | 1099 eligible flag | `Vendor.is1099eligible` |
| `isinactive` | string | Is inactive flag | `Vendor.isinactive` |
| `isjobresourcevend` | string | Is job resource vendor flag | `Vendor.isjobresourcevend` |
| `isperson` | string | Is person flag | `Vendor.isperson` |
| `laborcost` | number | Labor cost | `Vendor.laborcost` |
| `lastmodifieddate` | string | Last modified date | `Vendor.lastmodifieddate` |
| `legalname` | string | Legal name | `Vendor.legalname` |
| `payablesaccount` | integer | Payables account ID | `Vendor.payablesaccount` |
| `phone` | string | Phone number | `Vendor.phone` |
| `printoncheckas` | string | Print on check as | `Vendor.printoncheckas` |
| `printtransactions` | string | Print transactions flag | `Vendor.printtransactions` |
| `purchaseorderamount` | number | Purchase order amount | `Vendor.purchaseorderamount` |
| `purchaseorderquantity` | number | Purchase order quantity | `Vendor.purchaseorderquantity` |
| `purchaseorderquantitydiff` | number | Purchase order quantity difference | `Vendor.purchaseorderquantitydiff` |
| `receiptamount` | number | Receipt amount | `Vendor.receiptamount` |
| `receiptquantity` | number | Receipt quantity | `Vendor.receiptquantity` |
| `receiptquantitydiff` | number | Receipt quantity difference | `Vendor.receiptquantitydiff` |
| `representingsubsidiary` | integer | Representing subsidiary ID | `Vendor.representingsubsidiary` |
| `subsidiary` | integer | Subsidiary ID | `Vendor.subsidiary` |
| `terms` | integer | Terms ID | `Vendor.terms` |
| `unbilledorders` | number | Unbilled orders | `Vendor.unbilledorders` |
| `unbilledordersprimary` | number | Unbilled orders primary | `Vendor.unbilledordersprimary` |
| `url` | string | URL | `Vendor.url` |
| `workcalendar` | integer | Work calendar ID | `Vendor.workcalendar` |

### netsuite_classification

**Key Properties:** `id`

Classification dimension (also known as "Class") for tracking business segments or cost centers.

| Field Name | Type | Description | Source Table |
|------------|------|-------------|--------------|
| `id` | integer | Classification ID | `Classification.id` |
| `externalid` | string | External ID | `Classification.externalid` |
| `fullname` | string | Full name | `Classification.fullname` |
| `includechildren` | string | Include children flag | `Classification.includechildren` |
| `isinactive` | string | Is inactive flag | `Classification.isinactive` |
| `lastmodifieddate` | string | Last modified date | `Classification.lastmodifieddate` |
| `name` | string | Name | `Classification.name` |
| `parent` | string | Parent classification ID | `Classification.parent` |
| `subsidiary` | string | Subsidiary | `Classification.subsidiary` |

### netsuite_department

**Key Properties:** `id`

Department dimension for organizational hierarchy tracking.

| Field Name | Type | Description | Source Table |
|------------|------|-------------|--------------|
| `id` | integer | Department ID | `Department.id` |
| `externalid` | string | External ID | `Department.externalid` |
| `fullname` | string | Full name | `Department.fullname` |
| `includechildren` | string | Include children flag | `Department.includechildren` |
| `isinactive` | string | Is inactive flag | `Department.isinactive` |
| `lastmodifieddate` | string | Last modified date | `Department.lastmodifieddate` |
| `name` | string | Name | `Department.name` |
| `parent` | integer | Parent department ID | `Department.parent` |
| `subsidiary` | string | Subsidiary | `Department.subsidiary` |

### netsuite_location

**Key Properties:** `id`

Location dimension with address details and custom fields.

| Field Name | Type | Description | Source Table |
|------------|------|-------------|--------------|
| `id` | integer | Location ID | `Location.id` |
| `cseg1` | integer | Custom segment 1 | `Location.cseg1` |
| `taxrate` | number | Tax rate | `Location.custrecord1` |
| `openingdate` | string | Opening date | `Location.custrecord2` |
| `closingdate` | string | Closing date | `Location.custrecord3` |
| `lease_refid` | string | Lease reference ID | `Location.custrecord4` |
| `fullname` | string | Full name | `Location.fullname` |
| `isinactive` | string | Is inactive flag | `Location.isinactive` |
| `custrecord_bdc_lastupdatedbyimp_loc` | string | Last updated by import | `Location.custrecord_bdc_lastupdatedbyimp_loc` |
| `lastmodifieddate` | string | Last modified date | `Location.lastmodifieddate` |
| `mainaddress` | integer | Main address ID | `Location.mainaddress` |
| `makeinventoryavailable` | string | Make inventory available flag | `Location.makeinventoryavailable` |
| `name` | string | Name | `Location.name` |
| `subsidiary` | integer | Subsidiary ID | `Location.subsidiary` |
| `locationtype` | integer | Location type ID | `Location.locationtype` |
| `externalid` | string | External ID | `Location.externalid` |
| `usebins` | string | Use bins flag | `Location.usebins` |
| `addr1` | string | Address line 1 | `LocationMainAddress.addr1` |
| `addr2` | string | Address line 2 | `LocationMainAddress.addr2` |
| `city` | string | City | `LocationMainAddress.city` |
| `state` | string | State/Province | `LocationMainAddress.state` |
| `zip` | string | Zip/Postal code | `LocationMainAddress.zip` |
| `country` | string | Country | `LocationMainAddress.country` |
| `addrphone` | string | Address phone number | `LocationMainAddress.addrphone` |
| `attention` | string | Attention/Contact name | `LocationMainAddress.attention` |

### netsuite_customer

**Key Properties:** `id`

Customer master data (simplified extract).

| Field Name | Type | Description | Source Table |
|------------|------|-------------|--------------|
| `id` | integer | Customer ID | `Customer.id` |
| `entityid` | string | Entity ID | `Customer.entityid` |
| `companyname` | string | Company name | `Customer.companyname` |

### netsuite_employee

**Key Properties:** `id`

Employee master data (simplified extract).

| Field Name | Type | Description | Source Table |
|------------|------|-------------|--------------|
| `id` | integer | Employee ID | `Employee.id` |
| `entityid` | string | Entity ID | `Employee.entityid` |
| `companyname` | string | Full name (firstname + lastname) | `Employee.firstname + ' ' + Employee.lastname` |

## Performance & Pagination

### Concurrent Page Fetching

The tap uses **concurrent page fetching** to dramatically improve performance when extracting large datasets:

1. **Parallel Requests:** Multiple pages are fetched simultaneously (default: 5 concurrent requests)
2. **Ordered Results:** Pages are buffered and yielded in order to maintain Singer protocol compliance
3. **Connection Pooling:** A persistent HTTP session is reused across all requests
4. **Rate Limiting:** Semaphore controls ensure NetSuite isn't overwhelmed

**Performance Impact:** With `concurrent_requests=5`, a 1.3 million record extraction that previously took several hours can complete in 30-60 minutes.

**Tuning Concurrency:**
- Start with default of `5` concurrent requests
- Monitor NetSuite for rate limiting errors
- Gradually increase to `10` or `15` for faster syncs if no errors occur
- Reduce to `1` to disable concurrency (sequential mode) if issues arise

### Memory Optimization
The tap uses streaming architecture to handle large datasets efficiently:

1. **Page Fetching:** Data is fetched in pages (default: 1000 records per API call)
2. **Batch Writing:** Records are written in batches to reduce I/O overhead
3. **Optimized Transformation:** Uses pre-compiled field sets and dict comprehension for fast processing
4. **Immediate Release:** Memory is released after each batch is written

### Record Processing Optimizations

**Batch Writing**: Records are accumulated in batches (default: 1000) before writing to stdout. This reduces the overhead of individual write operations and improves throughput by 2-3x.

**Optimized Transformation**: Record transformation uses:
- Pre-compiled frozensets for field type checking
- Dict comprehension instead of loops
- Inline type conversion
- These optimizations provide ~30-50% speedup on transformation alone

### Handling NetSuite's Offset Limit

NetSuite SuiteQL has a **maximum offset of 99,000** records. The tap automatically handles this by:
- Using ID-based chunking when datasets exceed 100k records
- Filtering subsequent queries with `WHERE t.ID > last_processed_id`
- This allows extraction of unlimited records without hitting the offset limit

### Recommended Settings

| Dataset Size | page_size | concurrent_requests | Notes |
|--------------|-----------|---------------------|-------|
| < 100k records | 1000 | 5 | Good balance for small datasets |
| 100k - 1M records | 1000 | 5-10 | ID-chunking automatically engaged |
| > 1M records | 1000 | 10-15 | Higher concurrency for faster syncs |

**Note:** State is written after each page to ensure reliable checkpointing and recovery.

## Data Quality & Validation

The tap includes validation to ensure data integrity:

### Required Fields Validation
Records are **skipped** (with warning logged) if:
- `trans_acct_line_id` is NULL or empty
- `internal_id` is NULL or empty

This prevents downstream primary key constraint violations.

### Type Conversion

Fields are converted to appropriate types during extraction:

- **Integers**: ID fields are converted to integers (`internal_id`, `acct_id`, `location`, etc.)
- **Numbers**: Monetary amounts are converted to numbers/floats (`debit`, `credit`, `net_amount`)
- **Dates**: Date fields are formatted to YYYY-MM-DD (`transaction_date`, `account_last_modified`, etc.)
- **Datetimes**: Datetime fields are formatted to ISO 8601 (`created_date`)
- **Strings**: All other fields remain as strings

This ensures proper typing for downstream targets and enables direct SQL operations without additional casting.

## Required Data Access

The integration user/role must have SuiteQL Access.

## Testing Your Setup

Verify OAuth credentials work:

```bash
# Run discovery - this will authenticate and query schema
tap-netsuite-general-ledger --config sample_config.json --discover
```

If authentication fails, you'll see OAuth signature errors or 401 responses.

## Troubleshooting

### Common Issues

**401 Unauthorized / OAuth Signature Invalid**
- Verify all OAuth credentials are correct
- Ensure token/integration is not disabled in NetSuite
- Check that user/role has REST Web Services permission

**No Records Returned**
- Verify you have posted transactions in NetSuite
- Check that `last_modified_date` (if used) is not too recent
- Ensure user has access to the transaction records

**Offset Limit Errors**
- Should be handled automatically by ID-based chunking
- If errors persist, reduce `page_size` to 500

**Memory Issues**
- Reduce `page_size` from 1000 to 500 or lower
- Ensure sufficient system memory (recommend 2GB+ for large datasets)

**Broken Pipe Errors**
- Usually indicates the **target process terminated** (not the tap!)
- Check target logs for schema mismatches, constraint violations, or OOM errors
- Try reducing `page_size` to 500 or lower
- Verify catalog schema matches target table schema

**Rate Limiting / 429 Errors**
- Reduce `concurrent_requests` from default 5 to 3 or 1
- NetSuite may throttle requests during peak hours
- Contact NetSuite support to check your account's concurrency limits

**Slow Performance**
- Increase `concurrent_requests` from 5 to 10 or 15 for faster syncs
- Monitor for rate limiting when increasing concurrency
- Ensure adequate network bandwidth for concurrent connections

### Debug Logging

For detailed logging, set the log level:

```bash
export LOGGING_LEVEL=DEBUG
tap-netsuite-general-ledger --config sample_config.json --catalog catalog.json
```

## Development

### Install for Development

```bash
git clone https://github.com/ModernAnimal/tap-netsuite-general-ledger.git
cd tap-netsuite-general-ledger
pip install -e .
```

### Project Structure

```
tap-netsuite-general-ledger/
├── tap_netsuite_general_ledger/
│   ├── __init__.py          # Main entry point
│   ├── client.py            # NetSuite SuiteQL API client
│   ├── discover.py          # Schema discovery
│   ├── sync.py              # Data extraction and transformation
│   └── schemas/             # JSON schema definitions
│       └── netsuite_general_ledger_detail.json
├── setup.py                 # Package configuration
├── requirements.txt         # Dependencies
├── sample_config.json       # Example configuration
└── README.md               # This file
```

### Dependencies

- `singer-python>=5.0.0` - Singer specification implementation
- `aiohttp>=3.8.0` - Async HTTP client for NetSuite API

## License

This project is licensed under the GNU General Public License v3.0 - see the LICENSE file for details.

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-new-feature`)
3. Make your changes with clear commit messages
4. Add tests if applicable
5. Ensure all tests pass
6. Submit a pull request

## Support

For issues and questions:

1. Check existing [GitHub Issues](https://github.com/ModernAnimal/tap-netsuite-general-ledger/issues)
2. Create a new issue with:
   - Detailed description of the problem
   - Sample configuration (without credentials!)
   - Relevant log output
   - NetSuite version/environment details

## Credits

Built with ❤️ by [Modern Animal](https://github.com/ModernAnimal) for the Singer/Meltano community.