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
| `netsuite_account` | string | Your NetSuite account ID (e.g., "5665960") |
| `netsuite_consumer_key` | string | OAuth consumer key from Integration record |
| `netsuite_consumer_secret` | string | OAuth consumer secret from Integration record |
| `netsuite_token_id` | string | OAuth token ID from Access Token |
| `netsuite_token_secret` | string | OAuth token secret from Access Token |

### Optional Settings

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `last_modified_date` | string | `null` | Date for incremental sync (format: `YYYY-MM-DD`). Omit for full refresh. |
| `page_size` | integer | `1000` | Records per API request (max: 1000 per NetSuite limits). State is written after each page. |
| `concurrent_requests` | integer | `5` | Number of concurrent page requests to fetch in parallel. Increase for faster syncs (test with 5-10). |
| `record_batch_size` | integer | `1000` | Number of records to accumulate before writing to output. Larger batches reduce I/O overhead. |

### Sample Configuration

**Full Refresh (all posted transactions):**
```json
{
  "netsuite_account": "5665960",
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
  "netsuite_account": "5665960",
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

## SuiteQL Query Details

The tap constructs a SuiteQL query that:

### Joins
- `Transaction` (header table)
- `TransactionAccountingLine` (GL impact: debits/credits)
- `Account` (for account hierarchy)
- `TransactionLine` (for dimensions: department/class/location)

### Filters
- `t.Posting = 'T'` - Posted transactions only
- `tal.Posting = 'T'` - Posted accounting lines only
- `(tal.Debit IS NOT NULL) OR (tal.Credit IS NOT NULL)` - Lines with amounts
- Optional: `t.lastModifiedDate >= TO_DATE('YYYY-MM-DD')` for incremental sync

### Ordering
Results are ordered by `t.ID, t.TranDate, t.TranID, tal.TransactionLine` for consistent pagination.

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
- `transaction_id` is NULL or empty

This prevents downstream primary key constraint violations.

### Type Conversion

Fields are converted to appropriate types during extraction:

- **Integers**: ID fields are converted to integers (`internal_id`, `acct_id`, `location`, etc.)
- **Numbers**: Monetary amounts are converted to numbers/floats (`debit`, `credit`, `net_amount`)
- **Dates**: Date fields are formatted to YYYY-MM-DD (`transaction_date`, `account_last_modified`, etc.)
- **Datetimes**: Datetime fields are formatted to ISO 8601 (`created_date`)
- **Strings**: All other fields remain as strings

This ensures proper typing for downstream targets and enables direct SQL operations without additional casting.

## NetSuite Setup

### OAuth Authentication Setup

1. **Create Integration Record** (Setup > Integration > Manage Integrations > New)
   - Name: "Singer Tap - GL Extract"
   - State: Enabled
   - Token-Based Authentication: Checked
   - Note the **Consumer Key** and **Consumer Secret**

2. **Create Access Token** (Setup > Users/Roles > Access Tokens > New)
   - Application Name: Select your integration
   - User: Select user with appropriate permissions
   - Role: Select role with GL access
   - Note the **Token ID** and **Token Secret**

3. **Configure User/Role Permissions**
   - Permissions > Transactions > Find Transaction (View)
   - Permissions > Reports > SuiteAnalytics Workbook (View)
   - Permissions > Setup > Access Token Management (View/Edit)
   - Permissions > Setup > REST Web Services (Full)
   - Permissions > Setup > SOAP Web Services (Full)
   - Permissions > Setup > User Access Tokens (Full)

### Required Data Access

The integration user/role must have access to:
- `Transaction` records (all transaction types)
- `TransactionAccountingLine` records
- `Account` records (chart of accounts)
- `TransactionLine` records
- `PostingPeriod` records

### Testing Your Setup

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

### Running Tests

```bash
# Install test dependencies
pip install pytest pytest-asyncio

# Run tests
pytest
```

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

## Changelog

### v0.1.0 (Current)
- Complete rewrite to use SuiteQL REST API instead of RESTlet
- OAuth 1.0a HMAC-SHA256 authentication
- Memory-optimized streaming for large datasets
- ID-based chunking to handle >100k record datasets
- All fields returned as strings for flexible type casting
- Composite primary key support
- Incremental sync via `last_modified_date`
- Enhanced error handling and validation

## Credits

Built with ❤️ by [Modern Animal](https://github.com/ModernAnimal) for the Singer/Meltano community.
