# tap-netsuite-general-ledger

A [Singer](https://www.singer.io/) tap for extracting NetSuite General Ledger Detail.

## Overview

This tap extracts GL Detail data from NetSuite via a RESTlet API using OAuth 1.0a authentication. It's designed to work with a specific NetSuite saved search and RESTlet configuration.

**Important**: This tap performs a **FULL REFRESH** for each sync operation (to allow for potential deletions), which means:
- The target table is **TRUNCATED** before loading new data
- All data for the specified date range/period is completely reloaded
- This approach ensures any deletions or modifications in NetSuite are properly reflected in the target
- Ideal for rolling window scenarios where you want to refresh data for specific time periods

## Installation

```bash
pip install tap-netsuite-general-ledger
```

Or install from source:

```bash
git clone https://github.com/ModernAnimal/tap-netsuite-general-ledger.git
cd tap-netsuite-general-ledger
pip install -e .
```

## Configuration

The tap requires the following configuration parameters:

### Required Configuration

- `netsuite_account`: Your NetSuite account ID
- `netsuite_consumer_key`: OAuth consumer key
- `netsuite_consumer_secret`: OAuth consumer secret  
- `netsuite_token_id`: OAuth token ID
- `netsuite_token_secret`: OAuth token secret

### Optional Configuration

- `netsuite_script_id`: RESTlet script ID
- `netsuite_deploy_id`: RESTlet deployment ID
- `netsuite_search_id`: Saved search ID
- `start_date`: Start date for data extraction (ISO format)
- `period_id`: Specific period ID to extract
- `period_name`: Specific period name to extract (e.g., "Jan 2024")
- `date_from`: Start date filter (MM/DD/YYYY format)
- `date_to`: End date filter (MM/DD/YYYY format)

**Note**: See `example_rolling_window_config.json` for a complete configuration template with rolling window examples.

### Sample Configuration

```json
{
  "netsuite_account": "your_account_id",
  "netsuite_consumer_key": "your_consumer_key",
  "netsuite_consumer_secret": "your_consumer_secret",
  "netsuite_token_id": "your_token_id",
  "netsuite_token_secret": "your_token_secret",
  "netsuite_script_id": "1859",
  "netsuite_deploy_id": "2",
  "netsuite_search_id": "customsearch_gl_detail_report_2_7_3",
  "date_from": "01/01/2024",
  "date_to": "01/31/2024"
}
```

### Rolling Window Configuration Examples

**Monthly refresh for January 2024:**
```json
{
  "period_name": "Jan 2024",
  ...other config...
}
```

**Specific date range refresh:**
```json
{
  "date_from": "01/01/2024",
  "date_to": "01/31/2024",
  ...other config...
}
```

**Quarterly refresh using period ID:**
```json
{
  "period_id": "123",
  ...other config...
}
```

## Usage

### Discovery Mode

Generate a catalog of available streams:

```bash
tap-netsuite-general-ledger --config config.json --discover > catalog.json
```

### Sync Mode (Full Refresh)

Extract data using the catalog. **Note**: This will TRUNCATE the target table and reload all data for the specified date range:

```bash
tap-netsuite-general-ledger --config config.json --catalog catalog.json
```

### Rolling Window Example

For a rolling window approach where you refresh data for specific periods:

```bash
# Refresh data for January 2024 (truncates and reloads)
tap-netsuite-general-ledger --config config_jan_2024.json --catalog catalog.json

# Refresh data for February 2024 (truncates and reloads)
tap-netsuite-general-ledger --config config_feb_2024.json --catalog catalog.json
```

### With State

State is maintained for tracking sync history, but does not affect the full refresh behavior:

```bash
tap-netsuite-general-ledger --config config.json --catalog catalog.json --state state.json
```

## Streams

### netsuite_general_ledger_detail

**Replication Method**: `FULL_TABLE` (Full Refresh with Truncate)

The main stream containing GL detail records. Each sync operation will:
1. **TRUNCATE** the target table
2. **RELOAD** all data for the specified date range/period
3. Ensure data consistency and account for any deletions in NetSuite

**Fields**:

- `internal_id`: Internal ID of the transaction
- `document_number`: Document number
- `type`: Transaction type
- `journal_name`: Journal name
- `date`: Transaction date
- `period`: Posting period
- `subsidiary`: Subsidiary
- `account`: Account
- `amount_debit`: Debit amount
- `amount_credit`: Credit amount
- `amount_net`: Net amount
- `amount_transaction_total`: Transaction total amount
- `class`: Class
- `location`: Location
- `department`: Department
- `line`: Line number
- `name_line`: Line name
- `memo_main`: Main memo
- `memo_line`: Line memo
- `status`: Status
- `approval_status`: Approval status
- `date_created`: Date created
- `created_by`: Created by
- `name`: Name
- `posting`: Posting
- `company_name`: Company name

## Full Refresh Behavior

This tap is specifically designed for **FULL REFRESH** replication to ensure data integrity:

### How It Works

1. **Truncate**: The target table is completely truncated before each sync
2. **Reload**: All data for the specified date range/period is extracted and loaded
3. **Consistency**: Ensures any deletions, modifications, or corrections in NetSuite are reflected

### Use Cases

- **Rolling Window**: Refresh specific time periods (e.g., monthly GL closes)
- **Data Corrections**: Account for NetSuite adjustments or corrections
- **Audit Compliance**: Ensure target data exactly matches NetSuite
- **Deletion Handling**: Properly handle deleted transactions

### Best Practices

- Use specific date ranges (`date_from`/`date_to`) or periods (`period_name`/`period_id`)
- Run separate syncs for different time periods to maintain granular control
- Monitor sync duration for large date ranges
- Consider target system's truncate/load performance characteristics

## NetSuite Setup

This tap requires a NetSuite RESTlet script and saved search to be configured:

### RESTlet Configuration

1. Deploy the included RESTlet script (`netsuite_macro.js`) in NetSuite
2. Note the Script ID and Deployment ID (`netsuite_script_id` and `netsuite_deploy_id`)
3. Ensure proper permissions are configured

### Saved Search

1. Create or use the saved Search ID (`netsuite_search_id`)
2. Ensure it includes all required GL detail fields
3. Configure appropriate permissions

### Authentication

1. Set up OAuth 2.0 authentication in NetSuite
2. Generate consumer key/secret and token ID/secret
3. Ensure the authenticating user has appropriate permissions

## Development

Install development dependencies:

```bash
pip install -e ".[dev]"
```

Run tests:

```bash
pytest
```

### Testing Full Refresh

To test the full refresh functionality:

```bash
# Generate catalog
tap-netsuite-general-ledger --config your_config.json --discover > catalog.json

# Test full refresh with a small date range
tap-netsuite-general-ledger --config your_config.json --catalog catalog.json

# Verify TRUNCATE message is emitted before records
# Check that all records for the date range are extracted
```

## License

This project is licensed under the GNU General Public License v3.0 - see the LICENSE file for details.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## Support

For issues and questions:

1. Check the existing issues on GitHub
2. Create a new issue with detailed information
3. Include sample configuration (without credentials)
4. Include relevant log output
