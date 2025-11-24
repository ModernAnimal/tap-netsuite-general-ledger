import os
import sys
import requests
import pandas as pd
from requests_oauthlib import OAuth1
from psycopg2.extras import execute_values

parent_directory = os.path.abspath('..')
sys.path.append(parent_directory)

import database
import secrets_manager
from database import connection

# ===== NETSUITE CONFIG =====
ACCOUNT_ID = "5665960"
URL_ID = "5665960"
CONSUMER_KEY = secrets["CONSUMER_KEY"]
CONSUMER_SECRET = secrets["CONSUMER_SECRET"]
TOKEN_ID = secrets["TOKEN_ID"]
TOKEN_SECRET = secrets["TOKEN_SECRET"]

BASE_URL = f"https://{URL_ID}.suitetalk.api.netsuite.com"
SUITEQL_URL = f"{BASE_URL}/services/rest/query/v1/suiteql"

auth = OAuth1(
    client_key=CONSUMER_KEY,
    client_secret=CONSUMER_SECRET,
    resource_owner_key=TOKEN_ID,
    resource_owner_secret=TOKEN_SECRET,
    realm=ACCOUNT_ID,
    signature_method='HMAC-SHA256'
)

# ===== REDSHIFT CONFIG =====
REDSHIFT_DATABASE = "warehouse"
REDSHIFT_SCHEMA = "ingestion"


def fetch_all_data(auth, query):
    """Fetch all data from NetSuite using pagination"""
    all_items = []

    headers = {
        'Content-Type': 'application/json',
        'Prefer': 'transient'
    }

    payload = {"q": query}
    offset = 0
    limit = 1000
    page = 1

    while True:
        url = f"{SUITEQL_URL}?limit={limit}&offset={offset}"
        print(f"  Fetching page {page} (offset: {offset})...")

        response = requests.post(url, auth=auth, headers=headers, json=payload)

        if response.status_code == 200:
            data = response.json()
            items = data.get('items', [])

            if not items:
                break

            all_items.extend(items)
            print(f"    Retrieved {len(items)} records (Total: {len(all_items)})")

            if len(items) < limit:
                break

            offset += limit
            page += 1

        else:
            print(f"  Error: {response.status_code}")
            print(response.text)
            break

    return all_items


def truncate_table(table_name, schema):
    """Truncate a table in Redshift"""
    print(f"Truncating table {schema}.{table_name}...")

    with connection() as conn:
        cursor = conn.cursor()

        try:
            cursor.execute(f"TRUNCATE TABLE {schema}.{table_name}")
            conn.commit()
            print(f"  Table {schema}.{table_name} truncated successfully")
        except Exception as e:
            conn.rollback()
            print(f"  Error truncating table: {e}")
            raise
        finally:
            cursor.close()
            conn.close()


def load_to_redshift(df, table_name, schema, column_types):

    # Drop 'links' column
    if 'links' in df.columns:
        df = df.drop(columns=['links'])

    # Force column data types
    for col in df.columns:
        if col in column_types:
            col_type = column_types[col]

            if col_type == 'integer':
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('Int64')

            elif col_type == 'numeric':
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

            elif col_type == 'date':
                df[col] = pd.to_datetime(df[col], errors='coerce')
                df[col] = df[col].astype(object).where(df[col].notna(), None)

            elif col_type == 'boolean':
                # Convert T/F to boolean
                df[col] = df[col].apply(
                    lambda x: True if str(x).upper() == 'T' else False if str(x).upper() == 'F' else None)

            elif col_type == 'string':
                df[col] = df[col].astype(str).replace('nan', '').replace('None', '')

    print(f"Loading {len(df)} records to Redshift table '{schema}.{table_name}'...")

    with connection() as conn:
        cursor = conn.cursor()

        try:
            columns = df.columns.tolist()
            column_names = ','.join(columns)

            data = df.where(pd.notna(df), None).values.tolist()
            data = [tuple(row) for row in data]

            insert_sql = f"INSERT INTO {schema}.{table_name} ({column_names}) VALUES %s"

            batch_size = 5000
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]
                execute_values(cursor, insert_sql, batch, page_size=1000)
                print(f"  Inserted {min(i + batch_size, len(data))}/{len(data)} records...")

            conn.commit()
            print(f"Successfully loaded {len(df)} records to {schema}.{table_name}")

        except Exception as e:
            conn.rollback()
            print(f"Error loading data: {e}")
            raise
        finally:
            cursor.close()
            conn.close()


# Table configurations
TABLE_CONFIGS = {
    'account': {
        'redshift_table': 'account',
        'netsuite_query': """SELECT *
                             FROM Account
                             ORDER BY Account.id""",
        'column_types': {
            'id': 'integer',
            'balance': 'numeric',
            'lastmodifieddate': 'string',
            'parent': 'string',
            # Most other fields are strings by default
        }
    },
    'vendor': {
    'redshift_table': 'vendor',
    'netsuite_query': """SELECT vc.name as category, v.id, v.accountnumber, v.altname, v.balance, 
                         v.balanceprimary, v.comments, v.companyname, v.creditlimit, 
                         v.currency, v.custentity_2663_payment_method, 
                         v.datecreated, v.email, 
                         v.emailpreference, v.emailtransactions, v.entityid, 
                         v.expenseaccount, v.externalid, v.fax, v.faxtransactions, v.giveaccess, 
                         v.incoterm, v.is1099eligible, v.isinactive, v.isjobresourcevend, v.isperson, 
                         v.laborcost, v.lastmodifieddate, v.legalname, 
                         v.payablesaccount, v.phone, 
                         v.printoncheckas, v.printtransactions, v.purchaseorderamount, 
                         v.purchaseorderquantity, v.purchaseorderquantitydiff, v.receiptamount, 
                         v.receiptquantity, v.receiptquantitydiff, v.representingsubsidiary, 
                         v.subsidiary, v.terms, v.unbilledorders, v.unbilledordersprimary, v.url, 
                         v.workcalendar
                         FROM Vendor v
                         LEFT JOIN VendorCategory vc on v.category = vc.id
                         ORDER BY v.id""",
    'column_types': {
            'category': 'string',
            'id': 'integer',
            'accountnumber': 'string',
            'altname': 'string',
            'balance': 'numeric',
            'balanceprimary': 'numeric',
            'comments': 'string',
            'companyname': 'string',
            'creditlimit': 'numeric',
            'currency': 'integer',
            'custentity_2663_payment_method': 'integer',
            'datecreated': 'date',
            'email': 'string',
            'emailpreference': 'string',
            'emailtransactions': 'string',
            'entityid': 'string',
            'expenseaccount': 'integer',
            'externalid': 'string',
            'fax': 'string',
            'faxtransactions': 'string',
            'giveaccess': 'string',
            'incoterm': 'integer',
            'is1099eligible': 'string',
            'isinactive': 'string',
            'isjobresourcevend': 'string',
            'isperson': 'string',
            'laborcost': 'numeric',
            'lastmodifieddate': 'date',
            'legalname': 'string',
            'payablesaccount': 'integer',
            'phone': 'string',
            'printoncheckas': 'string',
            'printtransactions': 'string',
            'purchaseorderamount': 'numeric',
            'purchaseorderquantity': 'numeric',
            'purchaseorderquantitydiff': 'numeric',
            'receiptamount': 'numeric',
            'receiptquantity': 'numeric',
            'receiptquantitydiff': 'numeric',
            'representingsubsidiary': 'integer',
            'subsidiary': 'integer',
            'terms': 'integer',
            'unbilledorders': 'numeric',
            'unbilledordersprimary': 'numeric',
            'url': 'string',
            'workcalendar': 'integer',
        }
    },
    'classification': {
        'redshift_table': 'classification',
        'netsuite_query': """SELECT *
                             FROM classification
                             ORDER BY classification.id""",
        'column_types': {
            'id': 'integer',
            'lastmodifieddate': 'string',
            'parent': 'string',
        }
    },
    'department': {
        'redshift_table': 'department',
        'netsuite_query': """SELECT *
                             FROM Department
                             ORDER BY Department.id""",
        'column_types': {
            'id': 'integer',
            'lastmodifieddate': 'string',
            'parent': 'integer',
        }
    },
    'location': {
        'redshift_table': 'location',
        'netsuite_query': """SELECT id, cseg1, custrecord1 as TaxRate, custrecord2 as OpeningDate, custrecord3 as ClosingDate, custrecord4 as Lease_RefID, fullname, 
                     isinactive, custrecord_bdc_lastupdatedbyimp_loc, lastmodifieddate, mainaddress, makeinventoryavailable, name, subsidiary, locationtype, externalid 
                     FROM Location
                     ORDER BY Location.id""",
        'column_types': {
            'id': 'integer',
            'cseg1': 'integer',
            'taxrate': 'numeric',
            'openingdate': 'date',
            'closingdate': 'date',
            'lease_refid': 'string',
            'fullname': 'string',
            'isinactive': 'string',
            'custrecord_bdc_lastupdatedbyimp_loc': 'string',
            'lastmodifieddate': 'string',
            'mainaddress': 'integer',
            'makeinventoryavailable': 'string',
            'name': 'string',
            'subsidiary': 'integer',
            'locationtype': 'integer',
            'externalid': 'string',
        }
    },
    'entity': {
        'redshift_table': 'entity',
        'netsuite_queries': [
            """SELECT id, entityid, companyname, 'Customer' as entitytype, '' as category
               FROM customer""",
            """SELECT v.id, v.entityid, v.companyname, 'Vendor' as entitytype, vc.name as category
               FROM vendor v LEFT JOIN VendorCategory vc on v.category = vc.id""",
            """SELECT id, entityid, firstname || ' ' || lastname as companyname, 'Employee' as entitytype, '' as category
               FROM employee"""
        ],
        'column_types': {
            'id': 'integer',
        }
    }
}


def process_standard_table(table_name, config, auth, schema):
    print(f"\n{'=' * 60}")
    print(f"Processing Table: {table_name.upper()}")
    print(f"{'=' * 60}")

    # Truncate table before loading with current data
    truncate_table(config['redshift_table'], redshift_params, schema)

    # Get data from NetSuite
    print(f"Getting NetSuite Data...")
    data = fetch_all_data(auth, config['netsuite_query'])
    print(f"Total records: {len(data)}")

    if not data:
        print(f"No data found for {table_name}, skipping...")
        return 0


    df = pd.DataFrame(data)

    # Load to Redshift
    load_to_redshift(df, config['redshift_table'], schema, config['column_types'])

    return len(df)


def process_entity_table(config, auth, schema):
    #Entity - must union multiple tables
    print(f"\n{'=' * 60}")
    print(f"Processing Table: ENTITY")
    print(f"{'=' * 60}")

    # Truncate existing data
    truncate_table(config['redshift_table'], redshift_params, schema)

    all_data = []

    # Get data from each entity type
    entity_types = ['Customer', 'Vendor', 'Employee']
    for i, query in enumerate(config['netsuite_queries']):
        print(f"\nGetting {entity_types[i]} data...")
        data = fetch_all_data(auth, query)
        print(f"Total {entity_types[i]} records: {len(data)}")
        all_data.extend(data)

    print(f"\nTotal entity records: {len(all_data)}")

    if not all_data:
        print("No entity data found, skipping...")
        return 0

    # Convert to DataFrame
    df = pd.DataFrame(all_data)

    # Load to Redshift
    load_to_redshift(df, config['redshift_table'], schema, config['column_types'])

    return len(df)


# ===== MAIN =====
if __name__ == "__main__":

    # Define schema
    REDSHIFT_SCHEMA = 'ingestion'

    # Process each table
    total_records_loaded = 0

    # Load standard tables
    for table_name in ['account', 'classification', 'department', 'location','vendor']:
        try:
            records = process_standard_table(
                table_name,
                TABLE_CONFIGS[table_name],
                auth,
                REDSHIFT_SCHEMA
            )
            total_records_loaded += records
            print(f" {table_name.upper()} complete - {records} records loaded")
        except Exception as e:
            print(f"Failed to process {table_name.upper()}: {e}")
            # Continue with other tables even if one fails
            continue

    # Load entity table
    try:
        records = process_entity_table(
            TABLE_CONFIGS['entity'],
            auth,
            REDSHIFT_SCHEMA
        )
        total_records_loaded += records
        print(f"ENTITY complete - {records} records loaded")
    except Exception as e:
        print(f"Failed to process ENTITY: {e}")

    # Summary
    print(f"\n{'=' * 60}")
    print(f"ALL TABLES COMPLETE")
    print(f"Total records loaded: {total_records_loaded}")
    print(f"{'=' * 60}")