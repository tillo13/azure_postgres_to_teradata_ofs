# Import the required modules
import os
import pandas as pd
import psycopg2
import teradatasql
import time
from dotenv import load_dotenv
import json
import warnings
#ignore pandas alert to use SQLALchemy as psycopg2 is fine.
warnings.filterwarnings(action='ignore', message='pandas only supports SQLAlchemy connectable')

start_time = time.time()

# Function for converting date types to Teradata types
def convert_to_tera_types(dtype, col_name):
    """
    Converts pandas DataFrame column types to Teradata types.
    """
    print(f"\nProcessing column '{col_name}'. Pandas type: {dtype.name}...")

    # Mapping pandas types to Teradata types
    if dtype.kind in ['b', 'i', 'u']:
        print(f"The column '{col_name}' is of integer type in the pandas dataframe.")
        print(f"Converting '{col_name}' into INTEGER type for Teradata.")
        return 'INTEGER'
    if dtype.kind in ['f']:
        print(f"The column '{col_name}' is of float type in the pandas dataframe.")
        print(f"Converting '{col_name}' into DECIMAL(38,2) type for Teradata.")
        return 'DECIMAL(38,2)'
    if dtype.kind in ['O', 'S', 'U']:
        if col_name == 'attachment_exists':
            print(f"The column '{col_name}' needs special handling.")
            print(f"Converting '{col_name}' into INTEGER type for Teradata.")
            return 'INTEGER'
        print(f"The column '{col_name}' is of string-like type in the pandas dataframe.")
        print(f"Converting '{col_name}' into VARCHAR(500) type for Teradata.")
        return 'VARCHAR(500)'
    if dtype.kind in ['M', 'm']:
        print(f"The column '{col_name}' is of datetime-like type in the pandas dataframe.")
        print(f"Converting '{col_name}' into TIMESTAMP(0) type for Teradata.")
        return 'TIMESTAMP(0)'
    raise ValueError(f'Unrecognized type: {dtype.name} for column: {col_name}') 

print("Starting the sync process from Azure PostgreSQL to Teradata...")
load_dotenv()

# Gathering Teradata credentials
print("Step 1: Parsing environment variables and gathering Teradata credentials...")
teradata_info = {
    'host': os.getenv('2023oct10_TERADATA_HOST'), 
    'user': os.getenv('2023oct10_TERADATA_USER'), 
    'password': os.getenv('2023oct10_TERADATA_PASSWORD'), 
    'logmech': os.getenv('2023oct10_TERADATA_LOGMECH')
}

# Checking Postgres table name
postgres_table = os.getenv('2023oct9_AZURE_POSTGRES_DATABASE_INGRESS_TABLE')

# Constructing Postgres connection string
conn_str = "dbname={} user={} password={} host={} port={}".format(
        os.getenv('2023oct9_AZURE_POSTGRES_DATABASE'),
        os.getenv('2023oct9_AZURE_POSTGRES_USER'),
        os.getenv('2023oct9_AZURE_POSTGRES_PASSWORD'),
        os.getenv('2023oct9_AZURE_POSTGRES_HOST'),
        os.getenv('2023oct9_AZURE_POSTGRES_PORT')
)

# Initiating Postgres connection
print(f"Step 2: Initiating connection with Azure PostgreSQL for table: {postgres_table}...")
conn = psycopg2.connect(conn_str)

# Fetching data from Postgres
print("Step 3: Fetching data from PostgreSQL table and storing in a pandas DataFrame...")
query = f"SELECT * FROM {postgres_table}"
df = pd.read_sql_query(query, conn)
print("Fetched data successfully from Azure PostgreSQL.")

print("Step 4: Preprocessing data in the pandas dataframe...")
df['attachment_exists'] = df['attachment_exists'].apply(lambda x: int(x if pd.notnull(x) else 0))
df['creation_timestamp'] = df['creation_timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S').astype(str)
df['timestamp_from_endpoint'] = df['timestamp_from_endpoint'].dt.strftime('%Y-%m-%d %H:%M:%S').astype(str)
df['local_timestamp_from_endpoint'] = df['local_timestamp_from_endpoint'].dt.strftime('%Y-%m-%d %H:%M:%S').astype(str)

# Print dtypes
print("Step 5: Converting the pandas DataFrame column types to Teradata compatible types.")
print(df.dtypes)

print("Step 6: Preparing data for bulk insert...")
data_to_load = [tuple(row) for _, row in df.iterrows()] # prepare data for bulk insert

print("Step 7: Closing the Azure PostgreSQL connection...")
conn.close()

print("Step 8: Connecting to Teradata VantageCloud Lake...")
with teradatasql.connect(json.dumps(teradata_info)) as con:
    print("Successfully connected to Teradata.")
    cur = con.cursor()
        
    print("Step 9: Creating table structure compatible with the pandas DataFrame...")
    columns = ', '.join([f"{col} {convert_to_tera_types(df.dtypes[col], col)}" for col in df.columns])
        
    ft_table_name = 'ft_azure_postgres_bot_invoke_log_table'
    ofs_table_name = 'ofs_azure_postgres_bot_invoke_log_table'
        
    print("Step 10: Removing if any tables exist with the same name...")
    for table in [ft_table_name, ofs_table_name]:
        cur.execute(f"SELECT tablename FROM dbc.tablesV WHERE databasename = 'andy' AND tablename = '{table}';")
        if cur.fetchone():
            print(f"Table {table} exists. Attempting to drop.")
            cur.execute(f"DROP TABLE andy.{table};")
            print(f"Table {table} dropped.")
                
    print(f"\nStep 11: Creating the tables in Teradata VantageCloud Lake...")
    create_ft_table_query = f'CREATE TABLE andy.{ft_table_name} ({columns}) NO PRIMARY INDEX;'
    create_ofs_table_query = f'CREATE TABLE andy.{ofs_table_name} ({columns}) NO PRIMARY INDEX;'

    print(f"Creating Foreign Table (FT) using the following query: \n{create_ft_table_query}\n")
    cur.execute(create_ft_table_query)
    print("Foreign Table created successfully in Teradata.")
    
    print(f"Creating Object File System (OFS) table using the following query: \n{create_ofs_table_query}\n")
    cur.execute(create_ofs_table_query)
    print("Object File System table created successfully in Teradata.")

    print("\nStep 12: Inserting data into the tables...")
    try:
        vals = ', '.join(['?'] * len(df.columns))
        cur.executemany(f"INSERT INTO andy.{ft_table_name} VALUES ({vals})", data_to_load)
        cur.executemany(f"INSERT INTO andy.{ofs_table_name} VALUES ({vals})", data_to_load)
        print(f"Data successfully inserted into tables in Teradata VantageCloud Lake.")
    except teradatasql.DatabaseError as e:
        print("Failure while trying to insert data into Teradata's VantageCloud Lake.")
        print("Error details:", e)

    # Error handling for unexpected exceptions
    # Here we catch all other types of exceptions that are not DatabaseError
    except Exception as e:
        print(f"Unexpected error: {e.args}.")

print("\n####SUMMARY####")
print(f"Total number of rows fetched from PostgreSQL: {len(df)}")
print(f"Total number of rows inserted into Teradata VantageCloud Lake: {len(data_to_load)}")
elapsed_time = time.time() - start_time
print(f"Total operational time: {int(elapsed_time // 60)} minutes and {elapsed_time % 60:.5f} seconds.")
print("####OPERATION COMPLETE. DATA REPLICATED TO TERADATA OFS.####")