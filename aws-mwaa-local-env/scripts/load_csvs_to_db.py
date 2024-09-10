import csv
import logging

import pandas as pd

from sqlalchemy.exc import IntegrityError
from scripts.get_postgres_conn import get_postgres_conn
from scripts.get_list_of_csv_filepaths_to_load_to_db import get_list_of_csv_filepaths_to_load_to_db

def load_csvs_to_db(conn_id: str, staging_destination_dir: str, csv_schemas: pd.DataFrame) -> None:
    # list_of_csv_filepaths =  get_list_of_csv_filepaths_to_load_to_db('/tmp/staging')
    list_of_csv_filepaths =  get_list_of_csv_filepaths_to_load_to_db(staging_destination_dir)
    print(f">>> csvs provided: {list_of_csv_filepaths}")
    for csv_filepath in list_of_csv_filepaths:
        csv_filename = csv_filepath.split('/')[-1]
        csv_schema_for_this_csv = csv_schemas[csv_schemas['filename'] == csv_filename]
        table_name_for_this_csv = csv_filename.replace('.csv', '').lower()
        load_csv_to_db(conn_id, csv_filepath, table_name_for_this_csv, csv_schema_for_this_csv)

def load_csv_to_db(conn_id: str, csv_filepath: str, table_name: str, csv_schemas: pd.DataFrame) -> None:
    #  Create table
    df = pd.read_csv(csv_filepath)
    df.columns = [col.lower() for col in df.columns]
    mapping_col_to_dtype = dict(zip(csv_schemas['name_of_column'].str.lower(), csv_schemas['data_type']))

    #  Maintain primary keys to prevent de-duplication
    mapping_col_to_primary_key = dict(zip(csv_schemas['name_of_column'].str.lower(), csv_schemas['primary_key']))
    for (key, value) in mapping_col_to_primary_key.items():
        if value:
            mapping_col_to_dtype[key] += " PRIMARY KEY"

    column_definitions = [f'{col} {mapping_col_to_dtype[col]}' for col in df.columns]
    columns_sql = ', '.join(column_definitions)

    with open('/usr/local/airflow/dags/sql/create_table.sql', 'r') as f:
        sql_template = f.read()
        sql_create_table = sql_template.format(
            table_name=table_name,
            columns=columns_sql
        )

    conn = get_postgres_conn(conn_id).connect()
    conn.execute(sql_create_table)
    logging.info(f"Successfully created table '{table_name}' from '{csv_filepath}'.")

    #  Load non-duplicate data into table.
    try:
        df.to_sql(table_name, conn, if_exists='append', index=False, method='multi')
        logging.info(f"Successfully loaded data into '{table_name}' from '{csv_filepath}'.")
    except IntegrityError as e:
        logging.error(f"IntegrityError: {e}. Duplicate data found. Skipping duplicates.")
    finally:
        conn.close()
