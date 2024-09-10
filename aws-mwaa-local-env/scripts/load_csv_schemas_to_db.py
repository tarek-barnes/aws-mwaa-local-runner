import logging
import sqlalchemy

from sqlalchemy.exc import SQLAlchemyError

from scripts.execute_sql_file import execute_sql_file
from scripts.get_postgres_conn import get_postgres_conn

def load_csv_schemas_to_db(conn_id):
    try:
        with get_postgres_conn(conn_id).connect() as conn:
            logging.info("Successfully connected to the database.")

            #  Create CSV Schema Table
            execute_sql_file(conn, '/usr/local/airflow/dags/sql/create_csv_schemas_table.sql')
            logging.info("Successfully created CSV Schema table.")

            #  Load Schemas for raw files into CSV Schema Table
            execute_sql_file(conn, '/usr/local/airflow/dags/sql/load_thrive_takehome_schemas.sql')
            logging.info("Successfully added schemas to CSV Schema table.")
    except FileNotFoundError as e:
        logging.error(f"SQL file not found. Error: {e}")
        raise SQLAlchemyError(f"SQL file not found. Error: {e}")
    except sqlalchemy.exc.DatabaseError as e:
        logging.error(f"Database error: {e}")
        raise SQLAlchemyError(f"Database error: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
