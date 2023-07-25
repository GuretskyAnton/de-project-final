from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import datetime

import psycopg2
import vertica_python
import pandas as pd

import logging

# Create a logger
logger = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2022, 10, 1),
    "end_date": datetime(2022, 10, 31),
    "retries": 1,
}

def move_data_pg_to_vertica(execut_date, table, column_dt):
    # Get PostgreSQL/Vertica connection details from Airflow Connection
    pg_conn = BaseHook.get_connection('PG_WAREHOUSE_CONNECTION')
    vr_conn = BaseHook.get_connection('VERTICA_CONNECTION')

    try:
        # Connect to PostgreSQL
        with psycopg2.connect(
                dbname='db1',
                port=pg_conn.port,
                user=pg_conn.login,
                host=pg_conn.host,
                password=pg_conn.password
        ) as pg_conn:
            logger.info("Connection to PG successful!")
            
            # Execute SELECT statement for current period
            with pg_conn.cursor() as cursor:
                query = f"SELECT * FROM {table} WHERE {column_dt}::date = %s"
                cursor.execute(query, (execut_date,))

                rows = cursor.fetchall()
                logger.info(f"Retrieved {len(rows)} rows from {table} table in PG for {execut_date}.")

            # Create dataframe from retrieved data
            df = pd.DataFrame(rows, columns=[desc[0] for desc in cursor.description])

            if not df.empty:
                
                try:
                    # Connect to Vertica
                    with vertica_python.connect(
                            database='dwh',
                            port=vr_conn.port,
                            user=vr_conn.login,
                            host=vr_conn.host,
                            password=vr_conn.password
                    ) as vr_conn:
                        logging.info("Connection to VERTICA successful!")

                        # Load data into Vertica table
                        delete_statement = f"DELETE FROM STV2023060656__STAGING.{table} WHERE {column_dt}::date = %s"

                        copy_statement = f"""
                                            COPY STV2023060656__STAGING.{table}
                                            FROM stdin DELIMITER ','
                                            ENFORCELENGTH
                                            REJECTED DATA AS TABLE STV2023060656__STAGING.{table}_rej;
                        """

                        num_rows = len(df)
                        chunk_size = num_rows // 100 + 1
                        logger.info(f'Chunk_size is {chunk_size}')

                        with vr_conn.cursor() as cursor:

                            # Clear table
                            cursor.execute(delete_statement, (execut_date,))

                            start = 0
                            while start <= num_rows:
                                end = min(start + chunk_size, num_rows)
                                logger.info(f"loading rows {start}-{end}")
                                df.loc[start: end].to_csv('/lessons/data/data_raw.csv', index=False, header=False, sep=',')
                                with open('/lessons/data/data_raw.csv', 'rb') as chunk:
                                    cursor.copy(copy_statement, chunk, buffer_size=65536)

                                vr_conn.commit()
                                logger.info("loaded")
                                start += chunk_size + 1
                except vertica_python.Error as e:
                    logger.error(f"Error connecting to Vertica: {e}")
            else:
                logger.info('No rows to load')
    except psycopg2.Error as e:
        logger.error(f"Error connecting to PG: {e}")

# Run at midnight every day
with DAG("1_data_import", default_args=default_args, schedule_interval="0 0 * * *") as dag:
    t1 = PythonOperator(
        task_id="move_transactions_pg_to_vertica",
        python_callable=move_data_pg_to_vertica,
        provide_context=True,
        op_kwargs={
            'execut_date': "{{ execution_date.date().isoformat() }}",
            'table': "transactions",
            'column_dt': "transaction_dt",
        }
    )

    t2 = PythonOperator(
        task_id="move_currencies_pg_to_vertica",
        python_callable=move_data_pg_to_vertica,
        provide_context=True,
        op_kwargs={
            'execut_date': "{{ execution_date.date().isoformat() }}",
            'table': "currencies",
            'column_dt': "date_update"
        }
    )

    t1 >> t2