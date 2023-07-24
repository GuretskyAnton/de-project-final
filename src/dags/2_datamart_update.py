from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

import vertica_python

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2022, 10, 1),
    "end_date": datetime(2022, 11, 1),
    "retries": 1,
}

def move_data_to_global_metrics(execution_date):
    # Get Vertica connection details from Airflow Connection
    conn_details = BaseHook.get_connection('VERTICA_CONNECTION')

    with vertica_python.connect(
            database='dwh',
            port=conn_details.port,
            user=conn_details.login,
            host=conn_details.host,
            password=conn_details.password
    ) as conn:
        print("Connection to VERTICA successful!")

        # Load data into Vertica global_metrics table
        delete_statement = "DELETE FROM STV2023060656__DWH.global_metrics WHERE date_update::date = %s"

        insert_statement = """
                            INSERT INTO STV2023060656__DWH.global_metrics
                                (
                                    id,
                                    date_update,
                                    currency_from,
                                    amount_total,
                                    cnt_transactions,
                                    avg_transactions_per_account,
                                    cnt_accounts_make_transactions
                            )
                            SELECT
                                HASH(t.transaction_dt::date, t.currency_code) AS id,
                                t.transaction_dt::date AS date_update,
                                t.currency_code AS currency_from,
                                SUM((t.amount * curr_usd.currency_with_div)) AS amount_total,			
                                COUNT(*) AS cnt_transactions,
                                COUNT(*)::numeric / COUNT(DISTINCT t.account_number_from) AS avg_transactions_per_account,
                                COUNT(DISTINCT t.account_number_from) AS cnt_accounts
                            FROM STV2023060656__STAGING.transactions t
                                JOIN (
                                    SELECT 
                                        c.date_update,
                                        c.currency_code,
                                        c.currency_with_div
                                    FROM STV2023060656__STAGING.currencies c
                                    WHERE c.currency_code_with = 420
                                    UNION ALL
                                    SELECT DISTINCT
                                        c.date_update,
                                        420 AS currency_code,
                                        1.0 AS currency_with_div
                                    FROM STV2023060656__STAGING.currencies c
                                ) curr_usd ON t.transaction_dt::date = curr_usd.date_update::date AND t.currency_code = curr_usd.currency_code
                            WHERE 1=1
                                AND t.account_number_from > 0
                                AND t.status = 'done'
                                AND t.transaction_dt::date = %s
                                AND HASH(t.transaction_dt::date, t.currency_code) NOT IN (
                                    SELECT id FROM STV2023060656__DWH.global_metrics WHERE date_update::date = %s
                                )
                            GROUP BY
                                t.transaction_dt::date,
                                t.currency_code;
        """

        with conn.cursor() as cursor:
            # Clear table
            cursor.execute(delete_statement, (execution_date,))

            # Insert into global_metrics
            cursor.execute(insert_statement, (execution_date, execution_date,))

            rows_inserted = cursor.fetchall()

            print(f"Rows inserted: {rows_inserted} for the period {execution_date}.")

            conn.commit()

# Run at 1:00 AM every day
with DAG("2_datamart_update", default_args=default_args, schedule_interval="0 1 * * *") as dag:
    t1 = PythonOperator(
        task_id="move_data_to_global_metrics",
        python_callable=move_data_to_global_metrics,
        provide_context=True,
        op_kwargs={
            'execution_date': "{{ execution_date.date().isoformat() }}",
        }
    )

    t1