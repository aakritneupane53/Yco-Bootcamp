
from datetime import datetime
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


def check_snowflake_connection():

    # Connect using the Airflow connection
    hook = SnowflakeHook(
        snowflake_conn_id="snowflake_default"
    )

    # Execute a test query
    query = """
        SELECT
            CURRENT_USER(),
            CURRENT_ROLE(),
            CURRENT_DATABASE(),
            CURRENT_SCHEMA(),
            CURRENT_WAREHOUSE(),
            CURRENT_VERSION()
    """

    result = hook.get_first(query)

    # Display connection details in task logs
    logging.info("Snowflake connection successful!")
    logging.info("Username: %s", result[0])
    logging.info("Role: %s", result[1])
    logging.info("Database: %s", result[2])
    logging.info("Schema: %s", result[3])
    logging.info("Warehouse: %s", result[4])
    logging.info("Snowflake Version: %s", result[5])


with DAG(
    dag_id="snowflake_connection_check",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["snowflake", "connection-test"],
) as dag:

    test_connection = PythonOperator(
        task_id="test_snowflake_connection",
        python_callable=check_snowflake_connection,
    )
