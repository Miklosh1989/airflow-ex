from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from datetime import datetime, timedelta

# Default arguments for DAG
default_args = {
    'owner': 'your_name',
    'depends_on_past': False,
    'start_date': datetime(2023, 2, 25),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1)
}

# DAG definition
dag = DAG('mart_data.marketing_account',
          default_args=default_args,
          description='DAG for merging and writing data to mart_data.marketing_account table',
          schedule_interval='0 3 * * *')  # Runs daily at 3AM server time

# SQL queries to merge and write data to mart_data.marketing_account table
sql_query = """
MERGE INTO mart_data.marketing_account AS target
USING (
  SELECT
    COALESCE(a.Date, b.Date, c.Date Create) AS Date,
    COALESCE(a.Account_Name, b.Account_Name, c.Account_Name) AS Account_Name,
    SUM(a.Cost) AS Google_Ads_Cost,
    SUM(b.Cost) AS Yandex_Direct_Cost,
    COUNT(CASE WHEN c.Stage_of_lead != 'Junk' THEN 1 END) AS CRM_Leads
  FROM
    raw_data.google_ads AS a
    FULL OUTER JOIN raw_data.yandex_direct AS b ON a.Date = b.Date AND a.Account_Name = b.Account_Name
    FULL OUTER JOIN raw_data.crm_deals AS c ON a.Date = c.Date_Create AND a.Account_Name = c.Account_Name
    OR b.Date = c.Date_Create AND b.Account_Name = c.Account_Name
  GROUP BY
    1, 2
  ) AS source
ON target.Date = source.Date AND target.Account_Name = source.Account_Name
WHEN MATCHED THEN UPDATE SET
  target.Google_Ads_Cost = source.Google_Ads_Cost,
  target.Yandex_Direct_Cost = source.Yandex_Direct_Cost,
  target.CRM_Leads = source.CRM_Leads
WHEN NOT MATCHED THEN INSERT (
  Date, Account_Name, Google_Ads_Cost, Yandex_Direct_Cost, CRM_Leads
) VALUES (
  source.Date, source.Account_Name, source.Google_Ads_Cost, source.Yandex_Direct_Cost, source.CRM_Leads
);
"""

# Task to execute SQL query to merge and write data to mart_data.marketing_account table
execute_query = BigQueryExecuteQueryOperator(
    task_id='execute_query',
    sql=sql_query,
    use_legacy_sql=False,
    destination_dataset_table='mart_data.marketing_account',
    write_disposition='WRITE_TRUNCATE',
    time_partitioning={
        'type': 'DAY',
        'expiration_ms': 2592000000  # Sets expiration to 30 days
    },
    dag=dag
)

# Task to indicate the end of DAG
end_task = BashOperator(
    task_id='end_task',
    bash_command='echo "DAG execution completed successfully"',
    dag=dag
)

# Set the execution order of tasks in the DAG
execute_query >> end_task