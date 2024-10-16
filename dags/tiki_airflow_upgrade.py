from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.python import PythonOperator

from tiki_crawler_parquet import get_product_data

dag = DAG(
    dag_id="upgraded_tiki_job",
    default_args={
        "owner": "dodat",
        "start_date": days_ago(1)
    },
    schedule_interval="@daily"
)

collect_data = PythonOperator(
    task_id="get_product_data",
    python_callable=get_product_data,
    dag=dag
)

# tasks queue
collect_data