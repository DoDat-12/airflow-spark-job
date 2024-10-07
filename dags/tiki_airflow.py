from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from tiki_crawler import get_product_id, get_product_data
import psycopg2


def execute_query_with_psycopg(my_query, **kwargs):
    """
    Test Postgresql connection
    """
    print(my_query)  # 'value_1'
    conn_args = dict(
        host='host.docker.internal',
        user='postgres',
        password='joshuamellody',
        dbname='test',
        port=5432)
    conn = psycopg2.connect(**conn_args)
    cur = conn.cursor()
    cur.execute(my_query)

    for row in cur:
        print(row)


dag = DAG(
    dag_id = "TikiJob",
    default_args = {
        "owner": "dodat",
        "start_date": days_ago(1)
    },
    schedule_interval = "@daily"
)

# start pipeline
start = PythonOperator(
    task_id = "start",
    python_callable = lambda: print("Job started"),
    dag = dag
)

# test postgresql connection
# check_postgresql = PythonOperator(
#     task_id="test_postgresql",
#     python_callable=execute_query_with_psycopg,
#     op_kwargs={"my_query": 'select 1'},
#     dag = dag
# )

# use tiki api to get product id
collect_id = PythonOperator(
    task_id = "get_product_id",
    python_callable = get_product_id,
    dag = dag
)

# from product id use tiki api to get product information, store in local Postgresql
collect_data = PythonOperator(
    task_id = "get_product_data",
    python_callable = get_product_data,
    dag = dag
)

# get data from Postgresql to run Pyspark job
process = SparkSubmitOperator(
    task_id = "spark_process",
    conn_id = "spark-conn",
    application = "jobs/python/tiki_spark_job.py",
    # TODO: Fix conf
    # conf = {
    #     'io_url': 'jdbc:postgresql://host.docker.internal:5432/test',
    #     'input_table': 'product_data',
    #     'output_table': 'product_agg',
    #     'username': 'postgres',
    #     'password': 'joshuamellody',
    #     'filter_column': 'category_id',
    #     'filter_value': 1795,  # smartphone
    #     'agg_function': ['count', 'sum', 'distinct']
    # },
    dag = dag
)


end = PythonOperator(
    task_id = "end",
    python_callable = lambda: print("Job done"),
    dag = dag
)

start >> collect_id >> collect_data >> process >> end
