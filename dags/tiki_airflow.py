from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
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

# download postgresql jdbc for spark job
download_jar = BashOperator(
    task_id='download_postgresql_jar',
    bash_command='curl -o /usr/local/spark/jars/postgresql-42.2.5.jar https://jdbc.postgresql.org/download/postgresql-42.2.5.jar && chmod 777 /usr/local/spark/jars/postgresql-42.2.5.jar',
    dag = dag
)

# Application Arguments
postgres_url = "jdbc:postgresql://host.docker.internal:5432/test"
postgres_table_input = "product_data"
postgres_username = "postgres"
postgres_pwd = "joshuamellody"
filter_col = "category_id"
filter_value = str(1795)
aggregations = "count(id), sum(reviews_count)"
addition_col = "category_name"
postgres_table_output = "product_agg"

# get data from Postgresql to run Pyspark job
process = SparkSubmitOperator(
    task_id = "spark_process",
    conn_id = "spark-conn",
    application = "jobs/python/tiki_spark_job.py",
    jars = "/usr/local/spark/jars/postgresql-42.2.5.jar",
    driver_class_path = '/usr/local/spark/jars/postgresql-42.2.5.jar',
    verbose = True,
    application_args = [postgres_url, postgres_table_input, postgres_username, postgres_pwd, filter_col, filter_value, aggregations, addition_col, postgres_table_output],
    dag = dag
)


end = PythonOperator(
    task_id = "end",
    python_callable = lambda: print("Job done"),
    dag = dag
)

start >> collect_id >> collect_data >> download_jar >> process >> end
