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

# download postgresql jdbc
download_pos_jar = BashOperator(
    task_id='download_postgresql_jar',
    bash_command = """
        curl -o /usr/local/spark/jars/postgresql-42.2.5.jar https://jdbc.postgresql.org/download/postgresql-42.2.5.jar
        chmod 777 /usr/local/spark/jars/postgresql-42.2.5.jar
    """,
    dag = dag
)

# download sqlite jdbc
download_sqlite_jar = BashOperator(
    task_id="download_sqlite_jar",
    bash_command = """
        curl -o /usr/local/spark/jars/sqlite-jdbc-3.46.1.3.jar https://github.com/xerial/sqlite-jdbc/releases/download/3.46.1.3/sqlite-jdbc-3.46.1.3.jar
        chmod 777 /usr/local/spark/jars/sqlite-jdbc-3.46.1.3.jar
    """,
    dag = dag
)

# download mysql jdbc
download_mysql_jar = BashOperator(
    task_id="download_mysql_jar",
    bash_command="""
        curl -L -o /usr/local/spark/jars/mysql-connector-j-9.0.0.tar.gz https://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-j-9.0.0.tar.gz
        tar -xzf /usr/local/spark/jars/mysql-connector-j-9.0.0.tar.gz -C /usr/local/spark/jars/
        chmod -R 777 /usr/local/spark/jars/mysql-connector-j-9.0.0
    """,
    dag = dag
)

# Application Config
# username & password
postgres_username = "postgres"
postgres_pwd = "joshuamellody"
mysql_username = "root"
mysql_pwd = "joshuamellody"
# source
input_rdbms = "postgresql"  # available for postgresql, mysql, sqlite
input_port = str(5432)
input_database = "test"
input_table = "product_data"

output_rdbms = "postgresql"
output_port = str(5432)
output_database = "test"
output_table = "product_agg"
# filter & aggregation
# TODO: Fix this
filter_col = "category_id"
filter_con = "= 1795"
filter_conditions = "category_id = 1795"
aggregations = "count(id) AS number_of_products, sum(reviews_count) AS number_of_reviews"
group_cols = "category_id, category_name"

# url = "jdbc:postgresql://host.docker.internal:5432/test"

# get data from Postgresql to run Pyspark job
process_postgres = SparkSubmitOperator(
    task_id = "spark_to_postgres",
    conn_id = "spark-conn",
    application = "jobs/python/tiki_spark_job.py",
    jars = "/usr/local/spark/jars/postgresql-42.2.5.jar",
    driver_class_path = '/usr/local/spark/jars/postgresql-42.2.5.jar',
    verbose = True,
    application_args = [
        postgres_username,  # 1
        postgres_pwd,       # 2
        input_rdbms,        # 3
        input_port,         # 4
        input_database,     # 5
        input_table,        # 6
        postgres_username,  # 7
        postgres_pwd,       # 8
        output_rdbms,       # 9
        output_port,        # 10
        output_database,    # 11
        output_table,       # 12
        filter_col,         # 13
        filter_con,         # 14
        aggregations,       # 15
    ],
    dag = dag
)

process_mysql = SparkSubmitOperator(
    task_id = "spark_to_mysql",
    conn_id = "spark-conn",
    application = "jobs/python/tiki_spark_job.py",
    jars = "/usr/local/spark/jars/postgresql-42.2.5.jar,/usr/local/spark/jars/mysql-connector-j-9.0.0/mysql-connector-j-9.0.0.jar",
    # driver_class_path = "/usr/local/spark/jars/mysql-connector-j-8.0.33.jar",
    verbose = True,
    application_args = [
        postgres_username,
        postgres_pwd,
        input_rdbms,
        input_port,
        input_database,
        input_table,
        mysql_username,
        mysql_pwd,
        "mysql",  # output_rdbms
        str(3306),  # output_port
        "test",  # output_database
        "product_data",  # output_table
        filter_col,
        filter_con,
        aggregations,
    ],
    dag = dag
)

process_sqlite = SparkSubmitOperator(
    task_id = "spark_to_sqlite",
    conn_id = "spark-conn",
    application = "jobs/python/tiki_spark_job.py",
    jars = "/usr/local/spark/jars/postgresql-42.2.5.jar,/usr/local/spark/jars/sqlite-jdbc-3.46.1.3.jar",
    verbose = True,
    application_args = [
        postgres_username,
        postgres_pwd,
        input_rdbms,
        input_port,
        input_database,
        input_table,
        "",
        "",
        "sqlite",
        "",
        "",
        "product_data",
        filter_col,
        filter_con,
        aggregations,
    ]
)

join = BashOperator(
    task_id = 'join',
    bash_command='mkdir -p /usr/local/spark/output/sqlite && chmod 777 -R /usr/local/spark/output/sqlite'
)

end = PythonOperator(
    task_id = "end",
    python_callable = lambda: print("Job done"),
    dag = dag
)

start >> collect_id >> collect_data >> [download_pos_jar, download_mysql_jar, download_sqlite_jar] >> join >> [process_postgres, process_mysql, process_sqlite] >> end
