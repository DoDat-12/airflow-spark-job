from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from tiki_crawler import get_product_id, get_product_data
import psycopg2


# def execute_query_with_psycopg(my_query, **kwargs):
#     """
#     Test Postgresql connection
#     """
#     print(my_query)  # 'value_1'
#     conn_args = dict(
#         host='host.docker.internal',
#         user='postgres',
#         password='joshuamellody',
#         dbname='test',
#         port=5432)
#     conn = psycopg2.connect(**conn_args)
#     cur = conn.cursor()
#     cur.execute(my_query)

#     for row in cur:
#         print(row)


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

dwh_storing = SparkSubmitOperator(
    task_id = "dwh_stuff",
    conn_id = "spark-conn",
    application = "jobs/python/tiki_fake_dwh.py",
    jars = "/usr/local/spark/jars/postgresql-42.2.5.jar",
    driver_class_path = "/usr/local/spark/jars/postgresql-42.2.5.jar",
    verbose = True,
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

input_table1 = "brand"
filter1 = ""
input_table2 = "product"
filter2 = "discount_rate > 10"
join_type = "inner"
join_expression = "brand_id"

output_rdbms = "postgresql"
output_port = str(5432)
output_database = "test"

output_table1 = "product_count_brand"
aggregation1 = "count(id) AS number_of_products"
group_col1 = "brand_id, brand_name"
having_condition1 = "count(id) > 5"

output_table2 = "product_sum_brand"
aggregation2 = "sum(reviews_count) AS number_of_reviews"
group_col2 = "brand_id, brand_name"
having_condition2 = "sum(reviews_count) <> 0"

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
        input_table1,       # 6
        filter1,            # 7 (optional)
        input_table2,       # 8 (optional)
        filter2,            # 9 (optional)
        join_type,          # 10 (optional)
        join_expression,    # 11 (optional)
        postgres_username,  # 12
        postgres_pwd,       # 13
        output_rdbms,       # 14
        output_port,        # 15
        output_database,    # 16
        output_table1,      # 17
        aggregation1,       # 18
        group_col1,         # 19
        having_condition1,  # 20
        output_table2,      # 21
        aggregation2,       # 22
        group_col2,         # 23
        having_condition2,  # 24
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
        input_table1,
        filter1, 
        input_table2,
        filter2,
        join_type,
        join_expression,
        mysql_username,
        mysql_pwd,
        "mysql",  # output_rdbms
        str(3306),  # output_port
        "test",  # output_database
        output_table1,    
        aggregation1,     
        group_col1,       
        having_condition1,
        output_table2,    
        aggregation2,     
        group_col2,       
        having_condition2,
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
        input_table1,
        filter1,
        input_table2,
        filter2,
        join_type,
        join_expression,
        "",                 # no need username
        "",                 # no need password
        "sqlite",
        "",                 # no need port
        "",                 # no need database
        output_table1,      # 17
        aggregation1,       # 18
        group_col1,         # 19
        having_condition1,  # 20
        output_table2,      # 21
        aggregation2,       # 22
        group_col2,         # 23
        having_condition2,  # 24
    ],
    dag = dag
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

start >> collect_id >> collect_data >> [download_pos_jar, download_mysql_jar, download_sqlite_jar] >> join >> dwh_storing >> [process_postgres, process_mysql, process_sqlite] >> end
# start >> process_postgres >> end
