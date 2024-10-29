from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from tiki_crawler import get_data


dag = DAG(
    dag_id="TikiJobV3",
    default_args={
        "owner": "dodat",
        "start_date": days_ago(1),
    },
    schedule_interval="@daily",
)

# download jars file for connecting to database
setup = BashOperator(
    task_id="setup_jars_drivers",
    bash_command="""
        curl -o /usr/local/spark/jars/postgresql-42.2.5.jar https://jdbc.postgresql.org/download/postgresql-42.2.5.jar
        chmod 777 /usr/local/spark/jars/postgresql-42.2.5.jar
        curl -o /usr/local/spark/jars/sqlite-jdbc-3.46.1.3.jar https://github.com/xerial/sqlite-jdbc/releases/download/3.46.1.3/sqlite-jdbc-3.46.1.3.jar
        chmod 777 /usr/local/spark/jars/sqlite-jdbc-3.46.1.3.jar
        curl -L -o /usr/local/spark/jars/mysql-connector-j-9.0.0.tar.gz https://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-j-9.0.0.tar.gz
        tar -xzf /usr/local/spark/jars/mysql-connector-j-9.0.0.tar.gz -C /usr/local/spark/jars/
        chmod -R 777 /usr/local/spark/jars/mysql-connector-j-9.0.0
    """,
    dag=dag,
)

# crawl data and store in parquet files (gzip)
collect_data = PythonOperator(
    task_id="staging",
    python_callable=get_data,
    dag=dag,
)

# raw to warehouse
"""Tables list:
    - product
    - brand
    - category
"""
dwh_storing = SparkSubmitOperator(
    task_id="load_warehouse",
    conn_id="spark-conn",
    application="jobs/python/tiki_dwh.py",
    jars="/usr/local/spark/jars/postgresql-42.2.5.jar",
    driver_class_path="/usr/local/spark/jars/postgresql-42.2.5.jar",
    verbose=True,
    dag=dag,
)

# joining
multiple_join = SparkSubmitOperator(
    task_id="multiple_join",
    conn_id="spark-conn",
    application="jobs/python/tiki_multiple_join.py",
    jars="/usr/local/spark/jars/postgresql-42.2.5.jar",
    driver_class_path='/usr/local/spark/jars/postgresql-42.2.5.jar',
    verbose=True,
    application_args=[
        "postgres",                         # input_username
        "joshuamellody",                    # input_password
        "postgresql",                       # input_rdbms
        str(5432),                          # input_port
        "test",                             # input_database
        str(3),                                     # table_numbers
        "product",                          # input_table0
        "discount_rate > 10",               # input_filter0
        "brand",                            # input_table1
        "",                                 # input_filter1
        "category",                         # input_table2
        "category_id NOT IN (2, 28856)",    # input_filter2
        str(2),                                     # join number
        "0 1 brand_id inner",               # join0
        "3 2 category_id inner",            # join1
        "product_brand_category",                   # output
    ],
    dag=dag,
)

# aggregation
# aggregate average rating for each brand
agg_brand_average_rate = SparkSubmitOperator(
    task_id="agg_brand_average_rate",
    conn_id="spark-conn",
    application="jobs/python/tiki_spark_agg.py",
    jars="/usr/local/spark/jars/postgresql-42.2.5.jar",
    driver_class_path='/usr/local/spark/jars/postgresql-42.2.5.jar',
    verbose=True,
    application_args=[
        "postgres",                                             # input_username
        "joshuamellody",                                        # input_password
        "postgresql",                                           # input_rdbms
        str(5432),                                              # input_port
        "test",                                                 # input_database
        "product_brand_category",                               # input_table
        "rating_average > 0",                                   # filter
        "round(avg(rating_average), 2) as average_rating",      # aggregations
        "brand_id, category_id, brand_name",                    # group_cols
        "",                                                     # having_condition
        "postgres",                                             # output_username
        "joshuamellody",                                        # output_password
        "postgresql",                                           # output_rdbms
        str(5432),                                              # output_port
        "test",                                                 # output_database
        "agg_brand_average_rate",                               # output_table
    ],
    dag=dag,
)

agg_products_each_category = SparkSubmitOperator(
    task_id="agg_products_each_category",
    conn_id="spark-conn",
    application="jobs/python/tiki_spark_agg.py",
    jars="/usr/local/spark/jars/postgresql-42.2.5.jar",
    driver_class_path='/usr/local/spark/jars/postgresql-42.2.5.jar',
    verbose=True,
    application_args=[
        "postgres",                                             # input_username
        "joshuamellody",                                        # input_password
        "postgresql",                                           # input_rdbms
        str(5432),                                              # input_port
        "test",                                                 # input_database
        "product_brand_category",                               # input_table
        "",                                                     # filter
        "count(id) as number_of_products",                      # aggregations
        "category_id, category_name",                           # group_cols
        "",                                                     # having_condition
        "postgres",                                             # output_username
        "joshuamellody",                                        # output_password
        "postgresql",                                           # output_rdbms
        str(5432),                                              # output_port
        "test",                                                 # output_database
        "agg_products_each_category",                           # output_table
    ],
    dag=dag,
)

setup >> collect_data >> dwh_storing >> multiple_join
multiple_join.set_downstream(agg_products_each_category)
multiple_join.set_downstream(agg_brand_average_rate)
