from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from tiki_crawler import get_data


dag = DAG(
    dag_id="TikiJobV2",
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
dwh_storing = SparkSubmitOperator(
    task_id="load_warehouse",
    conn_id="spark-conn",
    application="jobs/python/tiki_dwh.py",
    jars="/usr/local/spark/jars/postgresql-42.2.5.jar",
    driver_class_path="/usr/local/spark/jars/postgresql-42.2.5.jar",
    verbose=True,
    dag=dag,
)

"""Join Job Arguments List:
    - input_username
    - input_password
    - input_rdbms
    - input_port
    - input_database
    - input_table1
    - input_filter1
    - input_table2
    - input_filter2
    - join_col
    - join_type
    - output_table
"""

# join product and brand table
join_product_brand = SparkSubmitOperator(
    task_id="join_product_brand",
    conn_id="spark-conn",
    application="jobs/python/tiki_spark_join.py",
    jars="/usr/local/spark/jars/postgresql-42.2.5.jar",
    driver_class_path='/usr/local/spark/jars/postgresql-42.2.5.jar',
    verbose=True,
    application_args=[
        "postgres",             # input_username
        "joshuamellody",        # input_password
        "postgresql",           # input_rdbms
        str(5432),              # input_port
        "test",                 # input_database
        "product",              # input_table1
        "discount_rate > 10",   # input_filter1
        "brand",                # input_table2
        "",                     # input_filter2
        "brand_id",             # join_col
        "inner",                # join_type
        "product_brand_tmp",    # output_table
    ],
    dag=dag,
)

# join product and category table
join_product_category = SparkSubmitOperator(
    task_id="join_product_category",
    conn_id="spark-conn",
    application="jobs/python/tiki_spark_join.py",
    jars="/usr/local/spark/jars/postgresql-42.2.5.jar",
    driver_class_path='/usr/local/spark/jars/postgresql-42.2.5.jar',
    verbose=True,
    application_args=[
        "postgres",                 # input_username
        "joshuamellody",            # input_password
        "postgresql",               # input_rdbms
        str(5432),                  # input_port
        "test",                     # input_database
        "product",                  # input_table1
        "discount_rate > 10",       # input_filter1
        "category",                 # input_table2
        "",                         # input_filter2
        "category_id",              # join_col
        "inner",                    # join_type
        "product_category_tmp",     # output_table
    ],
    dag=dag,
)

"""Aggregation Job Arguments List:
    - input_username
    - input_password
    - input_rdbms
    - input_port
    - input_database
    - input_table
    - filter
    - aggregations
    - group_cols
    - having_condition
    - output_username
    - output_password
    - output_rdbms
    - output_port
    - output_database
    - output_table
"""

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
        "product_brand_tmp",                                    # input_table
        "rating_average > 0",                                   # filter
        "round(avg(rating_average), 2) as average_rating",      # aggregations
        "brand_id, category_id, brand_name",                    # group_cols
        "a",                                                    # having_condition
        "postgres",                                             # output_username
        "joshuamellody",                                        # output_password
        "postgresql",                                           # output_rdbms
        str(5432),                                              # output_port
        "test",                                                 # output_database
        "agg_brand_average_rate",                               # output_table
    ],
    dag=dag,
)

# aggregate number of products each brand
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
        "product_category_tmp",                                 # input_table
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

join_category_brand_average = SparkSubmitOperator(
    task_id="join_category_brand_average",
    conn_id="spark-conn",
    application="jobs/python/tiki_spark_join.py",
    jars="/usr/local/spark/jars/postgresql-42.2.5.jar",
    driver_class_path='/usr/local/spark/jars/postgresql-42.2.5.jar',
    verbose=True,
    application_args=[
        "postgres",                 # input_username
        "joshuamellody",            # input_password
        "postgresql",               # input_rdbms
        str(5432),                  # input_port
        "test",                     # input_database
        "agg_brand_average_rate",   # input_table1
        "average_rating > 4.5",     # input_filter1
        "category",                 # input_table2
        "",                         # input_filter2
        "category_id",              # join_col
        "inner",                    # join_type
        "category_brand_average",   # output_table
    ],
    dag=dag,
)

setup >> collect_data >> dwh_storing >> [
    join_product_brand,
    join_product_category
]

join_product_brand.set_downstream(agg_brand_average_rate)
agg_products_each_category.set_upstream(join_product_category)
join_category_brand_average.set_upstream(
    [dwh_storing, agg_brand_average_rate]
)
