from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

dag = DAG(
    dag_id = "testSparkJob",
    default_args = {
        "owner": "dodat",
        "start_date": days_ago(1)
    },
    schedule_interval = "@daily"
)

start = PythonOperator(
    task_id = "start",
    python_callable = lambda: print("Job started")
)

pyspark_job = SparkSubmitOperator(
    task_id = "pyspark_job",
    conn_id = "spark-conn",
    application = "jobs/python/word_count_job.py",
    dag = dag
)

scala_job = SparkSubmitOperator(
    task_id = "scala_job",
    conn_id = "spark-conn",
    application="jobs/scala/work-count/target/scala-2.12/word-count_2.12-0.1.jar",
    dag = dag
)

end = PythonOperator(
    task_id = "end",
    python_callable = lambda: print("Job done")
)

start >> [pyspark_job, scala_job] >> end
