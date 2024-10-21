# A workspace to experiment with Apache Spark and Airflow in a Docker environment

## Prerequisites Setup

- Postgresql (pgAdmin 4)
- MySQL
- Sqlite

Remember to change username, password, database, table with yours

## Quick Setup

Create airflow-spark cluster

    docker compose up -d  
    # need to run twice to start webserver after init db

Access webserver at `localhost:8080/home`, username `admin` password `admin`

Set up Spark connection to Airflow

![spark-conn.png](./doc/spark-conn.png)

## TikiJob

![airflow_tiki_v2.png](./doc/airflow_tiki_v2.png)

## Spark Job Optimization

https://towardsdatascience.com/6-recommendations-for-optimizing-a-spark-job-5899ec269b4b

> `.\env\Scripts\activate`