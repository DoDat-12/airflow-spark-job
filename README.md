# A workspace to experiment with Apache Spark and Airflow in a Docker environment

## Prerequisites Setup

- Postgresql (pgAdmin 4)

## Quick Setup

Create airflow-spark cluster

    docker compose up -d  
    # need to run twice to start webserver after init db

Access webserver at `localhost:8080/home`, username `admin` password `admin`

Set up Spark connection to Airflow

![spark-conn.png](./doc/spark-conn.png)