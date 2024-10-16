FROM apache/airflow:latest-python3.12

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless && \
    apt-get autoremove -yqq --purge && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64

USER airflow

# Install dependencies
RUN pip install apache-airflow 
RUN pip install apache-airflow-providers-apache-spark 
RUN pip install pyspark 
RUN pip install pandas 
RUN pip install requests 
RUN pip install apache-airflow-providers-postgres 
RUN pip install pyarrow
