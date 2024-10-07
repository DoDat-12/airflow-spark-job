from pyspark.sql import SparkSession

# TODO: Fix jar file for postgres
spark = SparkSession \
        .builder \
        .appName("Tiki Data Processing") \
        .config("spark.jars", "/opt/bitnami/spark/jars/jars/postgresql-42.2.5.jar") \
        .getOrCreate()

# TODO: Fixing conf
# info from conf
# url = spark.conf.get("io_url")
# input_table = spark.conf.get("input_table")
# output_table = spark.conf.get("output_table")
# username = spark.conf.get("username")
# password = spark.conf.get("password")
# filter_column = spark.conf.get("category_id")
# filter_value = spark.conf.get("filter_value")
# agg_function = spark.conf.get("agg_function")


df = spark.read.format("jdbc") \
    .option("url", 'jdbc:postgresql://host.docker.internal:5432/test') \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", 'product_data') \
    .option("user", 'postgres') \
    .option("password", 'joshuamellody') \
    .load()

df.show()

spark.stop()
