from pyspark.sql import SparkSession

spark = SparkSession \
        .builder \
        .appName("Tiki Data Processing") \
        .config("spark.jars", "/opt/bitnami/spark/jobs/postgresql-42.2.5.jar") \
        .getOrCreate()

# info from conf
url = spark.conf.get("io_url")
input_table = spark.conf.get("input_table")
output_table = spark.conf.get("output_table")
username = spark.conf.get("username")
password = spark.conf.get("password")
filter_column = spark.conf.get("category_id")
filter_value = spark.conf.get("filter_value")
agg_function = spark.conf.get("agg_function")  # list


df = spark.read.format("jdbc") \
    .option("url", url) \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", input_table) \
    .option("user", username) \
    .option("password", password) \
    .load()

filtered_df = df.filter(f"{filter_column} = {filter_value}")

# res_df = 

filtered_df.show()
