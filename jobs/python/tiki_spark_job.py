import sys

from pyspark.sql import SparkSession

# TODO: Fix jar file for postgres
spark = SparkSession \
        .builder \
        .appName("Tiki Data Processing") \
        .getOrCreate()

postgres_url = sys.argv[1]
postgres_table_input = sys.argv[2]
postgres_username = sys.argv[3]
postgres_pwd = sys.argv[4]
filter_col, filter_val = sys.argv[5], sys.argv[6]
aggregations = sys.argv[7]
addition_col = sys.argv[8]
postgres_table_output = sys.argv[9]

df = spark.read.format("jdbc") \
    .option("url", postgres_url) \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", postgres_table_input) \
    .option("user", postgres_username) \
    .option("password", postgres_pwd) \
    .load()

df.createOrReplaceTempView("product_data")

res_df = spark.sql(
    f"""
    SELECT 
        {filter_col}, {addition_col}, {aggregations}
    FROM product_data
    WHERE
        {filter_col} = {filter_val}
    GROUP BY
        {filter_col}, {addition_col};        
    """
)

res_df.show()

# filtered_df.show()

# df.show()

res_df.write.format("jdbc") \
    .mode("overwrite") \
    .option("url", postgres_url) \
    .option("drive", "org.postgresql.Driver") \
    .option("dbtable", postgres_table_output) \
    .option("user", postgres_username) \
    .option("password", postgres_pwd) \
    .save()

spark.stop()
