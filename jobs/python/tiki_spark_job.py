import sys

from pyspark.sql import SparkSession

# TODO: Fix jar file for postgres
spark = SparkSession \
        .builder \
        .appName("Tiki Data Processing") \
        .getOrCreate()

input_username = sys.argv[1]
input_pwd = sys.argv[2]
# source
input_rdbms = sys.argv[3]
input_port = sys.argv[4]
input_database = sys.argv[5]
input_table = sys.argv[6]

output_username = sys.argv[7]
output_pwd = sys.argv[8]
output_rdbms = sys.argv[9]
output_port = sys.argv[10]
output_database = sys.argv[11]
output_table = sys.argv[12]
# filter & aggregation
filter_col = sys.argv[13]
filter_con = sys.argv[14]
aggregations = sys.argv[15]

if input_rdbms == "postgresql":
    input_driver = "org.postgresql.Driver"
elif input_rdbms == "mysql":
    input_driver = "com.mysql.cj.jdbc.Driver"
# elif input_rdbms == "sqlite":
#     input_driver = "org.sqlite.JDBC"


df = spark.read.format("jdbc") \
    .option("url", f"jdbc:{input_rdbms}://host.docker.internal:{input_port}/{input_database}") \
    .option("driver", input_driver) \
    .option("dbtable", input_table) \
    .option("user", input_username) \
    .option("password", input_pwd) \
    .load()

df.createOrReplaceTempView("product_data")

res_df = spark.sql(
    f"""
    SELECT 
        {filter_col}, {aggregations}
    FROM {input_table}
    WHERE
        {filter_col} {filter_con}
    GROUP BY
        {filter_col};        
    """
)

# res_df.show()
# filtered_df.show()
# df.show()

if output_rdbms == "sqlite":
    res_df.write.format("jdbc") \
        .mode("overwrite") \
        .option("url", "jdbc:sqlite:/usr/local/spark/output/sqlite/product_data.db") \
        .option("dbtable", output_table) \
        .option("driver", "org.sqlite.JDBC") \
        .save

else:
    if output_rdbms == "postgresql":
        output_driver = "org.postgresql.Driver"
    elif output_rdbms == "mysql":
        output_driver = "com.mysql.cj.jdbc.Driver"


    res_df.write.format("jdbc") \
        .mode("overwrite") \
        .option("url", f"jdbc:{output_rdbms}://host.docker.internal:{output_port}/{output_database}") \
        .option("driver", output_driver) \
        .option("dbtable", output_table) \
        .option("user", output_username) \
        .option("password", output_pwd) \
        .save()

spark.stop()
