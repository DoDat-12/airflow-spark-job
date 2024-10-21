import sys

from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Agg Job") \
    .getOrCreate()

# input authentication
input_username = sys.argv[1]
input_password = sys.argv[2]

# input source
input_rdbms = sys.argv[3]
input_port = sys.argv[4]
input_database = sys.argv[5]
input_table = sys.argv[6]

# aggregation
filter = sys.argv[7]
aggregations = sys.argv[8]
group_cols = sys.argv[9]
having_condition = sys.argv[10]

# output authentication
output_username = sys.argv[11]
output_password = sys.argv[12]

# output destination
output_rdbms = sys.argv[13]
output_port = sys.argv[14]
output_database = sys.argv[15]
output_table = sys.argv[16]

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
    .option("password", input_password) \
    .load()

if filter != "":
    df = df.filter(filter)

df.createOrReplaceTempView("df")

res_df = spark.sql(
    f"""
        SELECT {group_cols}, {aggregations}
        FROM df
        GROUP BY {group_cols};
    """
)

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
    .option("password", output_password) \
    .save()

spark.stop()
