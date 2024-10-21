import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Join Job").getOrCreate()

# input authentication
input_username = sys.argv[1]
input_pwd = sys.argv[2]

# input source
input_rdbms = sys.argv[3]
input_port = sys.argv[4]
input_database = sys.argv[5]
input_table1 = sys.argv[6]
input_filter1 = sys.argv[7]
input_table2 = sys.argv[8]
input_filter2 = sys.argv[9]
join_col = sys.argv[10]
join_type = sys.argv[11]

# output authentication
output_table = sys.argv[12]

if input_rdbms == "postgresql":
    input_driver = "org.postgresql.Driver"
elif input_rdbms == "mysql":
    input_driver = "com.mysql.cj.jdbc.Driver"

df1 = spark.read.format("jdbc") \
    .option("url", f"jdbc:{input_rdbms}://host.docker.internal:{input_port}/{input_database}") \
    .option("driver", input_driver) \
    .option("dbtable", input_table1) \
    .option("user", input_username) \
    .option("password", input_pwd) \
    .load()

df2 = spark.read.format("jdbc") \
    .option("url", f"jdbc:{input_rdbms}://host.docker.internal:{input_port}/{input_database}") \
    .option("driver", input_driver) \
    .option("dbtable", input_table2) \
    .option("user", input_username) \
    .option("password", input_pwd) \
    .load()

if input_filter1 != "":
    df1 = df1.filter(input_filter1)
if input_filter2 != "":
    df2 = df2.filter(input_filter2)

df = df1.join(df2, on=join_col, how=join_type)

df.write.format("jdbc") \
    .mode("overwrite") \
    .option("url", f"jdbc:{input_rdbms}://host.docker.internal:{input_port}/{input_database}") \
    .option("driver", input_driver) \
    .option("dbtable", output_table) \
    .option("user", input_username) \
    .option("password", input_pwd) \
    .save()

df.show(10, truncate=True)
spark.stop()
