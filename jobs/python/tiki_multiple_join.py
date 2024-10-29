import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Multiple join").getOrCreate()
df_list = []

# input authentication
input_username = sys.argv[1]
input_pwd = sys.argv[2]
# input source
input_rdbms = sys.argv[3]
input_port = sys.argv[4]
input_database = sys.argv[5]

if input_rdbms == "postgresql":
    input_driver = "org.postgresql.Driver"
elif input_rdbms == "mysql":
    input_driver = "com.mysql.cj.jdbc.Driver"

table_numbers = int(sys.argv[6])
for i in range(0, table_numbers):
    input_table = sys.argv[6 + i*2+1]
    input_filter = sys.argv[6 + i*2+2]
    df = spark.read.format("jdbc") \
        .option("url", f"jdbc:{input_rdbms}://host.docker.internal:{input_port}/{input_database}") \
        .option("driver", input_driver) \
        .option("dbtable", input_table) \
        .option("user", input_username) \
        .option("password", input_pwd) \
        .load()
    if input_filter != "":
        df = df.filter(input_filter)
    df_list.append(df)

join_number = int(sys.argv[6 + 2*table_numbers + 1])
for i in range(0, join_number):
    join_atts = sys.argv[6 + 2*table_numbers + 2 + i].split(" ")
    tmp = df_list[int(join_atts[0])].join(
        df_list[int(join_atts[1])],
        on=join_atts[2],
        how=join_atts[3]
    )
    df_list.append(tmp)

# output
output_table = sys.argv[6 + 2*table_numbers+1 + join_number+1]

df_list[-1].write.format("jdbc") \
    .mode("overwrite") \
    .option("url", f"jdbc:{input_rdbms}://host.docker.internal:{input_port}/{input_database}") \
    .option("driver", input_driver) \
    .option("dbtable", output_table) \
    .option("user", input_username) \
    .option("password", input_pwd) \
    .save()

spark.stop()
