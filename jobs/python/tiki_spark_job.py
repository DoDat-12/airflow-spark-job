import sys
import time

from pyspark.sql import SparkSession

# TODO: Fix jar file for postgres
spark = SparkSession \
        .builder \
        .appName("Tiki Data Processing") \
        .getOrCreate()

input_username = sys.argv[1]
input_pwd = sys.argv[2]

input_rdbms = sys.argv[3]
input_port = sys.argv[4]
input_database = sys.argv[5]

input_table1 = sys.argv[6]
filter1 = sys.argv[7]
input_table2 = sys.argv[8]
filter2 = sys.argv[9]
join_type = sys.argv[10]
join_expression = sys.argv[11]

output_username = sys.argv[12]
output_pwd = sys.argv[13]

output_rdbms = sys.argv[14]
output_port = sys.argv[15]
output_database = sys.argv[16]

output_table1 = sys.argv[17]
aggregation1 = sys.argv[18]
group_col1 = sys.argv[19]
having_condition1 = sys.argv[20]

output_table2 = sys.argv[21]
aggregation2 = sys.argv[22]
group_col2 = sys.argv[23]
having_condition2 = sys.argv[24]

if input_rdbms == "postgresql":
    input_driver = "org.postgresql.Driver"
elif input_rdbms == "mysql":
    input_driver = "com.mysql.cj.jdbc.Driver"
# elif input_rdbms == "sqlite":
#     input_driver = "org.sqlite.JDBC"

df1 = spark.read.format("jdbc") \
    .option("url", f"jdbc:{input_rdbms}://host.docker.internal:{input_port}/{input_database}") \
    .option("driver", input_driver) \
    .option("dbtable", input_table1) \
    .option("user", input_username) \
    .option("password", input_pwd) \
    .option("partitionColumn", join_expression) \
    .option("lowerBound", "15000") \
    .option("upperBound", "200000") \
    .option("numPartitions", "3") \
    .load()

df2 = spark.read.format("jdbc") \
    .option("url", f"jdbc:{input_rdbms}://host.docker.internal:{input_port}/{input_database}") \
    .option("driver", input_driver) \
    .option("dbtable", input_table2) \
    .option("user", input_username) \
    .option("password", input_pwd) \
    .option("partitionColumn", join_expression) \
    .option("lowerBound", "15000") \
    .option("upperBound", "200000") \
    .option("numPartitions", "3") \
    .load()

start_time = time.time()
if filter1 != "":
    df1 = df1.filter(filter1)

if filter2 != "":
    df2 = df2.filter(filter2)

df = df1.join(df2, on=join_expression, how=join_type)
# df.cache()
# df.show()
df.createOrReplaceTempView("df")
# spark.table("df").cache()

res_df1 = spark.sql(
    f"""
    SELECT 
        {group_col1}, {aggregation1}
    FROM df
    GROUP BY
        {group_col1}
    HAVING
        {having_condition1}       
    """
)

res_df2 = spark.sql(
    f"""
    SELECT 
        {group_col2}, {aggregation2}
    FROM df
    GROUP BY
        {group_col2}
    HAVING
        {having_condition2}       
    """
)

# res_df.show()
# filtered_df.show()
# df.show()

if output_rdbms == "sqlite":
    res_df1.write.format("jdbc") \
        .mode("overwrite") \
        .option("url", "jdbc:sqlite:/usr/local/spark/output/sqlite/product_data.db") \
        .option("dbtable", output_table1) \
        .option("driver", "org.sqlite.JDBC") \
        .save
    res_df2.write.format("jdbc") \
        .mode("overwrite") \
        .option("url", "jdbc:sqlite:/usr/local/spark/output/sqlite/product_data.db") \
        .option("dbtable", output_table2) \
        .option("driver", "org.sqlite.JDBC") \
        .save

else:
    if output_rdbms == "postgresql":
        output_driver = "org.postgresql.Driver"
    elif output_rdbms == "mysql":
        output_driver = "com.mysql.cj.jdbc.Driver"

    res_df1.write.format("jdbc") \
        .mode("overwrite") \
        .option("url", f"jdbc:{output_rdbms}://host.docker.internal:{output_port}/{output_database}") \
        .option("driver", output_driver) \
        .option("dbtable", output_table1) \
        .option("user", output_username) \
        .option("password", output_pwd) \
        .save()

    res_df2.write.format("jdbc") \
        .mode("overwrite") \
        .option("url", f"jdbc:{output_rdbms}://host.docker.internal:{output_port}/{output_database}") \
        .option("driver", output_driver) \
        .option("dbtable", output_table2) \
        .option("user", output_username) \
        .option("password", output_pwd) \
        .save()

print(f"Total time taken: {time.time() - start_time} seconds")
# spark.table("df").unpersist()
# df.unpersist()
spark.stop()
