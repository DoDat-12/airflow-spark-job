import sys

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
input_tables = sys.argv[6].split()
join_type = sys.argv[7]
join_expression = sys.argv[8]

output_username = sys.argv[9]
output_pwd = sys.argv[10]

output_rdbms = sys.argv[11]
output_port = sys.argv[12]
output_database = sys.argv[13]
output_table = sys.argv[14]

filter_conditions = sys.argv[15]
aggregations = sys.argv[16]
group_cols = sys.argv[17]
having_conditions = sys.argv[18]

if input_rdbms == "postgresql":
    input_driver = "org.postgresql.Driver"
elif input_rdbms == "mysql":
    input_driver = "com.mysql.cj.jdbc.Driver"
# elif input_rdbms == "sqlite":
#     input_driver = "org.sqlite.JDBC"

table_list = []

for table in input_tables:
    if table == "product":
        product_df = spark.read.format("jdbc") \
                    .option("url", f"jdbc:{input_rdbms}://host.docker.internal:{input_port}/{input_database}") \
                    .option("driver", input_driver) \
                    .option("dbtable", "product") \
                    .option("user", input_username) \
                    .option("password", input_pwd) \
                    .load()
        table_list.append(product_df)
    elif table == "brand":
        brand_df = spark.read.format("jdbc") \
                    .option("url", f"jdbc:{input_rdbms}://host.docker.internal:{input_port}/{input_database}") \
                    .option("driver", input_driver) \
                    .option("dbtable", "brand") \
                    .option("user", input_username) \
                    .option("password", input_pwd) \
                    .load()
        table_list.append(brand_df)
    elif table == "category":
        category_df = spark.read.format("jdbc") \
                    .option("url", f"jdbc:{input_rdbms}://host.docker.internal:{input_port}/{input_database}") \
                    .option("driver", input_driver) \
                    .option("dbtable", "category") \
                    .option("user", input_username) \
                    .option("password", input_pwd) \
                    .load()
        table_list.append(category_df)

df = table_list[0].join(table_list[1], on=join_expression, how=join_type)
# df.show()
df.createOrReplaceTempView("df")

res_df = spark.sql(
    f"""
    SELECT 
        {group_cols}, {aggregations}
    FROM df
    WHERE
        {filter_conditions}
    GROUP BY
        {group_cols}
    HAVING
        {having_conditions}       
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
