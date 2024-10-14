from pyspark.sql import SparkSession

# TODO: Fix jar file for postgres
spark = SparkSession \
        .builder \
        .appName("Tiki Storing") \
        .getOrCreate()

df = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://host.docker.internal:5432/test") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "product_data") \
    .option("user", "postgres") \
    .option("password", "joshuamellody") \
    .load()

product_df = df.select("id", 
                       "sku", 
                       "name", 
                       "short_description", 
                       "price", 
                       "original_price", 
                       "discount", 
                       "discount_rate", 
                       "rating_average", 
                       "reviews_count", 
                       "inventory_status", 
                       "brand_id", 
                       "category_id")

brand_df = df.select("brand_id", "brand_name") \
            .distinct()

category_df = df.select("category_id", "category_name") \
            .distinct()

product_df.write.format("jdbc") \
    .mode("overwrite") \
    .option("url", "jdbc:postgresql://host.docker.internal:5432/test") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "product") \
    .option("user", "postgres") \
    .option("password", "joshuamellody") \
    .save()

brand_df.write.format("jdbc") \
    .mode("overwrite") \
    .option("url", "jdbc:postgresql://host.docker.internal:5432/test") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "brand") \
    .option("user", "postgres") \
    .option("password", "joshuamellody") \
    .save()

category_df.write.format("jdbc") \
    .mode("overwrite") \
    .option("url", "jdbc:postgresql://host.docker.internal:5432/test") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "category") \
    .option("user", "postgres") \
    .option("password", "joshuamellody") \
    .save()