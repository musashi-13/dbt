from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("SocialMediaStreaming") \
    .getOrCreate()

# Define schema matching producer
schema = StructType([
    StructField("post_id", StringType()),
    StructField("user_id", StringType()),
    StructField("content", StringType()),
    StructField("hashtags", ArrayType(StringType())),
    StructField("post_timestamp", StringType()),
    StructField("likes", IntegerType())
])

# Read from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "top1,top2,top3") \
    .option("startingOffsets", "earliest") \
    .option("group.id", "spark-consumer")  \
    .option("failOnDataLoss", "false") \
    .load()

# Parse JSON from Kafka value
df = df.selectExpr("CAST(value AS STRING) as json") \
       .select(from_json("json", schema).alias("data")) \
       .select("data.*") \
       .withColumn("post_timestamp", to_timestamp("post_timestamp"))

# Create checkpoint directories
checkpoint_dir = "/home/musashi/dbt_project/checkpoint"
os.makedirs(checkpoint_dir, exist_ok=True)

# Write raw posts to MySQL
def write_posts_to_mysql(batch_df, batch_id):
    # before writing, turn the ARRAY<string> into a JSON string
    df_to_write = batch_df.withColumn("hashtags", to_json(col("hashtags")))
    
    # Print schema and sample data for debugging
    print("DataFrame Schema:")
    df_to_write.printSchema()
    print("Sample Data:")
    df_to_write.show(5, truncate=False)
    
    df_to_write.write \
        .format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/social_media_db?useSSL=false&allowPublicKeyRetrieval=true") \
        .option("dbtable", "posts") \
        .option("user", "spark_user") \
        .option("password", "sparkpass") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .mode("append") \
        .save()

query_raw = df.writeStream \
    .foreachBatch(write_posts_to_mysql) \
    .option("checkpointLocation", f"{checkpoint_dir}/raw") \
    .start()

# Compute aggregates: explode hashtags, add watermark, and group by 5-minute windows
df_agg = df.withWatermark("post_timestamp", "10 minutes") \
           .withColumn("hashtag", explode("hashtags")) \
           .groupBy(
               window("post_timestamp", "5 minutes"),
               "hashtag"
           ).agg(
               count("*").alias("post_count"),
               avg("likes").alias("avg_likes")
           )

# Write aggregates to MySQL
def write_agg_to_mysql(batch_df, batch_id):
    # Print schema and sample data for debugging
    print("Aggregate DataFrame Schema:")
    batch_df.printSchema()
    print("Aggregate Sample Data:")
    batch_df.show(5, truncate=False)
    
    batch_df_with_time = batch_df.withColumn("window_start", col("window.start")) \
                                 .withColumn("window_end", col("window.end")) \
                                 .drop("window")
    batch_df_with_time.write \
        .format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/social_media_db?useSSL=false") \
        .option("dbtable", "hashtag_aggregates") \
        .option("user", "spark_user") \
        .option("password", "sparkpass") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .mode("append") \
        .save()

query_agg = df_agg.writeStream \
    .foreachBatch(write_agg_to_mysql) \
    .outputMode("append") \
    .option("checkpointLocation", f"{checkpoint_dir}/agg") \
    .start()

# User activity aggregation
user_agg = df.withWatermark("post_timestamp", "10 minutes") \
             .groupBy(
                 window("post_timestamp", "5 minutes"),
                 "user_id"
             ).agg(
                 count("*").alias("post_count"),
                 sum("likes").alias("total_likes")
             )

# Write user activity to MySQL
def write_user_agg_to_mysql(batch_df, batch_id):
    batch_df_with_time = batch_df.withColumn("window_start", col("window.start")) \
                                 .withColumn("window_end", col("window.end")) \
                                 .drop("window")
    batch_df_with_time.write \
        .format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/social_media_db?useSSL=false") \
        .option("dbtable", "user_activity") \
        .option("user", "spark_user") \
        .option("password", "sparkpass") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .mode("append") \
        .save()

query_user_agg = user_agg.writeStream \
    .foreachBatch(write_user_agg_to_mysql) \
    .outputMode("append") \
    .option("checkpointLocation", f"{checkpoint_dir}/user_agg") \
    .start()

# Run all queries concurrently
try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    query_raw.stop()
    query_agg.stop()
    query_user_agg.stop()
    print("Stopped streaming queries")
