from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("SocialMediaBatch") \
    .getOrCreate()

# Read posts from MySQL
posts_df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/social_media_db?useSSL=false&allowPublicKeyRetrieval=true") \
    .option("dbtable", "posts") \
    .option("user", "spark_user") \
    .option("password", "sparkpass") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .load()

# Parse hashtags JSON string back to Array<String>
posts_df = posts_df.withColumn("hashtags", from_json(col("hashtags"), ArrayType(StringType())))

# Explode hashtags and compute aggregates
agg_df = posts_df.withColumn("hashtag", explode("hashtags")) \
                 .groupBy(
                     window("post_timestamp", "5 minutes"),
                     "hashtag"
                 ).agg(
                     count("*").alias("post_count"),
                     avg("likes").alias("avg_likes")
                 ) \
                 .withColumn("window_start", col("window.start")) \
                 .withColumn("window_end", col("window.end")) \
                 .drop("window")

# Load streaming aggregates from MySQL
stream_agg_df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/social_media_db?useSSL=false&allowPublicKeyRetrieval=true") \
    .option("dbtable", "hashtag_aggregates") \
    .option("user", "spark_user") \
    .option("password", "sparkpass") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .load() \
    .withColumnRenamed("post_count", "stream_post_count") \
    .withColumnRenamed("avg_likes", "stream_avg_likes")

# Compare results
comparison_df = agg_df.join(
    stream_agg_df,
    ["window_start", "window_end", "hashtag"],
    "inner"
).withColumn(
    "count_diff", col("post_count") - col("stream_post_count")
).withColumn(
    "avg_diff", col("avg_likes") - col("stream_avg_likes")
)

# Show differences
comparison_df.select("window_start", "window_end", "hashtag", "count_diff", "avg_diff").show()

# Count mismatches
mismatch_count = comparison_df.filter((col("count_diff") != 0) | (col("avg_diff") != 0)).count()
print(f"Number of mismatches: {mismatch_count}")

# Stop the Spark session
spark.stop()
