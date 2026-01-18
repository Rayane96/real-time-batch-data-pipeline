from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, FloatType, LongType, TimestampType
)
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import os

spark = SparkSession.builder \
    .appName("ClimateStreamingConsumer") \
    .config("spark.sql.streaming.fileSource.log.compactFileStatusCache", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("type", StringType(), True),
    StructField("subreddit", StringType(), True),
    StructField("title", StringType(), True),
    StructField("text", StringType(), True),
    StructField("created_utc", LongType(), True),
    StructField("score", IntegerType(), True)
])

input_path = os.path.join(os.path.expanduser("~"), "StreamingProject", "stream_in")

stream_df = spark.readStream \
    .schema(schema) \
    .option("maxFilesPerTrigger", 1) \
    .json(input_path)

# FIX: Prevent dropped rows
stream_df = stream_df.fillna({"title": "", "text": ""})

# Convert timestamp
stream_df = stream_df.withColumn(
    "time",
    F.from_unixtime(F.col("created_utc")).cast(TimestampType())
)

# Sentiment UDF
analyzer = SentimentIntensityAnalyzer()

def get_sent(text):
    text = text or ""
    return float(analyzer.polarity_scores(text)["compound"])

sentiment_udf = F.udf(get_sent, FloatType())

sent_df = stream_df.withColumn("vader_score", sentiment_udf(F.col("text")))

# Label sentiment
sent_df = sent_df.withColumn(
    "sentiment",
    F.when(F.col("vader_score") > 0.05, "positive")
     .when(F.col("vader_score") < -0.05, "negative")
     .otherwise("neutral")
)

output_path = os.path.join(os.path.expanduser("~"), "StreamingProject", "stream_out")
checkpoint_path = os.path.join(os.path.expanduser("~"), "StreamingProject", "checkpoint")

query = sent_df.writeStream \
    .trigger(processingTime="10 seconds") \
    .outputMode("append") \
    .format("parquet") \
    .option("path", output_path) \
    .option("checkpointLocation", checkpoint_path) \
    .start()

print("🔥 Spark Streaming FIXED and running")
print("Reading:", input_path)
print("Writing:", output_path)

query.awaitTermination()
