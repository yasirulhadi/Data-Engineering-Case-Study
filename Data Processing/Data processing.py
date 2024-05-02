import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.streaming import StreamingQuery, Trigger

# Define schema for ad impressions
ad_impressions_schema = StructType([
    StructField("ad_creative_id", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("timestamp", TimestampType(), False),
    StructField("website", StringType(), False)
])

# Define schema for clicks and conversions
clicks_conversions_schema = StructType([
    StructField("event_timestamp", TimestampType(), False),
    StructField("user_id", StringType(), False),
    StructField("campaign_id", StringType(), False),
    StructField("conversion_type", StringType(), False)
])

# Read ad impressions stream
ad_impressions_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-broker-1:9092,kafka-broker-2:9092") \
    .option("subscribe", "ad_impressions") \
    .option("startingOffsets", "earliest") \
    .load() \
    .select(F.from_json(F.col("value").cast("string"), ad_impressions_schema).alias("ad_impression")) \
    .select("ad_impression.*")

# Read clicks and conversions stream
clicks_conversions_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-broker-1:9092,kafka-broker-2:9092") \
    .option("subscribe", "clicks,conversions") \
    .option("startingOffsets", "earliest") \
    .load() \
    .select(F.from_json(F.col("value").cast("string"), clicks_conversions_schema).alias("click_conversion")) \
    .select("click_conversion.*")

# Join and correlate ad impressions with clicks and conversions
correlatedStream = ad_impressions_stream.join(
    clicks_conversions_stream,
    (F.col("ad_impression.user_id") == F.col("click_conversion.user_id")) &
    (F.col("ad_impression.timestamp") < F.col("click_conversion.event_timestamp")) &
    (F.col("click_conversion.event_timestamp") < F.col("ad_impression.timestamp") + F.expr("INTERVAL 1 HOUR")),
    "leftOuter"
)

# Write correlated data to data lake
query = correlatedStream \
    .writeStream \
    .format("parquet") \
    .option("path", "s3a://my-bucket/ad_analytics/correlated") \
    .option("checkpointLocation", "s3a://my-bucket/ad_analytics/checkpoint") \
    .trigger(Trigger.ProcessingTime("1 minute")) \
    .start()

query.awaitTermination()