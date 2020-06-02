# Send data from kafka to google cloud storage

from pyspark import SparkConf,SparkContext 
from pyspark.sql import SparkSession,SQLContext 
from pyspark.sql.functions import *
from pyspark.sql.types import * 
import time 
import datetime 

conf = SparkConf(). \
setAppName("Streaming Data"). \
setMaster("yarn-client")

#Setup a Spark Session
sc = SparkContext(conf=conf)
sqlcontext = SQLContext(sc)
spark = SparkSession \
        .builder \
        .appName("webevent-analysis") \
        .getOrCreate()

# Read data from the Kafka Topic
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers","cluster-e977-w-0:9092") \
    .option("subscribe","webevent") \
    .load() \
    .selectExpr("CAST(value as STRING)")

# Create a DataFrame From the Data
df_parsed = df.select("value")
schema = StructType(
                [
                
                StructField('timestamp', TimestampType(), True),
                StructField('lastPageVisited', StringType(), True),
                StructField('pageUrl', StringType(), True),
                StructField('eventType', StringType(), True),
                StructField('uiud', StringType(), True),
                StructField('sessionId', StringType(), True),
                StructField('referrer', StringType(), True),
                StructField('landingPage', StringType(), True),
                ]
        )

df_streaming_visits = df_parsed.withColumn("data", from_json("value",schema)).select(col('data.*'))

# Add Watermark for late data and apply aggreagations to the dataframe
df_pageviews  = df_streaming_visits    \
       .withWatermark(df_streaming_visits.timestamp, "200 seconds")
                                .groupBy(df_streaming_visits.uiud, df_streaming_visits.sessionId, window(df_streaming_visits.timestamp), "1800 seconds")        \
                                .agg(             \
                             collect_list(df_streaming_visits.pageUrl).alias("Pages"),
                               collect_list(df_streaming_visits.referrer).alias("referrer"),
                                collect_list(df_streaming_visits.landingPage).alias("LandingPageCode"),
                                collect_list(df_streaming_visits.timestamp).alias("sessionTimestamps"),
                                count(df_streaming_visits.eventType).alias("Pageviews")
                                        )

# For each step we trigger this fnction which add data to 
def foreach_batch_function(df, epoch_id):
    df.show()
    df.coalesce(1).write \
    .format("avro") \
    .mode("append") \
    .option("checkpointLocation",bucket_name+"/spark-agg-checkpoints/") \
    .option("path",bucket_name+"/stateless_aggregations/") \
    .save()

query = ( df_pageviews.writeStream.trigger(processingTime = "180 seconds")  
          .foreachBatch(foreach_batch_function)
          .outputMode("append") 
          .format("console") 
          .start()
          )
query.awaitTermination() 
