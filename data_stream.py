import findspark
findspark.init('/home/yannick/dev/spark-2.4.4-bin-hadoop2.7')

import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf


# Create a schema for incoming resources
schema = StructType([
    StructField("crime_id", IntegerType(), True),
    StructField("original_crime_type_name", TimestampType(), True),
    StructField("report_date", TimestampType(), True),
    StructField("call_date", TimestampType(), True),
    StructField("offense_date", TimestampType(), True),
    StructField("call_time", TimestampType(), True),
    StructField("call_date_time", TimestampType(), True),
    StructField("disposition", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("agency_id", IntegerType(), True),
    StructField("address_type", StringType(), True),
    StructField("common_location", StringType(), True)
    
])

# TODO based on the df.show, build schema


def run_spark_job(spark):

    # TODO Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "police.calls") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 200) \
        .option("maxRatePerPartition", 200) \
        .option("stopGracefullyOnShutdown", "true") \
        .load()

    # Show schema for the incoming resources for checks
    df.printSchema()

    # TODO extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*")

    # TODO select original_crime_type_name and disposition
    distinct_table = df.distinct()

    # count the number of original crime type
    #agg_df = 
    agg_df = df.agg(countDistinct("original_crime_type_name"))

    # TODO Q1. Submit a screen shot of a batch ingestion of the aggregation
    # TODO write output stream
    #query = agg_df \
   # left = spark.readStream \
    #    .format("rate") \
     #   .option("rowsPerSecond", "5") \
      #  .option("numPartitions", "1").load() \
       # .selectExpr("value AS row_id", "timestamp AS left_timestamp")

    # TODO create a streaming dataframe that we'll join on
    #right = spark.readStream \
     #   .format("rate") \
      #  .option("rowsPerSecond", "5") \
       # .option("numPartitions", "1").load() \
        #.where((rand() * 100).cast("integer") < 10) \
        #.selectExpr("(value - 50) AS row_id ", "timestamp AS right_timestamp") \
        #.where("row_id > 0")
    # TODO join using row_id
    #join_query = left.join(right, "row_id")


    # TODO attach a ProgressReporter
    query.awaitTermination()

    # TODO get the right radio code json path
    radio_code_json_filepath = "./radio_code.json"
    radio_code_df = spark.read.json(radio_code_json_filepath)

    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    # TODO rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    # TODO join on disposition column
    #join_query = agg_df.
    join_query = agg_df.join(agg_df, agg_df("disposition") === radio_code_df("disposition"))


    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # TODO Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .getOrCreate()

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()

