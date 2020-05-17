import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StringType

BROKER_URL = "kafka0:19092"
topic_name = "police_calls"

# TODO Create a schema for incoming resources
schema = StructType().add('crime_id', StringType(), False). \
    add('original_crime_type_name', StringType(), False). \
    add('report_date', StringType(), False). \
    add('call_date', StringType(), False). \
    add('offense_date', StringType(), False). \
    add('call_time', StringType(), False). \
    add('call_date_time', StringType(), False). \
    add('disposition', StringType(), False). \
    add('address', StringType(), False). \
    add('city', StringType(), False). \
    add('state', StringType(), False). \
    add('agency_id', StringType(), False).\
    add('address_type', StringType(), False).\
    add('common_location', StringType(), False)

def run_spark_job(spark):

    # TODO Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BROKER_URL) \
    .option("subscribe", topic_name) \
        .option("startingOffsets", "latest") \
        .option("maxOffsetsPerTrigger", 200) \
        .option("stopGracefullyOnShutdown", "true") \
        .load()

    # Show schema for the incoming resources for checks
    df.printSchema()

    # TODO extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value as string)")

    service_table = kafka_df.selectExpr('CAST(value AS STRING)').\
        select(from_json('value', schema).alias('DF')).select('DF.*')

    # TODO select original_crime_type_name and disposition
    distinct_table = service_table \
        .select('original_crime_type_name', 'disposition')


    # count the number of original crime type
    count = distinct_table.groupBy('original_crime_type_name').count().orderBy('count', ascending=False)



    # TODO Q1. Submit a screen shot of a batch ingestion of the aggregation
    # TODO write output stream
    query = count.writeStream.outputMode("Complete").format("console").start().awaitTermination()

    # TODO attach a ProgressReporter


    # TODO get the right radio code json path
    radio_code_json_filepath = "radio_code.json"
    radio_code_df = spark.read.option("multiLine", True).option("mode", "PERMISSIVE").json(radio_code_json_filepath)

    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    # TODO rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    # TODO join on disposition column
    join_query = distinct_table.join(radio_code_df, "disposition").\
        writeStream.outputMode("update").\
        format("console").\
        start().awaitTermination()



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


#./spark-submit --master spark://02e3ada5ebbb:7077  --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 data_stream.py

#./spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 data_stream.py
