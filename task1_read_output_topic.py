import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from configs import kafka_config, MY_NAME
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'

OUTPUT_TOPIC_NAME = f"output_{MY_NAME}"

spark = (SparkSession.builder
         .appName("KafkaStreaming")
         .master("local[*]")
         .config("spark.sql.debug.maxToStringFields", "200")
         .config("spark.sql.columnNameLengthThreshold", "200")
         .getOrCreate())

df = (spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0])
      .option("kafka.security.protocol", kafka_config['security_protocol'])
      .option("kafka.sasl.mechanism", kafka_config['sasl_mechanism'])
      .option("kafka.sasl.jaas.config",
            'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";')
      .option("subscribe", OUTPUT_TOPIC_NAME)
      .option("startingOffsets", "earliest")
      .option("maxOffsetsPerTrigger", "300")
      .load()
      )

json_schema = StructType([
    #StructField("athlete_id", IntegerType(), True),
    StructField("sport", StringType(), True),
    StructField("medal", StringType(), True),
    StructField("sex", StringType(), True),
    StructField("country_noc", StringType(), True),
    StructField("avg_height", DoubleType(), True),
    StructField("avg_weight", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

df_res = df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), json_schema).alias("data")).select("data.*")

display_df = (df_res.writeStream
                 .trigger(availableNow=True)
                 .outputMode("append")
                 .format("console")
                 .option("truncate", False)
                 .option("checkpointLocation", "/tmp/checkpoints-2")
                 .start()
                 .awaitTermination())
