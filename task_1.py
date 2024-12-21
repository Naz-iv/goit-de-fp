import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from configs import kafka_config, MY_NAME

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'

ATHLETE_TOPIC_NAME = f"athletes_{MY_NAME}"
OUTPUT_TOPIC_NAME = f"output_{MY_NAME}"

jdbc_url = "jdbc:mysql://217.61.57.46:3306/olympic_dataset"
jdbc_table_res = "athlete_event_results"
jdbc_table_bio = "athlete_bio"
jdbc_user = "neo_data_admin"
jdbc_password = "Proyahaxuqithab9oplp"

spark = SparkSession.builder.config("spark.jars", "mysql-connector-j-8.0.32.jar").appName("JDBCToKafka").getOrCreate()


# Task 1.1 Read athlets phisical data from MySQL with Spark
df_athlets_bio_all = spark.read.format('jdbc').options(url=jdbc_url,
    driver='com.mysql.cj.jdbc.Driver',
    dbtable=jdbc_table_bio,
    user=jdbc_user,
    password=jdbc_password).load()

df_athlets_bio_all.show()


# Task 1.2 Filtering
df_athlets_bio = df_athlets_bio_all.filter((df_athlets_bio_all.height.rlike('^[0-9]')) | (df_athlets_bio_all.weight.rlike('^[0-9]')))
df_athlets_bio.show()


# Task 1.3 Reading from Kafka-topic (writting to Kafka-topic in task_1_2_producer.py file)
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
      .option("subscribe", ATHLETE_TOPIC_NAME)
      .option("startingOffsets", "earliest")
      .option("maxOffsetsPerTrigger", "300")
      .load()
      )
json_schema = StructType([
    StructField("athlete_id", IntegerType(), True),
    StructField("sport", StringType(), True),
    StructField("medal", StringType(), True),
    StructField("athlete", StringType(), True),
    StructField("result_id", IntegerType(), True),
    StructField("event", StringType(), True)
])

df_athlets_res = df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), json_schema).alias("data")).select("data.*")


# Task 1.4 Merge competition results and athlets bio
df = df_athlets_res.join(df_athlets_bio, df_athlets_res.athlete_id == df_athlets_bio.athlete_id, "inner")


# Task 1.5 Count averege height and weight of athlets for sport, medal type, sex, country
avg_df = df.groupBy(
    "sport", "medal", "sex", "bio_country_noc"
).agg(
    avg("height").alias("avg_height"),
    avg("weight").alias("avg_weight"),
    current_timestamp().alias("timestamp")
).withColumn(
    "timestamp", date_format(col("timestamp"), "yyyy-MM-dd HH:mm:ss")
)


# Task 1.6 Create data stream with forEachBatch

db_properties = {
    "user": "neo_data_admin",
    "password": "Proyahaxuqithab9oplp",
    "driver": "com.mysql.cj.jdbc.Driver",
}
jdbc_url = "jdbc:mysql://217.61.57.46:3306/neo_data"

def process_and_write(batch_df, batch_id):

    # Record to Kafka
    batch_df.selectExpr("CAST(NULL AS STRING) AS key", "to_json(struct(*)) AS value"
    ).write.format("kafka").option("kafka.bootstrap.servers", "77.81.230.104:9092"
    ).option("kafka.security.protocol", "SASL_PLAINTEXT"
    ).option("kafka.sasl.mechanism", "PLAIN"
    ).option("kafka.sasl.jaas.config",
        "org.apache.kafka.common.security.plain.PlainLoginModule required username='admin' password='VawEzo1ikLtrA8Ug8THa';",
    ).option("topic", OUTPUT_TOPIC_NAME
    ).save()


    # Record to DB
    batch_df.write.jdbc(
        url=jdbc_url,
        table="nazarivankiv_enriched_athlete_avg",
        mode="append",
        properties=db_properties,
    )

avg_df.writeStream.foreachBatch(process_and_write).outputMode("update").start().awaitTermination()
