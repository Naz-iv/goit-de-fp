from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from pyspark.sql import SparkSession
from configs import kafka_config, MY_NAME
import json
import sys
import uuid
#import time

# Task 1.2 (Readng from msql table and writting to Kafka-topic)
ATHLETE_TOPIC_NAME = f"athletes_{MY_NAME}"
OUTPUT_TOPIC_NAME = f"output_{MY_NAME}"
NUM_PARTITIONS = 2
REPLICATION_FOLDER = 1
#TIME_SLEEP = 1

jdbc_config = {
    "url": "jdbc:mysql://217.61.57.46:3306/olympic_dataset",
    "table_bio": "athlete_bio",
    "table_events": "athlete_event_results",
    "table_athlete_avg": "nazarivankiv_enriched_athlete_avg",
    "user": "neo_data_admin",
    "password": "Proyahaxuqithab9oplp",
    "format": "jdbc",
    "driver": "com.mysql.cj.jdbc.Driver",
    "jar": "spark.jars",
}


admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password']
)

for new_topik in [ATHLETE_TOPIC_NAME, OUTPUT_TOPIC_NAME]:
    new_topic = NewTopic(name=new_topik, num_partitions=NUM_PARTITIONS, replication_factor=REPLICATION_FOLDER)
    try:
        admin_client.create_topics(new_topics=[new_topic], validate_only=False)
        print(f"Topic '{new_topik}' created successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")

print(admin_client.list_topics())
admin_client.close()

try:
    producer = KafkaProducer(
        bootstrap_servers=kafka_config['bootstrap_servers'],
        security_protocol=kafka_config['security_protocol'],
        sasl_mechanism=kafka_config['sasl_mechanism'],
        sasl_plain_username=kafka_config['username'],
        sasl_plain_password=kafka_config['password'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("Kafka producer initialized successfully.")
except Exception as e:
    print(f"Failed to initialize KafkaProducer: {e}")
    sys.exit(1)

# Read data to the Spark DataFrame
jdbc_table = "athlete_event_results"
spark = SparkSession.builder.config("spark.jars", "mysql-connector-j-8.0.32.jar").appName("JDBCToKafka").getOrCreate()
df = spark.read.format('jdbc').options(url=jdbc_config["url"],
                                       driver='com.mysql.cj.jdbc.Driver',
                                       dbtable=jdbc_table,
                                       user=jdbc_config["username"],
                                       password=jdbc_config["password"]).load()

try:
    for row in df.collect():
        message_value = row.asDict()
        producer.send(ATHLETE_TOPIC_NAME, key=str(uuid.uuid4()), value=message_value)
        producer.flush()
        print(f"Message sent to topic {ATHLETE_TOPIC_NAME}: {message_value} successfully.")
        #time.sleep(TIME_SLEEP)
    print(f"All data from the table '{jdbc_table}' has been sent to Kafka topic '{ATHLETE_TOPIC_NAME}'.")
except Exception as e:
    print(f"Error sending data to Kafka: {e}")
finally:
    producer.close()