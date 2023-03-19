# import json 
# from kafka import KafkaConsumer
# # import pandas as pd
# # import numpy as np
# # import psycopg2

# # connection_string = 'postgresql://minh:minh@localhost:5432/postgres'

# # connection = psycopg2.connect(connection_string)
# # connection.autocommit = True

# if __name__ == '__main__':
#     # Kafka Consumer 
#     consumer = KafkaConsumer(
#         'messages',
#         bootstrap_servers='localhost:9092',
#         auto_offset_reset='earliest'
#     )
#     for message in consumer:
#         # print(json.loads(message.value))
#         # print(json.loads(message).decode('utf-8'))
#         msg = message.value.decode("utf-8")
#         print(message.value.decode("utf-8"))
#     #     cur = connection.cursor()
#     #     cur.execute("""INSERT INTO """)
#     # cur.close()
import json
from confluent_kafka import Producer, KafkaException
from confluent_kafka.avro import AvroConsumer
from confluent_kafka import avro

def read_data():
    consumer_config = {
        "bootstrap.servers": "0.0.0.0:9092",
        "schema.registry.url": "http://localhost:8081",
        "group.id": "my-connsumer",
        "auto.offset.reset": "earliest",
        "receive.message.max.bytes": 2000000000
    }

    #print(key_schema)
    #print(value_schema)

    consumer = AvroConsumer(consumer_config)
    consumer.subscribe(['stream_comments'])

    while True:
      try:
        msg = consumer.poll(1)

        if msg is None:
          continue

        print("Key is :" + json.dumps(msg.key()))
        print("Value is :" + json.dumps(msg.value()))
        print("-------------------------")

      except KafkaException as e:
        print('Kafka failure ' + e)

    consumer.close()

def main():
    read_data()

if __name__ == "__main__":
    main()