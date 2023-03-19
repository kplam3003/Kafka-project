import json
import time, random
from confluent_kafka import KafkaException
from confluent_kafka.avro import AvroProducer
from confluent_kafka import avro
import pytchat
import time


# Streaming youtube
chat = pytchat.create(video_id='jfKfPfyJRdk')

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % msg.value().decode('utf-8'), str(err))
    else:
        print("Message produced: %s" % msg.value().decode('utf-8'))

def load_avro_schema_from_file():
    key_schema = avro.load("schema_key.avsc")
    value_schema = avro.load("schema_value.avsc")

    return key_schema, value_schema

def send_data():
    producer_config = {
        "bootstrap.servers": "0.0.0.0:9092",
        "schema.registry.url": "http://localhost:8081",
        "receive.message.max.bytes": 2000000000
    }

    key_schema, value_schema = load_avro_schema_from_file()

    try:
        while chat.is_alive():
            for c in chat.get().sync_items():
                producer = AvroProducer(producer_config, default_key_schema=key_schema, default_value_schema=value_schema)
                value_str = {
                    "type": "Youtube",
                    "comment_at": str(c.datetime),
                    "user_name": str(c.author.name), 
                    "cmt": str(c.message)
                }
                value_dumps = json.dumps(value_str)
                producer.produce(topic = "streams_cmt", headers = [], value = json.loads(value_dumps))
                producer.flush()
                print(f'Producing Message = {str(value_dumps)}')
            # Sleep for a random number of seconds
            time_to_sleep = random.randint(1, 11)
            time.sleep(time_to_sleep)

    except KafkaException as e:
        print('Kafka failure ' + e)

def main():
    send_data()

if __name__ == "__main__":
    main()