from kafka import KafkaProducer as Producer
from kafka import KafkaConsumer as Consumer


from kafka import client
from kafka.errors import KafkaError
from ksql import KSQLAPI

import os
import logging

logger = logging.getLogger(__name__)

KAFKA_ADDRESS = os.environ.get("KAFKA_ADDRESS", "localhost:9092")
KSQL_URL=os.environ.get("KSQL_URL", "http://localhost:8088")

def get_kafka_producer(new=False):
    if not new:
        _PRODUCER = getattr(get_kafka_producer, '_PRODUCER', None)
        if _PRODUCER is None:
            try:
                _PRODUCER = Producer(bootstrap_servers=(KAFKA_ADDRESS))
                setattr(get_kafka_producer, '_PRODUCER', _PRODUCER)
            except:
                print('No Kafka producers found  ')
        return _PRODUCER
    else:
        #return Producer(bootstrap_servers=(KAFKA_ADDRESS), value_serializer=lambda x: dumps(x).encode('utf-8'))
        return Producer(bootstrap_servers=(KAFKA_ADDRESS))


def get_kafka_consumer(group=None):
    return Consumer(bootstrap_servers=(KAFKA_ADDRESS), group_id=group, auto_offset_reset='latest',enable_auto_commit=True,auto_commit_interval_ms=1000)


def topic_exists(topic):

    kclient = client(KAFKA_ADDRESS)
    server_topics = kclient.topic_partitions
    if topic in server_topics:
        return True
    else:
        return False

def get_ksql_client():
    client = KSQLAPI(KSQL_URL)
    return client

def convert_data(parent_json):
    pass



def push_data_to_kafka(_topic,_key,_value):

    producer=get_kafka_producer()
    try:
        future = producer.send(topic=_topic,key=_key,value=_value)
        result = future.get(timeout=60)
        return result
    except KafkaError as e:
        print(e)
        print ("Exception")
        return 0


def print_data_from_kafka(_topic):
        try:
            consumer=get_kafka_consumer()
            print("received kafka consumer")
            consumer.subscribe(_topic)
            print("received kafka consumer topic")
            for message in consumer:
              print(message)
        except KafkaError as k:
            print (k)