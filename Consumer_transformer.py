from ListenerProducer import Utls
from ListenerProducer import redisUtils
import json
import datetime
from kafka.errors import KafkaError
from ListenerProducer.Utls import push_data_to_kafka

prefix="Address"

redis_client = redisUtils.get_redis()


# def transform_json_to_simple(_topic):
#         try:
#             consumer=Utls.get_kafka_consumer()
#             consumer.subscribe(_topic)
#             for message in consumer:
#                 if message is not None:
#                     json_original=json.loads(message.value.decode("utf-8"))
#                     event_time = datetime.datetime.utcfromtimestamp(json_original['x']['time'])
#                     print(json_original['x']['hash'])
#                     print(event_time.strftime("%H:%M"))
#                     push_data_to_kafka('txnx_per_min',_key=str(json_original['x']['hash']).encode('utf-8'),_value=str(event_time.strftime("%H:%M")).encode('utf-8'))
#
#         except KafkaError as e:
#             print ("exception")
#             print(e)

def push_simple_json(_json_original):
        event_time = datetime.datetime.utcfromtimestamp(_json_original['x']['time'])
        print("pushing the following values to txnx_per_min")
        print(_json_original['x']['hash'])
        print(event_time.strftime("%H:%M"))
        push_data_to_kafka('txnx_per_min', _key=str(_json_original['x']['hash']).encode('utf-8'),_value=str(event_time.strftime("%H:%M")).encode('utf-8'))

def set_redis_txnx_from_topic_minutes(_topic):
        try:
            consumer = Utls.get_kafka_consumer('getters')
            consumer.subscribe(_topic)
            for message in consumer:
                if message is not None:
                    transaction_json = json.loads(message.value.decode("utf-8"))
                    push_simple_json(transaction_json)
                    redis_client.lpush("transactions", json.dumps(transaction_json).encode('utf-8'))
                    redis_client.ltrim("transactions", 0, 99)
                    for i in transaction_json["x"]["out"]:
                        if not i["spent"]:
                            if redis_client.exists(prefix+str(i["addr"])):
                                redis_client.set(prefix+str(i["addr"]),str(int(i["value"]) + int(redis_client.get(prefix+str(i["addr"])))), ex=10800)
                            else:
                                redis_client.set(prefix+str(i["addr"]), str(i["value"]), ex=10800)
                                print(str(i["addr"]))
                                print(str(i["value"]))
                        else:
                            redis_client.set(prefix+str(i["addr"]), str("0"), ex=10800)

        except KafkaError as e:
            print("exception")
            print(e)


if __name__ == '__main__':

        print('Transforming data....')
        set_redis_txnx_from_topic_minutes('txns')


