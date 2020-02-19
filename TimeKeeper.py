from ListenerProducer import Utls
from apscheduler.schedulers.background import BackgroundScheduler
from ListenerProducer import redisUtils
import datetime

prefix="Time:"
redis_client=redisUtils.get_redis()

def set_increment_time(time):
    if redis_client.exists(time):
        #print(int(redis_client.get(time)))
        redis_client.set(time,int(redis_client.get(time))+1, ex=3600)
    else:
        redis_client.set(time, int(1), ex=3600)

if __name__ == '__main__':

    consumer=Utls.get_kafka_consumer('getter')
    consumer.subscribe('txnx_per_min')
    for message in consumer:
        if message is not None:
            print(message.key, message.value)
            set_increment_time(message.value)



