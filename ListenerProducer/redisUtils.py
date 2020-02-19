import redis
import os


REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = os.getenv('REDIS_PORT', '6379')
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', '')

def get_redis():


    try:
        r = redis.Redis(host='localhost', port=6379, db=0,password=REDIS_PASSWORD)
        return r
    except Exception as e:
        print(e)

if __name__ == '__main__':

    #print(get_redis().get('btc'))'
    redis_client=get_redis()
    for key in redis_client.keys():
        print('key{}:value{}'.format(key,redis_client.get(key)))
        redis_client.delete(key)
    # client.lpush('btc','value1')
    # print(client.exists('btc'))
