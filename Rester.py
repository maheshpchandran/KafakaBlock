from flask import Flask
from flask_restful import Resource, Api
from ListenerProducer import redisUtils
import operator
import json

redis_client=redisUtils.get_redis()
app = Flask(__name__)
api = Api(app)


class showTransactions(Resource):
    def get(self):

        transcations = {}
        list_transactions = [x for x in redis_client.lrange("transactions", 0, -1)]
        index = 0
        for i in list_transactions:
            transcations[index] = json.loads(i.decode("utf-8"))['x']['hash']
            index += 1
        return transcations


class transactionsCount(Resource):
    def get(self, min_value=None):
        try:
            keys = redis_client.keys('??:??')
            keys.sort()
            transaction_time_stamp = {}
            for key in keys:
                transaction_time_stamp[key.decode("utf-8")[1:]] = redis_client.get(key).decode("utf-8")

            if min_value is None:
                return transaction_time_stamp

            if min_value in transaction_time_stamp:
                return transaction_time_stamp[min_value]

        except Exception as e:
            return {'Error': e}


class highValueAddr(Resource):
    def get(self):
        try:
            keys = redis_client.keys('Address*')
            high_transcations = {}
            for key in keys:
                high_transcations[key.decode("utf-8")[1:]] = int(redis_client.get(key).decode("utf-8"))
            sorted_transactions = sorted(high_transcations.items(), key=operator.itemgetter(1), reverse=True)
            return sorted_transactions

        except Exception as e:
            return {'Error': e}


api.add_resource(showTransactions, '/show_transactions')
api.add_resource(transactionsCount, '/transactions_count_per_minute/<min_value>', '/transactions_count_per_minute/')
api.add_resource(highValueAddr, '/high_value_addr')
app.run(debug=True)