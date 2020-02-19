import os
import logging
import traceback

from ListenerProducer import Utls
from ListenerProducer import socket_con
import json

from ListenerProducer.Utls import push_data_to_kafka

logger = logging.getLogger(__name__)

if __name__ == '__main__':
    SOCKET_URL = os.getenv('SOCKET_URL', 'wss://ws.blockchain.info/inv')
    producer=Utls.get_kafka_producer()
    x=0
    while x <20:

        try:
            ws=socket_con.SocketConn(SOCKET_URL)
            received_string=ws.receive('{"op":"unconfirmed_sub"}')
            #print (received_string)
            transaction_json = json.loads(received_string)
            #print(transaction_json['x']['hash'])


        except Exception as e:
                print ("exception")
                print(e)

        try:
            #push_data_to_kafka('test',json.dumps(test_json).encode('utf-8'))
            push_data_to_kafka('txns', json.dumps(transaction_json['x']['hash']).encode('utf-8'), json.dumps(transaction_json).encode('utf-8'))
            x =x+1
        except BaseException as e:
            traceback.print_exc()
            print("exception")
            print(e)

