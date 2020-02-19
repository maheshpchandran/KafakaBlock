from websocket import create_connection




class SocketConn(object):

    def __init__(self,url,):
        self.ws = create_connection(url)
        print("Created ws connection")


    def receive(self,subscribe_text='{"op":"ping_tx"}'):
        self.ws.send(subscribe_text)
        print ("inside recive")
        return self.ws.recv()




