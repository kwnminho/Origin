
import json
import sys

class server_connection:
    def __init__(self,stream,context,socket):
        self.stream = stream
        self.context = context
        self.socket = socket

    def send(self,**kwargs):
        print kwargs
        msgMap = {}
        recordTime = None
        for k in kwargs.keys():
            if k == "recordTime":
                recordTime = kwargs[k]
            else:
                msgMap[k] = kwargs[k]
        msg = (self.stream,recordTime,msgMap)
        self.socket.send (json.dumps(msg))

    def close(self):
        print "closing socket"
        self.socket.close()
        
