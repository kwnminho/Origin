import json
import sys
from origin import data_types, config, timestamp
import struct
import ctypes

# returns string and size tuple
def makeFormatString(keyOrder, records):
    try:
        tsType = config["timestamp_type"]
    except KeyError:
        tsType = "uint"
    tsSize = data_types[tsType]["size"]
    fstr = "!I" + data_types[tsType]["format_char"]# network byte order

    dataLength = tsSize
    for entry in keyOrder:
        dataLength += data_types[records[entry]]["size"]
        fstr += data_types[records[entry]]["format_char"]
    return (fstr, dataLength)


class server_connection:
    def __init__(self,stream,streamID,keyOrder,format,records,context,socket):
        self.stream = stream
        self.streamID = streamID
        self.keyOrder = keyOrder
        try:
            self.format = format.lower()
        except AttributeError:
            self.format = None
        self.records = records
        self.context = context
        self.socket = socket
        if keyOrder is None:
            self.format_string, self.data_size = (None, None)
        else:
            self.format_string, self.data_size = makeFormatString(keyOrder,records)

    def send(self,**kwargs):
        msgData = [ self.streamID ]
        try:
            msgData.append(kwargs[timestamp])
        except KeyError:
            # 0 value timestamp means timestamp at server
            msgData.append(0)
        if self.format is None:
            for k in self.keyOrder:
                msgData.append(kwargs[k])
            self.socket.send( self.formatRecord(msgData) )
        elif self.format == "json":
            msgMap = {}
            for k in kwargs.keys():
                if k != config.timestamp:
                    msgMap[k] = kwqrgs[k]
            msgData.append(msgMap)
            self.socket.send(json.dumps(msgData))

    def close(self):
        print "closing socket"
        self.socket.close()
        
    def formatRecord(self, data):
        return struct.pack( self.format_string, *data )
