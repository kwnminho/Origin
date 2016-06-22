import json
import sys
from origin.server import data_types
from origin import config
import struct
import ctypes

# returns string and size tuple
def makeFormatString(keyOrder, records):
    try:
        tsType = config["timestamp_type"]
    except KeyError:
        tsType = "uint"
    tsSize = data_types[tsType]["size"]
    fstr = "!" + data_types[tsType]["format_char"]# network byte order

    dataLength = tsSize
    for entry in keyOrder:
        dataLength += data_types[records[entry]]["size"]
        fstr += data_types[records[entry]]["format_char"]
    return (fstr, dataLength)


class server_connection:
    def __init__(self,stream,keyOrder,records,context,socket):
        self.stream = stream
        self.keyOrder = keyOrder
        self.records = records
        self.context = context
        self.socket = socket
        self.format_string, self.data_size = makeFormatString(keyOrder,records)

    def send(self,**kwargs):
        msgData = [ kwargs["recordTime"] ]
        for k in self.keyOrder:
            msgData.append(kwargs[k])
        self.socket.send( self.formatRecord(msgData) )

    def close(self):
        print "closing socket"
        self.socket.close()
        
    def formatRecord(self, data):
        header = json.dumps([self.stream]) # options go into json array
        msg = ctypes.create_string_buffer( header, self.data_size + len(header) )
        struct.pack_into( self.format_string, msg, len(header), *data )
        return msg
