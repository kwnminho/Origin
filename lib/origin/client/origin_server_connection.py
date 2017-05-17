import json
import sys
import zmq
import struct
import ctypes
import traceback

from origin import data_types, TIMESTAMP

# returns string and size tuple
def makeFormatString(config, keyOrder, records):
    try:
        tsType = config.get('Server',"timestamp_type")
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
    def __init__(self, config, stream, streamID, keyOrder, format, records, context, socket):
        self.config = config
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

        self.socket.setsockopt(zmq.SNDTIMEO,2000)

        if keyOrder is None:
            self.format_string, self.data_size = (None, None)
        else:
            self.format_string, self.data_size = makeFormatString(self.config,keyOrder,records)

    def send(self,**kwargs):
        msgData = [ self.streamID ]
        try:
            msgData.append(kwargs[TIMESTAMP])
        except KeyError:
            #print "No timestamp specified, server will timestamp on arrival"
            msgData.append(0)

        if self.format is None:
            for k in self.keyOrder:
                msgData.append(kwargs[k])
            try:
                self.socket.send(self.formatRecord(msgData), zmq.NOBLOCK)
            except zmq.Again:
                print("Connection to Server Failed")
                #self.socket.close()
                exit(1)
            except:
                print "Uncaught exception:"
                print '-'*60
                traceback.print_exc(file=sys.stdout)
                print '-'*60
                print("Exiting")
                #self.socket.close()
                exit(1)

        elif self.format == "json":
            msgData[0] = self.stream
            msgMap = {}
            for k in kwargs.keys():
                if k != TIMESTAMP:
                    msgMap[k] = kwargs[k]
            msgData.append(msgMap)
            try: 
                self.socket.send(json.dumps(msgData), zmq.NOBLOCK)
            except zmq.Again:
                print("Connection to Server Failed")
                #self.socket.close()
                exit(1)
            except:
                print "Uncaught exception:"
                print '-'*60
                traceback.print_exc(file=sys.stdout)
                print '-'*60
                print("Exiting")
                #self.socket.close()
                exit(1)

    def close(self):
        print "closing socket"
        self.socket.close()
        
    def formatRecord(self, data):
        return struct.pack( self.format_string, *data )
