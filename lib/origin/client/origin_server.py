from origin.client import server_connection

from origin.client import float_field
from origin.client import integer_field
from origin.client import string_field
from origin import data_types

from origin import config

import zmq
import sys
import string
import struct
import json

def decode(measurementType):
    try:
        data_types[measurementType]
        return measurementType
    except:
        pass
    if measurementType == float_field:
        return "float"
    if measurementType == integer_field:
        return "int"
    if measurementType == string_field:
        return "string"
    return None

def declarationFormater(stream,records,keyOrder):
    decStr = [stream]
    for key in keyOrder:
        decStr.append(':'.join([key,records[key]]))
    return ','.join(decStr)


def formatStreamDeclaration(stream,records,keyOrder,format):
    measurements = records.keys()
    sentDict = {}
    for m in measurements:
        decodedType = decode(records[m])
        if decodedType == None:
            print "Programming error. Error should be caught before this"
            return None
        else:
            sentDict[m] = decodedType
    if (format is not None) and (format.lower() == "json"):
        if keyOrder is not None:
            print "Warning: JSON formatting selected and a key order has been defined. JSON object order is not gaurenteed therefore it is not recommended to use binary data packets."
        return json.dumps((stream,sentDict), sort_keys=True) # make deterministic
    else:
        return declarationFormater(stream,sentDict,keyOrder)

def simpleString(input):
    invalidChars = set(string.punctuation.replace("_",""))
    if any(char in invalidChars for char in input):
        return 1
    else:
        return 0

def validateStreamDeclaration(stream,template):
    fields = template.keys()
    error = False
    for f in fields:
        try:
            data_types[template[f]]
        except KeyError:
            print("type {} not recognized".format(template[f]))
            error = True
        if simpleString(f) != 0:
            print("Invalid field name: {}".format(f))
            error = True
    if simpleString(stream) != 0:
        print("Invalid stream name: {}".format(stream))
        error = True
    if not error:
        return 0
    return 1

class server:
    def __init__(self):
        pass

    def ping(self):
        return True

    def registerStream(self,stream,records,keyOrder=None,format=None):
        valid = validateStreamDeclaration(stream,records)

        if valid != 0:
            print "invalid stream declaration"
            return None

        port = config["origin_register_port"]
        msgport = config["origin_measure_port"]
        if format=='json':
            port = config["origin_json_register_port"]
            msgport = config["origin_json_measure_port"]

        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        host=config["origin_server"]
        socket.connect ("tcp://%s:%s" % (host,port))

        if (keyOrder is None) and (format is None):
            keyOrder = records.keys()
        registerComm = formatStreamDeclaration(stream,records,keyOrder,format)
        
        if(registerComm == None):
            if format is None:
                format = "comma-separated values"
            print "can't format stream into {}".format(format)
            return None

        socket.send(registerComm)
        confirmation = socket.recv()
        returnCode, msg = confirmation.split(',',1)
        print returnCode, msg

        if int(returnCode) != 0:
            print "Problem registering stream",stream
            print msg
            return None

        version, streamID = struct.unpack("!II",msg)
        print("successfully registered with streamID: {}, version: {}".format(streamID,version))
 
        # error checking
        socket_data = context.socket(zmq.PUSH)
        socket_data.connect("tcp://%s:%s"%(host,msgport))
        return server_connection(stream,streamID,keyOrder,format,records,context,socket_data)
