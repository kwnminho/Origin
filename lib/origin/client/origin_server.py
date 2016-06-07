from origin.client import server_connection

from origin.client import float_field
from origin.client import integer_field
from origin.client import string_field

from origin import config

import zmq
import json
import sys
import string

def decode(measurementType):
    if measurementType == float_field:
        return "float"
    if measurementType == integer_field:
        return "int"
    if measurementType == string_field:
        return "string"
    return None

def formatStreamDeclaration(stream,records):
    measurements = records.keys()
    sentDict = {}
    for m in measurements:
        decodedType = decode(records[m])
        if decodedType == None:
            print "Programming error. Error should be caught before this"
            return None
        else:
            sentDict[m] = decodedType
    return json.dumps([stream,sentDict])

def simpleString(input):
    invalidChars = set(string.punctuation.replace("_",""))
    if any(char in invalidChars for char in input):
        return 1
    else:
        return 0

def validateStreamDeclaration(stream,template):
    validTypes = [float_field,integer_field,string_field]
    fields = template.keys()
    error = False
    for f in fields:
        if template[f] not in validTypes:
            error = True
        if simpleString(f) != 0:
            error = True
    if simpleString(stream) != 0:
        error = True
    if not error:
        return 0
    return 1

class server:
    def __init__(self):
        pass

    def ping(self):
        return True

    def registerStream(self,stream,records):
        valid = validateStreamDeclaration(stream,records)

        if valid != 0:
            print "invalid stream declaration"
            return None

        port = config["origin_register_port"]

        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        host=config["origin_server"]
        socket.connect ("tcp://%s:%s" % (host,port))

        registerComm = formatStreamDeclaration(stream,records)
        
        if(registerComm == None):
            print "can't format stream into json"
            return None

        socket.send(registerComm)
        confirmation = socket.recv()
        confirmationDecoded = json.loads(confirmation)


        if confirmationDecoded[0] != 0:
            print "Problem registering stream",stream
            print confirmationDecoded[1]
            return None
        

        # error checking
        socket_data = context.socket(zmq.PUSH)
        msgport = config["origin_measure_port"]
        socket_data.connect("tcp://%s:%s"%(host,msgport))
        return server_connection(stream,context,socket_data)

