import os
import json
import mysql.connector
from origin.server import template_validation
from origin.server import measurement_validation

from origin import config

import struct

class mysql_destination:
    def readStreamdefTable(self):
        streamCreation = (
            "CREATE TABLE IF NOT EXISTS origin_streams ( "
            " name varchar(1024), "
            " version integer "
            " ) "
        )

        streamFieldCreation = (
            "CREATE TABLE IF NOT EXISTS "
            " origin_stream_fields (  "
            " stream_name varchar(1024), "
            " field_name varchar(1024), "
            " version integer, "
            " field_type varchar(100), "
            " keyIndex integer"
            " )"
        )

        cursor = self.cursor
        cursor.execute(streamCreation)
        cursor.execute(streamFieldCreation)

        currentStreamNamesVersions= []
        query = "SELECT name,version from origin_streams"
        cursor.execute(query)
        for name,version in cursor:
            currentStreamNamesVersions.append((name,version))

        currentStreamNameDefinitions = {}
        currentStreamVersions = {}
        for name,version in currentStreamNamesVersions:
            query = "SELECT field_name,field_type,keyIndex FROM origin_stream_fields WHERE stream_name=\"%s\" and version=%d"%(name,version)
            cursor.execute(query)
            definition = {}
            for field_name,field_type,keyIndex in cursor:
                definition[field_name] = {"type":field_type, "keyIndex":keyIndex}
            currentStreamNameDefinitions[name] = definition
            currentStreamVersions[name] = version
            
        for stream in currentStreamNameDefinitions.keys():
            for field_name in currentStreamNameDefinitions[stream].keys():
                print "  Field: %s (%s)"%(field_name,currentStreamNameDefinitions[stream][field_name])
        self.knownStreamVersions = currentStreamVersions
        self.knownStreams = currentStreamNameDefinitions
        self.cnx.commit()
                                        
    def __init__(self,logger):
        self.logger = logger
        self.cnx = mysql.connector.connect(user=config["mysql_local_user"],
                                           password=config["mysql_local_password"],
                                           host=config["mysql_local_server"],
                                           database=config["mysql_local_db"])
        self.cursor = self.cnx.cursor()

        self.readStreamdefTable()

    def registerStream(self,stream,template,keyOrder=None):
        updateDatabase = False
        destVersion = None
        self.logger.info("Attempt to register stream %s"%(stream))
        if stream not in self.knownStreams.keys():
            updateDatabase = True
            destVersion = 1
        else:
            if template_validation(template,self.knownStreams[stream]):
                self.logger.info("Known stream, %s matching current defintion, so no database modification needed"%(stream))
                updateDatabase = False
            else:
                updateDatabase = True
                destVersion = self.knownStreamVersions[stream] + 1

        if updateDatabase:
            cursor = self.cursor

            if destVersion == 1:
                query = "INSERT INTO origin_streams VALUES (\"%s\",%d)"%(stream,destVersion)
            else:
                query = "UPDATE origin_streams SET version=%d WHERE name=\"%s\""%(destVersion,stream) 
            print cursor.execute(query)

            for fieldName in template.keys():
                idx = None
                try:
                  for i, k in enumerate(keyOrder):
                    if k == fieldName:
                      idx = i
                      break
                except TypeError:
                  pass
                query = """INSERT INTO origin_stream_fields 
                        VALUES ("%s","%s",%d,"%s",%d)"""%(stream,fieldName,destVersion,template[fieldName],idx)
                cursor.execute(query)

            query = "CREATE TABLE IF NOT EXISTS measurements_%s_%d (id BIGINT NOT NULL AUTO_INCREMENT,measurementTime integer,"%(stream,destVersion)
            fields = []
            for fieldName in template.keys():
                fieldType = template[fieldName]
                fieldTypeSQL = None
                if fieldType == "string":
                    fieldTypeSQL = "TEXT"
                if fieldType == "int":
                    fieldTypeSQL="integer"
                if fieldType == "float":
                    fieldTypeSQL="float"
                fields.append((fieldName,fieldTypeSQL))
            for i in range(0,len(fields)):
                query = query + "%s %s"%(fields[i][0],fields[i][1])
                if i < len(fields) -1:
                    query = query + ","
            query = query + ", PRIMARY KEY (id))"

            cursor.execute(query)

            query = "DROP VIEW IF EXISTS measurements_%s "%(stream)
            cursor.execute(query)

            query = "CREATE VIEW measurements_%s AS select * from measurements_%s_%d"%(stream,stream,destVersion)
            cursor.execute(query)
            
            self.cnx.commit()

            # make sure we know the current streams after all that
            self.readStreamdefTable()
            
        return (0,"")

    def measurement(self,measurementTime,stream,measurements):
        if stream not in self.knownStreams.keys():
            print "trying to add a measurement to data on an unknown stream"
            return (1,"Unknown stream")

        if not measurement_validation(measurements,self.knownStreams[stream]):
            print "Measurement didn't validate against the pre-determined format"
            return (1,"Invalid measurements against schema")

        measurementArray = []
        keys = measurements.keys()
        keys.sort()
        for k in keys:
            measurementArray.append((k,measurements[k]))

        fmt = "(measurementTime,"
        valuePlaceholders = "(%s,"
        values = []
        values.append(measurementTime)
        for i in range(0,len(measurementArray)):
            fmt = fmt + measurementArray[i][0]
            valuePlaceholders = valuePlaceholders + "%s"
            values.append(measurementArray[i][1])
                
            if i != len(measurementArray)-1:
                fmt = fmt + ","
                valuePlaceholders = valuePlaceholders + ","
        fmt = fmt + ")"
        valuePlaceholders = valuePlaceholders + ")"

        query = "INSERT INTO measurements_%s_%d %s VALUES %s"%(stream,self.knownStreamVersions[stream],fmt,valuePlaceholders)

        self.cursor.execute(query,values)
        self.cnx.commit()
        
        result = 0
        resultText = ""
        return (result,resultText)

    def measurementOrdered(self,measurementTime,stream,measurements):
        meas = {}
        for key in self.knownStreams[stream]:
          idx = self.knownStreams[stream][key]["keyIndex"]
          meas[key] = measurements[idx]

        return self.measurement(measurementTime,stream,meas)

    def measurementBinary(self,stream,measurements):
        if stream not in self.knownStreams.keys():
          print "trying to add a measurement to data on an unknown stream"
          return (1,"Unknown stream")

        formatStr = '!i' # assumes 32b timestamp
        for key in self.knownStreams[stream]:
          dtype = self.knownStreams[stream][key]["type"]
          if  dtype == 'int':
            formatStr += 'i'
          elif dtype == 'float':
            formatStr += 'f'
          else:
            return (1,"Unsupported type '{}' in binary data".format(dtype))

        dtuple = struct.unpack_from(formatStr, measurements)
        measurementTime = dtuple[0]
        meas = list(dtuple[1:])
        return self.measurementOrdered(measurementTime,stream,meas)
