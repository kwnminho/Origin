import os
import json
import mysql.connector
from origin.server import template_validation
from origin.server import measurement_validation
from origin import data_types, current_time, config

import struct

class mysql_destination:
    def readStreamdefTable(self):
        streamCreation = (
            "CREATE TABLE IF NOT EXISTS origin_streams ( "
            " id INT NOT NULL AUTO_INCREMENT,"
            " name varchar(1024),"
            " version integer,"
            " PRIMARY KEY (id) "
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
        query = "SELECT id,name,version from origin_streams"
        cursor.execute(query)
        for id,name,version in cursor:
            currentStreamNamesVersions.append((id,name,version))

        currentStreamNameDefinitions = {}
        currentStreamVersions = {}
        for id,name,version in currentStreamNamesVersions:
            query = "SELECT field_name,field_type,keyIndex FROM origin_stream_fields WHERE stream_name=\"%s\" and version=%d"%(name,version)
            cursor.execute(query)
            definition = {}
            for field_name,field_type,keyIndex in cursor:
                definition[field_name] = {"type":field_type, "keyIndex":keyIndex}
            currentStreamNameDefinitions[name] = definition
            currentStreamVersions[name] = {"version": version, "id": id}
            
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

    # returns version and streamID
    def registerStream(self,stream,template,keyOrder=None):
        updateDatabase = False
        destVersion = None
        self.logger.info("Attempt to register stream %s"%(stream))
        if stream not in self.knownStreams.keys():
            updateDatabase = True
            destVersion = 1
        else:
            streamID = self.knownStreamVersions[stream]["id"]
            destVersion = self.knownStreamVersions[stream]["version"]
            if template_validation(template,self.knownStreams[stream]):
                self.logger.info("Known stream, %s matching current defintion, so no database modification needed"%(stream))
                updateDatabase = False
            else:
                updateDatabase = True
                destVersion += 1

        if updateDatabase:
            cursor = self.cursor

            if destVersion == 1:
                query = "INSERT INTO origin_streams (name, version) VALUES (\"%s\",%d)"%(stream,destVersion)
            else:
                query = "UPDATE origin_streams SET version=%d WHERE name=\"%s\""%(destVersion,stream) 
            print query
            print cursor.execute(query)
            streamID = cursor.lastrowid

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

            query = "CREATE TABLE IF NOT EXISTS measurements_%s_%d (id BIGINT NOT NULL AUTO_INCREMENT,measurementTime "%(stream,destVersion)
            try:
                query += data_types[config["timestamp_type"]]["mysql"]
            except KeyError:
                query += "INT UNSIGNED"
            query += ","

            fields = []
            for fieldName in template.keys():
                fieldType = template[fieldName]
                try:
                    fields.append( (fieldName,data_types[fieldType]["mysql"]) )
                except KeyError:
                    pass # TODO: Whatever the failure mode should be I guess

            for i in range(0,len(fields)):
                query = query + "%s %s,"%(fields[i][0],fields[i][1])
            query = query + "PRIMARY KEY (id))"

            cursor.execute(query)

            query = "DROP VIEW IF EXISTS measurements_%s "%(stream)
            cursor.execute(query)

            query = "CREATE VIEW measurements_%s AS select * from measurements_%s_%d"%(stream,stream,destVersion)
            cursor.execute(query)
            
            self.cnx.commit()

            # make sure we know the current streams after all that
            self.readStreamdefTable()

        print(destVersion, streamID)
        return (0,  struct.pack("!II",destVersion,streamID))

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

        query = "INSERT INTO measurements_%s_%d %s VALUES %s"%(stream,self.knownStreamVersions[stream]["version"],fmt,valuePlaceholders)

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

        formatStr = [None] * (len(self.knownStreams[stream]) + 2)
        formatStr[0] = '!' # use network byte order
        try:
            formatStr[1] = data_types[config["timestamp_type"]]["format_char"]
        except KeyError:
            formatStr[1] = data_types["uint"]["format_char"]

        for key in self.knownStreams[stream]:
          dtype = self.knownStreams[stream][key]["type"]
          idx = self.knownStreams[stream][key]["keyIndex"]
          try:
            formatStr[idx+2] = data_types[dtype]["format_char"]
            if not data_types[dtype]["binary_allowed"]:
              return (1,"Unsupported type '{}' in binary data.".format(dtype))
          except KeyError:
            return (1,"Type \"{}\" not recognized.".format(dtype))
        formatStr = ''.join(formatStr)

        dtuple = struct.unpack_from(formatStr, measurements)
        measurementTime = dtuple[0]
        if measurementTime == 0:
            measurementTime = current_time(config)
        meas = list(dtuple[1:])
        return self.measurementOrdered(measurementTime,stream,meas)

    def findStream(self, streamID):
        for stream in self.knownStreamVersions:
            if self.knownStreamVersions[stream]["id"] == streamID:
                return stream
        raise ValueError
