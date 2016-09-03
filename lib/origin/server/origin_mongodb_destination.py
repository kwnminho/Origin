from pymongo import MongoClient
import pymongo
from origin.server import destination
from origin import data_types, timestamp
import ConfigParser

class mongodb_destination(destination):
    def connect(self):
        self.client = MongoClient(
            host=self.config.get("MongoDB","server_ip"),
            port=self.config.getint("MongoDB","port")
        )
        self.db = self.client[self.config.get("MongoDB","db")]

    def readStreamDefTable(self):
        knownStreams = {}
        knownStreamVersions = {}
        if "knownStreams" in self.db.collection_names():
            for stream_obj in self.db.knownStreams.find():
                knownStreams[stream_obj["stream"]] = stream_obj
                knownStreamVersions[stream_obj["stream"]] = stream_obj["definition"]
        else:
            self.db.knownStreams.create_index([('id', pymongo.ASCENDING)], unique=True)
        self.knownStreams = knownStreams
        self.knownStreamVersions = knownStreamVersions

    def createNewStream(self,stream,version,template,keyOrder):
        if version > 1:
            stream_obj = self.db.knownStreams.find_one({"stream":stream})
            streamID = stream_obj['id']
        else:   # need a unique streamID, get max and increment
            streamID = 0
            for s in self.db.knownStreams.find():
                if s['id'] > streamID:
                    streamID = s['id']
            streamID += 1
            stream_obj = { 
                    "stream":stream, 
                    "id":streamID, 
                    "versions":[] 
                }
            
        # generate binary format string if possible
        err, formatStr = self.formatString( template, keyOrder )
        if err > 0:
            formatStr = ''
        # generate stream definition from template and key order
        definition = {}
        for i, key in enumerate(keyOrder):
            definition[key] = { "type": template[key], "keyIndex": i }
        # prepare object for insertion as document
        stream_obj["version"] = version
        stream_obj["definition"] = definition
        stream_obj["formatStr"] = formatStr
        stream_obj["versions"].append({
            "version"   : version,
            "id"        : streamID,
            "keyOrder"  : keyOrder,
            "formatStr" : formatStr,
            "definition": definition
        })
        # update/create document in db
        result = self.db.knownStreams.replace_one({'id': streamID}, stream_obj, upsert=True)
        return streamID

    def insertMeasurement(self,stream,measurements):
        stream_obj = self.knownStreams[stream]
        stream_collection = "{}_{}".format(stream, stream_obj["version"])
        for key in measurements: # mongodb doesn't do unsigned ints, because its dumb
            if key != timestamp: # could be smarter about this, but I'm tired
                measurements[key] = str(measurements[key])
        self.db[stream_collection].insert_one(measurements)

    # read stream data from storage between the timestamps given by time = [start,stop]
    def getRawStreamData(self,stream,start=None,stop=None,definition=None):
        start, stop = self.validateTimeRange(start,stop)
        self.logger.debug("Read request time range (start, stop): ({},{})".format(start,stop))

        stream_obj = self.knownStreams[stream]
        stream_collection = "{}_{}".format(stream, stream_obj["version"])

        if definition is None:
            definition = self.knownStreams[stream]["definition"]

        fieldList = [ field for field in definition ]
        data = {timestamp: []}
        for field in fieldList:
            data[field] = []

        results = self.db[stream_collection].find({ "$and": [
            { timestamp: {"$gte": start} },
            { timestamp: {"$lte": stop} }
        ]})

        for meas in results:
            for field in fieldList:
                # recast to native type from string because mongo is stupid
                dt = data_types[self.knownStreamVersions[stream][field]['type']]
                data[field].append(dt['type'](meas[field]))
            dt = data_types[self.config.get("Server","timestamp_type")]   
            data[timestamp].append(dt['type'](meas[timestamp]))

        return data
