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

    def createNewStreamDestination(self,stream_obj):
        streamID = stream_obj["id"]
        # update/create document in db
        self.db.knownStreams.replace_one({'id': streamID}, stream_obj, upsert=True)
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
