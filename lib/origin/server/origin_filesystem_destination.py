import json
from origin.server import destination
from origin import data_types, config, timestamp
import os
import numpy as np

def getDirectoryList(dir):
    return [ d for d in os.listdir(dir) if os.path.isdir(os.path.join(dir,d)) ]

def getCurrentStreamVersion(stream):
    stream_path = os.path.join(config['fs_data_path'],stream)
    with open(os.path.join(stream_path,'currentVersion.txt'), 'r') as f:
        version_dir = f.read().strip()
    return os.path.join(stream_path,version_dir)

class filesystem_destination(destination):
    def connect(self):
        data_dir = config['fs_data_path']
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
            self.logger.info("Creating data directory at: " + config['fs_data_path'])


    def readStreamDefTable(self):
        try:
            with open(config["fs_info_file"], 'r') as json_data:
                self.knownStreamVersions = json.load(json_data)
        except KeyError:
            self.logger.debug("Info file `{}` not found".format(config["fs_info_file"]))
            self.knownStreamVersions = {}

        self.knownStreams = {}
        dir_list = getDirectoryList(config['fs_data_path'])
        for stream in self.knownStreamVersions:
            if stream in dir_list:
                current_stream_version = getCurrentStreamVersion(stream)
                with open(os.path.join(current_stream_version,'definition.json'), 'r') as f:
                    self.knownStreams[stream] = json.load(json_data)
            else:
                self.logger.error("Stream '{}' found in stream list, but is not a group")

        for stream in self.knownStreams:
            print "="*10, " {} ".format(stream), "="*10
            for field_name in self.knownStreamVersions[stream]:
                print "  Field: %s (%s)"%(field_name,self.knownStreamVersions[stream][field_name])

    def createNewStream(self,stream,version,template,keyOrder):
        stream_path = os.path.join(os.path.join(config['fs_data_path'],stream))
        if version == 1:    # create a new stream group under root
            os.mkdir()
            streamID = len(getDirectoryList(config['fs_data_path']))
        else:
            streamID = self.knownStreamVersions[stream]["id"]
        # create a new subgroup for this instance of the current stream
        version_dir = stream+'_'+str(version)
        stream_ver = os.path.join( stream_path, version_dir )

        os.mkdir( os.path.join(stream_path, version_dir) )
        with open( os.path.join(stream_path, 'currentVersion.txt'), 'w' ) as f:
            f.write(version_dir)

        os.mknod( os.path.join(stream_path, version_dir, timestamp) )
        for field in template:
            os.mknod( os.path.join(stream_path, version_dir, field) )
    
        # update the stream inventory, in memory and on disk
        self.knownStreamVersions[stream] = {
                "version"   : version,
                "id"        : streamID,
                "keyOrder"  : keyOrder,
                "formatStr" : self.formatString(template,keyOrder),
        }
        with open(config["fs_info_file"], 'w') as json_data:
            json_data.write(json.dumps(self.knownStreamVersions))
        # create the stream field definition dict
        definition = {}
        for i, key in enumerate(keyOrder):
            definition[key] = { "type": template[key], "keyIndex": i }
        with open( os.path.join(stream_path, version_dir, 'definition.json'), 'w' ) as f:
            f.write( json.dumps(definition) )
        return streamID

    def insertMeasurement(self,stream,measurements):
        current_stream_version = getCurrentStreamVersion(stream)
        for field in measurements:
            with open( os.path.join(current_stream_version, field), 'a' ) as f:
                f.write(measurements[field] + '\n')

    # read stream data from storage between the timestamps given by time = [start,stop]
    # only takes timestamps in seconds
    def getRawStreamData(self,stream,start=None,stop=None,definition=None):
        start, stop = self.validateTimeRange(start,stop)
        self.logger.debug("Read request time range (start, stop): ({},{})".format(start,stop))
        # get data from buffer
        current_stream_version = getCurrentStreamVersion(stream)
        if definition is None:
            definition = self.knownStreams[stream]

        idx_start = None
        idx_stop = None
        try:
            f = open(os.path.join(current_stream_version,timestamp),'r')
            i = 0
            data = {}
            data[timestamp] = []
            while True:
                x = f.readline()
                x = x.strip()
                if not x: break
                x = long(x) 
                if (start_idx is None) and (x >= start):
                    start_idx = i
                if (start_idx is not None) and (x <= stop):
                    stop_idx = i
                    break
                if start_idx is not None:
                    data[timestamp] = data[timestamp].append(x)
                i += 1
            if (start_idx is not None) and (stop_idx is None):
                start_idx = i

        except:
            pass
        finally:
            f.close()

        if (idx_start is None) or (idx_stop is None):
            msg = "error in indexing (index start, index stop): ({},{})"
            self.logger.warning(msg.format(idx_start,idx_stop))
            raise IndexError

        self.logger.debug((idx_start,idx_stop))
        for field in definition:
            data[field] = []
            try:
                f = open(os.path.join(current_stream_version,field),'r')
                i = 0
                while i <= stop_idx:
                    x = f.readline()
                    if i >= start_idx:
                        x = x.strip()
                        if template[field] in ['int', 'uint', 'int8', 'uint8', 'int16', 'uint16']:
                            x = int(x)
                        elif template[field] in ['int64', 'uint64']:
                            x = long(x)
                        elif template[field] in ['float', 'double']:
                            x = float(x)
                        elif template[field] in ['string']:
                            x = x
                        else:
                            raise
                        data[field] = data[field].append(x)
            except:
                raise IndexError
        return data
        
    # read stream.field data from storage between the timestamps given by time = [start,stop]
    def getRawStreamFieldData(self,stream,field,start=None,stop=None):
        return self.getRawStreamData(stream=stream,start=start,stop=stop, definition={field:''}) # send dummy dict with single field
