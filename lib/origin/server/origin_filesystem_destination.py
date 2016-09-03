import json
from origin.server import destination
from origin import data_types, timestamp
import os
import numpy as np
import sys, traceback

def getDirectoryList(dir):
    return [ d for d in os.listdir(dir) if os.path.isdir(os.path.join(dir,d)) ]

def getCurrentStreamVersion(config,stream):
    stream_path = config.get('FileSystem','data_path')
    stream_path = os.path.join( config.get('Server', "var_path"), stream_path, stream )
    with open(os.path.join(stream_path,'currentVersion.txt'), 'r') as f:
        version_dir = f.read().strip()
    return os.path.join(stream_path,version_dir)

class filesystem_destination(destination):
    def connect(self):
        self.data_path = self.config.get('FileSystem','data_path')
        self.data_path = os.path.join( self.config.get('Server', "var_path"), self.data_path )
        self.info_file = os.path.join( self.data_path, self.config.get('FileSystem','info_file') )
        if not os.path.exists(self.data_path):
            os.makedirs(self.data_path)
            self.logger.info("Creating data directory at: " + self.data_path)

    def readStreamDefTable(self):
        knownStreams = {}
        knownStreamVersions = {}
        try:
            with open(self.info_file, 'r') as f:
                json_data = f.read()
            knownStreams = json.loads(json_data)
        except IOError:
            self.logger.debug("Info file `{}` not found".format(self.info_file))
            knownStreams = {}

        for stream in knownStreams:
            knownStreamVersions[stream] = knownStreams[stream]['definition']

        self.knownStreams=knownStreams
        self.knownStreamVersions=knownStreamVersions
        self.print_stream_info()

    def createNewStreamDestination(self,stream_obj):
        stream = stream_obj["stream"]
        version = stream_obj["version"]
        stream_path = os.path.join(os.path.join(self.data_path,stream))
        if version == 1:    # create a new stream group under root
            os.mkdir(stream_path)

        # create a new subgroup for this instance of the current stream
        version_dir = stream + '_' + str(version)
        stream_ver = os.path.join( stream_path, version_dir )
        os.mkdir( os.path.join(stream_path, version_dir) )
        # point currentVersion.txt file at most current version
        with open( os.path.join(stream_path, 'currentVersion.txt'), 'w' ) as f:
            f.write(version_dir)
        # update the main info file
        with open(self.info_file, 'w') as json_data:
            json_data.write(json.dumps(self.knownStreams))
        # create the stream field definition dict file
        with open( os.path.join(stream_path, version_dir, 'definition.json'), 'w' ) as f:
            f.write( json.dumps(stream_obj['definition']) )
        return stream_obj['id']

    def insertMeasurement(self,stream,measurements):
        current_stream_version = getCurrentStreamVersion(self.config,stream)
        for field in measurements:
            with open( os.path.join(current_stream_version, field), 'a' ) as f:
                f.write( str(measurements[field]) + '\n')

    # read stream data from storage between the timestamps given by time = [start,stop]
    # only takes timestamps in seconds
    def getRawStreamData(self,stream,start=None,stop=None,definition=None):
        start, stop = self.validateTimeRange(start,stop)
        # get data from buffer
        try:
            current_stream_version = getCurrentStreamVersion(self.config,stream)
        except IOError:
            self.logger.debug("Read requested on empty dataset. stream: {}", stream)
            raise IndexError
        if definition is None:
            definition = self.knownStreamVersions[stream]

        start_idx = None
        stop_idx = None
        try:
            tsfile = os.path.join(current_stream_version,timestamp)
            f = open(tsfile,'r')
            i = 0
            data = { timestamp : [] }
            dtype = data_types[self.config.get('Server','timestamp_type')]['type']
            while True:
                x = f.readline()
                x = x.strip()
                if not x: break
                x = dtype(x) 
                if (start_idx is None) and (x >= start):
                    start_idx = i
                if (start_idx is not None) and (x >= stop):
                    stop_idx = i
                    break
                if start_idx is not None:
                    data[timestamp].append(x)
                i += 1
            if (start_idx is not None) and (stop_idx is None):
                stop_idx = i

        except ValueError:
            self.logger.error("Error casting timestamp to type long. x=`{}` in {}".format(x, tsfile))
        except:
            self.logger.error("Unexpected exception reading from data file. Message code:")
            self.logger.error(traceback.print_exc(file=sys.stdout))
        finally:
            f.close()

        if (start_idx is None) or (stop_idx is None):
            msg = "error in indexing (index start, index stop): ({},{})"
            self.logger.warning(msg.format(start_idx,stop_idx))
            raise IndexError

        self.logger.debug((start_idx,stop_idx))
        for field in definition:
            data[field] = []
            dtype = self.knownStreamVersions[stream][field]['type']
            type_cast = data_types[dtype]['type']
            try:
                f = open(os.path.join(current_stream_version,field),'r')
                i = 0
                while i <= stop_idx:
                    x = f.readline()
                    if i >= start_idx:
                        x = type_cast(x.strip())
                        data[field].append(x)
                    i += 1
            except ValueError:
                pass # reached EOF
            except:
                self.logger.error("Unexpected exception reading from data file. Message code:")
                self.logger.error(traceback.print_exc(file=sys.stdout))
                raise IndexError
            finally:
                f.close()
        return data
