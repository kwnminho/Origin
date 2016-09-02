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
        try:
            with open(self.info_file, 'r') as f:
                json_data = f.read()
            self.knownStreamVersions = json.loads(json_data)
        except IOError:
            self.logger.debug("Info file `{}` not found".format(self.info_file))
            self.knownStreamVersions = {}

        self.knownStreams = {}
        dir_list = getDirectoryList(self.data_path)
        for stream in self.knownStreamVersions:
            if stream in dir_list:
                current_stream_version = getCurrentStreamVersion(self.config,stream)
                with open(os.path.join(current_stream_version,'definition.json'), 'r') as f:
                    json_data = f.read()
                self.knownStreams[stream] = json.loads(json_data)
            else:
                self.logger.error("Stream '{}' found in stream list, but is not a group")

        for stream in self.knownStreams:
            print "="*10, " {} ".format(stream), "="*10
            for field_name in self.knownStreamVersions[stream]:
                print "  Field: %s (%s)"%(field_name,self.knownStreamVersions[stream][field_name])

    def createNewStream(self,stream,version,template,keyOrder):
        # generate formatStr for stream if possible, strings are not supported in binary
        # do early to trigger exception before write to disk
        err, formatStr = self.formatString(template,keyOrder)
        if err > 0:
            formatStr = ''

        stream_path = os.path.join(os.path.join(self.data_path,stream))
        if version == 1:    # create a new stream group under root
            os.mkdir(stream_path)
            streamID = len(getDirectoryList(self.data_path))
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
                "formatStr" : formatStr
        }
        with open(self.info_file, 'w') as json_data:
            json_data.write(json.dumps(self.knownStreamVersions))
        # create the stream field definition dict
        definition = {}
        for i, key in enumerate(keyOrder):
            definition[key] = { "type": template[key], "keyIndex": i }
        with open( os.path.join(stream_path, version_dir, 'definition.json'), 'w' ) as f:
            f.write( json.dumps(definition) )
        return streamID

    def insertMeasurement(self,stream,measurements):
        current_stream_version = getCurrentStreamVersion(self.config,stream)
        for field in measurements:
            with open( os.path.join(current_stream_version, field), 'a' ) as f:
                f.write( str(measurements[field]) + '\n')

    # read stream data from storage between the timestamps given by time = [start,stop]
    # only takes timestamps in seconds
    def getRawStreamData(self,stream,start=None,stop=None,definition=None):
        start, stop = self.validateTimeRange(start,stop)
        self.logger.debug("Read request time range (start, stop): ({},{})".format(start,stop))
        # get data from buffer
        current_stream_version = getCurrentStreamVersion(self.config,stream)
        if definition is None:
            definition = self.knownStreams[stream]

        start_idx = None
        stop_idx = None
        try:
            tsfile = os.path.join(current_stream_version,timestamp)
            f = open(tsfile,'r')
            i = 0
            data = {}
            data[timestamp] = []
            while True:
                x = f.readline()
                x = x.strip()
                if not x: 
                    break
                x = long(x) 
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
            dtype = self.knownStreams[stream][field]['type']
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
