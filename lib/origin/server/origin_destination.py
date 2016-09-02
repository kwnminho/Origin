from origin.server import template_validation
from origin.server import measurement_validation
from origin import data_types, current_time, timestamp

import struct
import numpy as np
import sys

class destination:
    def __init__(self,logger,config):
        self.logger = logger
        self.config = config
        self.connect()
        self.readStreamDefTable()

    # connect/open (w/e) database/file
    def connect(self):
        raise NotImplementedError

    # reads stored metadata and fills into the knownStreams, and knownStreamVersions dictionaries
    def readStreamDefTable(self):
        raise NotImplementedError
    
    # creates a new stream or creates a new version of a stream based on template. 
    # also enters formatStr into the knownStreams dict
    # return streamID
    def createNewStream(self,stream,version,template,keyOrder):
        raise NotImplementedError

    def formatString(self,template,keyOrder):
        formatStr = '!' # use network byte order
        try:
            formatStr += data_types[self.config.get("Server","timestamp_type")]["format_char"]
        except KeyError:
            formatStr += data_types["uint"]["format_char"]

        for key in keyOrder:
          dtype = template[key]
          try:
            formatStr += data_types[dtype]["format_char"]
            if not data_types[dtype]["binary_allowed"]:
              return (1,"Unsupported type '{}' in binary data.".format(dtype))
          except KeyError:
            return (1,"Type \"{}\" not recognized.".format(dtype))
        return (0, formatStr)

    # returns version and streamID
    def registerStream(self,stream,template,keyOrder=None):
        update = False
        destVersion = None
        self.logger.info("Attempt to register stream %s"%(stream))
        if stream not in self.knownStreams.keys():
            update = True
            destVersion = 1
        else:
            streamID = self.knownStreamVersions[stream]["id"]
            destVersion = self.knownStreamVersions[stream]["version"]
            if template_validation(template,self.knownStreams[stream]):
                self.logger.info("Known stream, %s matching current defintion, so no database modification needed"%(stream))
                update = False
            else:
                update = True
                destVersion += 1
        if update:
            streamID = self.createNewStream(stream,destVersion,template,keyOrder)
            # update the current streams after all that
            self.readStreamDefTable()
        return (0, struct.pack("!II",destVersion,streamID))

    def insertMeasurement(self,stream,measurements):
        raise NotImplementedError

    def measurement(self,stream,measurements):
        if stream not in self.knownStreams.keys():
            self.logger.warning("trying to add a measurement to data on an unknown stream: {}".format(stream))
            return (1,"Unknown stream")

        if not measurement_validation(measurements,self.knownStreams[stream]):
            self.logger.warning("Measurement didn't validate against the pre-determined format")
            return (1,"Invalid measurements against schema")

        try:
            if measurements[timestamp] == 0:
                raise KeyError
        except KeyError:
            measurements[timestamp] = current_time(self.config)

        self.insertMeasurement(stream,measurements)
        result = 0
        resultText = ""
        return (result,resultText)

    def measurementOrdered(self,stream,ts,measurements):
        meas = {}
        for key in self.knownStreams[stream]:
          idx = self.knownStreams[stream][key]["keyIndex"]
          meas[key] = measurements[idx]
	meas[timestamp] = ts
        return self.measurement(stream,meas)

    def measurementBinary(self,stream,measurements):
        dtuple = struct.unpack_from(self.knownStreamVersions[stream]["formatStr"], measurements)
        meas = list(dtuple[1:])
	ts = dtuple[0]
        return self.measurementOrdered(stream,ts,meas)

    def findStream(self, streamID):
        for stream in self.knownStreamVersions:
            if self.knownStreamVersions[stream]["id"] == streamID:
                return stream
        raise ValueError

    # read stream data from storage between the timestamps given by time = [start,stop]
    def getRawStreamData(self,stream,start=None,stop=None):
        raise NotImplementedError
        
    # read stream.field data from storage between the timestamps given by time = [start,stop]
    def getRawStreamFieldData(self,stream,field,start=None,stop=None):
        raise NotImplementedError
        
    # get statistics on the stream data during the time window time = [start, stop]
    def getStatStreamData(self,stream,start=None,stop=None):
        try:
            streamData = self.getRawStreamData(stream,start,stop)
            data = {}
            for field in streamData:
                if field == timestamp:
                    data[field] = {'start': streamData[field][0], 'stop': streamData[field][1]}
                elif self.knownStreams[stream][field]['type'] == 'string':
                    data[field] = streamData[field] # TODO: figure out how to handle this
                else:
                    avg = np.nanmean(streamData[field])
                    std = np.nanstd(streamData[field])
                    max = np.nanmax(streamData[field])
                    min = np.nanmin(streamData[field])
                    data[field] = { 'average': avg, 'standard_deviation': std, 'max': max, 'min': min }
            result, resultText = (0,data)
        except:
            self.logger.exception("Exception in server code:")
            result, resultText = (1,"Could not process request.")
        finally:
            return (result,resultText)

    # get statistics on the stream.field data during the time window time = [start, stop]
    def getStatStreamFieldData(self,stream,field,start=None,stop=None):
      try:
        fieldData = self.getRawStreamFieldData(stream,field,start,stop)
        data = {}
        if self.knownStreams[stream][field]['type'] == 'string':
            data[field] = fieldData[field] # TODO: figure out how to handle this
        else:
            avg = np.nanmean(fieldData[field])
            std = np.nanstd(fieldData[field])
            max = np.nanmax(fieldData[field])
            min = np.nanmin(fieldData[field])
            data[field] = { 'average': avg, 'standard_deviation': std, 'max': max, 'min': min }
        result, resultText = (0,data)
      except:
        self.logger.exception("Exception in server code:")
        result, resultText = (1,"Could not process request.")
      finally:
        return (result,resultText)

    def validateTimeRange(self,start,stop):
        try:
            stop = long(stop)*2**32
        except TypeError:
            self.logger.debug("Using default stop time")
            stop = current_time(self.config)
        try:
            start = long(start)*2**32
        except TypeError:
            self.logger.debug("Using default start time")
            start = stop - 5*60L*2**32 # 5 minute range default
        if start > stop:
            self.logger.info("Requested read range out of order. Swapping range.")
            return (stop, start)
        else:
            return (start, stop)
