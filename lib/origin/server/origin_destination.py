from origin.server import template_validation
from origin.server import measurement_validation
from origin import data_types, current_time, timestamp

import struct
import numpy as np
import sys

################################################################################
#
#   Metadata format:
#   
#   knownStreams : {
#       `stream name` : {
#           stream     : `stream_name`,
#           id         : `streamID`,
#           version    : `version`,
#           keyOrder   : `keyOrder`,
#           formatStr  : `formatStr`,
#           definition : { # most recent definition object
#               `field1` : { "type":data_type, "keyIndex": `index` },
#               `field2` : { "type":data_type, "keyIndex": `index` },
#               ...
#           }
#           versions    : { # optional dict of older versions, version number is the hash
#               1   : `defintion_obj`, # see above for definition format
#               2   : `defintion_obj`,
#               ... 
#           }            
#       },
#       ...
#   }
#
#   # current versiion definitions
#   knownStreamVersions : {
#       stream  : `definition_obj,
#       stream  : `definition_obj,
#       ...
#   }
#
################################################################################


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
        # generate formatStr for stream if possible, strings are not supported in binary
        # do early to trigger exception before write to disk
        err, formatStr = self.formatString(template,keyOrder)
        if err > 0:
            formatStr = ''
        streamID = self.get_streamID(stream)

        definition = {}
        for i, key in enumerate(keyOrder):
            k = key.strip()
            definition[k] = {"type": template[k], "keyIndex": i}

        if version > 1:
            stream_obj = self.knownStreams[stream]
        else:
            stream_obj = { 
                "stream"     : stream.strip(), 
                "id"         : streamID, 
                "versions"   : []
            }

        stream_obj["version"] = version
        stream_obj["keyOrder"] = keyOrder 
        stream_obj["formatStr"] = formatStr
        stream_obj["definition"] = definition
        stream_obj["versions"].append({
            "version"    : version, 
            "keyOrder"   : keyOrder,
            "formatStr"  : formatStr,
            "definition" : definition,
        })

        # update the stream inventory
        self.knownStreams[stream] = stream_obj
        self.knownStreamVersions[stream] = stream_obj['definition']
        streamID = self.createNewStreamDestination(stream_obj) # id might get updated
        return streamID

    # creates a new stream or creates a new version of a stream based on template. 
    # also enters formatStr into the knownStreams dict
    # return streamID
    def createNewStreamDestintion(self,stream_obj):
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
        stream = stream.strip()
        self.logger.info("Attempt to register stream %s"%(stream))
        if stream not in self.knownStreams.keys():
            update = True
            destVersion = 1
        else:
            streamID = self.knownStreams[stream]["id"]
            destVersion = self.knownStreams[stream]["version"]
            if template_validation(template,self.knownStreamVersions[stream]):
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

        if not measurement_validation(measurements,self.knownStreamVersions[stream]):
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
        return (result,resultText,measurements)

    def measurementOrdered(self,stream,ts,measurements):
        meas = {}
        for key in self.knownStreamVersions[stream]:
          idx = self.knownStreamVersions[stream][key]["keyIndex"]
          meas[key] = measurements[idx]
	meas[timestamp] = ts
        return self.measurement(stream,meas)

    def measurementBinary(self,stream,measurements):
        dtuple = struct.unpack_from(self.knownStreams[stream]["formatStr"], measurements)
        meas = list(dtuple[1:])
	ts = dtuple[0]
        return self.measurementOrdered(stream,ts,meas)

    def findStream(self, streamID):
        for stream in self.knownStreams:
            if self.knownStreams[stream]["id"] == streamID:
                return stream
        raise ValueError

    # read stream data from storage between the timestamps given by time = [start,stop]
    def getRawStreamData(self,stream,start=None,stop=None):
        raise NotImplementedError
        
    # read stream.field data from storage between the timestamps given by time = [start,stop]
    def getRawStreamFieldData(self,stream,field,start=None,stop=None):
        # send dummy dict with single field
        return self.getRawStreamData(stream=stream,start=start,stop=stop, definition={field:''}) 

    # get statistics on the stream data during the time window time = [start, stop]
    def getStatStreamData(self,stream,start=None,stop=None):
        try:
            streamData = self.getRawStreamData(stream,start,stop)
            data = {}
            for field in streamData:
                if field == timestamp:
                    data[field] = {'start': streamData[field][0], 'stop': streamData[field][1]}
                elif self.knownStreamVersions[stream][field]['type'] == 'string':
                    data[field] = streamData[field] # TODO: figure out how to handle this
                else:
                    avg = np.nanmean(streamData[field])
                    std = np.nanstd(streamData[field])
                    max = np.nanmax(streamData[field])
                    min = np.nanmin(streamData[field])
                    data[field] = { 'average': avg, 'standard_deviation': std, 'max': max, 'min': min }
            result, resultText = (0,data)
        except (ValueError, IndexError):
            msg = "No data in requested time window."
            result, resultText = (1, dict(error=msg))
        except KeyError:
            msg = "Requested stream `{}` does not exist.".format(stream)
            self.logger.info(msg)
            result, resultText = (1, dict(streams=self.knownStreams, error=msg))
        except:
            self.logger.exception("Exception in server code:")
            msg = "Could not process request."
            result, resultText = (1, dict(streams=self.knownStreams, error=msg))
        finally:
            return (result,resultText)

    # get statistics on the stream.field data during the time window time = [start, stop]
    def getStatStreamFieldData(self,stream,field,start=None,stop=None):
      try:
        fieldData = self.getRawStreamFieldData(stream,field,start,stop)
        data = {}
        if self.knownStreamVersions[stream][field]['type'] == 'string':
            data[field] = fieldData[field] # TODO: figure out how to handle this
        else:
            avg = np.nanmean(fieldData[field])
            std = np.nanstd(fieldData[field])
            max = np.nanmax(fieldData[field])
            min = np.nanmin(fieldData[field])
            data[field] = { 'average': avg, 'standard_deviation': std, 'max': max, 'min': min }
        result, resultText = (0,data)
      except (ValueError, IndexError):
        msg = "No data in requested time window."
        result, resultText = (1, dict(error=msg))
      except KeyError:
        msg = "Requested stream `{}` does not exist.".format(stream)
        self.logger.info(msg)
        result, resultText = (1, dict(streams=self.knownStreams, error=msg))
      except NotImplementedError:
        msg = "Requested stream field `{}.{}` does not exist.".format(stream, field)
        self.logger.info(msg)
        result, resultText = (1, dict(streams=self.knownStreams, error=msg))
      except:
        self.logger.exception("Exception in server code:")
        msg = "Could not process request."
        result, resultText = (1, dict(streams=self.knownStreams, error=msg))
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
            self.logger.warning("Requested read range out of order. Swapping range.")
            self.logger.debug("Read request time range (start, stop): ({},{})".format(start,stop))
            return (stop, start)
        else:
            self.logger.debug("Read request time range (start, stop): ({},{})".format(start,stop))
            return (start, stop)

    def print_stream_info(self):
        for stream in self.knownStreamVersions:
            self.logger.info("")
            self.logger.info("="*20 + " {} ".format(stream) + "="*20)
            self.logger.info("  StreamID: {}".format(self.knownStreams[stream]['id']))
            for field_name in self.knownStreamVersions[stream]:
                self.logger.info("  Field: {} ({})".format(
                    field_name, 
                    self.knownStreamVersions[stream][field_name]['type'])
                )
        self.logger.info("")

    def get_streamID(self,stream):
        if stream in self.knownStreams:
            return self.knownStreams[stream]['id']
        streamID = 0
        for s in self.knownStreams:
            sid = self.knownStreams[s]['id']
            if sid > streamID:
                streamID = sid
        return streamID + 1
