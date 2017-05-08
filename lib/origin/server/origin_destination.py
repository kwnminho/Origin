"""
This module provides the Destination class that holds all the basic API methods for interfacing
with a destination. 

Destintations are databases, filesystems, specific file formats (HDF5, CSV), etc.
"""

import struct
import numpy as np


from origin.server import template_validation
from origin.server import measurement_validation
from origin import data_types, current_time, TIMESTAMP, registration_validation

################################################################################
#
#   Metadata format:
#   
#   knownStreams : {
#       `stream name` : {
#           stream     : `stream_name`,
#           id         : `stream_id`,
#           version    : `version`,
#           key_order  : `key_order`,
#           format_str : `format_str`,
#           definition : { # most recent definition object
#               `field1` : { "type":data_type, "key_index": `index` },
#               `field2` : { "type":data_type, "key_index": `index` },
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


class Destination(object):
    '''A class representing a data storage location, such as a database, filesystem,
    or file format.

    This class defines a common API that a real destination class must inherit from.
    Destination specific methods that are required will raise a NotImplementedError if not
    overridden.
    '''

    def __init__(self, logger, config):
        self.logger = logger
        self.config = config
        self.known_streams = {}
        self.known_stream_versions = {}

        self.connect()
        self.read_stream_def_table()

    def connect(self):
        '''connect/open (w/e) database/file'''
        raise NotImplementedError

    def close(self):
        '''Disconnects and prepares to stop'''
        raise NotImplementedError

    def read_stream_def_table(self):
        '''reads stored metadata and fills into the knownStreams, and 
        knownStreamVersions dictionaries
        '''
        raise NotImplementedError
    
    def create_new_stream(self, stream, version, template, key_order):
        '''Creates a new stream or creates a new version of a stream based on template.
        also enters format_str into the knownStreams dict.
        return streamID
        '''

        # generate format_str for stream if possible, strings are not supported in binary
        # do early to trigger exception before write to disk
        err, format_str = self.format_string(template, key_order)
        if err > 0:
            format_str = ''
        stream_id = self.get_stream_id(stream)

        definition = {}

        if key_order is not None:
            for i, key in enumerate(key_order):
                k = key.strip()
                definition[k] = {"type": template[k], "key_index": i}
        else:
            for key in template:
                k = key.strip()
                definition[k] = {"type": template[k], "key_index": -1}

        if version > 1:
            stream_obj = self.known_streams[stream]
        else:
            stream_obj = { 
                "stream"     : stream.strip(), 
                "id"         : stream_id, 
                "versions"   : []
            }

        stream_obj["version"] = version
        stream_obj["key_order"] = key_order 
        stream_obj["format_str"] = format_str
        stream_obj["definition"] = definition
        stream_obj["versions"].append({
            "version"    : version, 
            "key_order"   : key_order,
            "format_str"  : format_str,
            "definition" : definition,
        })

        # update the stream inventory
        self.known_streams[stream] = stream_obj
        self.known_stream_versions[stream] = stream_obj['definition']
        stream_id = self.create_new_stream_destination(stream_obj) # id might get updated
        return stream_id

    def create_new_stream_destination(self, stream_obj):
        '''Store a new stream or creates a new version of a stream in the destination.
        return streamID
        '''
        raise NotImplementedError

    def format_string(self, template, key_order):
        '''Generates a format string for unpacking native data packets.
        Returns a tuple of (error, format_str) where error=0 for success.
        '''
        if key_order is None:
            return (1, "No key_order specified")

        format_str = '!' # use network byte order
        try:
            format_str += data_types[self.config.get("Server", "timestamp_type")]["format_char"]
        except KeyError:
            format_str += data_types["uint"]["format_char"]

        for key in key_order:
            self.logger.debug('key: %s', key)
            dtype = template[key]
            try:
                format_str += data_types[dtype]["format_char"]
                if not data_types[dtype]["binary_allowed"]:
                    return (1, "Unsupported type '{}' in binary data.".format(dtype))
            except KeyError:
                return (1, "Type \"{}\" not recognized.".format(dtype))
        return (0, format_str)

    # returns version and streamID
    def register_stream(self, stream, template, key_order=None):
        '''Register a new stream or new version of a new stream with the server.
        Returns a tuple of (error, stream_ver) where error=0 for success, and 
        stream_ver is a byte string that serves as a unique identifier.
        '''
        update = False
        dest_version = None
        stream = stream.strip()
        self.logger.info("Attempt to register stream %s"%(stream))
        if stream not in self.known_streams.keys():
            update = True
            dest_version = 1
        else:
            stream_id = self.known_streams[stream]["id"]
            dest_version = self.known_streams[stream]["version"]
            if template_validation(template, self.known_stream_versions[stream]):
                msg = "Known stream, {} matching current defintion."
                msg += " No database modification needed."
                self.logger.info(msg.format(stream))
                update = False
            # its a new version
            else:
                update = True
                dest_version += 1

        if update:
            valid, msg = registration_validation(stream, template, key_order)
            if not valid:
                return (1, msg)
            stream_id = self.create_new_stream(stream, dest_version, template, key_order)
            # update the current streams after all that
            self.read_stream_def_table()
        return (0, struct.pack("!II", stream_id, dest_version))

    def insert_measurement(self, stream, measurements):
        '''Save formated measurement to the destination.'''
        raise NotImplementedError

    def measurement(self, stream, measurements):
        '''Perfoms measurement validation, timestamps data if missing,
        Then saves to destination.

        Returns a tuple of (error, result_text, measurements) where error=0 for success
        error: 0 for successful operation
        result_text: message to return to client
        measurements: processed data, empty dict if error
        '''
        if stream not in self.known_streams.keys():
            msg = "trying to add a measurement to data on an unknown stream: {}"
            self.logger.warning(msg.format(stream))
            return (1, "Unknown stream", {})

        if not measurement_validation(measurements, self.known_stream_versions[stream]):
            self.logger.warning("Measurement didn't validate against the pre-determined format")
            return (1, "Invalid measurements against schema", {})

        try:
            if measurements[TIMESTAMP] == 0:
                raise KeyError
        except KeyError:
            measurements[TIMESTAMP] = current_time(self.config)

        self.insert_measurement(stream, measurements)
        result = 0
        result_text = ""
        return (result, result_text, measurements)

    def measurement_ordered(self, stream, time_stamp, measurements):
        '''Process a list of implicitly ordered measurements, then save to destination.

        Returns a tuple of (error, result_text, measurements) where error=0 for success
        error: 0 for successful operation
        result_text: message to return to client
        measurements: processed data, empty dict if error
        '''
        meas = {}
        for key in self.known_stream_versions[stream]:
            idx = self.known_stream_versions[stream][key]["key_index"]
            meas[key] = measurements[idx]
        meas[TIMESTAMP] = time_stamp
        return self.measurement(stream, meas)

    def measurement_binary(self, stream, measurements):
        '''Process a binary list of implicitly ordered measurements, then save to destination.

        Returns a tuple of (error, result_text, measurements) where error=0 for success
        error: 0 for successful operation
        result_text: message to return to client
        measurements: processed data, empty dict if error
        '''
        dtuple = struct.unpack_from(self.known_streams[stream]["format_str"], measurements)
        meas = list(dtuple[1:])
        time_stamp = dtuple[0]
        return self.measurement_ordered(stream, time_stamp, meas)

    def find_stream(self, stream_id):
        '''Look up stream based on the stream id number'''
        for stream in self.known_streams:
            if self.known_streams[stream]["id"] == stream_id:
                return stream
        raise ValueError

    def get_raw_stream_data(self, stream, start=None, stop=None, definition=None):
        '''read stream data from storage between the timestamps
        given by time = [start,stop]
        Returns a tuple with (error, data, msg), error is 0 for success, 
        msg holds error msg or '', data is dictionary with fields as the keys and
        data lists as the values
        '''
        raise NotImplementedError
        
    def get_raw_stream_field_data(self, stream, field, start=None, stop=None):
        '''read stream.field data from storage between the timestamps 
        given by time = [start,stop]
        Returns a tuple with (error, data, msg), error is 0 for success, 
        msg holds error msg or '', data is dictionary with fields as the keys and
        data lists as the values
        '''
        return self.get_raw_stream_data(stream, start=start, stop=stop, definition={field:''}) 

    def get_stat_stream_data(self, stream, start=None, stop=None):
        '''Get statistics on the stream data during the time window time = [start, stop]
        Returns a tuple with (error, data, msg), error is 0 for success, 
        msg holds error msg or '', data is dictionary with fields as the keys and
        statistical data sub-dictionaries as the values
        '''
        try:
            result, stream_data, result_text = self.get_raw_stream_data(stream, start, stop)
            data = {}
            for field in stream_data:
                if field == TIMESTAMP:
                    data[field] = {'start': stream_data[field][0], 'stop': stream_data[field][1]}
                elif self.known_stream_versions[stream][field]['type'] == 'string':
                    data[field] = stream_data[field] # TODO: figure out how to handle this
                else:
                    # some stats need to be converted back to the native python 
                    # type for JSON serialization
                    dtype = data_types[self.known_stream_versions[stream][field]['type']]["type"]
                    avg = np.nanmean(stream_data[field])
                    std = np.nanstd(stream_data[field])
                    max = dtype(np.nanmax(stream_data[field]))
                    min = dtype(np.nanmin(stream_data[field]))
                    data[field] = {
                        'average': avg,
                        'standard_deviation': std, 
                        'max': max, 
                        'min': min
                    }

        except Exception:
            self.logger.exception("Exception in server code:")
            msg = "Could not process request."
            result, data, result_text = (1, {}, msg)
        
        return(result, data, result_text)

    def get_stat_stream_field_data(self, stream, field, start=None, stop=None):
        '''get statistics on the stream.field data during the time window 
        time = [start, stop]
        '''
        try:
            result, field_data, result_text = self.get_raw_stream_field_data(stream, field, start, stop)
            data = {}
            if self.known_stream_versions[stream][field]['type'] == 'string':
                data[field] = field_data[field] # TODO: figure out how to handle this
            else:
                # some stats need to be converted back to the native python type for JSON
                #  serialization
                dtype = data_types[self.known_stream_versions[stream][field]['type']]["type"]
                avg = np.nanmean(field_data[field])
                std = np.nanstd(field_data[field])
                max = dtype(np.nanmax(field_data[field]))
                min = dtype(np.nanmin(field_data[field]))
                data[field] = {
                    'average': avg,
                    'standard_deviation': std,
                    'max': max,
                    'min': min
                }

        except Exception:
            self.logger.exception("Exception in server code:")
            msg = "Could not process request."
            result, data, result_text = (1, {}, msg)

        return(result, data, result_text)

    def validate_time_range(self, start, stop):
        '''Make sure time range is valid, if not rearrange start and stop times
        Returns tuple of validated (start, stop) times in 32b format
        '''
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
            self.logger.debug("Read request time range (start, stop): ({},{})".format(start, stop))
            return (stop, start)

        self.logger.debug("Read request time range (start, stop): ({},{})".format(start, stop))
        return (start, stop)

    def print_stream_info(self):
        '''Print user readable display of current streams in destination'''
        for stream in self.known_stream_versions:
            self.logger.info("")
            self.logger.info("="*20 + " {} ".format(stream) + "="*20)
            self.logger.info("  stream_id: {}".format(self.known_streams[stream]['id']))
            for field_name in self.known_stream_versions[stream]:
                self.logger.info("  field: {} ({})".format(
                    field_name, 
                    self.known_stream_versions[stream][field_name]['type']
                    ))
        self.logger.info("")

    def get_stream_id(self, stream):
        '''Generate a new stream id by dynamically checking the id of all 
        known streams and incrementing
        ''' 
        if stream in self.known_streams:
            return self.known_streams[stream]['id']
        stream_id = 0
        for s in self.known_streams:
            sid = self.known_streams[s]['id']
            if sid > stream_id:
                stream_id = sid
        return stream_id + 1
