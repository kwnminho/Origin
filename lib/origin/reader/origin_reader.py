"""
This module provides a client subscription class that holds all the basic API
methods for subscribing to a data stream.
"""

import zmq
import json
import logging

log = logging.getLogger(__name__)


class Reader(object):
    """!@brief A class representing a data stream reader to a data server.

    This class handles asynchronous read events with a data server.
    """

    def __init__(self, config):
        """!@brief Initialize the subscriber

        @param config is a ConfigParser object
        """
        self.config = config
        self.connected = False
        self.known_streams = {}
        self.stream_list = []
        self.setup()
        self.connect()

        if self.connected:
            self.get_available_streams()

    def connect(self):
        """!@brief Open a connection to the data server on the read port.
        """
        try:
            self.socket.connect("tcp://{}:{}".format(self.ip, self.port))
        except:
            log.exception("Error connecting to data server")
            self.connected = False
        else:
            self.connected = True

    def get_available_streams(self):
        """!@brief Request the knownStreams object from the server.

        @return knownStreams
        """
        # Sending an empty JSON object requests an object containing the
        # available streams
        self.socket.send('{}')
        try:
            err, known_streams = json.loads(self.socket.recv())
        except:
            log.exception("Error connecting to data server")
        else:
            self.update_known_streams(known_streams['streams'])
        return self.known_streams

    def get_stream_data(self, stream, start=None, stop=None, fields=[], raw=False):
        """!@brief Request raw stream data in time window from sever.

        @param stream A string holding the stream name
        @param start 32b Unix timestamp that defines the start of the data
            window
        @param stop 32b Unix timestamp that defines the end of the data window
        @param fields A list of fields from the stream that should be returned
        @return data A dictionary containing data for each field in
            the time window
        """
        if not self.is_stream(stream):
            raise KeyError

        request = {
            'stream': stream.strip(),
            'start' : start,
            'stop'  : stop,
            'raw'   : raw,
        }
        if fields != []:
            if self.is_fields(stream, fields):
                request['fields'] = fields
            else:
                log.error('There was an issue with the specified fields.')
                return {}
        self.socket.send(json.dumps(request))
        try:
            msg = self.socket.recv()
            data = json.loads(msg)
        except:
            msg = "There was an error communicating with the server"
            log.exception(msg)
            data = (1, {'error': msg, 'stream': {}})

        if data[0] != 0:
            msg = "The server responds to the request with error message: `{}`"
            log.error(msg.format(data[1]["error"]))
            known_streams = data[1]['stream']
            if known_streams != {}:
                log.info('Updating stream definitions from server.')
                self.update_known_streams(known_streams)
            return {}
        else:
            return data[1]

    def get_stream_raw_data(self, stream, start=None, stop=None, fields=[]):
        """!@brief Request raw stream data in time window from sever.

        @param stream A string holding the stream name
        @param start 32b Unix timestamp that defines the start of the data
            window
        @param stop 32b Unix timestamp that defines the end of the data window
        @param fields A list of fields from the stream that should be returned
        @return data A dictionary containing raw data for each field in
            the time window
        """
        return self.get_stream_data(stream, start=start, stop=stop, fields=fields, raw=True)

    def get_stream_stat_data(self, stream, start=None, stop=None, fields=[]):
        """!@brief Request stream data statistics in time window from sever.

        @param stream A string holding the stream name
        @param start 32b Unix timestamp that defines the start of the data
            window
        @param stop 32b Unix timestamp that defines the end of the data window
        @param fields A list of fields from the stream that should be returned
        @return data A dictionary containing statistical data for each field in
            the time window
        """
        return self.get_stream_data(stream, start=start, stop=stop, fields=fields, raw=False)

    def is_fields(self, stream, fields):
        """!@brief Check that all the requested fields exist in the stream.

        @param stream A string holding the stream name
        @param fields A list of strings holding the field names
        @return True if fields are defined in stream, False otherwise
        """
        ok = True
        for field in fields:
            if field in self.known_streams[stream]:
                ok = False
                msg = "field: `{}` not listed in known_streams['{}']"
                log.warning(msg.format(field, stream))
        return ok

    def is_stream(self, stream):
        """!@brief Check that the requested stream exists on the server.

        @param stream A string holding the stream name
        @return True if stream is in known_streams, False otherwise
        """
        return stream.strip() in self.stream_list

    def setup(self):
        """!@brief extract configuration settings from the config object.
        """
        self.ip = self.config.get('Server', 'ip')
        self.port = self.config.getint('Server', 'read_port')
        self.timeout = self.config.getint('Reader', 'timeout')
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)

    def update_known_streams(self, known_streams):
        """@!brief Update the known_streams defintion with new data.

        @param known_streams A dictionary containing the streams that exist on
            the server.
        """
        self.known_streams = known_streams
        self.stream_list = self.known_streams.keys()
