'''
Unit tests for origin destination classes
'''

import sys
from os import path, getcwd, remove
import logging
import ConfigParser
import struct

import pytest 

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()

FULL_BIN_PATH = path.abspath(getcwd())
FULL_BASE_PATH = path.dirname(FULL_BIN_PATH)
FULL_LIB_PATH = path.join(FULL_BASE_PATH, "lib")
logger.warning(FULL_BASE_PATH)
FULL_CFG_PATH = path.join(FULL_BASE_PATH, "config")
sys.path.append(FULL_LIB_PATH)

logger.debug("FULL_BIN_PATH: %s", FULL_BIN_PATH)
logger.debug("FULL_BASE_PATH: %s", FULL_BASE_PATH)
logger.info("FULL_LIB_PATH: %s", FULL_LIB_PATH)
logger.debug("FULL_CFG_PATH: %s", FULL_CFG_PATH)

CONFIGFILE = path.join(FULL_CFG_PATH, "origin-server-test.cfg")
print(FULL_CFG_PATH)
CONFIG = ConfigParser.ConfigParser()
CONFIG.read(CONFIGFILE)

CONFIG.set('Server', 'base_path', FULL_BASE_PATH) 

class TestDest(object):

    def setup(self):
        ''' runs before every parameterized function and initializes the destination'''
        tmpdir = path.join(FULL_BASE_PATH, "var", "test")
        CONFIG.set("Server", "var_path", tmpdir)
        dest = CONFIG.get("Server", "destination").lower()
        logger.debug("Testing with destination: %s", dest)

        if  dest == "mysql":
            from origin.server import MySQLDestination
            self.dest = MySQLDestination(logger, CONFIG)

        elif dest == "hdf5":
            from origin.server import HDF5Destination
            self.dest = HDF5Destination(logger, CONFIG)

        elif dest == "filesystem":
            from origin.server import FilesystemDestination
            self.dest = FilesystemDestination(logger, CONFIG)

        elif dest == "mongodb":
            from origin.server import MongoDBDestination
            self.dest = MongoDBDestination(logger, CONFIG)

        elif dest == '':
            logger.critical("No destination specified in configs. Killing server...")
            sys.exit(1)
            
        else:
            logger.critical("Unrecognized destination %s specified. Killing server...", dest)
            sys.exit(1)

    def teardown(self):
        ''' runs after every parameterized function and clears the datafile'''
        self.dest.close()
        data_path = CONFIG.get("Server", "var_path")
        if CONFIG.get("Server", "destination") == "hdf5":
            filename = path.join(
                data_path, 
                CONFIG.get("HDF5", "data_path"), 
                CONFIG.get("HDF5", "data_file")
            )
            remove(filename)


    @pytest.mark.parametrize("stream,template,key_order,expected_id,expected_ver", [
        ("test", {'key1':'int', 'key2':'float'}, ['key1', 'key2'], 1, 1),
        ("test_2", {'key3':'int8', 'key4':'float64'}, ['key3', 'key4'], 1, 1),
        ("test_2", {'key3':'int8', 'key4':'float64'}, None, 1, 1),
        ("tes54t_2", {'key3':'int16', 'key4':'string', '3':'double'}, ['key4', 'key3', '3'], 1, 1)
    ])

    def test_register_streams(self, stream, template, key_order, expected_id, expected_ver):
        '''should successfully register streams'''
        result, idn = self.dest.register_stream(stream, template, key_order=key_order)
        assert result == 0
        # the stream and version numbers start at 1
        assert idn == struct.pack("!II", expected_id, expected_ver)


    @pytest.mark.parametrize("stream,template,key_order", [
        ("tes,t", {'key1':'int', 'key2':'float'}, ['key1', 'key2']),
        ("test_2", {'key3':'int8', 'key4':'float64'}, ['key3']),
        ("tes54t_2", {'key3':'int16', 'key4':'string', '3':'double2'}, ['key4', 'key3', '3'])
    ])

    def test_no_register_streams(self, stream, template, key_order):
        '''should NOT register streams'''
        result, msg = self.dest.register_stream(stream, template, key_order=key_order)
        logger.warning(result)
        logger.warning(msg)
        assert result == 1
        # the stream and version numbers start at 1
        assert type(msg) == str
        assert msg != ''