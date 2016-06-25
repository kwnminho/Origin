import origin_config
import sys

config = None

def configure(bundle):
    global config
    if bundle=="site":
        config = origin_config.configSite
    if bundle=="test":
        config = origin_config.configTest
    if bundle=="mon":
        config = origin_config.configMon

    if config == None:
        print "Unknown configuration .... ", bundle
        sys.exit(1)

# special measurement field, if field is not listed timestamp is made at server
timestamp = "measurementTime" 

from origin_current_time import current_time
from origin_data_types import data_types

