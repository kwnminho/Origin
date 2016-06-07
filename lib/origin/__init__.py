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
