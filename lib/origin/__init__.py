
import athena_config
import sys

config = None

def configure(bundle):
    global config
    if bundle=="site":
        config = athena_config.configSite
    if bundle=="test":
        config = athena_config.configTest
    if bundle=="mon":
        config = athena_config.configMon

    if config == None:
        print "Unknown configuration .... ", bundle
        sys.exit(1)




