import calendar
import time
from origin import config

# Just figures out the current time in the format that origin wants
# Unix time (in UTC)
def current_time():
    if "timestamp_type" in config:
        if config["timestamp_type"] == "uint64":
            return long(time.time()*2**32)
    else:
        return calendar.timegm(time.gmtime())
