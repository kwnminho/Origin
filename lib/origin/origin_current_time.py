"""
Timestamping related functions
"""

import calendar
import time

def current_time(config):
    '''Figures out the current time in the format that origin wants'''

    #Unix time (in UTC)
    if config.get('Server', "timestamp_type") == "uint64":
        return long(time.time()*2**32)
    return calendar.timegm(time.gmtime())
