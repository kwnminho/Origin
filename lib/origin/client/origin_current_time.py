import calendar
import time

# Just figures out the current time in the format that origin wants
# Unix time (in UTC)
def current_time():
    return calendar.timegm(time.gmtime())
