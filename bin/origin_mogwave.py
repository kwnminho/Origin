#!/usr/bin/env python

import sys
import os
fullBinPath=os.path.abspath(os.getcwd() + "/" + sys.argv[0])
fullLibPath=os.path.abspath(os.path.dirname(os.path.dirname(fullBinPath))+"/lib")
sys.path.append(fullLibPath)
import time

import random
import calendar
import time
import sys

import origin
if len(sys.argv) > 1:
  configBundle = sys.argv[1]
  origin.configure(configBundle)
else:
  origin.configure("site")


from origin.client import monitoring_record
from origin.client import server
from origin.client import server_connection
from origin import current_time, timestamp

from devices.moglabs.mogwave.mogwave_freq_rec import retrieve_reading

# something that represents the connection to the server
# might need arguments.... idk
serv = server()

# alert the server that we are going to be sending this type of data
connection = serv.registerStream(
    stream="Rb_Mogwave",
    records={
        "f_air": "float",
        "f_vac0RH": "float",
        "f_vac100RH": "float",
        "f_vacNTP": "float",
        "temp_box_c": "float",
        "temp_sensor_c": "float",
        "pressure_torr": "float"
    })

# perhaps print some useful message. Perhaps try to reconnect....
# print "problem establishing connection to server..."
# sys.exit(1)


# This might need to be more complicated, but you get the gist. Keep sending records forever
time.sleep(5)

while True:
    print "sending...."
    data = retrieve_reading()
    data[timestamp] = current_time(origin.config)
    connection.send(**data)
    #print("toy1: {}\ntoy2: {}".format(t1,t2))
    time.sleep(10)
