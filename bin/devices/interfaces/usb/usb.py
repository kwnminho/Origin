import serial.tools.list_ports
from ftdi import ftdi
ftdi = ftdi.ftdi()
import json

import os.path
here = os.path.dirname(os.path.abspath(__file__))

class usb:
    def __init__(self):
        pass
    def get_usb_devices(self):
        return list(serial.tools.list_ports.comports())
    #Get all info from usb devices connected
    def get_info(self):
        all_usb_devices = self.get_usb_devices()
        usb_mapping = self.update_mapping(all_usb_devices)

        all_info = []
        for device in all_usb_devices:
            for device_map in usb_mapping:
                if (device_map['usb_id']==device[2]):
                    #Switch on usb device protocol
                    serial_port = device[0]
                    device_id   = device[2]
                    device_baud_rate = device_map["baud"]
                    print(device_baud_rate)
                    if(ftdi.is_ftdi(device_id)):
                        #baud rate could be passed in here
                        device_info = ftdi.get_info(serial_port, device_baud_rate)
                        all_info.append(device_info)
                    else:
                        print("usb device is using a protocol not yet supported")
        return all_info

    #Updates a json file that maps usb device ids to the baud rate that should
    #be used for that device
    def update_mapping(self, usb_devices):
        currently_known_devices = []
        with open(os.path.join(here + "/usb_mappings.txt")) as f:
            currently_known_devices = json.loads(f.read())
        #If usb device has never been seen before, add it to the mapping file
        for device in usb_devices:
            device_id = device[2]
            already_seen_before = False
            if(len(currently_known_devices)>0):
                for known_device in currently_known_devices:
                    known_device_id = known_device["usb_id"]
                    if(device_id==known_device_id):
                        already_seen_before = True
            if(not already_seen_before):
                new_device = {"usb_id":device_id, "baud":38400, "name":""}
                print("new usb device found: " + device_id)
                currently_known_devices.append(new_device)

        text_file = open(os.path.join(here + "/usb_mappings.txt"), "w")
        text_file.write(json.dumps(currently_known_devices))
        text_file.close()
        return currently_known_devices
