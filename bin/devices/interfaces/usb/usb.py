import serial.tools.list_ports
from ftdi import ftdi
ftdi = ftdi.ftdi()
class usb:
    def __init__(self):
        pass
    def get_usb_devices(self):
        return list(serial.tools.list_ports.comports())
    #This
    def get_info(self):
        all_usb_devices = self.get_usb_devices()
        all_info = []
        for device in all_usb_devices:
            #Switch on usb device protocol
            serial_port = device[0]
            device_id   = device[2]
            if(ftdi.is_ftdi(device_id)):
                device_info = ftdi.get_info(serial_port)
                all_info.append(device_info)
            else:
                print("usb device is using a protocol not yet supported")
        return all_info
