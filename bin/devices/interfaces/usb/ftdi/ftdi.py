import serial

#Import devices
from devices.Arroyo_4205_DR import Arroyo_4205_DR
Arroyo_4205_DR = Arroyo_4205_DR()
from devices.Arroyo_5235 import Arroyo_5235
Arroyo_5235 = Arroyo_5235()
from devices.Arroyo_4304 import Arroyo_4304
Arroyo_4304 = Arroyo_4304()
from devices.Arroyo_5240 import Arroyo_5240
Arroyo_5240 = Arroyo_5240()

class ftdi:
    def __init__(self):
        pass
    #determines if usb device suppports ftdi protocol
    def is_ftdi(self, usb_device_id):
        # the formatting of the device id seems to change between versions of windows
        # this has some obvious failure modes, but I dont care right now
        if( usb_device_id.find('0403') < 0 ):
            return False
        else:
            vendor_id = '0403'
        if( usb_device_id.find('6001') < 0 ):
            return False
        else:
            vendor_id = '6001'
        if( usb_device_id.find('SER=') >= 0 ):
            print(usb_device_id.split('SER=')[1])
        return True
        
    #query ftdi device
    def ftdi_query_device(self, port, query):
        ser = serial.Serial(port=port, baudrate=38400, timeout=1)
        ser.write(query)
        #First line returned is an echo of the query
        ser.readline()
        #second line has the actual return message
        data = ser.readline()
        return data

    #get device id string
    def get_ftdi_id_string(self, port):
        return self.ftdi_query_device(port, '*IDN?\r\n')

    #Get just make and model of the device
    def get_ftdi_make_model(self, port):
        id_str = self.get_ftdi_id_string(port)
        print(id_str)
        split_id = id_str.split(' ')
        make = split_id[0]
        model = split_id[1]
        make_model = make+'_'+model
        return make_model

    #Get info about ftdi device on usb port
    def get_info(self, port, baudrate):
        #self.ftdi_query_device(port, 'BEEP\r\n')
        make_model = self.get_ftdi_make_model(port)
        #Switch to get info specific to device make and model
        if(make_model=='Arroyo_4205-DR'):
            return Arroyo_4205_DR.get_info(port, baudrate)
        elif(make_model=='Arroyo_4304'):
            return Arroyo_4304.get_info(port, baudrate)
        elif(make_model=='Arroyo_5235'):
            return Arroyo_5235.get_info(port, baudrate)
        elif(make_model=='Arroyo_5240'):
            return Arroyo_5240.get_info(port, baudrate)
        else:
            print("ftdi device make and model not recognized")
            return None
