import serial

class arroyo:

    def query_device(self, port, query):
        ser = serial.Serial(port=port, baudrate=38400, timeout=1)
        ser.write(query)
        #First line returned is an echo of the query
        ser.readline()
        #second line has the actual return message
        data = ser.readline()
        return data

    def get_brand(self, port):
        id_string = self.query_device(port, '*IDN?\r\n')
        print(id_string)
        split_id = id_string.split(' ')
        if(len(split_id)>1):
            return split_id[0]
        else:
            return ''

    #Function to check if port is an arroyo device
    #this function is implemented by main scanning class to determine which
    #device library to talk to device with.
    def is_arroyo(self, port):
        try:
            if(self.get_brand(port)=='Arroyo'):
                return True
            else:
                return False
        #On communication error, return false
        except:
            return False

    #function to get arroyo device status
    #This is used by the main scanning class to get a dictionary of info about
    #the device on the port.
    #def get_device_status(self, port):

arr = arroyo()
print(arr.get_brand('COM3'))
