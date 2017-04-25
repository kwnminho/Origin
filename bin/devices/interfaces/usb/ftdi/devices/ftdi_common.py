import serial
class ftdi_common:
    def __init__(self):
        pass
    def arroyo_query(self, port, query, baudrate):
        ser = serial.Serial(port=port, baudrate=baudrate, timeout=1)
        #add \r\n to message to terminate it
        ser.write(query+'\r\n')
        #First line returned is an echo of the query
        ser.readline()
        #second line has the actual return message, remove last two chars which
        #are /r/n
        data = ser.readline()[:-2]
        return data
