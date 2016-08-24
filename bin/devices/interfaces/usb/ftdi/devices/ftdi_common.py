import serial
class ftdi_common:
    def __init__(self):
        pass
    def arroyo_query(self, port, query, baudrate=38400):
        ser = serial.Serial(port=port, baudrate=baudrate, timeout=1)
        ser.write(query)
        #First line returned is an echo of the query
        ser.readline()
        #second line has the actual return message
        data = ser.readline()
        return data
