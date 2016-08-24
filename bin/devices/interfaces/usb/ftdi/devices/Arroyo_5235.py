import ftdi_common
fc = ftdi_common.ftdi_common()

class Arroyo_5235:
    def __init__(self):
        pass
    def get_temp_setpoint(self, port):
        temp = fc.arroyo_query(port, 'TEC:SET:R?\r\n')
        return temp
    def get_info(self, port):
        #return ftdi.ftdi_query_device(port, '*IDN?\r\n')
        return self.get_temp_setpoint(port)
