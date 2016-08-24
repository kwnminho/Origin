import ftdi_common
fc = ftdi_common.ftdi_common()
class Arroyo_4205_DR:
    def __init__(self):
        pass
    def get_temp_setpoint(self, port):
        temp = fc.arroyo_query(port, 'TEC:SET:T?')
    def get_info(self, port):
        #return ftdi.ftdi_query_device(port, '*IDN?\r\n')
        return '4205'
