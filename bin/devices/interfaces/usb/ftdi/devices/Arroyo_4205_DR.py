import ftdi_common
fc = ftdi_common.ftdi_common()
import collections
class Arroyo_4205_DR:
    def __init__(self):
        pass
    def get_mode(self, port, baudrate):
        return fc.arroyo_query(port, 'LASer:MODE?', baudrate)
    def get_id(self, port, baudrate):
        return fc.arroyo_query(port, '*IDN?', baudrate)
    def get_serial(self, port, baudrate):
        return self.get_id(port, baudrate).split(' ')[2]
    def get_sensor(self, port, baudrate):
        return fc.arroyo_query(port, 'LASer:LDI?', baudrate)
    def get_V(self, port, baudrate):
        return fc.arroyo_query(port, 'LASer:LDV?', baudrate)
    def get_I(self, port, baudrate):
        return fc.arroyo_query(port, 'LASer:LDI?', baudrate)
    def get_I_setpoint(self, port, baudrate):
        return fc.arroyo_query(port, 'LASer:SET:LDI?', baudrate)
    def get_info(self, port, baudrate):
        mode = self.get_mode(port, baudrate)
        set_point = 0.0
        if(mode=='ILBW'):
            set_point = self.get_I_setpoint(port, baudrate)
        else:
            print('Error on port ' + str(port) +': not using current based set point')
            return []
        serial = self.get_serial(port, baudrate)
        sensor = self.get_sensor(port, baudrate)
        V      = self.get_V(port, baudrate)
        I      = self.get_I(port, baudrate)
        state = (('type', 'LASer'),
                ('brand', 'Arroyo'),
                ('model', '4205'),
                ('serial', serial),
                ('mode', mode),
                ('set_point', set_point),
                ('sensor', sensor),
                ('V', V),
                ('I', I))
        return collections.OrderedDict(state)
