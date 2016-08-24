import ftdi_common
fc = ftdi_common.ftdi_common()
import collections
class Arroyo_4205_DR:
    def __init__(self):
        pass
    def get_mode(self, port):
        return fc.arroyo_query(port, 'LASer:MODE?')
    def get_id(self, port):
        return fc.arroyo_query(port, '*IDN?')
    def get_serial(self, port):
        return self.get_id(port).split(' ')[2]
    def get_sensor(self, port):
        return fc.arroyo_query(port, 'LASer:LDI?')
    def get_V(self, port):
        return fc.arroyo_query(port, 'LASer:LDV?')
    def get_I(self, port):
        return fc.arroyo_query(port, 'LASer:LDI?')
    def get_I_setpoint(self, port):
        return fc.arroyo_query(port, 'LASer:SET:LDI?')
    def get_info(self, port):
        mode = self.get_mode(port)
        print(mode)
        set_point = 0.0
        if(mode=='ILBW'):
            set_point = self.get_I_setpoint(port)
        else:
            print('Error on port ' + str(port) +': not using current based set point')
            return []
        serial = self.get_serial(port)
        sensor = self.get_sensor(port)
        V      = self.get_V(port)
        I      = self.get_I(port)
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
