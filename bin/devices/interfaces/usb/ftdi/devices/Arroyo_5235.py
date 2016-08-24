import ftdi_common
fc = ftdi_common.ftdi_common()
import collections
class Arroyo_5235:
    def __init__(self):
        pass
    def get_R_setpoint(self, port):
        return fc.arroyo_query(port, 'TEC:SET:R?')
    def get_T_setpoint(self, port):
        return fc.arroyo_query(port, 'TEC:SET:T?')
    def get_ITE_setpoint(self, port):
        return fc.arroyo_query(port, 'TEC:SET:ITE?')
    def get_mode(self, port):
        return fc.arroyo_query(port, 'TEC:MODE?')
    def get_id(self, port):
        return fc.arroyo_query(port, '*IDN?')
    def get_serial(self, port):
        return self.get_id(port).split(' ')[2]
    def get_sensor(self, port):
        return fc.arroyo_query(port, 'TEC:SENsor?')
    def get_V(self, port):
        return fc.arroyo_query(port, 'TEC:V?')
    def get_ITE(self, port):
        return fc.arroyo_query(port, 'TEC:ITE?')
    def get_info(self, port):
        mode = self.get_mode(port)
        set_point = 0.0
        if(mode=='R'):
            set_point = self.get_R_setpoint(port)
        elif(mode=='T'):
            set_point = self.get_T_setpoint(port)
        elif(mode=='ITE'):
            set_point = self.get_ITE_setpoint(port)
        else:
            print('Error on port ' + str(port) +': setpoint mode not recognized')
            return []
        serial = self.get_serial(port)
        sensor = self.get_sensor(port)
        V      = self.get_V(port)
        I      = self.get_ITE(port)
        state = (('type', 'TEC'),
                ('brand', 'Arroyo'),
                ('model', '5235'),
                ('serial', serial),
                ('mode', mode),
                ('set_point', set_point),
                ('sensor', sensor),
                ('V', V),
                ('I', I))
        return collections.OrderedDict(state)
