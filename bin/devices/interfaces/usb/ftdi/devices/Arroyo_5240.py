import ftdi_common
fc = ftdi_common.ftdi_common()
import collections

class Arroyo_5240:
    def __init__(self):
        pass
    def get_R_setpoint(self, port, baudrate):
        return fc.arroyo_query(port, 'TEC:SET:R?', baudrate)
    def get_T_setpoint(self, port, baudrate):
        return fc.arroyo_query(port, 'TEC:SET:T?', baudrate)
    def get_ITE_setpoint(self, port, baudrate):
        return fc.arroyo_query(port, 'TEC:SET:ITE?', baudrate)
    def get_mode(self, port, baudrate):
        return fc.arroyo_query(port, 'TEC:MODE?', baudrate)
    def get_id(self, port, baudrate):
        return fc.arroyo_query(port, '*IDN?', baudrate)
    def get_serial(self, port, baudrate):
        return self.get_id(port, baudrate).split(' ')[2]
    def get_sensor(self, port, baudrate):
        return fc.arroyo_query(port, 'TEC:SENsor?', baudrate)
    def get_V(self, port, baudrate):
        return fc.arroyo_query(port, 'TEC:V?', baudrate)
    def get_ITE(self, port, baudrate):
        return fc.arroyo_query(port, 'TEC:ITE?', baudrate)
    def get_info(self, port, baudrate):
        mode = self.get_mode(port, baudrate)
        set_point = 0.0
        if(mode=='R'):
            set_point = self.get_R_setpoint(port, baudrate)
        elif(mode=='T'):
            set_point = self.get_T_setpoint(port, baudrate)
        elif(mode=='ITE'):
            set_point = self.get_ITE_setpoint(port, baudrate)
        else:
            print('Error on port ' + str(port) +': setpoint mode not recognized')
            return []
        serial = self.get_serial(port, baudrate)
        sensor = self.get_sensor(port, baudrate)
        V      = self.get_V(port, baudrate)
        I      = self.get_ITE(port, baudrate)
        state = (('type', 'TEC'),
                ('brand', 'Arroyo'),
                ('model', '5240'),
                ('serial', serial),
                ('mode', mode),
                ('set_point', set_point),
                ('sensor', sensor),
                ('V', V),
                ('I', I))
        return collections.OrderedDict(state)
