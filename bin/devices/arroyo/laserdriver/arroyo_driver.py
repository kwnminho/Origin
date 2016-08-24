import serial
import time
import sys
import glob

def list_serial_ports():
    if sys.platform.startswith('win'):
        ports = ['COM%s' % (i + 1) for i in range(256)]
    elif sys.platform.startswith('linux') or sys.platform.startswith('cygwin'):
        # this excludes your current terminal "/dev/tty"
        ports = glob.glob('/dev/tty[A-Za-z]*')
    elif sys.platform.startswith('darwin'):
        ports = glob.glob('/dev/tty.*')
    else:
        raise EnvironmentError('Unsupported platform')

    result = []
    for port in ports:
        try:
            s = serial.Serial(port)
            print(s)
            s.close()
            result.append(port)
        except (OSError, serial.SerialException):
            pass
    return result


def query_device(port, query):
    ser = serial.Serial(port=port, baudrate=38400, timeout=1)
    ser.write(query)
    ser.readline()
    data = ser.readline()
    return data

def get_serial_number(PORT):
    return_str = query_device(PORT, '*IDN?\r\n')
    return return_str.split(' ')[2]

def get_model_number(PORT):
    return_str = query_device(PORT, '*IDN?\r\n')
    return return_str.split(' ')[1]

def get_brand(PORT):
    return_str = query_device(PORT, '*IDN?\r\n')
    return return_str.split(' ')[0]


def get_device_status(port):
    device_brand = get_brand(port)
    device_model_number = get_model_number(port)
    device_serial_number = get_serial_number(port)
    #print(


#list_serial_ports();
out = query_device('COM3', '*IDN?\r\n')
print(out)
