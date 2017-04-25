#--------------------------------------------------
# MWM wavemeter python example
# (c) MOGLabs 2014
#--------------------------------------------------
import matplotlib.pyplot as plt
import sys
import pylab as plb
from scipy.optimize import curve_fit
from scipy import asarray as ar,exp
import numpy as np


def retrieve_reading():
    ipaddr = '192.168.1.107'
    ipport=7802
    usbport='COM4'

    #----------------------------------------
    # Establish communications channel
    #----------------------------------------
    usb=True
    usb=False  # prefer TCP/IP but set this to True to use USB


    if not usb:
      import socket
      print "Connecting to wavemeter at IP:port = ",ipaddr,":",ipport
      s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
      try:
        s.connect((ipaddr,ipport))
        # set non-blocking so send/recv can return
        # without having done anything
        s.setblocking(0) 
        s.settimeout(3) # timeout if frozen more than 3 seconds
      except:
        print 'Failed to connect to ',ipaddr,ipport
        sys.exit()
    else: # must be USB
      import serial # Serial comms (USB Virtual COM Port)
      print "Connecting to wavemeter via USB port %s" % usbport
      # establish serial connection
      # timeout must be longer than maximum exposure time
      s=serial.Serial(usbport,1,timeout=1.5) 
      s.send=s.write # redefine send and recv to use usb
      s.recv=s.readline
      s.flushInput()

    #----------------------------------------
    # Demonstrate simple command
    #----------------------------------------
    s.send("info\n")
    print "Connected to "+s.recv(20).strip()

    #----------------------------------------
    # Initialise sensor
    #----------------------------------------
    s.send("get,row\n")
    row=int(s.recv(20),16)
    # define fov: two rows 2592 pixels each
    rows=1229
    cols=2592
    cmd="cam,fov,%d,0,1,%d\n" % (row,cols-1)
    s.send(cmd)
    ok=s.recv(80)
    bufsize=cols*2 # two bytes per pixel
    maxy=2*4096 # 12-bit per pixel, two rows summed

    #----------------------------------------
    # Find optimum exposure time
    # cam,atime,<max> finds optimum but with
    # maximum of <max> milliseconds
    #----------------------------------------
    cmd="cam,atime,1.5\n"
    s.send(cmd)
    shutterStr=s.recv(80)
    shutterTime=float(shutterStr)
    qcal = [7.91483760E+02,-8.70098142E-03,0.0,0.0]
    print "Exposure time: %.2fms" % shutterTime

    if (400-shutterTime < 1):
      print "Insufficient input power"
      sys.exit()

    #----------------------------------------
    # Acquire spectrum
    #----------------------------------------
    buflen=0
    bindata=''
    s.send('spe\n')
    if (usb):
      bindata=s.read(bufsize)
    else: # deal with fragmented tcp/ip packets
      while buflen<bufsize:
        chunk=s.recv(bufsize-buflen)
        chlen=len(chunk)
        bindata+=chunk
        buflen+=chlen

    image=[]
    # convert to list
    for i in xrange(0,bufsize/2):
        word=ord(bindata[2*i])+(ord(bindata[2*i+1])<<8)
        image.append(word)
    y=image[0:cols]

    s.send("temp,1\n") # box
    temp_box = float(s.recv(20))
    print "Temperature (box): ", temp_box
    s.send("temp,2\n") # sensor
    temp_sen = float(s.recv(20))
    print "Temperature (sensor): ", temp_sen
    s.send("press\n")      # pressure (hPa)
    pressure = float(s.recv(20))*0.750062 # cnvert to torr
    print "Pressure (torr): ", pressure

    s.close()

    #----------------------------------------
    # Fit Gaussian
    #----------------------------------------
    n = len(y)
    x = np.arange(n)
    y = np.array(map(float,y))/max(y)

    peak = np.argmax(y)
    mean = sum(x*y)/n
    print mean
    sigma = sum(y*(x-mean)**2)/n
    print sigma

    def gaus(x,a,x0,sigma,bg):
        return a*exp(-(x-x0)**2/(2*sigma**2))+bg

    popt,pcov = curve_fit(gaus,x,y,p0=[1,peak,3,0])
        
    print popt[2]
       
    # plt.plot(x,y,'b+:',label='data')
    # plt.plot(x,gaus(x,*popt),'ro:',label='fit')
    # plt.ylim(0,1.1)
    # plt.xlim(peak-4*popt[2],peak+4*popt[2])
    # plt.legend()
    # plt.title('Fig. 3 - Fit for Time Constant')
    # plt.xlabel('Pixel')
    # plt.ylabel('Signal')
    # plt.show()
        
    wavelength = 0
    for i in range(4):
        wavelength += qcal[i]*(popt[1]**i)
    print "wavelength: ", wavelength, " nm, air"

    # saturation partial pressure of water
    # Goffe-Gratch equation
    # pressure in mbarr
    # temp in K!!!
    def Pw0(temp_k):
        P0 = 1013 # mbarr
        T0 = 273.15 # K
        calc = 10.79586*(1.0-T0/temp_k)-5.02808*np.log10(temp_k/T0)
        calc += 1.50474E-04*(1.0-(10**(-8.29692*(temp_k/T0-1))))
        calc += 0.42873E-3*(10**(4.76955*(1-T0/temp_k))-1.0) - 2.2195983
        return P0*10**(calc)    

    # index of refraction of air
    # from Edlen 196X paper
    # sigma: 1/lambda [1/um] wavelength is vacuum wavelength
    # Ps: air (O2, N2, CO2 etc.. ) partial pressure [torr]
    # Pw: water vapor partial pressure [torr]
    # temp: in C!!!!
    # RH is relative humidity: [0.0, 1.0]

    def ndelta1(sigma):
        return 1.0E-8*(8342.13+2406030.0/(130.0-sigma**2) + 15997.0/(38.9-sigma**2))

    # air
    def delta_n_EdlenS(sigma, temp_c, Ps):
        return Ps*ndelta1(sigma)*(1+Ps*(0.817-0.0133*temp_c)*1.0E-6)/(720.775*(1.0+0.0036610*temp_c))

    # water    
    def delta_n_EdlenW(sigma, temp_c, Pw):
        return -Pw*(5.7224-0.0457*sigma**2)*1.0E-8

    # full, RH is relative humidity: [0.0, 1.0]
    def n_Edlen(sigma, temp_c, Ps, RH):
        mb2torr = 0.750062
        Pw = Pw0(temp_c + 273.15)*RH*mb2torr
        return 1.0 + delta_n_EdlenS(sigma, temp_c, Ps) + delta_n_EdlenW(sigma, temp_c, Pw)

    # wavelength in nm
    # need to do iterations since formaula is defined in vacuum wavelength
    def air2vac(wavelength, temp_c, Ps, RH):
        sigma = 1.0E3/(wavelength) # starting point
        iterations = 3
        #testing = [wavelength]
        for i in range(iterations):
            wave_vac = wavelength * n_Edlen(sigma, temp_c, Ps, RH)
            sigma = 1.0E3/wave_vac # improved guess at vacuum wavelength
            #testing = testing.append(wave_vac)
            print wave_vac
        
        return wave_vac
        
    #----------------------------------------
    # Report measured wavelength
    #----------------------------------------

    vac_ntp = 299792458.0/air2vac(wavelength, 20.0, 760.0, 0.0)
    vac_0RH = 299792458.0/air2vac(wavelength, temp_box, pressure, 0.0)
    vac_100RH = 299792458.0/air2vac(wavelength, temp_box, pressure, 1.0)
    print "F: ", vac_ntp, " GHz, vac (NTP)"
    print "F: ", vac_0RH, " GHz, vac (env 0% RH)"
    print "F: ", vac_100RH, " GHz, vac (env 100% RH)"
    
    data = {
        "f_air": 299792458.0/wavelength,
        "f_vac0RH": vac_0RH,
        "f_vac100RH": vac_100RH,
        "f_vacNTP": vac_ntp,
        "temp_box_c": temp_box,
        "temp_sensor_c": temp_sen,
        "pressure_torr": pressure
    }
    return data
    
if __name__ == "__main__":
    print retrieve_reading()