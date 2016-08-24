from interfaces.usb import usb
usb = usb.usb()

class originIO:
    def __init__(self):
        pass
    def get_all_devices_info(self):
        usb_info = usb.get_info()
        return usb_info

io = originIO()
io.get_all_devices_info()
