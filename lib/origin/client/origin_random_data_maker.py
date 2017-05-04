import random
import struct
import string
import numpy as np

from origin import data_types

def random_data( dtype ):
    if dtype.find("int") != -1:
        x = long(random.randint(0, 2**32-1))
        x *= 2**32
        x += random.randint(0, 2**32-1)
        size = data_types[dtype]["size"]
        data = struct.pack("!Q", x)[8-size:]
        return struct.unpack("!"+data_types[dtype]["format_char"], data)[0]
        
    elif (dtype == "float64") or (dtype == "double"):
        return random.random()
    elif (dtype == "float") or (dtype == "float32"):
        return float(np.float32(random.random())) # reduce accuracy so end2end testing is accurate
    elif dtype == "string":
        length = int(data_types["string"]["numpy"][1:])
        return ''.join(random.choice(string.lowercase) for i in range(length))
    return None
