from origin.server import data_types

# should check that the fields are consistent. That they aren't trying
# to insert a string into a float field
def measurement_validation(measurement,template):
    mk = measurement.keys()
    mk.sort()
    tk = template.keys()
    tk.sort()
    if mk != tk:
        print("mkey != tkey")
        return False

    for fieldName in measurement.keys():
        fieldType = None
        fieldTypeName = template[fieldName]["type"]
        try:
            # try to cast data as the expected type
            data_types[fieldTypeName]["type"](measurement[fieldName])
        except:
            print "Data: {} is not of type: {}".format(measurement[fieldName], data_types[fieldTypeName["type"]])
            return False
    return True
            
