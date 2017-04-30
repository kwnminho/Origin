from origin import data_types, timestamp

# should check that the fields are consistent. That they aren't trying
# to insert a string into a float field
def measurement_validation(measurement,template):
    m = measurement
    m.pop(timestamp,0)
    mk = m.keys()
    mk.sort()
    tk = template.keys()
    tk.sort()
    if mk != tk:
        logger.error("Measurement validation error: mkey != tkey")
        return False

    for fieldName in measurement.keys():
        fieldType = None
        fieldTypeName = template[fieldName]["type"]
        try:
            # try to cast data as the expected type
            data_types[fieldTypeName]["type"](measurement[fieldName])
        except:
            msg = "Data: {} is not of type: {}"
            logger.error(msg.format(measurement[fieldName], data_types[fieldTypeName["type"]]))
            return False
    return True
            
