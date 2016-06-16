
# should check that the fields are consistent. That they aren't trying
# to insert a string into a float field
def measurement_validation(measurement,template):
    mk = measurement.keys()
    mk.sort()
    tk = template.keys()
    tk.sort()
    if mk != tk:
        return False

    for fieldName in measurement.keys():
        fieldType = None
        fieldTypeName = template[fieldName][0]
        if fieldTypeName == "float":
            fieldType = float
        elif fieldTypeName == "int":
            fieldType = int
        elif fieldTypeName == "string":
            fieldType = str
        else:
            return False

        try:
            dummy = fieldType(measurement[fieldName])
        except:
            return False
    return True
            
