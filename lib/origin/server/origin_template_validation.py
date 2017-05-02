"""
Function for making sure two stream templates match 
"""

# should check that the values are in the list of known values
# TODO: check that key order has not changed?
def template_validation(template_dest, template_reference):
    '''making sure stream data matches the template'''
    if len(template_dest) != len(template_reference):
        return False
    try: 
        for key in template_dest:
            k = key.strip()
            if template_dest[k] != template_reference[k]["type"]:
                return False
    except KeyError:
        return False
    return True
