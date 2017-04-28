
# should check that the values are in the list of known values
# TODO: check key order is preserved?
def template_validation(templateDest,templateReference):
  if len(templateDest) != len(templateReference):
    return False
  try: 
    for key in templateDest:
      k = key.strip()
      if templateDest[k] != templateReference[k]["type"]:
        return False
  except:
    return False
  return True
