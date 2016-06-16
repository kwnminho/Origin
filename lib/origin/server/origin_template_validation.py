
# should check that the values are in the list of known values
def template_validation(templateDest,templateReference):
  if len(templateDest) != len(templateReference):
    return False
  try: 
    for key in templateDest:
      if templateDest[key] != templateReference[key]["type"]:
        return False
  except:
    return False
  return True
