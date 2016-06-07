class monitoring_record:
    def __init__(self,recordType,defaultValue=None):
        self.type = recordType
        if defaultValue == None:
            self.hasDefault = True
            self.defaultValue = defaultValue
        else:
            self.hasDefault = False
            self.defaultValue = None
