import os
import json

from origin.server import measurement_validation
from origin.server import template_validation

class filesystem_destination:
    def __init__(self,cachedir):
        if not os.path.exists(cachedir):
            os.mkdir(cachedir)
        self.cachedir = cachedir

        streamdef = "%s/stream-def"%(cachedir)
        if not os.path.exists(streamdef):
            os.mkdir(streamdef)
        self.streamdef = streamdef

        streamdata = "%s/stream-data"%(cachedir)
        if not os.path.exists(streamdata):
            os.mkdir(streamdata)
        self.streamdata = streamdata

        knownStreamDirs = os.listdir(streamdef)
        knownStreams = {}
        for k in knownStreamDirs:
            infile = open("%s/stream-def/%s"%(cachedir,k))
            streamTemplate = {}
            for line in infile:
                tokens = line.rstrip().lstrip().split()
                streamTemplate[tokens[0]] = tokens[1] 

            print "Found known stream '",k,"'"
            knownStreams[k] = streamTemplate
        self.knownStreams = knownStreams
        self.knownStreamDirs = knownStreamDirs
        
    def registerStream(self,stream,template):
        result = None
        resultText = None
        
        if stream not in self.knownStreams.keys():
            print "Uknown stream",stream
            print "Will register it."
            outfile = open("%s/%s"%(self.streamdef,stream),"w")

            outdir = "%s/%s"%(self.streamdata,stream)
            if not os.path.exists(outdir):
                os.mkdir(outdir)
            
            self.knownStreams[stream] = template

            for name in template.keys():
                print >> outfile,name,template[name]

        if template_validation(template,self.knownStreams[stream]):
            print "it appears legitimate"
            result = 0
            resultText = ""
        else:
            print "it doesn't validate"
            result = 1
            resultText = "Problem with template compared to old template"
                
        return (result,resultText)

    def measurement(self,measurementTime,stream,measurements):
        if stream not in self.knownStreams.keys():
            print "trying to add a measurement to data on an unknown stream"
            return (1,"Unknown stream")

        if not measurement_validation(measurements,self.knownStreams[stream]):
            print "Measurement didn't validate against the pre-determined format"
            return (1,"Invalid measurements against schema")
        outdir = self.streamdata

        outfile = open("%s/%s/%d"%(outdir,stream,measurementTime),'a')
        for m in measurements.keys():
            print >> outfile,m,measurements[m]
        outfile.close()

        result = 0
        resultText = ""
        return (result,resultText)


            
        
