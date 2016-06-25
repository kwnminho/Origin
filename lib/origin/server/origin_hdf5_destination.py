import h5py
import json
from origin.server import destination
from origin import data_types, config, timestamp
import os

class hdf5_destination(destination):
    def connect(self):
        data_dir = config['data_path']
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
            self.logger.info("Creating data directory at: " + config['data_path'])
        try:
            self.hdf5_file = h5py.File(config['data_file'], 'r+')
            self.logger.info("Opened data file: {}".format(config['data_file']))
        except IOError:
            try:
                self.hdf5_file = h5py.File(config['data_file'], 'w')
                self.logger.info("New data file: {}".format(config['data_file']))
            except IOError:
                self.logger.error("Unable to create data file: " + config['data_file'])


    def readStreamDefTable(self):
        try:
            self.knownStreamVersions = json.loads(self.hdf5_file.attrs['knownStreamVersions'])
        except KeyError:
            self.logger.debug("knownStreamVersions attribute not found") 
            self.knownStreamVersions = {}

        self.knownStreams = {}
        for stream in self.knownStreamVersions:
            if stream in self.hdf5_file:
                current_stream_version = self.hdf5_file[stream].attrs['currentVersion']
                definition = self.hdf5_file[current_stream_version].attrs['definition']
                self.knownStreams[stream] = json.loads(definition)
            else:
                self.logger.error("Stream '{}' found in stream list, but is not a group")

        for stream in self.knownStreams:
            print "="*10, " {} ".format(stream), "="*10
            for field_name in self.knownStreamVersions[stream]:
                print "  Field: %s (%s)"%(field_name,self.knownStreamVersions[stream][field_name])

    def createNewStream(self,stream,version,template,keyOrder):
        if version == 1:    # create a new stream group under root
            stream_group = self.hdf5_file.create_group(stream)
            streamID = len(self.hdf5_file)
        else:
            stream_group = self.hdf5_file[stream]
            streamID = self.knownStreamVersions[stream]["id"]
        # create a new subgroup for this instance of the current stream
        stream_ver = stream_group.create_group( stream + '_' + str(version) )

        # soft link is not working?
        stream_group.attrs['currentVersion'] = stream_ver.name
        # stream_group['currentVersion'] = h5py.SoftLink(stream_ver)

        # data sets for each field plus the timestamp
        stream_ver.create_dataset(
                timestamp
                , (2**10,)
                , maxshape = (None,)
                , dtype=data_types[config['timestamp_type']]['numpy']
        )
        for field in template:
            stream_ver.create_dataset(
                    field
                    , (2**10,)
                    , maxshape = (None,)
                    , dtype=data_types[template[field]]['numpy']
            )
    
        # update the stream inventory, in memory and on disk
        self.knownStreamVersions[stream] = {
                "version"   : version,
                "id"        : streamID,
                "keyOrder"  : keyOrder,
                "formatStr" : self.formatString(template,keyOrder),
        }
        self.hdf5_file.attrs['knownStreamVersions'] = json.dumps(self.knownStreamVersions)
        # create the stream field definition dict
        definition = {}
        for i, key in enumerate(keyOrder):
            definition[key] = { "type": template[key], "keyIndex": i }
        stream_ver.attrs['definition'] = json.dumps(definition)
        return streamID

    def insertMeasurement(self,stream,measurements):
        dset = self.hdf5_file[self.hdf5_file[stream].attrs['currentVersion']]
        if 'row_count' in dset.attrs:
            row_count = dset.attrs['row_count'] + 1
        else:
            row_count = 0
        dset.attrs['row_count'] = row_count # move pointer for next entry
        self.logger.debug("Stream `{}` current row pointer: {}".format(stream,row_count))

        for field in measurements:
            dset[field][row_count] = measurements[field]
