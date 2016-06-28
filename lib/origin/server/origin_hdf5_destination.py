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
        # also make a buffer dataset for each field a as a pseudo circular buffer
        chunksize=(config['hdf5_chunksize'],) 
        buff_size = (2*chunksize[0],)
        print buff_size
        stream_ver.create_dataset(
                timestamp
                , chunksize
                , maxshape = (None,)
                , dtype=data_types[config['timestamp_type']]['numpy']
                , chunks=chunksize
                , compression=config['hdf5_compression']
        )
        stream_ver.create_dataset(
                timestamp + "_buffer"
                , buff_size
                , maxshape = buff_size
                , dtype=data_types[config['timestamp_type']]['numpy']
                , chunks=chunksize
        )
        for field in template:
            stream_ver.create_dataset(
                    field
                    , chunksize
                    , maxshape = (None,)
                    , dtype=data_types[template[field]]['numpy']
                    , chunks=chunksize
                    , compression=config['hdf5_compression']
            )
            stream_ver.create_dataset(
                    field + "_buffer"
                    , buff_size
                    , maxshape = buff_size
                    , dtype=data_types[template[field]]['numpy']
                    , chunks=chunksize
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
        dgroup = self.hdf5_file[self.hdf5_file[stream].attrs['currentVersion']]
        if 'row_count' in dgroup.attrs:
            row_count = dgroup.attrs['row_count']
            row_count_buffer = dgroup.attrs['row_count_buffer'] + 1
            if (row_count_buffer == dgroup[timestamp + '_buffer'].shape[0]):
                self.logger.debug("Buffer is full. Moving completed chunk to archive.")
                length = dgroup[timestamp].shape[0]
                chunksize=config['hdf5_chunksize'] 
                for field in measurements:
                    if row_count == 0:
                        self.logger.debug("First transfer to archive. Moving both chunks.")
                        temp_chunk = dgroup[field+'_buffer']
                        dgroup[field+'_buffer'][:chunksize] = temp_chunk[chunksize:]
                    else:
                        temp_chunk = dgroup[field+'_buffer'][chunksize:]
                        dgroup[field+'_buffer'][:chunksize] = temp_chunk
                    dgroup[field].resize((length+config['hdf5_chunksize'],))
                    dgroup[field][row_count:] = temp_chunk
                if row_count == 0:
                    row_count += chunksize
                row_count += chunksize
                row_count_buffer -= chunksize
        else:
            row_count = 0
            row_count_buffer = 0

        dgroup.attrs['row_count'] = row_count # move pointer for next entry
        dgroup.attrs['row_count_buffer'] = row_count_buffer # move pointer for next entry
        self.logger.debug("Stream `{}` current row pointer: {}".format(stream,row_count))
        self.logger.debug("Stream `{}` current row buffer pointer: {}".format(stream,row_count_buffer))

        self.logger.debug("Datasets `{}.*` shape: {}".format(stream,dgroup[timestamp].shape))
        for field in measurements:
            dgroup[field+"_buffer"][row_count_buffer] = measurements[field]
