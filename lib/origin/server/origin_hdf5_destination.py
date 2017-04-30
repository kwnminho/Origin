import h5py
import json
from origin.server import destination
from origin import data_types, timestamp
import os
import numpy as np

class hdf5_destination(destination):
    def connect(self):
        data_dir = self.config.get('HDF5','data_path')
        if not os.path.exists( data_dir ):
            data_dir = os.path.join(self.config.get("Server",'var_path'), data_dir)

        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
            self.logger.info("Creating data directory at: " + self.config.get('HDF5','data_path'))

        f = os.path.join( data_dir, self.config.get('HDF5','data_file') )
        try:
            self.hdf5_file = h5py.File(f, 'r+')
            self.logger.info("Opened data file: {}".format(f))
        except IOError:
            try:
                self.hdf5_file = h5py.File(f, 'w')
                self.logger.info("New data file: {}".format(f))
            except IOError:
                self.logger.error("Unable to create data file: {}".format(f))

    def readStreamDefTable(self):
        knownStreams = {}
        knownStreamVersions = {}
        try:
            knownStreams = json.loads(self.hdf5_file.attrs['knownStreams'])
        except KeyError:
            self.logger.debug("knownStreams attribute not found") 
            knownStreams = {}

        for stream in knownStreams:
            knownStreamVersions[stream] = knownStreams[stream]['definition']

        self.knownStreams=knownStreams
        self.knownStreamVersions=knownStreamVersions
        self.print_stream_info()

    def createNewStreamDestination(self,stream_obj):
        stream = stream_obj['stream']
        version = stream_obj['version']
        if version == 1:    # create a new stream group under root
            stream_group = self.hdf5_file.create_group(stream)
        else:
            stream_group = self.hdf5_file[stream]
        # create a new subgroup for this instance of the current stream
        stream_ver = stream_group.create_group( stream + '_' + str(version) )

        # soft link is not working?
        stream_group.attrs['currentVersion'] = stream_ver.name
        # stream_group['currentVersion'] = h5py.SoftLink(stream_ver)

        # data sets for each field plus the timestamp
        # also make a buffer dataset for each field a as a pseudo circular buffer
        chunksize=(self.config.getint('HDF5','chunksize'),) 
        buff_size = chunksize
        #print buff_size
        tstype = self.config.get('Server','timestamp_type')
        compression = self.config.get('HDF5','compression')
        stream_ver.create_dataset(
                timestamp
                , chunksize
                , maxshape = (None,)
                , dtype=data_types[tstype]['numpy']
                , chunks=chunksize
                , compression=compression
        )
        stream_ver.create_dataset(
                timestamp + "_buffer"
                , buff_size
                , maxshape = buff_size
                , dtype=data_types[tstype]['numpy']
                , chunks=chunksize
        )
        template = stream_obj['definition']
        for field in template:
            print template
            print field
            dt = data_types[template[field.strip()]['type']]['numpy']
            stream_ver.create_dataset(
                    field
                    , chunksize
                    , maxshape = (None,)
                    , dtype=dt
                    , chunks=chunksize
                    , compression=compression
            )
            stream_ver.create_dataset(
                    field + "_buffer"
                    , buff_size
                    , maxshape = buff_size
                    , dtype=dt
                    , chunks=chunksize
            )
    
        self.hdf5_file.attrs['knownStreams'] = json.dumps(self.knownStreams)
        stream_ver.attrs['definition'] = json.dumps(self.knownStreamVersions[stream])
        return stream_obj['id']

    def insertMeasurement(self,stream,measurements):
        dgroup = self.hdf5_file[self.hdf5_file[stream].attrs['currentVersion']]
        if 'row_count' in dgroup.attrs:
            row_count = dgroup.attrs['row_count']
            row_count_buffer = dgroup.attrs['row_count_buffer'] + 1
            buffer_size = dgroup[timestamp + '_buffer'].shape[0]
            if (row_count_buffer == buffer_size):
                self.logger.debug("Buffer is full. Moving completed chunk to archive and wrapping pointer around.")
                length = dgroup[timestamp].shape[0]
                chunksize=self.config.getint('HDF5','chunksize')
                for field in measurements:
                    dgroup[field][row_count:] = dgroup[field+'_buffer']
                    dgroup[field].resize((length+chunksize,))
                row_count += buffer_size
                row_count_buffer = 0
        else:
            row_count = 0
            row_count_buffer = 0

        dgroup.attrs['row_count'] = row_count # move pointer for next entry
        dgroup.attrs['row_count_buffer'] = row_count_buffer # move pointer for next entry

        for field in measurements:
            if (field != timestamp) and (self.knownStreamVersions[stream][field]['type'] == "string"):
                measurements[field] = measurements[field].encode("ascii","ignore")
            dgroup[field+"_buffer"][row_count_buffer] = measurements[field]
	self.hdf5_file.flush()

    # read stream data from storage between the timestamps given by time = [start,stop]
    # only takes timestamps in seconds
    def getRawStreamData(self,stream,start=None,stop=None,definition=None):
        start, stop = self.validateTimeRange(start,stop)
        self.logger.debug("Read request time range (start, stop): ({},{})".format(start,stop))
        # get data from buffer
        try:
            stream_group = self.hdf5_file[self.hdf5_file[stream].attrs['currentVersion']]
        except KeyError:
            raise KeyError

        if definition is None:
            definition = self.knownStreamVersions[stream]

        # read ring buffers and pointers in case the pointer advances during read
        raw_data = {}
        raw_data[timestamp] = { 
                'pointer': stream_group.attrs['row_count_buffer'] # pointer in ring buffer
                ,'array':  stream_group[timestamp + '_buffer']
        }
        try:
            for field in definition:
                raw_data[field] = { 
                    'pointer': stream_group.attrs['row_count_buffer'] # pointer in ring buffer
                    ,'array':  stream_group[field + '_buffer']
                }
        except KeyError:
            # use this error to differentiate between a stream error and a field error
            raise NotImplementedError # I am a shitty person and I know it

        # correct the ring buffer order to linear
        pointer = raw_data[timestamp]['pointer']+1
        array = raw_data[timestamp]['array']

        # check if the buffer has the requested range first, it usually will be
        buff_start = array[pointer]
        if pointer == 0:
            buff_stop =  array[-1]
        else:
            buff_stop =  array[pointer-1]

        if buff_start > start: # if not go look for it
            raw_data[timestamp] = array[:pointer] # data after pointer has already been saved to the archive
            for field in definition:
                raw_data[field] = raw_data[field]['array'][:pointer]
            raw_data = self.getArchivedStreamData(stream_group,start,stop,raw_data,definition)
        else:   # otherwise all requested data is contained in the buffer
            raw_data[timestamp] = np.concatenate((array[pointer:],array[:pointer]))
            for field in definition:
                field_pointer = raw_data[field]['pointer']+1
                offset = field_pointer - pointer
                array = raw_data[field]['array']
                length = len(array)
                # in the ring buffer the oldest data could have been overwritten during the read
                if offset < 0:  
                    self.logger.debug("negative offset")
                    offset += length
                raw_data[field] = np.concatenate((
                    np.full(offset,None)
                    ,array[pointer+offset:]
                    ,array[:pointer+offset]
                ))[:length]

        # now filter the data down to the requested range
        idx_start = None
        idx_stop = None
        for i,ts in enumerate(raw_data[timestamp]):
            if (idx_start is None) and (ts >= start):
                idx_start = i
            elif ts > stop:
                idx_stop = i-1
                break
        if idx_stop is None:
            idx_stop = -1
        if (idx_start is None) or (idx_stop is None):
            self.logger.warning("error in indexing (index start, index stop): ({},{})".format(idx_start,idx_stop))
            raise IndexError
        self.logger.debug((idx_start,idx_stop))
        data = {}
        data[timestamp] = raw_data[timestamp][idx_start:idx_stop].tolist() # json method cant handle numpy array
        for field in definition:
            data[field] = raw_data[field][idx_start:idx_stop].tolist() # json method cant handle numpy array
        return data
        
    # get raw_data in range from the archived data
    def getArchivedStreamData(self,stream_group,start,stop,buffer_data,definition):
        time_dset = stream_group[timestamp]
        row_pointer = stream_group.attrs['row_count']
        old_data={}
        # make as big as it needs to be then resize
        old_data[timestamp] = np.zeros(row_pointer)
        for field in definition:
            old_data[field] = np.zeros(row_pointer) 
        chunksize = self.config.getint('HDF5','chunksize')
        row_pointer -= chunksize
        while row_pointer >= 0:
            chunk = time_dset[row_pointer:row_pointer+chunksize]
            if chunk[0] < stop:
                old_data[timestamp][row_pointer:row_pointer+chunksize] = chunk
                for field in definition:
                    old_data[field][row_pointer:row_pointer+chunksize] = stream_group[field][row_pointer:row_pointer+chunksize]
            if chunk[0] < start:
                old_data[timestamp] = np.concatenate((old_data[timestamp][row_pointer:],buffer_data[timestamp]))
                for field in definition:
                    old_data[field] = np.concatenate((old_data[field][row_pointer:],buffer_data[field]))
                break
            row_pointer -= chunksize
        return old_data
        
