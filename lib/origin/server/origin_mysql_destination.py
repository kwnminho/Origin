"""
This module extends the Destination class to work with a MySQL database.
"""

import mysql.connector
from origin.server import Destination
from origin import data_types, timestamp


class MySQLDestination(Destination):
    '''A class for storing data in a MySQL database.'''

    def connect(self):
        self.cnx = mysql.connector.connect(
            user=self.config.get("MySQL", "user"),
            password=self.config.get("MySQL", "password"),
            host=self.config.get("MySQL", "server_ip"),
            database=self.config.get("MySQL", "db")
        )
        self.cursor = self.cnx.cursor()

    def read_stream_def_table(self):
        stream_creation = (
            "CREATE TABLE IF NOT EXISTS origin_streams ( "
            " id INT NOT NULL AUTO_INCREMENT,"
            " name varchar(1024),"
            " version integer,"
            " PRIMARY KEY (id) "
            " ) "
        )

        stream_field_creation = (
            "CREATE TABLE IF NOT EXISTS "
            " origin_stream_fields (  "
            " stream_name varchar(1024), "
            " field_name varchar(1024), "
            " version integer, "
            " field_type varchar(100), "
            " keyIndex integer"
            " )"
        )

        cursor = self.cursor
        cursor.execute(stream_creation)
        cursor.execute(stream_field_creation)

        # no json object to read, so we need to build from data
        # could change to store data in a json blob later
        current_stream_versions = []
        query = "SELECT id,name,version from origin_streams"
        cursor.execute(query)
        for id, name, version in cursor:
            current_stream_versions.append((id, name, version))

        known_stream_versions = {}
        known_streams = {}

        for id, name, version in current_stream_versions:
            query = """SELECT field_name,field_type,key_index 
            FROM origin_stream_fields 
            WHERE stream_name=\"%s\" and version=%d"%(name,version)
            """
            cursor.execute(query)

            definition = {}
            for field_name, field_type, key_index in cursor:
                definition[field_name] = {"type":field_type, "key_index":key_index}

            known_stream_versions[name] = definition
            # generate key_order this should probably be nicer
            key_order = [None] * len(known_stream_versions[name])
            template = {}
            for key in definition:
                key_order[definition[key]["key_index"]] = key
                template[key] = definition[key]["type"]

            err, format_str = self.format_string(template, key_order)
            if err > 0:
                format_str = ''

            known_streams[name] = { # not including older versions since it is hard right now
                "stream"     : name,
                "id"         : id, 
                "version"    : version, 
                "key_order"   : key_order, 
                "format_str"  : format_str,
                "definition" : definition,
                "versions"   : []
            }
            
        self.known_stream_versions = known_stream_versions
        self.known_streams = known_streams
        self.print_stream_info()
        self.cnx.commit()

    def create_new_stream_destination(self, stream_obj):
        cursor = self.cursor
        stream = stream_obj["stream"]
        version = stream_obj["version"]

        if version == 1:
            query = """INSERT INTO origin_streams (name, version) VALUES (\"{}\",{})"""
            query = query.format(stream, version)
        else:
            query = "UPDATE origin_streams SET version={} WHERE name=\"{}\""
            query = query.format(version, stream) 
        cursor.execute(query)
        #streamID = cursor.lastrowid #this doesn't seem to work with update, even though it should
        cursor.execute("SELECT id FROM origin_streams WHERE name=\"{}\" LIMIT 1".format(stream))
        stream_id = cursor.fetchone()[0]
        # overwrite streamID using the correct one
        self.known_streams[stream]['id'] = stream_id

        fields = []
        definition = stream_obj['definition']
        for field_name in definition.keys():
            idx = None
            field_type = definition[field_name]['type']
            idx = definition[field_name]['keyIndex']
            try:
                fields.append((field_name, data_types[field_type]["mysql"]))
            except KeyError:
                pass

            query = """INSERT INTO origin_stream_fields 
                    VALUES ("{}","{}",{},"{}",{})"""
            cursor.execute(query.format(stream, field_name, version, field_type, idx))

        query = """CREATE TABLE IF NOT EXISTS measurements_{}_{} 
                (id BIGINT NOT NULL AUTO_INCREMENT,{} """
        query.format(stream, version, timestamp)
        try:
            query += data_types[self.config.get("Server", "timestamp_type")]["mysql"]
        except KeyError:
            query += "INT UNSIGNED"
        query += ","


        for i in range(0, len(fields)):
            f0, f1 = fields[i]
            query += "{} {},".format(f0, f1)
        query += "PRIMARY KEY (id))"
        cursor.execute(query)
        query = "DROP VIEW IF EXISTS measurements_{} ".format(stream)
        cursor.execute(query)
        query = """CREATE VIEW measurements_{} 
                AS SELECT * FROM measurements_{}_{}""".format(stream, stream, version)
        cursor.execute(query)
        self.cnx.commit()
        return stream_id

    def insert_measurement(self, stream, measurements):
        measurement_array = []
        keys = measurements.keys()
        keys.sort()
        for k in keys:
            measurement_array.append((k, measurements[k]))

        fmt = ["("]
        values = []
        for entry in measurement_array:
            fmt += [entry[0], ',']
            values.append(entry[1])
        fmt[-1] = ")"
        value_placeholders = "(" + ','.join(["%s"]*len(measurement_array)) + ")"

        version = self.known_streams[stream]["version"]
        query = """INSERT INTO measurements_%s_%d %s VALUES %s"""
        query = query.format(stream, version, ''.join(fmt), value_placeholders)

        self.cursor.execute(query, values)
        self.cnx.commit()

    # read stream data from storage between the timestamps given by time = [start,stop]
    def get_raw_stream_data(self, stream, start=None, stop=None, definition=None):
        start, stop = self.validate_time_range(start, stop)

        if definition is None:
            definition = self.known_stream_versions[stream]
        
        field_list = [field for field in definition]
        query = "SELECT %s FROM measurements_%s_%d WHERE %s BETWEEN %d AND %d"
        values = (
            ",".join(field_list),
            stream, 
            self.known_streams[stream]["version"], 
            timestamp, 
            start, 
            stop
        )
        #print query % values
        self.cursor.execute(query % values)

        data = {}
        for field in field_list:
            data[field] = []

        for row in self.cursor.fetchall():
            for i, field in enumerate(field_list):
                data[field].append(row[i])

        return data
