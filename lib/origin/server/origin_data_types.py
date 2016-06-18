# dict of datatypes recognized by the server
# entries keys are the server string and contain: the mysql data type, the format char for the python struct library, and if the type is allowed in the binary data format

# for the python struct ilbrary see:
data_types = {}

### integer types
# 32b - default
data_types["int"] = { "mysql":"INT", "format_char":"i", "binary_allowed": True, "size": 4, "type": int }
data_types["uint"] = { "mysql":"INT UNSIGNED", "format_char":"I", "binary_allowed": True, "size": 4, "type": long }
data_types["int32"] = data_types["int"]
data_types["uint32"] = data_types["uint"]
# 64b
data_types["int64"] = { "mysql":"BIGINT", "format_char":"q", "binary_allowed": True, "size": 8, "type": long }
data_types["uint64"] = { "mysql":"BIGINT UNSIGNED", "format_char":"Q", "binary_allowed": True, "size": 8, "type": long }
# 8b
data_types["int8"] = { "mysql":"TINYINT", "format_char":"b", "binary_allowed": True, "size": 1, "type": int }
data_types["uint8"] = { "mysql":"TINYINT UNSIGNED", "format_char":"B", "binary_allowed": True, "size": 1 , "type": int }
# 16b
data_types["int16"] = { "mysql":"SMALLINT", "format_char":"h", "binary_allowed": True, "size": 2, "type": int }
data_types["uint16"] = { "mysql":"SMALLINT UNSIGNED", "format_char":"H", "binary_allowed": True, "size": 2, "type": int }

### floating point types
# 32b
data_types["float"] = { "mysql":"FLOAT", "format_char":"f", "binary_allowed": True, "size": 4, "type": float }
# 64b
data_types["double"] = { "mysql":"DOUBLE", "format_char":"d", "binary_allowed": True, "size": 8, "type": float}

### other types
data_types["string"] = { "mysql":"TEXT", "format_char":"s", "binary_allowed": False, "size": None, "type": str }
