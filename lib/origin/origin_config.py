import os.path
here = os.path.dirname(os.path.abspath(__file__))
root = os.path.dirname(os.path.dirname(here))
var_path = os.path.join(root,"var")
print var_path

# This file is not for committing. Don't commit it 'cause it has
# passwords in it
configTest={
  "origin_server"           : "127.0.0.1",
  "origin_register_port"    : "5556",
  "origin_measure_port"     : "5557", 
  "origin_alert_port"       : "5558",
  "origin_read_port"       : "5559",
  "alert_check_period"      : "30",
  "mysql_local_server"      : "127.0.0.1",
  "mysql_local_db"          : "origintest",
  "mysql_local_user"        : "test",
  "mysql_local_password"    : "test",
  #"mysql_remote_server":"",
  #"mysql_remote_db":"",
  #"mysql_remote_user":"",
  #"mysql_remote_password":"",
  "timestamp_type"  : "uint64",
  "data_path"       : os.path.join(var_path,"data"),
  "data_file"       : os.path.join(var_path,"data","origintest.hdf5"),
  "hdf5_chunksize"  : 2**10, # for testing (make 1kB to 1MB)
  "hdf5_compression"  : 'gzip', # False for no compression
}

configSite={
        "origin_server":"hexlabmonitor.physics.wisc.edu",
  "origin_register_port"    : "5556",
  "origin_measure_port"     : "5557", 
  "origin_alert_port"       : "5558",
  "origin_read_port"       : "5559",
  "alert_check_period"      : "30",
  "mysql_local_server"      : "127.0.0.1",
  "mysql_local_db"          : "origin",
  "mysql_local_user"        : "_user_",
  "mysql_local_password"    : "_password_",
  #"mysql_remote_server":"",
  #"mysql_remote_db":"",
  #"mysql_remote_user":"",
  #"mysql_remote_password":"",
  "timestamp_type"  : "uint64",
  "data_path"       : os.path.join(var_path,"data"),
  "data_file"       : os.path.join(var_path,"data","origin.hdf5"),
  "hdf5_chunksize"  : 2**16,
  "hdf5_compression"  : 'gzip', # False for no compression
}
