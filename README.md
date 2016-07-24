# ![](bloop.png)

# Origin

Origin is a simple monitoring and alert server, based on ZeroMQ and JSON messaging, making it extremely portable and easy to use.

## How it works

The server exposes two ports, a registration port and a measurement port.
To begin logging a new data source, a registration packet is sent to the monitoring server registration port describing the data that will be logged.
The server creates a new stream entry based on the registration information.
Then the device can send data to the server for logging to the measurement port, where it is automatically entered into the database.

The alert server ...

## Install

### Dependencies
The default data storage is h5py.

* python 2.7
* [pyzmq](http://zeromq.org/bindings:python)
* [h5py](http://docs.h5py.org/en/latest/build.html)
* [mysql.connector](http://cdn.mysql.com/Downloads/Connector-Python/mysql-connector-python-1.2.3.zip)

```bash
pip install pyzmq
pip install h5py
pip install http://cdn.mysql.com/Downloads/Connector-Python/mysql-connector-python-1.2.3.zip
```

For windows check the recommended installation stuff on the packages website (or use canopy or a package manager).
The pip install for mysql.connector worked fo rme on windows, and is probably the easiest way to do it.

### OPTIONAL: Configure MySQL (only if you want to use it)

You will need a MySQL server, which can be set up locally, then make a new database and add a user that has read/write permissions.

```bash
mysql -u root -p
```

```sql
CREATE DATABASE origintest;
CREATE USER '_user_'@'localhost' IDENTIFIED BY '_password_';
GRANT ALL PRIVILEGES ON origintest.* to '_user_'@'localhost';
FLUSH PRIVILEGES;
```

### Configure Origin

We now need to enter project specific information to the configuration file for the server.

```bash
git clone http://github.com/Orthogonal-Systems/Origin
cd Origin
git checkout dev
vim lib/origin/origin_config.py
```

The configuration file holds multiple configurations.
To begin with we are just going to test the server locally so enter your information into the configuration object.
The main configuration object `configSite` can be the same except change `origin_server` to the exposed ip address of the machine.

```python
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
```

### Run the server

You should now be able to run the server using the test configuration.

```bash
python bin/origin-server test
```

Monitor the logs in another terminal session.
```bash
tail -f var/ORIGIN.log
```
Windows open powershell, cd to Origin directory:
```powershell
Get-Content var\ORIGIN.log -wait
```

And run the toy inserter.
```bash
python bin/origin-toy-client test
```

The logs should show a new table added to the database.

```bash
2016-07-23 19:56:24,315 - Monitor - INFO - Successfully Started Logging
2016-07-23 19:56:24,315 - Monitor - INFO - Creating data directory at: [x\x\x]\Origin\var\data
2016-07-23 19:56:24,316 - Monitor - INFO - New data file: [x\x\x]\Origin\var\data\origintest.hdf5
2016-07-23 19:56:24,318 - Monitor - DEBUG - knownStreamVersions attribute not found
2016-07-23 19:56:24,322 - Monitor - INFO - IOLoop Configured
2016-07-23 20:02:09,812 - Monitor - INFO - Received registration of stream toy
2016-07-23 20:02:09,813 - Monitor - INFO - Attempt to register stream toy
```

### Checking data entry

You can either open the HDF5 directly with a viewer application.
Or you can use the example reader, which uses the built in API.

```bash
python bin/origin-toy-reader test
```

The server should be responding with alternating statistics and raw data for the toy stream.

### OPTIONAL: MySQL checking data entry
You can check that the data was actually inserted.

```bash
mysql -u _user_ -p
```

```sql
USE origintest;
SELECT * FROM measurements_toy;
```

```bash
+----+-----------------+-----------+-----------+
| id | measurementTime | toy1      | toy2      |
+----+-----------------+-----------+-----------+
|  1 |      1465326087 |   0.88671 |  0.980564 |
|  2 |      1465326092 |  0.624428 |  0.228306 |
|  3 |      1465326097 |   0.77994 |  0.616023 |
|  4 |      1465326102 |  0.122097 |  0.148885 |
|  5 |      1465326107 |  0.781741 |  0.851939 |
|  6 |      1465326112 | 0.0570459 |  0.891965 |
|  7 |      1465326117 |  0.197933 |  0.736311 |
|  8 |      1465326122 |  0.515716 | 0.0314361 |
|  9 |      1465326127 |  0.669868 |  0.737255 |
| 10 |      1465326132 |  0.445365 |  0.298424 |
| 11 |      1465326137 |  0.938865 |  0.320306 |
| 12 |      1465326142 |   0.38073 |  0.745032 |
| 13 |      1465326147 |  0.872034 |  0.987444 |
+----+-----------------+-----------+-----------+
13 rows in set (0.00 sec)
```

### Finish

Once the coniguration object `configSite` is edited, logging from an external source can be start.
To do so just run on the server:

```bash
python bin/origin-server
```

and the monitoring device:
```bash
python bin/origin-toy-client
```
