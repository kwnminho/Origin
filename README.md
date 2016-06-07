# ![](bloop.svg =100px)

# Origin

Origin is a simple monitoring and alert server, based on ZeroMQ and JSON messaging, making it extremely portable and easy to use.

## How it works

The server exposes two ports, a registration port and a measurement port.
To begin logging a new data source, a registration packet is sent to the monitoring server registration port describing the data that will be logged.
The server creates a new database table based on the registration information.
Then the device can send data to the server for logging to the measurement port, where it is automatically entered into the database.

The alert server ...

## Install

### Dependencies

* python 2.7
* pyzmq
* mysql.connector

```bash
pip install pyzmq
pip install http://cdn.mysql.com/Downloads/Connector-Python/mysql-connector-python-1.2.3.zip
```

### Configure MySQL

You will need a MySQL server, which can be set up locally, then make a new database and add a user that has read/write permissions.

```bash
mysql -u root -p
```

```sql
CREATE DATABASE origintest;
CREATE USER '_user_'@'localhost' IDENTIFIED BY '_password_';
GRANT ALL PRIVILEGES ON origintest to '_user_'@'localhost';
FLUSH PRIVILEGES;
```

### Configure Origin

We now need to enter project specific information to the configuration file for the server.

```bash
git clone http://github.com/Orthogonal-Systems/Origin
cd Origin
vim lib/origin/origin_config.py
```

The configuration file holds multiple configurations.
To begin with we are just going to test the server locally so enter your information into the configuration object.
The main configuration object `configSite` can be the same except change `origin_server` to the exposed ip address of the machine.

```python
configTest={
  "athena_server":"127.0.0.1",
  "athena_register_port":"5556",
  "athena_measure_port":"5557",
  "athena_alert_port":"5558",
  "alert_check_period":"30",
  "mysql_local_server":"127.0.0.1",
  "mysql_local_db":"origintest",
  "mysql_local_user":"_user_",
  "mysql_local_password":"_password_",
  #"mysql_remote_server":"",
  #"mysql_remote_db":"",
  #"mysql_remote_user":"",
  #"mysql_remote_password":"",
}
```

### Run the server

You should now be able to run the server.

```bash
./bin/origin-server
```

Monitor the logs in another terminal session.
```bash
tail -f var/ORIGIN.log
```

And run the toy inserter.
```bash
./bin/origin-toy-client
```

The logs should show a new table added to the database.

```bash
show output
```

You can check that the data was actually inserted.

```bash
mysql -u _user_ -p
```

```sql
USE origintest;
SELECT * FROM measurements_test;
```

```bash
show output
```
