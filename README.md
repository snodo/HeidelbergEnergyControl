# HeidelbergEnergyControl
Daemon to collect data from Heidelberg Energy Control wallbox via RS485 Modbus and write them to InfluxDB

# Installation
* clone repo
* configure variables for DEVICE, INFLUX_* and files (make sure folders exist)
* ./engcon2influx.py

The script is running in an endless loop now, so it's recommended to run it in screen or create a systemd service.

# Set charge current
./engcon2influx.py -c 10 # to set to 10 ampere
