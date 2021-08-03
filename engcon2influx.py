#!/usr/bin/env python3
import argparse
import datetime
import logging
import logging.handlers as log_handlers
import os
import re
import socket
import sys
import threading
import time

def do_long_running_imports():
    global InfluxDBClient, ModbusClient, ModbusException, mq_tools
    from influxdb import InfluxDBClient
    from pymodbus.client.sync import ModbusSerialClient as ModbusClient
    from pymodbus.exceptions import ModbusException

    from openhab2 import mq_tools



SOCKETFILE = "/run/engcon2influx/engcon2influx.sock"
LOGFILE = "/var/log/engcon2influx.log"
DEVICE = "/dev/ttyUSB0"
INFLUX_SERVER = "localhost"
INFLUX_PORT = 8086
INFLUX_USERNAME = "<user>"
INFLUX_PASSWORD = "<pw>"
INFLUX_DATABASE = "<db>"

def setup_logging():
    global socklogger, syslogger
    FORMAT = '%(asctime)-18s | %(levelname)-8s | %(name)-14s %(message)s'
    log_handler = log_handlers.WatchedFileHandler(LOGFILE, encoding="UTF-8")
    logging.basicConfig(format=FORMAT, level=logging.WARNING, handlers=[log_handler,]) # default log level set to WARNING
    syslogger = logging.getLogger("SYSTEM")
    syslogger.setLevel(logging.DEBUG) # custom log level DEBUG
    socklogger = logging.getLogger("SOCKET")
    socklogger.setLevel(logging.WARNING) # custom log level WARNING

class SocketDaemon(threading.Thread):
    def __init__(self, bridge):
        super().__init__()
        self.bridge = bridge
        self.daemon = True

    def run(self):
        server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        server.bind(SOCKETFILE)
        server.listen(1)
        while True:
            con, addr = server.accept()
            cmd = con.recv(1024).decode("ASCII", "replace")
            socklogger.debug("received: %r" % cmd)
            if cmd != "":
                if cmd.startswith("current="):
                    m = re.match("current=(.*)", cmd)
                    arg = m.group(1)
                    try:
                        current = float(arg)
                    except:
                        current = None
                    if not current is None:
                        self.bridge.setCurrent(current)
                else:
                    socklogger.warning("unknown command: %s" % cmd)
            con.close()

class EnergyControl2InfluxBridge:
    def __init__(self):
        self.updateCurrent = None

    def sockExists(self):
        if os.path.exists(SOCKETFILE):
            return True
        return False

    def sockIsListening(self):
        client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        con_result = client.connect_ex(SOCKETFILE)
        if con_result == 0:
            client.close()
        return con_result == 0

    def setCurrent(self, value):
        self.updateCurrent = int(value * 10)

    def run(self, verbose=False):
        if self.sockExists():
            if self.sockIsListening():
                print("Unable to start - seems like the service is already running!")
                return
            else:
                os.remove(SOCKETFILE)
        syslogger.info("engcon2influx system starting...")
        sock_daemon = SocketDaemon(self)
        sock_daemon.start()
        socklogger.info("listening socket created")

        client = ModbusClient(method="rtu", port=DEVICE, timeout=1.0, stopbits=1, bytesize=8, parity='E', baudrate=19200)
        client.connect()
        syslogger.info("RS485 ModBus connection established")
        if verbose:
            print("serial connected")

        registerTable = (
            # cmd: 3/4 lesen Daten aus, 6 schreibt
            # 3/4, adr, len, name, printFormat, factor
            # 6, adr, name, setValue
            (4,   4, 1, "Modbus Registers Version", "%x", 1),
            (4,   5, 1, "Charging State", "%s", 1, ("", "", "A1", "A2", "B1", "B2", "C1", "C2", "derating", "E", "F", "ERR")),
            (4,   6, 1, "L1 Current RMS [A]", "%.1f", 10),
            (4,   7, 1, "L2 Current RMS [A]", "%.1f", 10),
            (4,   8, 1, "L3 Current RMS [A]", "%.1f", 10),
            (4,   9, 1, "PCB-Temp [°C]", "%.1f", 10),
            (4,  10, 1, "Voltage L1 RMS [V]", "%.1f", 1),
            (4,  11, 1, "Voltage L2 RMS [V]", "%.1f", 1),
            (4,  12, 1, "Voltage L3 RMS [V]", "%.1f", 1),
            (4,  13, 1, "Lock State", "%s", 1, ("locked", "unlocked")),
            (4,  14, 1, "Power (L1+L2+L3) [VA]", "%d", 1),
            (4,  15, 2, "Energy since PowerOn [VAh]", "%d", 1), # 2 byte - erst high dann low
            (4,  17, 2, "Energy since Install [VAh]", "%d", 1), # 2 byte - erst high dann low
            (4, 100, 1, "HW: Max Current [A]", "%d", 1),
            (4, 101, 1, "HW: Min Current [A]", "%d", 1),
            (3, 257, 1, "ModBus Timeout [s]", "%.3f", 1000),
            #(6, 257, "ModBus Timeout [ms]", 60000), # so könnte man auf 60s timeout stellen, aber unnötig
            (6, 258, "Standby Function", 4), # 0 an [default], 4 aus - aber an lassen
            #(6, 259, "Remote lock", 1), # 0 locked, 1 unlocked [default] - unlocked lassen ... steht zwar R/W dran, geht aber wohl nur write
            (6, 261, "Control: Max Current [A]", None), # übergeben wird in Zehntel Ampere, also 160 = 16 A
            (3, 261, 1, "Control: Max Current [A]", "%.1f", 10),
        )

        # Loop running every 2 seconds
        while True:
            if verbose:
                print(time.strftime("%H:%M:%S"))
            influx_json = []
            for registerEntry in registerTable:
                regCmd = registerEntry[0]
                if regCmd in (3, 4):
                    if len(registerEntry) == 7:
                        regAdr, regLen, regName, regTempl, regFactor, choices = registerEntry[1:]
                    else:
                        regAdr, regLen, regName, regTempl, regFactor = registerEntry[1:]
                        choices = None
                    if regCmd == 3:
                        vartype = "holding_register"
                        data = client.read_holding_registers(regAdr, count=regLen, unit=1)
                    elif regCmd == 4:
                        vartype = "input_register"
                        data = client.read_input_registers(regAdr, count=regLen, unit=1)
                    if isinstance(data, ModbusException):
                       if verbose:
                          print("%-30s" % regName, "<ERROR>")
                    else:
                        val = data.getRegister(0)
                        if regLen == 2:
                            val = data.getRegister(0) * 2**16 + data.getRegister(1)
                        rawVal = val
                        if regFactor != 1:
                            val /= regFactor
                        mqttVal = "%f" % val
                        if choices and val < len(choices):
                            val = "%d (%s)" % (val, choices[val])
                        if verbose:
                            print("%-30s" % regName, regTempl % val)
                        mq_tools.publish("/wallbox/addr%d" % regAdr, mqttVal)
                        now = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()
                        influx_json.append({"measurement": "wallbox", "fields": {"value": rawVal}, "tags": {"varname": "addr%d" % regAdr, "vartype": vartype}, "time": now})
                elif regCmd == 6:
                    regAdr, regName, regSet = registerEntry[1:]
                    # regSet for register 261 is controlled by self.updateCurrent
                    if regAdr == 261:
                        regSet = self.updateCurrent
                        self.updateCurrent = None # reset after it is used
                    if not regSet is None: # only do the work for values != None
                        sr = client.write_register(regAdr, regSet, unit=1)
                        if verbose:
                            print("%-30s" % regName, "sending %d - Error: %s" % (regSet, sr.isError()))
                        if regAdr == 261:
                            syslogger.info("Charge current updated to %.1f" % (regSet / 10))
            # write to InfluxDB
            try:
                influxclient = InfluxDBClient(INFLUX_SERVER, INFLUX_PORT, INFLUX_USERNAME, INFLUX_PASSWORD, INFLUX_DATABASE)
                influxclient.write_points(influx_json, retention_policy="two_weeks_only")
            except Exception as err:
                if verbose:
                    print("ERROR with InfluxDB:")
                    print(err)
                syslogger.error("ERROR with InfluxDB:")
                syslogger.error(err)
            if verbose:
                print("")
            time.sleep(2) # loop is repeated every 2 seconds



def chargecurrent_float(arg):
    try:
        arg = float(arg)
    except ValueError:
        raise argparse.ArgumentTypeError("%s not a floating-point literal" % arg)

    if arg != 0 and (arg < 6.0 or arg > 16.0):
        raise argparse.ArgumentTypeError("%s can only be 0 or 6-16" % arg)
    return arg

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("-v", action="store_true", dest="verbose", help="Turn on verbosity")
    ap.add_argument("-c", "--current", type=chargecurrent_float, help="Set maximum charge current")
    args = ap.parse_args()

    if not args.current is None:
        client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            client.connect(SOCKETFILE)
        except socket.error as err:
            print("Unable to connect to a running process: %s" % err, file=sys.stderr)
            sys.exit(1)
        else:
            cmd = "current=%.1f" % args.current
            client.send(cmd.encode("ASCII"))
            print("Current will be set to %.1f A in next iteration" % args.current)
    else:
        do_long_running_imports()
        setup_logging()
        bridge = EnergyControl2InfluxBridge()
        bridge.run(args.verbose)

