# venus.JK Monitor v1.2.0
Service to integrate a jk bms  into cerbos gui

The script has been developed with my current RV setup in mind.

The Python script create a virtual `com.victronenergy.battery` and push the values readed from `aiobmsble` libraries

you need to install via pip the `aiobmsble` 

### Configuration

* #### Manual
    see `config.ini` and amend for your needs.
    - `Name`: jk bms name
    - `Serial`: device serial 
    - `Model`: jk bms model
    - `Interval`: interval to query the jk bms
    - `SocDetectionVoltage`: 100% soc detection voltage - use 0.1 less the absorbtion voltage
    - `BatteryCapacity`: battery capacity in ah
    - `debug`: set log level to debug

### Installation
* #### prerequisites

    1. install pip and aiobmsble on venus os:
        - `opkg update`
        - `opkg install python3-pip`
        - `pip3 install aiobmsble`
        - determinate your python vrsion
        - `mkdir -p /usr/lib/python3.12/site-packages/statistics`
        - `touch /usr/lib/python3.12/site-packages/statistics/__init__.py`
        - `echo "def fmean(data): return sum(data) / len(data)" > /usr/lib/python3.12/site-packages/statistics/__init__.py`
    2. get the jk bms name from the jk app
* #### SetupHelper
    1. install [SetupHelper](https://github.com/kwindrem/SetupHelper)
    2. enter `Package Mager` in Settings
    3. Enter `Inactive Packages`
    4. on `new` enter the following:
        - `package name` -> `JkMonitor`
        - `GitHub user` -> `mebitek`
        - `GitHub branch or tag` -> `master`
    5. go to `Active packages` and click on `JkMonitor`
        - click on `download` -> `proceed`
        - click on `install` -> `proceed`

### Debugging
You can turn debug off on `config.ini` -> `debug=false`

The log you find in /var/log/JkMonitor

`tail -f -n 200 /data/log/JkMonitor/current`

You can check the status of the service with svstat:

`svstat /service/JkMonitor`

It will show something like this:

`/service/JkMonitor: up (pid 10078) 325 seconds`

If the number of seconds is always 0 or 1 or any other small number, it means that the service crashes and gets restarted all the time.

When you think that the script crashes, start it directly from the command line:

`python /data/JkMonitor/JkMonitor.py`

and see if it throws any error messages.

If the script stops with the message

`dbus.exceptions.NameExistsException: Bus name already exists: com.victronenergy.battery.jkbms"`

it means that the service is still running or another service is using that bus name.


### Hardware

tested with Jk BMS BD4A8S4P