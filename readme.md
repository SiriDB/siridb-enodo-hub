
<p align="center"><img src="https://github.com/transceptor-technology/siridb-enodo-hub/raw/development/assets/logo_full.png" alt="Enodo"></p>

# Enodo Hub

## API's

-   REST API
-   Socket.io API

## Deployment and internal communication

### Hub

The enode hub communicates and guides both the listener as the worker. The hub tells the listener to which series it needs to pay attention to, and it tells the worker which series should be analysed.
Clients can connect to the hub for receiving updates, and polling for data. Also a client can use the hub to alter the data about which series should be watched.


## Getting started

To get the Enodo Hub setup you need to following the following steps:

### Locally

1. Install dependencies via `pip3 install -r requirements.txt`
2. Setup a .conf file file `python3 main.py --create_config` There will be made a `default.conf` next to the main.py.
3. Fill in the `default.conf` file
4. Call `python3 main.py --config=default.conf` to start the hub.

### Docker
....