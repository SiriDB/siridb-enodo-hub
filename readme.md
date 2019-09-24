
<p align="center"><img src="https://github.com/transceptor-technology/siridb-enodo-hub/raw/development/assets/logo_full.png" alt="Enodo"></p>

# Enodo Hub

## REST API

- `/series` GET Get list of series
- `/series` POST Create new serie
    ```json
    {
        "name": 'seriename',
        "model": '1',
        "model_parameters": []
        ...
    }
    ```
- `/series/<name>` GET Get details of a specific serie
- `/series/<name>` DELETE Remove specific serie
- `/settings` GET Get current settings
- `/settings` POST Overwrite settings
    ```json
    {
      "settingName": "value"
    }
    ```
- `/siridb/status` GET Get siriDB connection status
- `/enodo/status` GET Get Enodo status
- `/enodo/log` GET Get event log of Enodo
- `/enodo/clients` GET Get list of connected clients, listeners and workers 

## Deployment and communication

message types:

 type        | desc           |
|:------------- |:-------------|
| HANDSHAKE      | General type for sending handshake |
| UNKNOW_CLIENT  | General type for sending server has received handshake from unknown client |
| SHUTDOWN      | General type for sender is shutingdown      |
| ADD_SERIE | General type for adding serie      |
| REMOVE_SERIE | General type for removing serie      |
| UPDATE_SERIES | Type for complete new list of series to watch      |
| HANDSHAKE_OK | General type for successful received handshake      |
| HANDSHAKE_FAIL | General type for a not succesful handshake      |
| HEARTBEAT | General type for sending heartbeat      |
| LISTENER_ADD_SERIE_COUNT | Type send by listener for serie datapoint count update      |
| RESPONSE_OK | General ok response      |
| TRAIN_MODEL | Type to execute training for certain model for certain serie |
| FIT_MODEL   | Type to fit model for certain serie |
| FORECAST_SERIE | Type to calculate forecast for a certain serie |

### Listener

The enode listener listens to pipe socket with siridb server. It sums up the totals of added datapoints to each serie. 
It periodically sends an update to the enode hub. The listener only keeps track of series that are registered via an ADD_SERIE of the UPDATE_SERIE message. The listener is seperated from the enode hub, so that it can be placed close to the siridb server, so it can locally access the pipe socket.
Every interval for heartbeat and update can be configured with the listener.conf file next to the main.py

### Worker

The enode worker executes fitting and forecasting models/algorithms. The worker uses significant CPU and thus should be placed on a machine that has low CPU usage.
The worker can create different models (ARIMA/prophet) for series, train models with new data and calculate forecasts for a certain serie.

### Hub

The enode hub communicateds and guides both the listener as the worker. The hub tells the listener to which series it needs to pay attention to, and it tells the worker which serie should be analysed.
Clients can connect to the hub for receiving updates, and polling for data. Also a client can use the hub to alter the data about which series should be watched.



