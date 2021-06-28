
<p align="center"><img src="https://github.com/siridb/siridb-enodo-hub/raw/development/assets/logo_full.png" alt="Enodo"></p>

# Enodo Hub



## Deployment and internal communication

### Hub

The enode hub communicates and guides both the listener as the worker. The hub tells the listener to which series it needs to pay attention to, and it tells the worker which series should be analysed.
Clients can connect to the hub for receiving updates, and polling for data. Also a client can use the hub to alter the data about which series should be watched.


## Getting started

To get the Enodo Hub setup you need to following the following steps:

### Locally

1. Install dependencies via `pip3 install -r requirements.txt`
2. Setup a .conf file file `python3 main.py --create-config` There will be made a `default.conf` next to the main.py.
3. Fill in the `default.conf` file
4. Call `python3 main.py --config=default.conf` to start the hub.
5. Fill `in settings.endo` file, which you can find in the data folder by the path set in the conf file with key: `enodo_base_save_path`
6. Restart Hub
7. You can also setup the config by environment variables. These names are identical to those in the default.conf file, except all uppercase.

## API's

-   REST API
-   Socket.io API

### REST API

#### Get all series
call http://localhost/api/series (GET)

#### Get series details
call http://localhost/api/series/{series_name} (GET)

#### Create Series
call http://localhost/api/series (POST)
```
{
	"name": "series_name_in_siridb",
	"config": {
		"job_models": {
			"job_base_analysis": "prophet",
			"job_forecast": "prophet",
			"job_anomaly_detect": "prophet"
		},
		"job_schedule": {
			"job_base_analysis": 200
		},
		"min_data_points": 2,
		"model_params": {
			"points_since": 1563723900
		}
	}
}
```

#### Get all event output streams
call http://localhost/api/enodo/event/output (GET)

#### Create event output stream
call http://localhost/api/enodo/event/output (POST)
```
{
	"output_type": 1,
	"data": {
		"for_severities": ["warning", "error"],
		"url": "url_to_endpoint",
		"headers": {
			"authorization": "Basic abcdefghijklmnopqrstuvwxyz"
		},
		"payload": "{\n  \"title\": \"{{event.title}}\",\n  \"body\": \"{{event.message}}\",\n  \"dateTime\": {{event.ts}},\n  \"severity\": \"{{event.severity}}\"\n}"
	}
}
```

#### Delete event output stream
call http://localhost/api/enodo/event/output/{output_id} (DELETE)


### Socket.IO Api (WebSockets)

When sending payload in a request, use the data structure same as in the REST API calls, except the data will be wrapped in an object : `{"data": ...}`.

#### Get series
event: `/api/series`

#### Get series Details
event: `/api/series/details`

#### Create series
event: `/api/series/create`

#### Delete series
event: `/api/series/delete`

#### Get all event output stream
event: `/api/event/output`

#### Create event output stream
event: `/api/event/output/create`

#### Delete event output stream
event: `/api/event/output/delete`

 