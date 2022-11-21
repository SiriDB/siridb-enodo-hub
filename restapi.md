
# Worker pools
The hub divides workers into pools. Within a pool, workers are also divided by their `job_type_id`. So when running a job, the hub needs to know in which pool and for which job type. For each combination of `pool_id` and `job_type_id` a lookup/scaling table is generated based on the number of workers available for that combination.

# Enodo internal vocabulary
- `pool_id`: the index of a pool (0,1,2....)
- `job_type_id`: the id that corresponds with a job type (forecast, anomaly detection..)
- `pool_idx`: the bitshifted combination between pool_id and job_type_id (see `gen_pool_idx` func in hub)
- `worker_id`: the index of a worker within a pool/job_type combo
- `worker_idx`: the bitshifted combination between pool_id, job_type_id and worker_id. (see `gen_worker_idx` func in hub) Makes it easy to use one int value for a quick lookup of a certain worker. Also you are able to determine the pool_id and job_type_id from this idx

# REST API

## Run job for series
You can request enodo to run a job. You can give an output id (`responseOutputID`) which will be used to send the result to

```
curl --request POST \
  --url 'http://localhost/api/series/forecast_test2/run?byName=1&poolID=0&responseOutputID=1017' \
  --header 'Authorization: Basic qweqweqw=' \
  --header 'Content-Type: application/json' \
  --data '{
	"meta": {
		"assetID": "1234abcd"
	},
	"config": {
		"config_name": "forecast",
		"job_type_id": 1,
		"module": "prophet@0.2.0-beta0.1.2",
		"max_n_points": 20000,
		"module_params": {
			"periods": 200,
			"smooth": true,
			"forecast_freq": "30T",
			"changepoint_range": 0.95,
			"uncertainty_samples": 1000
		}
	}
}'
```

- `job_type_id` is the job type, `1` equals a forecast job.
- `max_n_points` is the amount of historic points we will use to create our model

In model params:

- `periods` is the amount of periods the forecast needs to be. So the range of the forecast will be `periods * forecast_freq`
- `smooth` determines if we apply smoothing to our historic data. When applied, we will reduce the time needed to fit our model
- `changepoint_range` will determin how much of our historic data is used to determine changepoints. When we have a small dataset, it is important that this value is as high as possible (range: 0.0 - 1.0)
- `uncertainty_samples` Will determine the precision of our `yhat_lower` and `yhat_upper` (0 - 1000) the more closer to 1000 the more accurate it will be, but this adds a bit of extra time to our fitting)


## Get Outputs for events
Get active outputs for events

```
curl --request GET \
  --url http://localhost/api/enodo/output/event \
  --header 'Authorization: Basic qweqweqw='
```

## Get outputs for results
Get active output for results

```
curl --request GET \
  --url http://localhost/api/enodo/output/result \
  --header 'Authorization: Basic qweqweqw='
```

## Delete an output
Delete an output by its type (event or result) and id

```
curl --request DELETE \
  --url http://localhost/api/enodo/output/{type}/{id} \
  --header 'Authorization: Basic qweqweqw='
```

## Add output
Add an event or result output

```
curl --request POST \
  --url http://localhost/api/enodo/output/result \
  --header 'Authorization: Basic qweqweqw=' \
  --header 'Content-Type: application/json' \
  --data '{
	"url": "http://hub:8720/enodo",
	"params": {
		"assetId": "${request.meta.assetId}"
	},
	"headers": {
		"Authorization": "Basic 2312",
		"Content-Type": "application/json"
	},
	"payload": "{${?response.error,error}${?response.meta.accuracy,accuracy}\"forecast\": ${response.result},\"name\": \"${response.series_name}\"}"
}'
```

The params and payload fields can have string templating syntax in them, but also added extra's such as `?` for optionals and `{original_path, new_property_name}`

## Get worker stats
Get stats about workers in a pool

```
curl --request GET \
  --url http://localhost/api/worker/stats/{pool_id} \
  --header 'Authorization: Basic qweqweqw='
```

## Get worker state for a series
Query the responsible worker for the current state the worker has for a specified series

```
curl --request GET \
  --url http://localhost/api/series/{series_name}/state/{pool_id}/{job_type_id} \
  --header 'Authorization: Basic qweqweqw='
```


## Get workers
Get works the hub knowns

```
curl --request GET \
  --url http://localhost/api/worker/{pool_id} \
  --header 'Authorization: Basic qweqweqw='
```

## Delete worker
Delete a worker. The hub will always delete the latest worker in the given pool/job_type combo.

```
curl --request DELETE \
  --url http://localhost/api/worker/{pool_id}/{job_type_id} \
  --header 'Authorization: Basic qweqweqw=' \
```

## Add worker
Add a worker to a given `pool_id` for a given `job_type_id`

```
curl --request POST \
  --url http://localhost/api/worker/{pool_id} \
  --header 'Authorization: Basic qweqweqw=' \
  --header 'Content-Type: application/json' \
  --data '{
	"hostname": "localhost",
	"port": 9105,
	"worker_config": {
		"job_type_id": 1,
		"config": {}
	}
}'
```