|endpoint|method|description|args|
|--------|------|-----------|----|
|/api/series|GET|Returns a list of monitored series|        Query args:            filter (String): regex filter|
|/api/series|POST|Add new series to monitor.||
|/api/series/{series_name}|GET|Returns all details|        Query args:            fields (String, comma seperated): list of fields to return|
|/api/series/{rid}/output|GET|Returns forecast data of a specific series.|        Query args:            fields (String, comma seperated): list of fields to return|
|/api/series/{rid}|DELETE|Remove series with specific name|        Query args:            byName (string): 1, 0 or true, false|
|/api/series/{rid}/job/{job_config_name}|DELETE|Remove a job config from a series|        Query args:            byName (string): 1, 0 or true, false|
|/api/series/{rid}/job|POST|Add a job config to a series||
|/api/series/{rid}/resolve/{job_config_name}|GET|Resolve a job from a series|        Query args:            byName (string): 1, 0 or true, false|
|/api/template/series|GET|Get all series config templates|        Query args:            fields (String, comma seperated): list of fields to return|
|/api/template/series|POST|Add a series config template||
|/api/template/series/{rid}|DELETE|Remove a series config template||
|/api/template/series/{rid}/static|PUT|Update a series config template||
|/api/template/series/{rid}|PUT|Update a series config template||
|/api/enodo/module|GET|Returns list of possible modules with corresponding parameters|        Query args:            fields (String, comma seperated): list of fields to return|
|/api/enodo/event/output|GET|Get all event outputs|        Query args:            fields (String, comma seperated): list of fields to return|
|/api/enodo/event/output/{output_id}|DELETE|Add a new event output||
|/api/enodo/event/output|POST|Add a new event output|        JSON POST data:            output_type (int): type of output stream|
|/api/enodo/stats|GET|Return stats and numbers from enodo domain|        Query args:            fields (String, comma seperated): list of fields to return|
|/api/enodo/label|GET|Return enodo labels and last update timestamp||
|/api/enodo/label|POST|Add enodo label|        JSON POST data:            description (String): description of the label|
|/api/enodo/label|DELETE|Remove enodo label||
|/api/settings|GET|Returns current settings dict|        Query args:            fields (String, comma seperated): list of fields to return|
|/api/settings|POST|Update settings|        JSON POST data:            key/value pairs settings|
|/api/enodo/status|GET|Get status of enodo hub|        Query args:            fields (String, comma seperated): list of fields to return|
|/api/enodo/log|GET|Returns enodo event log||
|/api/enodo/clients|GET|Return connected listeners and workers||
|/api/siridb/query|GET|Run siridb query||
|/status/ready|GET|Get ready status of this hub instance||
|/status/live|GET|Get liveness status of this hub instance||
