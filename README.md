# spark-event-vis

This is a notebook to process and visualize Spark event log data.

## Interactive use

You can install dependencies with `pipenv install` and launch the notebook with `pipenv run jupyter notebook`.  Open `metrics.ipynb` and then edit the first notebook cell to point `metrics_file` to the path of a Spark event log.

## Batch use and log conversion

As above, install dependencies with `pipenv install`.  

### Preprocessing

You can then run the preprocessor over one or several log files with `pipenv run ./preprocess.py`.  This script has online help, but here's something to get you started:

`pipenv run python preprocess.py --master 'local[*]' --per-app-db --fail-fast  application_161108976543_0123 --db metrics.db --outdir outputs`

where `application_161108976543_0123` is a Spark event log file.  You can specify one log file or several.  After running this command, `outputs` will contain SQLite databases for each event log and `metrics.db` will contain summary information for every event log.

### Interactive query

You can then start an API server for these databases with this command:

`pipenv run datasette outputs/*.db --config sql_time_limit_ms:50000 --config max_returned_rows:1024768 --cors --metadata outputs/metadata.json --template-dir=templates`

Navigate to [localhost:8001](localhost:8001) to interact with individual tables.  Note that the schema of these tables will change in the future.

### Scripting with tools

If you have `dot` installed, you can run this example script against the existing API:

`curl http://localhost:8001/${APP_NAME}/plan-graph.json\?_shape=array | ./querygraph.py | dot -Tpdf > graph.pdf`

where `${APP_NAME}` is an application ID from the first step above.

# Copyright

Copyright (c) 2020-2021 NVIDIA Corporation
