## Apache Airflow and DAGs
This package contains the docker compose to set up Apache Airflow locally or in production. It also contains all the PW DAGs.

## Installation

### Local installation:

1. Execute the local_install.sh script:
```
sh local_install.sh
```

2. Copy the `airflow.cfg.example` into `airflow.cfg` and configure the paths to point to this folder.
```
cp airflow.cfg.example airflow.cfg
```

3. Make sure the variable `AIRFLOW_HOME` is configured in your system and it is pointing to this folder.

4. From now on, you can start the webserver and scheduler(this one is necessary to scan the DAGs folder and add our custom DAGs) with the next commands:
```
airflow webserver
airflow scheduler
```

### On a server:
1. Configure `.env` file:
```
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
```

2. Execute init script:
```
docker-compose up airflow-init
```

3. Start the docker-compose:
```
docker-compose up
```

## Commands
There is a set of commands to interact with Airflow from the command line. You can use them using `docker-compose run airflow-worker airflow [COMMAND]` or the utility shell file `./airflow.sh [COMMAND]`.

### Info
Shows the status information of Airflow.
`./airflow.sh info`

### Bash
Opens a bash connection on the Airflow container.
`./airflow.sh bash`

### Python
Opens the Python command line in the Airflow container.
`./airflow.sh python`

## Info links
- (Airflow Docker setup)[https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html]
