## Apache Airflow and DAGs
This package contains the docker compose to set up Apache Airflow locally or in production. It also contains all the PW DAGs.

## Installation
1. Create the database and the required `.env` file:
```
docker-compose up airflow-init
```

2. Start the docker-compose:
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
