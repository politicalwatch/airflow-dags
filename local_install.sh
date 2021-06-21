# airflow needs a home, ~/airflow is the default,
# but you can lay foundation somewhere else if you prefer
# (optional)
export AIRFLOW_HOME=$(pwd)
echo "export AIRFLOW_HOME=${AIRFLOW_HOME}" > ~/.bashrc
source ~/.bashrc

AIRFLOW_VERSION=2.1.0
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
# For example: 3.6
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
# For example: https://raw.githubusercontent.com/apache/airflow/constraints-2.1.0/constraints-3.6.txt
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

# initialize the database
airflow db init

airflow users create \
    --username admin \
    --firstname Admin \
    --lastname McAdmin \
    --role Admin \
    --email admin@example.com

# start the web server, default port is 8080
airflow webserver --port 8081

# start the scheduler
# open a new terminal or else run webserver with ``-D`` option to run it as a daemon
airflow scheduler

# visit localhost:8081 in the browser and use the admin account you just
# created to login. Enable the example_bash_operator dag in the home page
