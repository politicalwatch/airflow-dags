FROM apache/airflow:2.7.2
COPY requirements.txt .
RUN pip install -r requirements.txt