FROM apache/airflow:3.2.2

COPY requirements.txt requirements-test.txt ./

RUN pip install --no-cache-dir --user \
  -r requirements.txt \
  -r requirements-test.txt \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.2.2/constraints-3.13.txt"