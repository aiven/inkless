FROM python:3.12-slim
RUN pip install --no-cache-dir "confluent-kafka>=2.5,<3" "psycopg[binary]>=3.2,<4"
