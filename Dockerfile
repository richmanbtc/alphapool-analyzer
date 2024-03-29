FROM python:3.10.6

RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir \
    numpy==1.23.3 \
    pandas==1.5.2 \
    "git+https://github.com/richmanbtc/alphapool.git@v0.1.5#egg=alphapool" \
    dataset==1.5.2 \
    psycopg2==2.9.3 \
    joblib==1.2.0 \
    'google-cloud-bigquery[bqstorage,pandas]==3.4.1' \
    SQLAlchemy==1.4.45

ADD . /app
ENV ALPHAPOOL_LOG_LEVEL debug
WORKDIR /app
CMD python -m src.main
