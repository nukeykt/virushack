FROM python:3.7-stretch
COPY requirements.txt parse_radius_logs.py radius_logs_cron_parser.py radius.log.1.gz main.py ./
RUN pip install -r requirements.txt
CMD python radius_logs_cron_parser.py; uvicorn main:app --port 80 --host 0.0.0.0