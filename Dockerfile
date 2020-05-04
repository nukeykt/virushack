FROM python:3.7-stretch
COPY requirements.txt parse_radius_logs.py radius_logs_cron_parser.py radius.log.1.gz main.p ./
RUN pip install -r requirements.txt
RUN python radius_logs_cron_parser.py
CMD uvicorn main:app --port 80