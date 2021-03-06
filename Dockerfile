FROM python:3.7-stretch
COPY requirements.txt parse_radius_logs.py radius_logs_cron_parser.py radius.log.1.gz main.py parse_snmp.sh ./
RUN pip install -r requirements.txt
RUN chmod +x parse_snmp.sh
RUN apt-get update --assume-yes && \
    apt-get upgrade --assume-yes && \
    apt-get install --assume-yes sshpass && \
    apt-get install --assume-yes snmp

CMD python radius_logs_cron_parser.py; uvicorn main:app --port 80 --host 0.0.0.0