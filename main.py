from datetime import datetime, timedelta
from pathlib import Path
import json
import joblib
from fastapi import FastAPI
import json

path_top_stop_switches = Path('top_stop_switches')
path_top_stop_users = Path('top_stop_users')
path_no_traffic_users = Path('no_traffic_users')
path_user_stats = Path('user_stats')
path_stats = Path('stats')

top_stop_switches = None
top_stop_users = None
no_traffic_users = None
user_stats = None
stats = None
last_upd_time = datetime.now() - timedelta(minutes=5)


def update_report():
    if (datetime.now() - globals()['last_upd_time']) < timedelta(minutes=1):
        return False, "Last update earlier than minute ago!"
    if not path_top_stop_switches.exists():
        return False, "Report doesn't exist!"
    globals()['top_stop_switches'] = joblib.load(path_top_stop_switches)
    globals()['top_stop_users'] = joblib.load(path_top_stop_users)
    globals()['no_traffic_users'] = joblib.load(path_no_traffic_users)
    globals()['user_stats'] = joblib.load(path_user_stats)
    globals()['stats'] = joblib.load(path_stats)
    globals()['last_upd_time'] = datetime.now()
    return True, "Success!"


update_report()

app = FastAPI()


@app.get("/update")
def root():
    success, report = update_report()
    return {"message": report}


@app.get("/top_stop_users")
def get_top_stop_users():
    return json.loads(top_stop_users.to_json(orient='records'))


@app.get("/top_stop_switches")
def get_top_stop_switches():
    return json.loads(top_stop_switches.to_json(orient='records'))


@app.get("/no_traffic_users")
def get_no_traffic_users():
    return json.loads(no_traffic_users.to_json(orient='records'))


@app.get("/user_stats")
def get_user_stats():
    return user_stats


@app.get("/stats")
def get_stats():
    return stats


@app.get("/snmp_raw")
def get_snmp_raw():
    file_name = 'snmp_log.json'
    with open(file_name, 'r') as f:
        obj = json.load(f)
        return obj

@app.get('/snmp_stat')
def get_snmp_stat():
    file_name = 'snmp_log.json'
    with open(file_name, 'r') as f:
        objs = json.load(f)
        d = {}
        for item in objs[0]['items']:
          d[item['device_name']] = {
            'temperatures': [],
            'cpus': []
          }
        for obj in objs:
          for item in obj['items']:
            d[item['device_name']]['temperatures'].append(item['temperature'])
            d[item['device_name']]['cpus'].append(item['cpu'])
            d[item['device_name']]['device_name'] = item['device_name']
            d[item['device_name']]['ip'] = item['ip']
        for dd_key in d:
          dd = d[dd_key]
          dd['average_temperature'] = sum(map(int, dd['temperatures'])) / len(dd['temperatures'])
          dd['average_cpu'] = sum(map(int, dd['cpus'])) / len(dd['cpus'])
          dd['max_temperature'] = max(map(int, dd['temperatures']))
          dd['min_temperature'] = min(map(int, dd['temperatures']))
          dd['max_cpu'] = max(map(int, dd['cpus']))
          dd['min_cpu'] = min(map(int, dd['cpus']))
        return d