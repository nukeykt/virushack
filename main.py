import json
from datetime import datetime, timedelta
from pathlib import Path

import joblib
from fastapi import FastAPI

path_top_stop_switches = Path('top_stop_switches')
path_top_stop_users = Path('top_stop_users')
path_no_traffic_users = Path('no_traffic_users')
path_user_stats = Path('user_stats')
path_stats = Path('stats')
path_user_info = Path('user_info')
path_snmp_info = Path('snmp_info')

top_stop_switches = None
top_stop_users = None
no_traffic_users = None
user_stats = None
stats = None
user_info = None
snmp_info = None
last_upd_time = datetime.now() - timedelta(minutes=5)


def update_report():
    if (datetime.now() - globals()['last_upd_time']) < timedelta(minutes=1):
        return False, "Last update earlier than minute ago!"
    if not (path_top_stop_switches.exists() and path_top_stop_users.exists() and path_no_traffic_users.exists() and
            path_user_stats.exists() and path_stats.exists() and path_user_info.exists() and path_snmp_info.exists()):
        return False, "Report doesn't exist!"
    globals()['top_stop_switches'] = joblib.load(path_top_stop_switches)
    globals()['top_stop_users'] = joblib.load(path_top_stop_users)
    globals()['no_traffic_users'] = joblib.load(path_no_traffic_users)
    globals()['user_stats'] = joblib.load(path_user_stats)
    globals()['stats'] = joblib.load(path_stats)
    globals()['user_info'] = joblib.load(path_user_info)
    globals()['snmp_info'] = joblib.load(path_snmp_info)
    globals()['last_upd_time'] = datetime.now()
    return True, "Success!"


update_report()

app = FastAPI()


@app.get("/update")
def root():
    """"""
    success, report = update_report()
    return {"message": report}


@app.get("/top_stop_users")
def get_top_stop_users():
    if top_stop_users is None:
        return {'message': "No report!"}
    return json.loads(top_stop_users.to_json(orient='records'))


@app.get("/top_stop_switches")
def get_top_stop_switches():
    if top_stop_switches is None:
        return {'message': "No report!"}
    return json.loads(top_stop_switches.to_json(orient='records'))


@app.get("/no_traffic_users")
def get_no_traffic_users():
    if no_traffic_users is None:
        return {'message': "No report!"}
    return json.loads(no_traffic_users.to_json(orient='records'))


@app.get("/user_stats")
def get_user_stats():
    if user_stats is None:
        return {'message': "No report!"}
    return user_stats


@app.get("/stats")
def get_stats():
    if stats is None:
        return {'message': "No report!"}
    return stats


@app.get("/user_info/{login}")
def get_stats(login):
    if user_info is None:
        return {'message': "No report!"}
    if login not in user_info:
        return {'message': "This login is inactive or doesn't exist"}
    return user_info[login]


@app.get("/snmp_raw")
def get_snmp_raw():
    file_name = 'snmp_log.json'
    if not Path(file_name).exists():
        return {'message': "No report!"}
    with open(file_name, 'r') as f:
        obj = json.load(f)
        return obj


@app.get('/snmp_stat')
def get_snmp_stat():
    if snmp_info is None:
        return {'message': "No report!"}
    return snmp_info
