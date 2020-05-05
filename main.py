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
path_top_delay_users = Path('top_delay_users')

top_stop_switches = None
top_stop_users = None
no_traffic_users = None
user_stats = None
stats = None
user_info = None
snmp_info = None
top_delay_users = None
incident_users = []
last_upd_time = datetime.now() - timedelta(minutes=5)


def update_report():
    if (datetime.now() - globals()['last_upd_time']) < timedelta(minutes=1):
        return False, "Last update earlier than minute ago!"
    if not (path_top_stop_switches.exists() and path_top_stop_users.exists() and path_no_traffic_users.exists() and
            path_user_stats.exists() and path_stats.exists() and path_user_info.exists() and path_snmp_info.exists()):
        return False, "Report doesn't exist!"
    globals()['incident_users'] = []
    globals()['top_stop_switches'] = joblib.load(path_top_stop_switches)
    globals()['top_stop_switches'] = json.loads(globals()['top_stop_switches'].to_json(orient='records'))

    globals()['top_stop_users'] = joblib.load(path_top_stop_users)
    logins_top_stop_users = tuple(globals()['top_stop_users']['login'].tolist())
    globals()['top_stop_users'] = json.loads(globals()['top_stop_users'].to_json(orient='records'))

    globals()['no_traffic_users'] = joblib.load(path_no_traffic_users)
    logins_no_traffic_users = tuple(globals()['no_traffic_users']['login'].tolist())
    globals()['no_traffic_users'] = json.loads(globals()['no_traffic_users'].to_json(orient='records'))

    globals()['user_stats'] = joblib.load(path_user_stats)
    globals()['stats'] = joblib.load(path_stats)
    globals()['user_info'] = joblib.load(path_user_info)
    globals()['snmp_info'] = joblib.load(path_snmp_info)

    globals()['top_delay_users'] = joblib.load(path_top_delay_users)
    logins_top_delay_users = tuple(globals()['top_delay_users']['login'].tolist())
    globals()['top_delay_users'] = json.loads(globals()['top_delay_users'].to_json(orient='records'))
    for login in logins_no_traffic_users + logins_top_delay_users + logins_top_stop_users:
        if login not in globals()['user_info']:
            continue
        globals()['incident_users'].append(globals()['user_info'][login])
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
    return top_stop_users


@app.get("/top_stop_switches")
def get_top_stop_switches():
    if top_stop_switches is None:
        return {'message': "No report!"}
    return top_stop_switches


@app.get("/no_traffic_users")
def get_no_traffic_users():
    if no_traffic_users is None:
        return {'message': "No report!"}
    return no_traffic_users


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


@app.get('/snmp_stat')
def get_snmp_stat():
    if snmp_info is None:
        return {'message': "No report!"}
    return snmp_info


@app.get('/top_delay_users')
def get_top_delay_users():
    if top_delay_users is None:
        return {'message': "No report!"}
    return top_delay_users


@app.get('/upd_time')
def get_last_update_time():
    return {'last_update_time': last_upd_time}


@app.get('/')
def get_incidents():
    if any([last_upd_time is None, top_stop_users is None, top_stop_switches is None, top_delay_users is None,
            no_traffic_users is None, stats is None, user_stats is None, snmp_info is None]):
        return {'message': "No report!"}
    return {'last_update_time': last_upd_time, 'top_stop_users': top_stop_users, 'top_stop_switches': top_stop_switches,
            'top_delay_users': top_delay_users, 'no_traffic_users': no_traffic_users, 'stats': stats,
            'user_stats': user_stats, 'snmp_stat': snmp_info}


@app.get('/users')
def get_incident_users():
    return incident_users
