import gzip
import json
import os
import signal
import sys
from datetime import datetime, timedelta
from pathlib import Path
from subprocess import run, PIPE

import joblib
import numpy as np
import pandas as pd
import requests
from apscheduler.scheduler import Scheduler
from loguru import logger

from parse_radius_logs import parse_line, is_useful, columns, df_aggregation, convert_time

path = Path('radius.log.1.gz')
UPDATE_INTERVAL_MIN = 5


def login_preprocess(df):
    df_pr = df.copy()
    df_pr[df_pr['session_id'].isna()]['session_id'] = ''
    df_pr[df_pr['login'].isna()]['login'] = ''
    df_pr[df_pr['switch'].isna()]['switch'] = ''
    df_pr[df_pr['client_ip'].isna()]['client_ip'] = ''
    df_pr[df_pr['client_mac'].isna()]['client_mac'] = ''
    df_pr[df_pr['traffic_in_sum'].isna()]['traffic_in_sum'] = 0
    df_pr[df_pr['traffic_out_sum'].isna()]['traffic_out_sum'] = 0
    df_pr = df_pr[df_pr['correct_login']]
    return df_pr


def get_top_users_by_stop(df):
    df_group = df[df['type'] == 'Stop'].groupby('login')
    df_new = df_group.first()
    df_new['count'] = df_group.size()
    df_new = df_new.reset_index()
    df_new = df_new[df_new['count'] > 2]
    return df_new[['login', 'count']].sort_values(by='count', ascending=False).reset_index(drop=True)


def get_top_switch_by_stop(df):
    df_group = df[df['type'] == 'Stop'].groupby('switch')
    df_new = df_group.first()
    df_new['count'] = df_group.size()
    df_new = df_new.reset_index()
    df_new = df_new[df_new['count'] > 2]
    return df_new[['switch', 'count']].sort_values(by='count', ascending=False).reset_index(drop=True)


def get_no_traffic_users_by_stop(df):
    df_group = df[df['type'] == 'Stop'].groupby('login')
    df_new = df_group.first()
    df_new = df_new.reset_index()
    df_new = df_new[(df_new['traffic_in_sum'] == 0) | (df_new['traffic_out_sum'] == 0)]
    return df_new[['login']]


def get_count_statistics(df):
    stat = {
        'stop': df[df['type'] == 'Stop'].shape[0],
        'start': df[df['type'] == 'Start'].shape[0],
        'alive': df[df['type'] == 'Alive'].shape[0],
        'failed': df[df['type'] == 'Failed'].shape[0],
        'disconnect': df[df['type'] == 'Disconnect'].shape[0],
        'added': df[df['type'] == 'Add'].shape[0],
        'skip': df[df['type'] == 'Skip'].shape[0],
        'nak': df[df['type'] == 'NAK'].shape[0],
        'shutdown': df[df['type'] == 'Shutdown'].shape[0]
    }
    return stat


def get_user_count_statistics(df):
    stat = {
        'stop': df[df['type'] == 'Stop'].shape[0],
        'start': df[df['type'] == 'Start'].shape[0],
        'alive': df[df['type'] == 'Alive'].shape[0],
        'failed': df[df['type'] == 'Failed'].shape[0],
        'disconnect': df[df['type'] == 'Disconnect'].shape[0],
        'skip': df[df['type'] == 'Skip'].shape[0]
    }
    return stat


def get_user_info(df):
    user_info = {}
    to_delete = []
    if Path('user_info').exists():
        user_info = joblib.load('user_info')
    # delete users who end session earlier than 5 minute before
    for login in user_info:
        if user_info[login]['session_end'] is not None:
            to_delete.append(login)
    for login in to_delete:
        del user_info[login]
    for line in df.iterrows():
        row = line[1]
        if len(row['login']) != 11 and len(row['login']) != 8:
            continue
        login = row['login']
        if login not in user_info and (row['type'] == 'Alive' or row['type'] == 'Start'):
            user_info[login] = {
                'switch': row['switch'],
                'ip': row['client_ip'],
                'mac': row['client_mac'],
                'session_start': row['time'],
                'session_end': None,
                'session_id': row['session_id'],
                'session_sec': (datetime.now() - row['time']).total_seconds(),
                'traffic_in': row['traffic_in_sum'],
                'traffic_out': row['traffic_out_sum'],
                'active': True
            }
        if row['type'] == 'Alive':
            user_info[login]['traffic_in'] += row['traffic_in_sum']
            user_info[login]['traffic_out'] += row['traffic_out_sum']
        if row['type'] == 'Stop' and login in user_info:
            user_info[login]['session_end'] = row['time']
            user_info[login]['session_sec'] = (
                    user_info[login]['session_end'] - user_info[login]['session_start']).total_seconds()
            user_info[login]['active'] = False
    return user_info


def get_snmp_report():
    file_name = 'snmp_log.json'
    until_timestamp = datetime.now().timestamp()
    from_timestamp = (datetime.now() - timedelta(minutes=UPDATE_INTERVAL_MIN)).timestamp()
    with open(file_name, 'r') as f:
        objs = json.load(f)
    d = {}
    for item in objs[0]['items']:
        d[item['device_name']] = {
            'temperatures': [],
            'cpus': [],
            'device_name': item['device_name'],
            'ip': item['ip']
        }
    for obj in objs:
        if not (until_timestamp > obj['timestamp'] > from_timestamp):
            continue
        for item in obj['items']:
            d[item['device_name']]['temperatures'].append(item['temperature'])
            d[item['device_name']]['cpus'].append(item['cpu'])
    for dd_key in d:
        dd = d[dd_key]
        if len(dd['temperatures']) == 0:
            dd['temperatures'].append(-1)
        if len(dd['cpus']) == 0:
            dd['cpus'].append(-1)
        dd['temperatures'] = np.array(dd['temperatures']).astype(float)
        dd['cpus'] = np.array(dd['cpus']).astype(float)
        dd['max_temperature'] = int(np.max(dd['temperatures']))
        dd['min_temperature'] = int(np.min(dd['temperatures']))
        dd['mean_temperature'] = int(np.mean(dd['temperatures']))
        dd['max_cpu_load'] = int(np.max(dd['cpus']))
        dd['min_cpu_load'] = int(np.min(dd['cpus']))
        dd['mean_cpu'] = int(np.mean(dd['cpus']))
        del dd['temperatures'], dd['cpus']
    return d


def get_time(line):
    tokens = line.split()
    time = '2020 ' + tokens[0] + ' ' + tokens[1] + ' ' + tokens[2]
    time = convert_time(time)
    time = time.replace(day=datetime.now().day)  # симулирую сегодняшний день
    return time


def send_request_to_update():
    logger.info('Sending request')
    r = requests.get('http://0.0.0.0:80/update')
    if r.status_code == 200:
        logger.info('Successfully sent!')
    else:
        logger.info('Error sending(')


def parse():
    until_time = datetime.now()
    from_time = until_time - timedelta(minutes=UPDATE_INTERVAL_MIN)
    logger.info('Parsing...')

    with gzip.open(path, 'rt', encoding='utf-8', errors='ignore') as f:
        lines = [line for line in f if is_useful(line) and from_time <= get_time(line) <= until_time]
    df = pd.DataFrame([parse_line(line) for line in lines], columns=columns)
    df = df[df['type'] != 'Delete_this_stuff']
    df = df.sort_values('time').reset_index(drop=True)
    df = df_aggregation(df)
    logger.info('Creating report...')
    users_df = login_preprocess(df)
    top_stop_users = get_top_users_by_stop(users_df)
    top_stop_switches = get_top_switch_by_stop(users_df)
    no_traffic_users = get_no_traffic_users_by_stop(users_df)
    stats = get_count_statistics(df)
    user_stats = get_user_count_statistics(users_df)
    user_info = get_user_info(users_df)
    snmp_info = get_snmp_report()
    joblib.dump(top_stop_users, 'top_stop_users')
    joblib.dump(top_stop_switches, 'top_stop_switches')
    joblib.dump(no_traffic_users, 'no_traffic_users')
    joblib.dump(stats, 'stats')
    joblib.dump(user_stats, 'user_stats')
    joblib.dump(user_info, 'user_info')
    joblib.dump(snmp_info, 'snmp_info')
    logger.info('Works end!')
    send_request_to_update()


def parse_snmp():
    logger.info('Parsing snmp...')
    file_name = 'snmp_log.json'
    prev = []
    try:
        with open(file_name, 'r') as f:
            prev = json.load(f)
    except:
        logger.info('no prev snmp log file')
    items = []
    res = run('./parse_snmp.sh', stdout=PIPE)
    out = res.stdout.decode('utf-8')
    for line in out.split('\n')[1:-1]:
        a = line.split()
        items.append({
            "device_name": a[0],
            "ip": a[1],
            "temperature": a[2],
            "cpu": a[3],
        })
    obj = {
        "timestamp": datetime.now().timestamp(),
        "items": items,
    }
    prev.append(obj)
    with open(file_name, 'w+') as f:
        json.dump(prev, f)
    logger.info('Finish snmp!')


def main():
    fpid = os.fork()
    if fpid != 0:
        # Running as daemon now. PID is fpid
        sys.exit(0)
    parse_snmp()
    parse()
    sched = Scheduler()
    sched.add_cron_job(parse, minute='*/2')
    sched.add_cron_job(parse_snmp, minute='*/1')
    sched.start()
    signal.pause()


if __name__ == '__main__':
    main()
