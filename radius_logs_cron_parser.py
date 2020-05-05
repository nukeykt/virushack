import gzip
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
import signal
import joblib
import pandas as pd
import requests
from apscheduler.scheduler import Scheduler
from loguru import logger
import json
from subprocess import run, PIPE

from parse_radius_logs import parse_line, is_useful, columns, df_aggregation, convert_time

path = Path('radius.log.1.gz')
UPDATE_INTERVAL_MIN = 5


def login_preprocess(df):
    df_pr = df.copy()
    df_pr[df_pr['session_id'].isna()] = ''
    df_pr = df_pr[df_pr['correct_login'] == True]
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
    df_new['traffic'] = df_group['traffic_in_sum'].sum() + df_group['traffic_out_sum'].sum()
    df_new = df_new.reset_index()
    df_new = df_new[df_new['traffic'] == 0]
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


def get_time(line):
    tokens = line.split()
    time = '2020 ' + tokens[0] + ' ' + tokens[1] + ' ' + tokens[2]
    time = convert_time(time)
    time = time.replace(day=datetime.now().day)  # симулирую сегодняшний день
    return time


def send_request_to_update():
    logger.info('Sending request')
    r = requests.get('http://127.0.0.1:80/update')
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
    joblib.dump(top_stop_users, 'top_stop_users')
    joblib.dump(top_stop_switches, 'top_stop_switches')
    joblib.dump(no_traffic_users, 'no_traffic_users')
    joblib.dump(stats, 'stats')
    joblib.dump(user_stats, 'user_stats')
    logger.info('Works end!')
    send_request_to_update()

def parse_snmp():
    file_name = 'snmp_log.json'
    prev = []
    try:
      with open(file_name, 'r') as f:
        prev = json.load(f)
    except:
      print('no prev snmp log file')
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


def main():
    fpid = os.fork()
    if fpid != 0:
        # Running as daemon now. PID is fpid
        sys.exit(0)
    parse()
    parse_snmp()
    sched = Scheduler()
    sched.add_cron_job(parse, minute='*/5')
    sched.add_cron_job(parse_snmp, minute='*/2')
    sched.start()
    signal.pause()


if __name__ == '__main__':
    main()
