import gzip
import string
from pathlib import Path

import pandas as pd
from loguru import logger

unique_dates = {}

columns = ['time', 'session_id', 'type', 'login', 'switch', 'bras_ip', 'client_mac',
           'client_ip', 'traffic_in', 'traffic_out', 'delay', 'error_code', 'correct_login']


def is_login_correct(line):
    """Проверяет логин на возможность расшифровки ip-коммутатора"""
    line = line.split('@dhcp')[0].split()[-1]
    line = line.replace("'", '')
    line = line.lower()
    if len(line.split('-')) != 2:
        return False
    left, right = line.split('-')

    def is_hex(s):
        return all(c in string.hexdigits for c in s)

    return is_hex(left) and is_hex(right)


def is_useful(line):
    """Проверяет, подойдет ли строка для анализа."""
    line = line.lower()
    return ('ppp,start' in line) or ('ppp,stop' in line) or ('ppp,alive' in line) or (
            'failed' in line and 'no username' not in line and 'authenticate-only' not in line and ')@pppoe' not in line) or \
           ('added' in line) or ('skiped' in line) or ('disconnect' in line) or ('nak response' in line) or (
               'shutting down') in line


def define_log_type(line):
    """Определяет тип строки."""
    line = line.lower()
    if 'ppp,start' in line:
        return 'Start'
    if 'ppp,alive' in line:
        return 'Alive'
    if 'ppp,stop' in line:
        return 'Stop'
    if 'failed' in line and 'no username' not in line and 'authenticate-only' not in line and ')@pppoe' not in line:
        return 'Failed'
    if 'added' in line:
        return 'Add'
    if 'skiped' in line:
        return 'Skip'
    if 'disconnect' in line:
        return 'Disconnect'
    if 'nak response' in line:
        return 'NAK'
    if 'shutting down' in line:
        return 'Shutdown'
    raise Exception('NON useful line')


def convert_time(time):
    """Оптимальный(быстрый) считыватель времени."""
    if time in unique_dates:
        return unique_dates[time]
    unique_dates[time] = pd.to_datetime(time, format='%Y %b %d %X')
    return unique_dates[time]


def parse_line(line):
    """Стандартизирует строку"""
    try:
        log_type = define_log_type(line)
        left = line.split('(', 1)[0].strip().split()
        correct_login = is_login_correct(line)
        error_code = 0
        time = '2020 ' + left[0] + ' ' + left[1] + ' ' + left[2]
        datetime = convert_time(time)

        if log_type == 'NAK' or log_type == 'Shutdown':
            return [datetime, '', log_type, '', '', '', '', '', 0, 0, 0, error_code, correct_login]

        if log_type == 'Add':
            session_id = left[9].strip()
            return [datetime, session_id, log_type, '', '', '', '', '', 0, 0, 0, 0, correct_login]

        left[8] = left[8].replace("'", '')
        left[8] = left[8].replace('\\', '')
        login = left[8].split('@')[0]
        switch = left[8].split('@')[0].split('-')[0]

        if log_type == 'Disconnect':
            session_id = left[-1].replace('"', '').replace('$', '')
            bras_ip = left[-2]
            return [datetime, session_id, log_type, login, switch, bras_ip, '', '', 0, 0, 0, error_code, correct_login]

        if log_type == 'Skip':
            return [datetime, '', log_type, login, switch, '', '', '', 0, 0, 0, error_code, correct_login]

        if left[12] == 'FAILED':
            if left[-2] == 'code':
                error_code = int(left[-1].split(',')[0])
            if left[-3] == 'code':
                error_code = int(left[-2].split(',')[0])
            if left[-4] == 'code':
                error_code = int(left[-3].split(',')[0])
            if left[-5] == 'code':
                error_code = int(left[-4].split(',')[0])

        if '(' not in line or ')' not in line:
            return [datetime, '', log_type, login, switch, '', '', '', 0, 0, 0, 0, correct_login]

        center = line.split('(', 1)[1].strip().rsplit(')', 1)[0].split(',')

        traffic_in = 0
        traffic_out = 0
        client_mac = ''
        client_ip = ''

        if log_type == 'Failed' and error_code != -42:
            session_id = center[-2].strip()
            bras_ip = center[0].split(':')[0]
            client_mac = center[-1]
            client_ip = ''
        else:
            session_id = center[6].strip()
            if log_type == 'Stop' or log_type == 'Alive':
                traffic_in = int(center[13])
                traffic_out = int(center[14])

            bras_ip = center[1].split(':')[0]

            if len(center) >= 11:
                client_mac = center[9]
                client_ip = center[10]
        delay = float(line.split()[-1])

        return [datetime, session_id, log_type, login, switch, bras_ip, client_mac, client_ip,
                traffic_in, traffic_out, delay, error_code, correct_login]
    except BaseException:
        datetime = convert_time('2000 May 1 00:00:00')
        return [datetime, '', 'Delete_this_stuff', '', '', '', '', '', 0, 0, 0, 0, False]


def df_aggregation(df):
    """Агрегирует датафрейм по session_id."""
    df_grouped = df.groupby(['time', 'type', 'session_id', 'login'])
    q = df_grouped.first()
    q['traffic_in_sum'] = df_grouped['traffic_in'].sum()
    q['traffic_out_sum'] = df_grouped['traffic_out'].sum()
    q['traffic_in_mean'] = df_grouped['traffic_in'].mean()
    q['traffic_out_mean'] = df_grouped['traffic_out'].mean()
    q.drop(columns=['traffic_in', 'traffic_out'], inplace=True)

    q['delay_mean'] = df_grouped['delay'].mean()
    q['delay_max'] = df_grouped['delay'].max()

    q['size'] = df_grouped.size()
    return q.reset_index()


def parse_file(path):
    logger.info('Parsing file {}'.format(path))
    new_path = Path('processed_data/{}'.format('processed_{}'.format(path.name)))
    if new_path.exists():
        return None
    with gzip.open(path, 'rt', encoding='cp1251', errors='ignore') as f:
        df = pd.DataFrame([parse_line(line) for line in f if is_useful(line)], columns=columns)
    df = df[df['type'] != 'Delete_this_stuff']
    df = df.sort_values('time').reset_index(drop=True)
    df = df_aggregation(df)
    logger.info('Save processed file into {}'.format(new_path))
    df.to_csv(new_path, index=None, compression='gzip')
    del df


def main():
    Path('processed_data/').mkdir(parents=True, exist_ok=True)
    for path in (Path('data/').glob('*.gz')):
        parse_file(path)


if __name__ == '__main__':
    main()
