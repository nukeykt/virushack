import os
import sys
from pathlib import Path
import signal
import requests
from apscheduler.scheduler import Scheduler
import json

proxies = {
 "https": "149.248.51.132:80",
}


def report():
  no_traffic_users = requests.get('http://84.201.149.40/no_traffic_users').json()
  res = 'Абоненты с нулевым трафиком:\n'
  for user in no_traffic_users:
    res += user['login'] + ', '
  
  top_stop_users = requests.get('http://84.201.149.40/top_stop_users').json()
  res += '\n\nАбоненты с частой переавторизацией:\n'
  for user in top_stop_users:
    res += user['login'] + ', '

  top_stop_switches = requests.get('http://84.201.149.40/top_stop_switches').json()
  res += '\n\nМаршрутизаторы с частой переавторизацией:\n'
  for user in top_stop_switches:
    res += user['switch'] + ', '
  
  res += '\n\nСостояние устройств:\n'
  snmp = requests.get('http://84.201.149.40/snmp_stat').json()
  for device_name in snmp:
    device = snmp[device_name]
    res += device_name + '\n'
    if device['mean_temperature'] > 0:
      res += 'Средняя температура:' + str(device['mean_temperature']) + '\n'
    if device['current_temperature'] > 0:
      res += 'Текущяя температура:' + str(device['current_temperature']) + '\n'
    if device['mean_cpu'] > 0:
      res += 'Средняя загрузка ЦПУ:' + str(device['mean_cpu']) + '\n'
    if device['current_cpu'] > 0:
      res += 'Текущяя загрузка ЦПУ:' + str(device['current_cpu']) + '\n'
    res += '\n'
  requests.get('https://api.telegram.org/bot1109194190:AAEBfa3H6Rrl9S8qKTC2JA8VTVrz9j4-P74/sendMessage?chat_id=@vhack_report_channel&text=' + res, proxies=proxies)


def main():
    fpid = os.fork()
    if fpid != 0:
        # Running as daemon now. PID is fpid
        sys.exit(0)
    sched = Scheduler()
    sched.add_cron_job(report, minute='*/10')
    sched.start()
    signal.pause()


if __name__ == '__main__':
  main()
