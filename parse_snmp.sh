#!/bin/bash
# =============Инструкция=============
#
# Работает на убунту
# 
# Для запуска должны быть установлены через apt:
#   - sshpass
#   - snmp
#
# Ссылка на oids: https://docs.google.com/spreadsheets/d/1h2Hl89BEAMto2PVoyc5d6EFeJeRfagBAVaaq4_uQ6mM/edit?usp=sharing
# 
# Формат вывода:
# первая строка N - кол-во устройств
# N строк вида: IP DEVICE_NAME TEMPERATURE CPU_USAGE
# 
# -1 означает отсутвие значения

TCP_PORT=42062

DEVICE_NAMES=("ELTEX_MES1124M" "Huawei_S2326TP-EI" "QTECH_QSW_2800" "Raisecom_ISCOM2128EA" "EdgeCore_ES3528M" "Raisecom_ISCOM2924GF-4C")
DEVICES=("10.73.161.47" "10.73.161.51" "10.73.161.52" "10.73.161.53" "10.73.161.54" "10.73.161.55")
TEMPS=(".1.3.6.1.4.1.89.53.15.1.9" "1.3.6.1.4.1.2011.5.25.31.1.1.1.1.11.67108873" "" ".1.3.6.1.4.1.8886.1.1.4.2.1.0" "" ".1.3.6.1.4.1.8886.1.1.4.2.1.0")
CPUS=(".1.3.6.1.4.1.89.1.7" ".1.3.6.1.4.1.2011.6.3.4.1.2" ".1.3.6.1.4.1.27514.100.1.11.2.0" ".1.3.6.1.4.1.8886.1.1.1.5.1.1.1.3.1" ".1.3.6.1.4.1.259.6.10.94.1.39.2.3" ".1.3.6.1.4.1.8886.1.1.1.5.1.1.1.3.1")

N=${#DEVICES[@]}
echo $N

fetch() {
  sshpass -p Che5iasu@e ssh -L $TCP_PORT:localhost:$TCP_PORT -o StrictHostKeyChecking=no vhack@vhack.ulrt.net "snmpwalk -c vhack2020 -v 2c $1 $2"
}

for (( i = 0; i < N; i++ ))
do
  DEVICE_NAME=${DEVICE_NAMES[i]}
  DEVICE=${DEVICES[i]}
  TEMP_OID=${TEMPS[i]}
  CPU_OID=${CPUS[i]}
  TEMP=-1
  if [[ $TEMP_OID != "" ]]; then
    IFS=' ' read -ra TMP <<< $(fetch $DEVICE $TEMP_OID)
    TEMP=${TMP[-1]}
  fi
  IFS=' ' read -ra TMP <<< $(fetch $DEVICE $CPU_OID)
  CPU=${TMP[-1]}
  echo $DEVICE_NAME $DEVICE $TEMP $CPU
done