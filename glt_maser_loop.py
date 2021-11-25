#!/usr/bin/env python
#Nimesh Patel
# April 2021
# log maser and maser house variables to redis
# Added sending emails for maser house temperature warning
# using python function from Locutus

import time
import json
import datetime
from collections import defaultdict

import redis
import requests
from bs4 import BeautifulSoup
from sendemail import sendEmail

import parse_maser


def getMaserHouse(maserVariables, extSensorFailure):
    rMaserHouse = requests.get('http://192.168.1.69', auth=('root', 'nti'))
    soupMaserHouse = BeautifulSoup(rMaserHouse.text, 'html.parser')
    is0 = soupMaserHouse.find_all(id='is0')[0].get_text()
    is1 = soupMaserHouse.find_all(id='is1')[0].get_text()
    is2 = soupMaserHouse.find_all(id='is2')[0].get_text()
    try:
        es0 = soupMaserHouse.find_all(id='es0')[0].get_text()
        es1 = soupMaserHouse.find_all(id='es1')[0].get_text()
        es2 = soupMaserHouse.find_all(id='es2')[0].get_text()
    except:
        extSensorFailure = True

    maserVariables['maserHouseTemp'] = is0.split(' ')[0]
    maserVariables['maserHouseHumid'] = is1.split(' ')[0]
    maserVariables['maserHouseDewPt'] = is2.split(' ')[0]
    if not extSensorFailure:
        maserVariables['maserHouseTempExt'] = es0.split(' ')[0]
        maserVariables['maserHouseHumidExt'] = es1.split(' ')[0]
        maserVariables['maserHouseDewPtExt'] = es2.split(' ')[0]
    else:
        maserVariables['maserHouseTempExt'] = '0.0'
        maserVariables['maserHouseHumidExt'] = '0.0'
        maserVariables['maserHouseDewPtExt'] = '0.0'
    return maserVariables, extSensorFailure


def getMaser():
    r = requests.get('http://192.168.1.73/monit.htm')
    return r.text


def setTimes(maserVariables, ts, st):
    maserVariables['ts'] = ts
    maserVariables['date-time'] = st
    return maserVariables


def checkAlarms(maserVariables, messageSentDate, extSensorFailure, maserHouseTempLimit, hotAlarmFlag):
    if (float(maserVariables['maserHouseTemp']) > maserHouseTempLimit):
        hotAlarmFlag = True
    if (float(maserVariables['maserHouseTempExt']) > maserHouseTempLimit):
        hotAlarmFlag = True
    if hotAlarmFlag and (datetime.date.today() > messageSentDate):
        message = 'Temperature inside the Maser House is ' + maserVariables['maserHouseTemp']
        sendEmail(message)
        print('Sent maser house temperature warning email.')
        messageSentDate = datetime.date.today()
        hotAlarmFlag = False

    if extSensorFailure and (datetime.date.today() > messageSentDate):
        message = 'External sensor failure!'
        sendEmail(message)
        print('Sent maser house external sensor failure message email.')
        messageSentDate = datetime.date.today()
        extSensorFailure = False

    return messageSentDate, hotAlarmFlag


def write_to_redis(red, maserVariables, ts):
    maserJson = json.dumps(maserVariables)
    print(maserJson)
    red.zadd('maserData', ts, maserJson)
    for key in maserVariables:
        print(key, maserVariables[key])
        red.hset('maser', key, maserVariables[key])


def maser_loop():
    red = redis.StrictRedis(host='192.168.1.11', port=6379, db=0)

    messageSentDate = datetime.date.today() - datetime.timedelta(1)
    maserHouseTempLimit = 30.0
    hotAlarmFlag = False
    extSensorFailure = False
    # initialize messageSentDate to one day before date of starting the script

    while True:
        maserVariables = defaultdict(dict)
        ts = time.time()
        st = datetime.datetime.fromtimestamp(ts).strftime('%d %b %Y %H:%M:%S')

        maserVariables, extSensorFailure = getMaserHouse(maserVariables, extSensorFailure)

        rtext = getMaser()
        maserVariables = parse_maser.parseMaserVariables(maserVariables, rtext)
        setTimes(maserVariables, ts, st)

        messageSentDate, hotAlarmFlag = checkAlarms(maserVariables, messageSentDate, extSensorFailure, maserHouseTempLimit, hotAlarmFlag)

        write_to_redis(red, maserVariables, ts)

        time.sleep(60)


if __name__ == '__main__':
    maser_loop()
