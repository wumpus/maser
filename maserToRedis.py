#!/usr/bin/python3
#Nimesh Patel
# April 2021
# log maser and maser house variables to redis

# Added sending emails for maser house temperature warning
# using python function from Locutus

import time
import redis
import json
import datetime
import requests
from collections import defaultdict
from bs4 import BeautifulSoup
from sendemail import sendEmail


def cleanUpString(s):
    s2 = s.replace(' ','_')
    s3 = s2.replace('[','_')
    s4 = s3.replace(']','')
    strEncode = s4.encode("ascii","ignore")
    return(strEncode.decode())

red = redis.StrictRedis(host='192.168.1.11',port=6379,db=0)

messageSentDate = datetime.date.today() - datetime.timedelta(1)
maserHouseTempLimit = 30.0
hotAlarmFlag = False
extSensorFailure = False
# initialize messageSentDate to one day before date of starting the script
 
while True:
    maserVariables = defaultdict(dict)
    ts = time.time()
#    st=datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    st=datetime.datetime.fromtimestamp(ts).strftime('%d %b %Y %H:%M:%S')
# #time.strftime("%b %d %Y %H:%M:%S", time.gmtime(t))
    rMaserHouse = requests.get('http://192.168.1.69',auth=('root','nti'))
    soupMaserHouse = BeautifulSoup(rMaserHouse.text,'html.parser')
    is0=soupMaserHouse.find_all(id='is0')[0].get_text()
    is1=soupMaserHouse.find_all(id='is1')[0].get_text()
    is2=soupMaserHouse.find_all(id='is2')[0].get_text()
    try:
        es0=soupMaserHouse.find_all(id='es0')[0].get_text()
        es1=soupMaserHouse.find_all(id='es1')[0].get_text()
        es2=soupMaserHouse.find_all(id='es2')[0].get_text()
    except:
        extSensorFailure= True


    r = requests.get('http://192.168.1.73/monit.htm')
    soup = BeautifulSoup(r.text,'html.parser')
    allWords = soup.findAll(text=True)
    allWordsFiltered = [x for x in allWords if x != '\n']
    for i in range(0,len(allWordsFiltered),2):
        if i == 0:
            v = 'imaser_timestamp'
            maserTimestampWords = allWordsFiltered[1].split(' ')
            maserTimeStamp0 = maserTimestampWords[0]+ ' ' +maserTimestampWords[1]
            maserVariables[v]=cleanUpString(maserTimeStamp0)
        else:
            v = allWordsFiltered[i].lower()
            maserVariables[cleanUpString(v)]=allWordsFiltered[i+1]
    maserVariables['maserHouseTemp']=is0.split(' ')[0]
    maserVariables['maserHouseHumid']=is1.split(' ')[0]
    maserVariables['maserHouseDewPt']=is2.split(' ')[0]
    if not extSensorFailure :
        maserVariables['maserHouseTempExt']=es0.split(' ')[0]
        maserVariables['maserHouseHumidExt']=es1.split(' ')[0]
        maserVariables['maserHouseDewPtExt']=es2.split(' ')[0]
    else:
        maserVariables['maserHouseTempExt']='0.0'
        maserVariables['maserHouseHumidExt']='0.0'
        maserVariables['maserHouseDewPtExt']='0.0'

    maserVariables['ts']=ts
    maserVariables['date-time']=st
    if (float(maserVariables['maserHouseTemp'])>maserHouseTempLimit):
        hotAlarmFlag=True
    if (float(maserVariables['maserHouseTempExt'])>maserHouseTempLimit):
        hotAlarmFlag=True
    if hotAlarmFlag and (datetime.date.today() > messageSentDate):
        message = 'Temperature inside the Maser House is '+\
                       maserVariables['maserHouseTemp']
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

    maserJson = json.dumps(maserVariables)
    print(maserJson)
    red.zadd('maserData',ts,maserJson)
    for key in maserVariables:
        print(key,maserVariables[key])
        red.hset('maser',key,maserVariables[key])
    time.sleep(60)
