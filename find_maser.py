#!/usr/bin/env python

# look for a maser in the local network

#    LMT http://192.168.2.40/maser/
#    GLT http://192.168.1.73/monit.htm -- asked Nimesh
#    KP http://192.168.1.248/monit.htm -- downloaded
#    SMT not on .0. but how about 10.128.0. nope -- did find some 503s (10.128.0.99)
#    SMA not on .0. but how about 128.171.116. nope -- did find some 401s

import requests


for i in range(0, 255):
    ip = '192.168.0.{}'.format(i)
    for path in ('/maser/', '/monit.htm'):
        url = 'http://{}{}'.format(ip, path)
        #print(url)

        try:
            resp = requests.get(url, timeout=0.01)
        except requests.exceptions.RequestException:
            continue

        print('found something at', url, resp.status_code)
        
        if resp.status_code == 404:
            continue
        if resp.status_code != 200:
            print('for {} got {}'.format(url, resp.status_code))
            continue
        print('found', url)
