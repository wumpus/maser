#!/usr/bin/env python

import sys
from collections import defaultdict

import parse_maser


for f in sys.argv[1:]:
    with open(f, encoding='iso8859') as fd:
        maserVariables = defaultdict(dict)
        maserVariables = parse_maser.parseMaserVariables(maserVariables, fd.read())

        print(maserVariables)
