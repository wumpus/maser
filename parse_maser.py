#!/usr/bin/env python

# refactored from code written by Nimesh Patel

from bs4 import BeautifulSoup


def cleanUpString(s):
    s2 = s.replace(' ', '_')
    s3 = s2.replace('[', '_')
    s4 = s3.replace(']', '')
    strEncode = s4.encode("ascii", "ignore")
    return(strEncode.decode())


def parseMaserVariables(maserVariables, rtext):
    soup = BeautifulSoup(rtext, 'html.parser')  # XXX lxml
    allWords = soup.findAll(text=True)
    allWordsFiltered = [x for x in allWords if x != '\n']
    for i in range(0, len(allWordsFiltered), 2):
        if i == 0:
            v = 'imaser_timestamp'
            maserTimestampWords = allWordsFiltered[1].split(' ')
            maserTimeStamp0 = maserTimestampWords[0] + ' ' + maserTimestampWords[1]
            maserVariables[v] = cleanUpString(maserTimeStamp0)
        else:
            v = allWordsFiltered[i].lower()
            maserVariables[cleanUpString(v)] = allWordsFiltered[i+1]

    return maserVariables
