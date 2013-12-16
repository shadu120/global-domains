#!/usr/bin/python
# -*- coding: UTF-8 -*-
import os
import sys

from spiderqueue import HTTPSQSQueue


class spiderTools():
    def __init__(self):
        pass

    def feed(self, qName, cFile):
        '''
        Feed HTTPSQSQueue with domains from cached file
        '''
        if not os.path.exists(cFile):
            print 'File does not exists!'
            return
        f     = open(cFile)
        lines = f.readlines()
        count = 0
        for line in lines:
            HTTPSQSQueue.put(qName,line.strip())
            count = count + 1
        f.close()
        print HTTPSQSQueue.status(qName)

    def reset(self, qName):
        '''
        reset a HTTPSQSQueue
        '''
        HTTPSQSQueue.reset(qName)
        print HTTPSQSQueue.status(qName)


if __name__ == '__main__' :
    reload(sys)
    sys.setdefaultencoding("UTF-8")
    if len(sys.argv) == 4 and sys.argv[1] == 'feed':
        spiderTools().feed(sys.argv[2], sys.argv[3])
    if len(sys.argv) == 3 and sys.argv[1] == 'reset':
        spiderTools().reset(sys.argv[2])