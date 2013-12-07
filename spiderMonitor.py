#!/usr/bin/python env
# -*- coding: UTF-8 -*-
import re
import urllib2
import urlparse
import threading
import os
import sys
import time
import json
import md5
import hashlib
from spiderdb import MySSDB
from spiderqueue import HTTPSQSQueue

reload(sys)
sys.setdefaultencoding("UTF-8")

DomainBlacklist  = []
DomainDiggerList = []

DOMAINQUEUE01    = 'domain_01'
DOMAINQUEUE02    = 'domain_02'

DomainsProcessed = 0
DomainsStored    = 0
def getMD5(strMessage):
    strMD5 = ''
    md5c   = hashlib.md5()
    md5c.update(strMessage)
    strMD5 = md5c.hexdigest()[8:24]
    return strMD5

class DomainProcessor(threading.Thread):
    ThreadStartTime  = time.time()
    ThreadCanExit    = False
    ThreadTimeOut    = 60 * 2
    DomainsProcessed = 0
    DomainsStored    = 0
    _UserAgent       = 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.110 Safari/537.36'

    def __init__(self, tid):
        threading.Thread.__init__(self)
        self._tid = tid

    def run(self):
        while True:
            domain = HTTPSQSQueue.get(DOMAINQUEUE02)
            if '' == domain:
                time.sleep(1)
                continue
            if self.isDomainInBlacklist(domain):
                continue
            self.fuck(domain)

    def fuck(self, domain):
        global DomainsProcessed
        global DomainsStored
        DomainsProcessed  = DomainsProcessed + 1
        if not MySSDB.isDomainInDB('hdm', domain):
            MySSDB.setHItem('hdm', getMD5(domain), domain)
            HTTPSQSQueue.put(DOMAINQUEUE01, domain)
            DomainsStored = DomainsStored    + 1
        else:
            if not MySSDB.isDomainInDB('hdp', domain):
                MySSDB.setHItem('hdp', getMD5(domain), str(time.time()))

    def isDomainInBlacklist(self, domain):
        for black in DomainBlacklist:
            if domain.endswith(black):
                return True
        return False

    
class Monitor(threading.Thread):
    _BlacklistFileModifyTime = 0
    _BlacklistFileName       = 'blacklist.txt'
    _StartTime               = time.time()
    def __init__(self):
        threading.Thread.__init__(self)
        
    def run(self):
        while True:
            self.reportDBStatus()
            self.refreshBlackList()
            self.checkDomainsQueue()
            time.sleep(15)

    def reportDBStatus(self):
        try:
            c1 = MySSDB.getHSize('hdm')
            c2 = MySSDB.getHSize('hdp')
            c3 = int(json.loads(HTTPSQSQueue.status(DOMAINQUEUE01).replace('\n', '')).pop('unread'))
            c4 = int(json.loads(HTTPSQSQueue.status(DOMAINQUEUE02).replace('\n', '')).pop('unread'))
            print '--------------------------->DB: %d / %d , Mem:%d / %d , Queue: %d / %d , Time:%f' % (c1, c2, DomainsProcessed, DomainsStored, c3, c4, time.time() - self._StartTime)
        except Exception,e:
            print e


    def checkDomainsQueue(self):
        try:
            ItemsTotoal = int(json.loads(HTTPSQSQueue.status(DOMAINQUEUE02).replace('\n', '')).pop('unread'))
            if ItemsTotoal > 100000:
                HTTPSQSQueue.reset(DOMAINQUEUE02)                
                print '---------------------->HTTPSQSQueue %s will be full, waiting for reset!!!!!!!!!!!!!' % DOMAINQUEUE02
            ItemsTotoal = int(json.loads(HTTPSQSQueue.status(DOMAINQUEUE01).replace('\n', '')).pop('unread'))
            if ItemsTotoal > 100000:
                HTTPSQSQueue.reset(DOMAINQUEUE01)                
                print '---------------------->HTTPSQSQueue %s will be full, waiting for reset!!!!!!!!!!!!!' % DOMAINQUEUE01
        except Exception, e:
            print e

    def refreshBlackList(self):
        if not os.path.exists(self._BlacklistFileName):return
        global DomainBlacklist
        BlacklistFileModifyTime = os.stat(self._BlacklistFileName).st_mtime
        if self._BlacklistFileModifyTime == BlacklistFileModifyTime:
            return
        try:
            f = open(self._BlacklistFileName)
            lines = f.readlines()
            blacklists = []
            for line in lines:
                domain = line.strip().replace('\n', '').replace('\n', '')
                if len(domain) > 0:blacklists.append(domain)
            DomainBlacklist = blacklists[:]
            blacklists = None
            self._BlacklistFileModifyTime = BlacklistFileModifyTime
            print 'Get %d domains in blacklist' % len(DomainBlacklist)
        except Exception, e:
            print e
        finally:
            f.close()
                

if __name__ == '__main__' :
    DomainProcessor(0).start()
    Monitor().start()