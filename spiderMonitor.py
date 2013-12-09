#!/usr/bin/python
# -*- coding: UTF-8 -*-
import random
import urllib2
import urlparse
import threading
import os
import sys
import time
import json
from copy import deepcopy
from Zeander import MD5, C
from spiderdb import MySSDB
from spiderqueue import HTTPSQSQueue

DOMAINQUEUE01    = 'domain_01'
DOMAINQUEUE02    = 'domain_02'

DomainsProcessed = 0
DomainsStored    = 0

class DomainProcessor(threading.Thread):
    ThreadStartTime          = time.time()
    ThreadCanExit            = False
    ThreadTimeOut            = 60 * 2
    
    DomainsProcessed         = 0
    DomainsStored            = 0
    
    _UserAgent               = 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.110 Safari/537.36'
    
    _BlacklistFileModifyTime = 0
    _BlacklistFileName       = 'blacklist.txt'

    DomainSuffixRepeatMax    = 100 * 2

    DomainSuffixCacheMaxLen  = 2000
    _DomainSuffixCache       = {}

    InternalBlackListMaxLen  = 2000
    ExternalBlacklistMaxLen  = 1000
    _InternalBlacklist       = []
    _ExternalBlacklist       = []

    def __init__(self, tid):
        threading.Thread.__init__(self)
        self._tid = tid

    def run(self):
        while True:
            domain = HTTPSQSQueue.get(DOMAINQUEUE02)
            if '' == domain and None == time.sleep(1): continue
            if self.isDomainInBlacklist(domain): 
                C.Info('Domain in black list: %s' % domain, C.DEBUG)
                continue
            self.fuck(domain)
            self.refreshBlacklist(domain)
            self.monitor()


    def fuck(self, domain):
        global DomainsProcessed, DomainsStored
        DomainsProcessed  = DomainsProcessed + 1
        if not MySSDB.isDomainInDB('hdm', domain):
            MySSDB.setHItem('hdm', MD5(domain), domain)
            HTTPSQSQueue.put(DOMAINQUEUE01, domain)
            DomainsStored = DomainsStored    + 1
        else:
            if not MySSDB.isDomainInDB('hdp', domain):
                MySSDB.setHItem('hdp', MD5(domain), str(time.time()))
    def monitor(self):
        if os.path.exists('debug.dump'):self.dump()
    def refreshBlacklist(self, domain):
        self.refreshInternalBlacklist(domain)
        self.refreshExternalBlacklist()
    def isDomainInBlacklist(self, domain):
        return self.isDomainInExternalBlacklist(domain) or self.isDomainInInternalBlacklist(domain)
    def isDomainInInternalBlacklist(self, domain):
        for black in self._InternalBlacklist:
            if domain.replace('.', '').endswith(black): return True
        return False
    def isDomainInExternalBlacklist(self, domain):
        for black in self._ExternalBlacklist:
            if domain.replace('.', '').endswith(black):
                return True
        return False
    def refreshInternalBlacklist(self, domain):
        black = domain.replace('.', '')[-7:]
        if self._DomainSuffixCache.has_key(black):
            self._DomainSuffixCache[black] = self._DomainSuffixCache[black] + 1
            if self._DomainSuffixCache[black] > self.DomainSuffixRepeatMax/2 and not black in self._InternalBlacklist:
                self._InternalBlacklist.append(black)
        else:
            self._DomainSuffixCache[black] = 1

        if len(self._DomainSuffixCache) > self.DomainSuffixCacheMaxLen:
            tempList = sorted(self._DomainSuffixCache, key=self._DomainSuffixCache.get)
            for i in range(0, len(tempList)/2):
                self._DomainSuffixCache.pop(tempList[i])

        if len(self._InternalBlacklist) > self.InternalBlackListMaxLen:
            self._InternalBlacklist = self._InternalBlacklist[self.InternalBlackListMaxLen/2:]
    
    def refreshExternalBlacklist(self):
        if (random.randrange(1,11) % 3 == 0):return
        if not os.path.exists(self._BlacklistFileName):return
        BlacklistFileModifyTime = os.stat(self._BlacklistFileName).st_mtime
        if self._BlacklistFileModifyTime == BlacklistFileModifyTime:
            return
        try:
            f = open(self._BlacklistFileName)
            lines = f.readlines()
            blacklists = []
            for line in lines:
                domain = line.strip().replace('\n', '').replace('\r', '').replace('.', '')
                if len(domain) > 0:blacklists.append(domain)
            self._ExternalBlacklist = blacklists[:]
            blacklists = None
            self._BlacklistFileModifyTime = BlacklistFileModifyTime
            C.Info('Get %2d domains in blacklist' % len(self._ExternalBlacklist), C.INFO)
        except Exception, e:
            C.Info(str(e), C.ERROR)
        finally:
            f.close()

    def dump(self):
        C.Info('DUMP================================================', C.NOTICE)
        for item in self._DomainSuffixCache.iteritems():
            C.Info('%4d/%s' % (item[1], item[0]), C.NOTICE)
        for black in self._InternalBlacklist:
            C.Info('Black domain:%s' % black, C.NOTICE)
        C.Info('DomainSuffixCache:%4d, InternalBlacklist:%4d, ExternalBlacklist:%4d' % (len(self._DomainSuffixCache), len(self._InternalBlacklist), len(self._ExternalBlacklist)), C.NOTICE)
        C.Info('DUMP================================================', C.NOTICE)
    
class Monitor(threading.Thread):
    _StartTime = time.time()
    _QueueUnRead01 = 0
    _QueueUnRead02 = 0
    def __init__(self):
        threading.Thread.__init__(self)
        
    def run(self):
        while True:
            self.reportDBStatus()
            self.checkDomainsQueue()
            time.sleep(15)

    def reportDBStatus(self):
        C.Info('DB: %d / %d , Mem:%d / %d , Queue: %d / %d , Time:%.f' % \
            (MySSDB.getHSize('hdm'), MySSDB.getHSize('hdp'), \
                DomainsProcessed, DomainsStored, \
                self._QueueUnRead01, self._QueueUnRead02, \
                time.time() - self._StartTime), \
            C.INFO)

    def checkDomainsQueue(self):
        if (random.randrange(0, 10) % 3) == 0 : return
        self._QueueUnRead01 = self.getQueueUnRead(DOMAINQUEUE01)
        self._QueueUnRead02 = self.getQueueUnRead(DOMAINQUEUE02)
        if self._QueueUnRead01 > 100000:
            HTTPSQSQueue.reset(DOMAINQUEUE01)                
            C.Info('HTTPSQSQueue %s will be full, waiting for reset!!!!!!!!!!!!!' % DOMAINQUEUE01, C.ALERT)
        if self._QueueUnRead02 > 100000:
            HTTPSQSQueue.reset(DOMAINQUEUE02)                
            C.Info('HTTPSQSQueue %s will be full, waiting for reset!!!!!!!!!!!!!' % DOMAINQUEUE02, C.ALERT)

    def getQueueUnRead(self, qName):
        qStatus = HTTPSQSQueue.status(qName).replace('\n', '')
        UnRead  = 0
        try:
            UnRead = int(json.loads(qStatus).pop('unread'))
        except Exception, e:
            C.Info(str(e), C.ERROR)
        return UnRead



if __name__ == '__main__' :
    reload(sys)
    sys.setdefaultencoding("UTF-8")
    DomainProcessor(0).start()
    Monitor().start()