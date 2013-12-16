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
from Zeander import MD5, C, TLD
from spiderdb import MySSDB
from spiderqueue import HTTPSQSQueue

SSDBHOST         = '127.0.0.1'
SSDBPORT         = 8888

DOMAINQUEUE01    = 'domain_01'
DOMAINQUEUE02    = 'domain_02'

DomainsProcessed = 0
DomainsStored    = 0

class DomainProcessor(threading.Thread):
    _tid                     = 0
    _ssdb                    = None

    ThreadStartTime          = time.time()
    ThreadCanExit            = False
    ThreadTimeOut            = 60 * 2
    
    DomainsProcessed         = 0
    DomainsStored            = 0
    
    _UserAgent               = 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.110 Safari/537.36'

    _tld                     = None   # Top Level Domain parser form Zeander
    _BlacklistFileModifyTime = 0
    _BlacklistFileName       = 'blacklist.txt'

    TLDUserPartRepeatMax     = 200 * 2

    TLDUserPartCacheMaxLen   = 2000
    _TLDUserPartCache        = {}

    InternalBlackListMaxLen  = 2000
    ExternalBlacklistMaxLen  = 1000
    _InternalBlacklist       = []
    _ExternalBlacklist       = []


    def __init__(self, tid = 0):
        threading.Thread.__init__(self)
        self._tid  = tid
        self._tld  = TLD()
        self._ssdb = MySSDB(SSDBHOST, SSDBPORT)

    def run(self):
        while True:
            domain = HTTPSQSQueue.get(DOMAINQUEUE02).lower()
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
        if not self._ssdb.isDomainInDB('hdm', domain):
            self._ssdb.setHItem('hdm', MD5(domain), domain)
            HTTPSQSQueue.put(DOMAINQUEUE01, domain)
            DomainsStored = DomainsStored    + 1

    def monitor(self):
        if os.path.exists('debug.dump'):self.dump()
    def refreshBlacklist(self, domain):
        self.refreshInternalBlacklist(domain)
        self.refreshExternalBlacklist()
    def isDomainInBlacklist(self, domain):
        return self.isDomainInExternalBlacklist(domain) or self.isDomainInInternalBlacklist(domain)
    def isDomainInInternalBlacklist(self, domain):
        domain_user_part = self._tld.getTLD(domain)[0]
        if not '' == domain_user_part:
            for black in self._InternalBlacklist:
                if domain_user_part == black: return True
        return False
    def isDomainInExternalBlacklist(self, domain):
        for black in self._ExternalBlacklist:
            if domain.endswith(black): return True
        return False

    def refreshInternalBlacklist(self, domain):
        '''
        www.chinaz.com.cn -> chinaz -> CacheDictionary
        '''
        black = self._tld.getTLD(domain)[0]
        if '' == black : return
        if self._TLDUserPartCache.has_key(black):
            self._TLDUserPartCache[black] = self._TLDUserPartCache[black] + 1
            if self._TLDUserPartCache[black] > self.TLDUserPartRepeatMax/2 and not black in self._InternalBlacklist:
                self._InternalBlacklist.append(black)
        else:
            self._TLDUserPartCache[black] = 1

        if len(self._TLDUserPartCache) > self.TLDUserPartCacheMaxLen:
            tempList = sorted(self._TLDUserPartCache, key=self._TLDUserPartCache.get)
            for i in range(0, len(tempList)/2):
                self._TLDUserPartCache.pop(tempList[i])
            tempList = None

        if len(self._InternalBlacklist) > self.InternalBlackListMaxLen:
            self.saveInternalBlacklist()
    
    def refreshExternalBlacklist(self):
        if     (random.randrange(1,11) % 3 == 0)      :return
        if not os.path.exists(self._BlacklistFileName):return
        BlacklistFileModifyTime = os.stat(self._BlacklistFileName).st_mtime
        if self._BlacklistFileModifyTime == BlacklistFileModifyTime:
            return
        try:
            f          = open(self._BlacklistFileName)
            lines      = f.readlines()
            blacklists = []
            for line in lines:
                domain = line.strip().replace('\n', '').replace('\r', '')
                if len(domain) > 0:blacklists.append(domain)
            self._ExternalBlacklist = blacklists[:]
            blacklists = None
            self._BlacklistFileModifyTime = BlacklistFileModifyTime
            C.Info('Get %2d domains in blacklist' % len(self._ExternalBlacklist), C.INFO)
        except Exception, e:
            C.Info(str(e), C.ERROR)
        finally:
            f.close()

    def saveInternalBlacklist(self):
        f=open('blacklist02.log', 'a+')
        try:
            
            for i in range(0, self.InternalBlackListMaxLen/2):
                f.write(self._InternalBlacklist[i] + '\n')
            C.Info('internal blacklist saved', C.ALERT)
        except Exception, e:
            C.Info(str(e), C.ERROR)
        finally:
            self._InternalBlacklist = self._InternalBlacklist[self.InternalBlackListMaxLen/2:]
            f.close()

    def dump(self):
        C.Info('DUMP================================================', C.NOTICE)
        for item in self._TLDUserPartCache.iteritems():
            C.Info('%4d/%s' % (item[1], item[0]), C.NOTICE)
        for black in self._InternalBlacklist:
            C.Info('Black domain:%s' % black, C.NOTICE)
        C.Info('TLDUserPartCache:%4d, InternalBlacklist:%4d, ExternalBlacklist:%4d' % (len(self._TLDUserPartCache), len(self._InternalBlacklist), len(self._ExternalBlacklist)), C.NOTICE)
        C.Info('DUMP================================================', C.NOTICE)
    
class Monitor(threading.Thread):
    _StartTime     = time.time()
    _QueueUnRead01 = 0
    _QueueUnRead02 = 0
    _ssdb          = None
    def __init__(self):
        threading.Thread.__init__(self)
        self._ssdb = MySSDB(SSDBHOST, SSDBPORT)
        
    def run(self):
        while True:
            self.checkDomainsQueue()
            self.reportDBStatus()
            time.sleep(15)

    def reportDBStatus(self):
        '''
        SSDB should be in separated instance, otherwise there will be drmastic problems!!!
        '''
        TotalDomainInDB = self._ssdb.getHSize('hdm')
        TotalTimeUsed   = time.time() - self._StartTime
        AverageSpeed    = float(DomainsStored / TotalTimeUsed)

        C.Info('DB: %d, Mem:%d/%d, Queue:%d/%d, Time:%.fm, Speed:%.f/s' % \
            (TotalDomainInDB, \
                DomainsProcessed, DomainsStored, \
                self._QueueUnRead01, self._QueueUnRead02, \
                TotalTimeUsed/60, AverageSpeed), \
            C.INFO)

    def checkDomainsQueue(self):
        self._QueueUnRead01 = self.getQueueUnRead(DOMAINQUEUE01)
        self._QueueUnRead02 = self.getQueueUnRead(DOMAINQUEUE02)
        if self._QueueUnRead01 > 10000:
            C.Info('HTTPSQSQueue %s will be full, waiting for cache!!!!!!!!!!!!!' % DOMAINQUEUE01, C.ALERT)
            self.cacheHTTPSQSQueue(DOMAINQUEUE01)
            C.Info('HTTPSQSQueue cached', C.ALERT)
        if self._QueueUnRead02 > 10000:
            C.Info('HTTPSQSQueue %s will be full, waiting for reset!!!!!!!!!!!!!' % DOMAINQUEUE02, C.ALERT)
            for i in range(0, 100):HTTPSQSQueue.put(DOMAINQUEUE01, HTTPSQSQueue.get(DOMAINQUEUE02))
            HTTPSQSQueue.reset(DOMAINQUEUE02)                

    def getQueueUnRead(self, qName):
        qStatus = HTTPSQSQueue.status(qName).replace('\n', '')
        UnRead  = 0
        try:
            UnRead = int(json.loads(qStatus).pop('unread'))
        except Exception, e:
            C.Info(str(e), C.ERROR)
        return UnRead

    def cacheHTTPSQSQueue(self,qName):
        while self.getQueueUnRead(qName) > 1000:
            domains = []
            for i in range(0, 100):
                domains.append(HTTPSQSQueue.get(DOMAINQUEUE01))
            
            try:
                cacheFileName = qName + '-' + time.strftime('%Y%m%d%H%M%S') + '.qc'
                f = open(cacheFileName, 'a+')
                try:
                    f.write('\n'.join(domains))
                except:
                    f.close()
            except:
                pass


if __name__ == '__main__' :
    reload(sys)
    sys.setdefaultencoding("UTF-8")
    DomainProcessor(0).start()
    Monitor().start()