#!/usr/bin/python
# -*- coding: UTF-8 -*-
import re
import urllib2
import urlparse
import threading
import sys
import time
from Zeander import MD5, C
from spiderqueue import HTTPSQSQueue


DOMAINQUEUE01  = 'domain_01'
DOMAINQUEUE02  = 'domain_02'
DomainSpidders = []

class DomainSpidder(threading.Thread):
    ThreadStartTime    = time.time()
    ThreadRefreshTime  = time.time()
    ThreadCanExit      = False
    ThreadTimeOut      = 60 * 2
    _UserAgent         = 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.110 Safari/537.36'
    TotalProcessed     = 0

    def __init__(self, tid):
        threading.Thread.__init__(self)
        self._tid = tid

    def isThreadDead(self):
        if time.time() - self.ThreadRefreshTime > self.ThreadTimeOut:
            self.ThreadCanExit = False

    def isDomainValid(self, domain):
        if not len(domain) > 0                        : return False
        if domain[-1] in '.1234567890-'               : return False
        for item in domain:
            if item in ',&><~!@#$%^&*()+=[]|/\\\"\''  : return False
        return True

    def parseDomainFromUrl(self, url):
        domain = ''
        try:
            domain = urlparse.urlparse(url)[1].split(':')[0].lower().strip()
            if not self.isDomainValid(domain):domain = ''
        except Exception, e:
            C.Info('%s : %s' % (str(e), url), C.ERROR)
        finally:
            return domain
    
    def parseUrlsFromHTMLContent(self, HTMLContent):
        results = []
        try:
            urls = re.findall(r'''<a(\s*)(.*?)(\s*)href(\s*)=(\s*)([\"\s]*)([^\"\']+?)([\"\s]+)(.*?)>''' ,HTMLContent,re.S|re.I)
            for url in urls:
                u = url[6]
                if u.startswith('http://') or u.startswith('https://') or u.startswith('www.'):
                    results.append(u)
        except Exception, e:
            pass
        finally:
            return list(set(results))

    def getHTMLContentFromUrl(self, url):
        htmlContent = ''
        try:
            rq  = urllib2.Request(url)
            rq.add_header('User-Agent', self._UserAgent)
            rs = urllib2.urlopen(rq, timeout=20)
            htmlContent = rs.read()
        except Exception,e:
            C.Info('%s : %s' % (str(e), url), C.ERROR)
        finally:
            return htmlContent

    def fuckDomain(self, originalDomain):
        time1       = time.time()
        newDomains  = []
        hc          = self.getHTMLContentFromUrl('http://' + originalDomain)
        urls        = self.parseUrlsFromHTMLContent(hc)
        for url in urls:
            domain  = self.parseDomainFromUrl(url)
            if not domain in newDomains and not domain == originalDomain and not domain == '':
                HTTPSQSQueue.put(DOMAINQUEUE02, domain)
                newDomains.append(domain)
        C.Info('(%2d) get %3d new domains from %s in %.fs' % (self._tid, len(newDomains), originalDomain, time.time()-time1), C.DEBUG)
        newDomains = []

    def fuck(self):
        while True:
            originalDomain = HTTPSQSQueue.get(DOMAINQUEUE01)
            if originalDomain == '':
                time.sleep(5)
                continue
            self.fuckDomain(originalDomain)
            self.ThreadRefreshTime = time.time()
            self.TotalProcessed  = self.TotalProcessed + 1
            info = '*****(%2d) processed %d domains in %.fs' % (self._tid, self.TotalProcessed, self.ThreadRefreshTime-self.ThreadStartTime)
            C.Info(info, C.DEBUG)
            if self.ThreadCanExit == True: break

    def run(self):
        self.fuck()

class Monitor(threading.Thread):
    ThreadStartTime  = time.time()
    TotalProcessed   = 0
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        while True:
            self.ShowStatus()
            time.sleep(15)
            pass
    def ShowStatus(self):
        self.TotalProcessed = 0
        for spider in DomainSpidders:
            self.TotalProcessed = self.TotalProcessed + spider.TotalProcessed
        TimeUsed = time.time() - self.ThreadStartTime
        info = 'Totoal Domains:%d, Time used:%.fm, Speed:%.f/m' % (self.TotalProcessed, TimeUsed/60, float(self.TotalProcessed * 60/TimeUsed))
        C.Info(info, C.INFO)

if __name__ == '__main__' :
    reload(sys)
    sys.setdefaultencoding("UTF-8")
    if len(sys.argv) < 2:
        sys.exit()
    if len(sys.argv) > 2:
        HTTPSQSQueue.put(DOMAINQUEUE01,sys.argv[2])
    for i in range(0,int(sys.argv[1])):
        DomainSpidders.append(DomainSpidder(i))
    for digger in DomainSpidders:
        digger.start()
    Monitor().start()