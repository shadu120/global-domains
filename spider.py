#!/usr/bin/python env
# -*- coding: UTF-8 -*-
import re
import urllib2
import urlparse
import threading
import os
import sys
import time
import md5
import hashlib
from spiderdb import MySSDB
from spiderqueue import HTTPSQSQueue
reload(sys)
sys.setdefaultencoding("UTF-8")

DOMAINQUEUE01  = 'domain_01'
DOMAINQUEUE02  = 'domain_02'
DomainSpidders = []

def getMD5(strMessage):
    strMD5 = ''
    md5c   = hashlib.md5()
    md5c.update(strMessage)
    strMD5 = md5c.hexdigest()[8:24]
    return strMD5

class DomainSpidder(threading.Thread):
    ThreadStartTime  = time.time()
    ThreadCanExit    = False
    ThreadTimeOut    = 60 * 2
    _UserAgent       = 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.110 Safari/537.36'

    def __init__(self, tid):
        threading.Thread.__init__(self)
        self._tid = tid

    def isThreadDead(self):
        if time.time() - self.ThreadStartTime > self.ThreadTimeOut:
            self.ThreadCanExit = False

    def parseDomainFromUrl(self, url):
        domain = ''
        try:
            domain = urlparse.urlparse(url)[1].split(':')[0].lower().strip()
            if len(domain) > 0 and domain[-1] in ('0','1','2','3','4','5', '6', '7', '8', '9') and domain.find('.') == -1:
                domain = ''
        except Exception, e:
            print e, domain
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
            print e, url
            pass
        finally:
            return htmlContent

    def fuckDomain(self, originalDomain):
        domains = []
        domains.append(originalDomain)
        hc          = self.getHTMLContentFromUrl('http://' + originalDomain)
        urls        = self.parseUrlsFromHTMLContent(hc)
        for url in urls:
            domain  = self.parseDomainFromUrl(url)
            if not domain in domains :
                HTTPSQSQueue.put(DOMAINQUEUE02, domain)
                domains.append(domain)
        print '(%3d) get %4d domains from %s' % (self._tid, len(domains), originalDomain)
        domains = []

    def fuck(self):
        while True:
            originalDomain = HTTPSQSQueue.get(DOMAINQUEUE01)
            if originalDomain == '':
                time.sleep(5)
                continue
            self.fuckDomain(originalDomain)
            self.ThreadStartTime = time.time()
            if self.ThreadCanExit == True:
                break

    def run(self):
        self.fuck()
                

if __name__ == '__main__' :
    if len(sys.argv) < 3:
        sys.exit()
    HTTPSQSQueue.reset(DOMAINQUEUE01)
    HTTPSQSQueue.put(DOMAINQUEUE01,sys.argv[1])
    for i in range(0,int(sys.argv[2])):
        DomainSpidders.append(DomainSpidder(i))
    for digger in DomainSpidders:
        digger.start()
