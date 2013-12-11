#!/usr/bin/python
# -*- coding: UTF-8 -*-
import os
import md5
import hashlib
import time
from copy import deepcopy

def MD5(strMessage):
    '''
    return md5 hex string
    '''
    strMD5 = ''
    md5c   = hashlib.md5()
    md5c.update(strMessage)
    strMD5 = md5c.hexdigest()[8:24]
    return strMD5


class C():
    EMERG  = 0
    ALERT  = 1
    CRIT   = 2
    ERROR  = 3
    WARN   = 4
    NOTICE = 5
    INFO   = 6
    DEBUG  = 7

    Priority = {0:'EMERG', 1:'ALERT', 2:'CRIT', 3:'ERROR', 4:'WARN', 5:'NOTICE', 6:'INFO', 7:'DEBUG'}

    @staticmethod
    def Info(msg, pri = 6):
        if pri <= C.CheckDebugLevel():
            msg = '[%s][%s]:%s' % ( C.Priority[pri], time.strftime('%Y-%m-%d %H:%M:%S'),msg)
            print msg

    @staticmethod
    def CheckDebugLevel():
        if os.path.exists('debug.%s' % C.Priority[0].lower()) :return C.EMERG
        if os.path.exists('debug.%s' % C.Priority[1].lower()) :return C.ALERT
        if os.path.exists('debug.%s' % C.Priority[2].lower()) :return C.CRIT
        if os.path.exists('debug.%s' % C.Priority[3].lower()) :return C.ERROR
        if os.path.exists('debug.%s' % C.Priority[4].lower()) :return C.WARN
        if os.path.exists('debug.%s' % C.Priority[5].lower()) :return C.NOTICE
        if os.path.exists('debug.%s' % C.Priority[7].lower()) :return C.DEBUG
        return C.INFO

class TLD():
    '''
    # Source path of Mozilla's effective TLD names file.
    http://mxr.mozilla.org/mozilla/source/netwerk/dns/src/effective_tld_names.dat?raw=1
    '''
    TLD_DATA_FILE = './res/effective_tld_names.dat'
    tld_name_list = []

    def __init__(self):
        self._initTLDNameList()
        pass

    def _initTLDNameList(self):
        try:
            fhandle            = open(self.TLD_DATA_FILE)
            self.tld_name_list = set([line.strip() for line in fhandle if line[0] not in '/\n'])
        except Exception, e:
            C.Info(str(e), C.ERROR)
        finally:
            try:
                fhandle.close()
            except Exception, e2:
                C.Info(str(e2), C.ERROR)

    def getTLD(self, domain, active_only=True):
        '''
        input :chinaz.com
        output:('chinaz', 'com.cn', 'chinaz.com.cn', 'mail.chinaz.com.cn')
        '''
        domain_parts = domain.split('.')
        for i in range(0, len(domain_parts)):
            sliced_domain_parts = domain_parts[i:]

            match = '.'.join(sliced_domain_parts)
            wildcard_match = '.'.join(['*'] + sliced_domain_parts[1:])
            inactive_match = "!%s" % match

            # Match tlds
            if (match in self.tld_name_list or wildcard_match in self.tld_name_list or (active_only is False and inactive_match in self.tld_name_list)):
                return (domain_parts[i-1], ".".join(domain_parts[i:]), ".".join(domain_parts[i-1:]), domain)
        return ('', '', '', '')



if __name__ == '__main__':
    x = TLD()
    import time
    print '%f' % time.time()
    print x.getTLD('mail.chinaz.com.cn')
    print '%f' % time.time()
    print x.getTLD('localhost')
    print '%f' % time.time()
