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

    Priority = {0:'EMERG', 1:'ALERT', 2:'CRIT', 3:'ERROR', 4:'WARN', 5:'NOTICE', 6:'Info', 7:'Debug'}

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

if __name__ == '__main__':
    C.Info('dd')
    C.Info('zz', C.DEBUG)
    C.Info('xx', C.INFO)