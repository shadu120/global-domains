#!/usr/bin/python env
import time
from httpsqs import HTTPSQS

host     = '127.0.0.1'
port     = 1218
password = 'xxoo'
httpsqs  = HTTPSQS(host = '127.0.0.1', port=1218, password = 'xxoo')

class HTTPSQSQueue():

    @staticmethod
    def put(qname, item):
        return httpsqs.put(qname, item)

    @staticmethod
    def get(qname):
        item = httpsqs.get(qname)
        if item == False:
            return ''
        else:
            return item

    @staticmethod
    def status(qname):
        return httpsqs.status(qname)

    @staticmethod
    def reset(qname):
        return httpsqs.reset(qname)

if __name__ == '__main__':
    print HTTPSQSQueue.put('dv', '111')
    print HTTPSQSQueue.put('dv', '111')
    print HTTPSQSQueue.put('dv', '111')
    print HTTPSQSQueue.get('dv')
    print HTTPSQSQueue.status('dv')
    print HTTPSQSQueue.reset('dv')
