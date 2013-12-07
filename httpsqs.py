#!/us/bin/python env
# -*- coding: utf-8 -*-
# API for HTTPSQL  by shadu{AT} foxmail.com
# HTTPSQL HelpDoc: http://blog.s135.com/httpsqs
# HTTPSQL Project: https://code.google.com/p/httpsqs/
# Server Argments: httpsqs -d -p 1218 -x ./ -a xxoo

"""

ulimit -SHn 65535

wget http://httpsqs.googlecode.com/files/libevent-2.0.12-stable.tar.gz
tar zxvf libevent-2.0.12-stable.tar.gz
cd libevent-2.0.12-stable/
./configure --prefix=/usr/local/libevent-2.0.12-stable/
make
make install
cd ../

wget http://httpsqs.googlecode.com/files/tokyocabinet-1.4.47.tar.gz
tar zxvf tokyocabinet-1.4.47.tar.gz
cd tokyocabinet-1.4.47/
./configure --prefix=/usr/local/tokyocabinet-1.4.47/
#Note: In the 32-bit Linux operating system, compiler Tokyo cabinet, please use the ./configure --enable-off64 instead of ./configure to breakthrough the filesize limit of 2GB.
#./configure --enable-off64 --prefix=/usr/local/tokyocabinet-1.4.47/
make
make install
cd ../

wget http://httpsqs.googlecode.com/files/httpsqs-1.7.tar.gz
tar zxvf httpsqs-1.7.tar.gz
cd httpsqs-1.7/
make
make install
cd ../

## you may need this ...
apt-get install zlib1g-dev
apt-get install libbz2-dev


"""
import socket
import json
import urllib, urllib2

class HTTPSQS():
	timeout = 15

	def __init__(self, host = '127.0.0.1', port=1218, password = 'xxoo'):
		self.host 		= host
		self.port		= port
		self.password	= password

	def _getHttpContent(self, targetUrl, postData = None):
		request	= urllib2.Request(targetUrl, data=postData)
		r 		= ''
		try:
			f   = urllib2.urlopen(request, timeout=self.timeout)
			r   = f.read()
		except urllib2.HTTPError, e1:
			print e1
		except urllib2.URLError, e2:
			print e2
		except socket.timeout, e3:
			print e3
		except Exception, e:
			print e
		finally:
			pass
		return r


	#curl "http://host:port/?charset=utf-8&name=your_queue_name&opt=get&auth=mypass123"
	def get(self, queueName, charset='utf-8'):
		targetUrl   = "http://%s:%s/?charset=%s&name=%s&opt=get&auth=%s" % (self.host, self.port, charset, queueName, self.password)
		queueResult = self._getHttpContent(targetUrl)
		if queueResult == 'HTTPSQS_ERROR' or queueResult == '' or queueResult == 'HTTPSQS_GET_END':
			return False
		else:
			return queueResult

	#curl "http://host:port/?name=your_queue_name&opt=put&data=经过URL编码的文本消息&auth=mypass123"
	def put(self, queueName, queueData, charset='utf-8'):
		targetUrl   = "http://%s:%d/?charset=%s&name=%s&opt=put&auth=%s" % (self.host, self.port, charset, queueName, self.password)
		postData    = urllib.urlencode({'data':queueData})[5:]
		queueResult =  self._getHttpContent(targetUrl, postData)
		if queueResult == 'HTTPSQS_PUT_OK':
			return True
		elif queueResult == 'HTTPSQS_PUT_END':
			return queueResult
		else:
			return False

	#curl "http://host:port/?name=your_queue_name&opt=status_json&auth=mypass123"
	def status(self, queueName):
		targetUrl = "http://%s:%d/?name=%s&opt=status_json&auth=%s" % (self.host, self.port, queueName, self.password)
		return self._getHttpContent(targetUrl)

	#curl "http://host:port/?name=your_queue_name&opt=reset&auth=mypass123"
	def reset(self, queueName):
		targetUrl = "http://%s:%d/?name=%s&opt=reset&auth=%s" % (self.host, self.port, queueName, self.password)
		return self._getHttpContent(targetUrl) == 'HTTPSQS_RESET_OK'

if __name__ == '__main__':
	httpsqs = HTTPSQS('127.0.0.1')
	print httpsqs.put('SDQ', 'www.hiw3.com')
	print httpsqs.put('SDQ', 'www.vrv.com.cn')
	print httpsqs.put('SDQ', 'www.qq.com')
	#print httpsqs.get('SDQ')
	print httpsqs.status('SDQ')
	#print httpsqs.reset('SDQ')
	#j = json.loads(httpsqs.status('domain'))
	#print j
