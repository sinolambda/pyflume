#coding=utf-8
'''
Created on 2013-07-18

@author: Felix
'''
from genpy.flume import ThriftSourceProtocol
from genpy.flume.ttypes import ThriftFlumeEvent

from thrift.transport import TTransport, TSocket
from thrift.protocol import TCompactProtocol

class _Transport(object):
    def __init__(self, thrift_host, thrift_port, timeout=None, unix_socket=None):
        self.thrift_host = thrift_host
        self.thrift_port = thrift_port
        self.timeout = timeout
        self.unix_socket = unix_socket
        
        self._socket = TSocket.TSocket(self.thrift_host, self.thrift_port, self.unix_socket)
        self._transport_factory = TTransport.TFramedTransportFactory()
        self._transport = self._transport_factory.getTransport(self._socket)
        
    def connect(self):
        try:
            if self.timeout:
                self._socket.setTimeout(self.timeout)
            if not self.is_open():
                self._transport = self._transport_factory.getTransport(self._socket)
                self._transport.open()
        except Exception, e:
            print(e)
            self.close()
    
    def is_open(self):
        return self._transport.isOpen()
    
    def get_transport(self):
        return self._transport
    
    def close(self):
        self._transport.close()
        
class FlumeClient(object):
    def __init__(self, thrift_host, thrift_port, timeout=None, unix_socket=None):
        self._transObj = _Transport(thrift_host, thrift_port, timeout=timeout, unix_socket=unix_socket)
        self._protocol = TCompactProtocol.TCompactProtocol(trans=self._transObj.get_transport())
        self.client = ThriftSourceProtocol.Client(iprot=self._protocol, oprot=self._protocol)
        self._transObj.connect()
        
    def send(self, event):
        try:
            self.client.append(event)
        except Exception, e:
            print(e)
        finally:
            self._transObj.connect()
    
    def send_batch(self, events):
        try:
            self.client.appendBatch(events)
        except Exception, e:
            print(e)
        finally:
            self._transObj.connect()
    
    def close(self):
        self._transObj.close()
    
if __name__ == '__main__':
    import random
    flume_client = FlumeClient('192.168.1.141', 4141)
    event = ThriftFlumeEvent({'a':'hello', 'b':'world'}, 'events under hello world2')
    events = [ThriftFlumeEvent({'a':'hello', 'b':'world'}, 'events under hello world%s' % random.randint(0, 1000)) for _ in range(100)]
    
    flume_client.send(event)
    flume_client.send_batch(events)
    flume_client.close()
    
        