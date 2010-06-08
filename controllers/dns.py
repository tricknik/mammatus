"""
Mammatus, a DNS-centric HA platform 

Dmytri Kleiner <dk@telekommunisten.net>, 2010
"""

import os, socket, urlparse
from twisted.internet import reactor
from twisted.internet.task import deferLater
from twisted.names import dns, common

class MammatusResolver(common.ResolverBase):
    def setModel(self, model):
        self.model = model
    def _lookup(self, name, cls, type, timeout):
        def resolve(addr):
            RecordA = dns.Record_A(addr)
            RR = dns.RRHeader(name=name,type=type,cls=cls,ttl=1,payload=RecordA)
            return ([RR], [], [])
        d = deferLater(reactor, 0, self.model.getHostByName, name)
        d.addCallback(resolve)
        return d

def getResolver(model):
    resolver = MammatusResolver()
    resolver.setModel(model)
    return resolver

