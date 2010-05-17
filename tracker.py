from twisted.internet.protocol import Factory, Protocol
from twisted.internet import reactor, defer
from twisted.names import dns 
from twisted.names.dns import RRHeader, Record_A, A, IN
from twisted.names import common, client, server

import socket
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.connect(('telekommunisten.org', 0)) 
ImaIPAddr = s.getsockname()[0]
ImaRecordA = Record_A(ImaIPAddr,'1')

class MammatusResolver(common.ResolverBase):
    def __init__(self):
        print "INIT!!"
        common.ResolverBase.__init__(self)
        self._waiting = {}

    def _lookup(self, name, cls, type, timeout):
        print "LOOKUP!!", name
        key = (name, type, cls)
        waiting = self._waiting.get(key)
        if waiting is None:
            self._waiting[key] = []
            d = defer
            d = defer.fail(IOError("No domain name servers available"))
            def cbResult(result):
                for d in self._waiting.pop(key):
                    d.callback(result)
                ImaRR = RRHeader(name=name,type=A,cls=IN,ttl=1,payload=ImaRecordA)
                return ([ImaRR], [], [])
            d.addBoth(cbResult)
        else:
            d = defer.Deferred()
            waiting.append(d)
        return d


verbosity = 0
resolver = MammatusResolver()
f = server.DNSServerFactory(clients=[resolver], verbose=verbosity)
p = dns.DNSDatagramProtocol(f)
f.noisy = p.noisy = verbosity

reactor.listenUDP(53, p)
reactor.listenTCP(53, f)
print 'Mammatus DNS Active'
reactor.run()

