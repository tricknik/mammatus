from twisted.internet.protocol import Factory, Protocol
from twisted.internet import reactor
from twisted.names import dns 
from twisted.names.dns import RRHeader, Record_A, A, IN
from twisted.names import client, server

import socket
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.connect(('telekommunisten.org', 0)) 
ImaIPAddr = s.getsockname()[0] '
ImaRecordA = Record_A(ImaIPAddr,'1')
ImaRR = RRHeader(name='mammacloud',type=A,cls=IN,ttl=1,payload=ImaRecordA)
class MammatusResolver(client.Resolver):
    def filterAnswers(self, message):
        return ([ImaRR], [], [])

verbosity = 0
resolver = MammatusResolver(servers=[('10.1.1.1', 53)])
f = server.DNSServerFactory(clients=[resolver], verbose=verbosity)
p = dns.DNSDatagramProtocol(f)
f.noisy = p.noisy = verbosity

reactor.listenUDP(53, p)
reactor.listenTCP(53, f)
reactor.run()

