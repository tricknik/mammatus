"""
Mammatus, a DNS-centric HA platform 

Dmytri Kleiner <dk@telekommunisten.net>, 2010
"""

import os, socket, urlparse
from twisted.application import service, internet
from twisted.internet import reactor, defer
from twisted.names import server as names_server, dns, common, client

#########
# On the surface of it, a DNS resolver which only responds
# with it's own IP address may seem to be the most idiotic class
# ever written. But HAHA! For us, the only thing better than
# a dummy resolver is a useful idiot.
##
class MammatusIdioticResolver(common.ResolverBase):
    """ Respond to all requests with own IP address
    """
    def __init__(self, manager):
        common.ResolverBase.__init__(self)
        self._waiting = {}
        self.manager = manager
        #########
        # Create a DNS A Record for the IP address of this computer
        # by connecting to some outside place and checking the socket
        ##
        def getRecordA():
            """ Return an A record with external IP address of localhost
            """
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                # umm, I know google is never, never, never-ever down right?
                s.connect(('google.com', 0)) 
                IPAddr = s.getsockname()[0]
            except:
                IPAddr = "127.0.0.1"
            return dns.Record_A(IPAddr,'1')
        self.Record_A = getRecordA()

    def _lookup(self, name, cls, type, timeout):
        key = (name, type, cls)
        waiting = self._waiting.get(key)
        if waiting is None:
            self._waiting[key] = []
            d = defer.fail(IOError("No domain name servers available"))
            def cbResult(result):
                for d in self._waiting.pop(key):
                    d.callback(result)
                RR = dns.RRHeader(name=name,type=dns.A,cls=dns.IN,ttl=1,payload=self.Record_A)
                return ([RR], [], [])
            d.addBoth(cbResult)
        else:
            d = defer.Deferred()
            waiting.append(d)
        return d

def getResolver(manager):
    return MammatusIdioticResolver(manager)

