"""
Mammatus, a DNS-centric HA platform 

Dmytri Kleiner <dk@telekommunisten.net>, 2010

The "simplest thing that could possibly work" edition

This is an initial implementation of the simplest possible
scenario for Mammatus; exploiting the ubiquity of DNS fail-over 
to create a high availablity webserver that only redirects a get 
request to a content delivery network.
"""

import os, socket, urlparse
from twisted.application import service, internet
from twisted.internet import defer
from twisted.web import server as web_server, resource
from twisted.names import server as names_server, dns, hosts, common, client

#########
# Create a DNS A Record for the IP address of this computer
# by connecting to some outside place and checking the socket
##
def getRecordA():
    """ Return an A record with external IP address of localhost
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    # umm, I know google is never, never, never-ever down right?
    s.connect(('google.com', 0)) 
    IPAddr = s.getsockname()[0]
    return dns.Record_A(IPAddr,'1')
RECORD_A = getRecordA()


#########
# On the surface of it, a DNS resolver which only responds
# with it's own IP address may seem to be the most idiotic class
# ever written. But HAHA! For us, the only thing better than
# a dummy resolver is a usefull idiot.
##
class MammatusIdioticResolver(common.ResolverBase):
    """ Respond to all requests with own IP address
    """
    def __init__(self):
        common.ResolverBase.__init__(self)
        self._waiting = {}
    def _lookup(self, name, cls, type, timeout):
        key = (name, type, cls)
        waiting = self._waiting.get(key)
        if waiting is None:
            self._waiting[key] = []
            d = defer.fail(IOError("No domain name servers available"))
            def cbResult(result):
                for d in self._waiting.pop(key):
                    d.callback(result)
                RR = dns.RRHeader(name=name,type=dns.A,cls=dns.IN,ttl=1,payload=RECORD_A)
                return ([RR], [], [])
            d.addBoth(cbResult)
        else:
            d = defer.Deferred()
            waiting.append(d)
        return d

#########
# What could be a better companion to a DNS Resolver
# that always claims to be whatever domain you are looking for
# than an HTTP server that always tells you to go somewhere else?
# Wharever you want It's OVER HERE!!! 
# Umm, No it's not. Try over there.
##
class MammatusRedirectToCdn(resource.Resource):
    """ Redirect All GET requests to CDN
    """
    isLeaf = True
    def render_GET(self, request):
        url = self.getStorageUrl(request)
        request.redirect(url)
        request.finish()
        return web_server.NOT_DONE_YET
    def getStorageUrl(self, request):
        """ Return base URL for content delivery network
        """
        #########
        # Special url components can be added here, but for example
        # we just us a static base url
        ##
        cdnUrl = "http://www.archive.org/"
        requestUrl = request.uri
        return urlparse.urljoin(cdnUrl, requestUrl)
        
#########
# Mammatus is the giver of names, on TCP and UDP.
##
verbosity = 0
resolver = MammatusIdioticResolver()
tcpFactory = names_server.DNSServerFactory(clients=[resolver], verbose=verbosity)
udpFactory = dns.DNSDatagramProtocol(tcpFactory)
tcpFactory.noisy = udpFactory.noisy = verbosity
dns_service = service.MultiService()
internet.TCPServer(53, tcpFactory).setServiceParent(dns_service)
internet.UDPServer(53, udpFactory).setServiceParent(dns_service)

#########
# Mammatus feeds you, over HTTP.
##
httpFactory = web_server.Site(MammatusRedirectToCdn())
web_service = internet.TCPServer(80, httpFactory)

#########
# All Hail Mammatus, the many breasted goddess
# of commodity cloud computing! Ween at her
# numerous nipples, and hunger no more.
##
application = service.Application("Mammatus Tracker")
web_service.setServiceParent(application)
dns_service.setServiceParent(application)

