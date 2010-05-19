"""
Mammatus, a DNS-centric HA platform 

Dmytri Kleiner <dk@telekommunisten.net>, 2010

The "Running Dog Lackey" edition

This is an implementation of the simplest possible
scenario for Mammatus; exploiting the ubiquity of DNS fail-over 
to create a high availablity webserver that only redirects a get 
request to a content delivery network.

This version uses _mammatus TXT and SRV record form
the domain of the recieved request to find the storage
nodes.
"""

import os, socket, urlparse
from twisted.application import service, internet
from twisted.internet import reactor, defer
from twisted.internet.task import deferLater
from twisted.web import server as web_server, resource
from twisted.names import server as names_server, dns, common, client
from random import choice

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
# Dictionary of CDN baseUrls for domains
# Set when request for a given domain is recieved
##

BASEURLS = {}


#########
# On the surface of it, a DNS resolver which only responds
# with it's own IP address may seem to be the most idiotic class
# ever written. But HAHA! For us, the only thing better than
# a dummy resolver is a useful idiot.
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
# Wharever you want It's OVER HERE!!! 
# Umm, No it's not. Try over there.
##
class MammatusRedirectToCdn(resource.Resource):
    """ Redirect All GET requests to CDN
    """
    isLeaf = True
    def render_GET(self, request):
        def gotFailure(failure):
            request.write("lookup for " + str(failure.value.message.queries[0].name) + " failed")
            request.finish()
        def getZone(uri):
            domainlevels = uri.split(".")
            levelcount = len(domainlevels)
            if levelcount < 2:
                domainlevels = socket.getfqdn().split(".")
                levelcount = len(domainlevels)
            registeredDomain = domainlevels[levelcount-2:levelcount]
            subdomain = domainlevels[:levelcount-2]
            subdomain.append("_mammatus")
            mammatus_key = ".".join(subdomain) 
            root_zone = ".".join(registeredDomain)
            return (mammatus_key, root_zone)
        url = urlparse.urlparse(request.uri).netloc
        (mammatus_key, root_zone) = getZone(url)
        d = client.lookupText(".".join((mammatus_key, root_zone)))
        def gotError(failure):
            service_fqdn = ".".join(("_mammatus._tcp", root_zone))
            d = client.lookupService(service_fqdn) 
            def gotSrvRecord(result):
                (answer, authority, additional) = result
                serviceUri = str(answer[0].payload.target)
                d = client.lookupText(".".join(getZone(serviceUri)))
                d.addCallbacks(getStorageUrls, gotFailure)
                return d
            d.addCallbacks(gotSrvRecord, gotFailure)
            return d
        def getStorageUrls(result):
            (answer, authority, additional) = result
            if answer and answer[0].payload.data:
                #request.write(str(answer[0].payload.data[0]))
                #request.finish()
                return choice(["http://www.archive.org",])
            else:
                return gotError(None)
        d.addCallbacks(getStorageUrls, gotError)
        def redirect(url):
            endpoint =  urlparse.urljoin(url, request.uri)
            request.redirect(endpoint)
            request.finish()
            return None
        d.addCallback(redirect)
        return web_server.NOT_DONE_YET

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
# Attach method called by tac
##
def attach(application):
    web_service.setServiceParent(application)
    dns_service.setServiceParent(application)

