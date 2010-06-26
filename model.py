"""
Mammatus, a DNS-centric HA platform 

Dmytri Kleiner <dk@telekommunisten.net>, 2010
"""

import socket, urlparse
from twisted.internet import reactor, defer
from twisted.internet.task import deferLater
from twisted.names import client
from twisted.web.client import Agent
from twisted.web.http_headers import Headers
from random import choice

class MammatusConfiguration:
    def __init__(self):
        self.get = None
        self.resolve = None
        self.endpoints = []
    def parseText(self, txt):
        endpointtoken = "endpoint:"
        endpointtokenlength = len(endpointtoken)
        gettoken = "get="
        gettokenlength = len(gettoken)
        resolvetoken = "resolve="
        resolvetokenlength = len(resolvetoken)
        for e in str(txt).split(";"):
            item = e.strip()
            if item[:endpointtokenlength] == endpointtoken:
                self.endpoints.append(item[endpointtokenlength:])
            elif self.get == None and item[:gettokenlength] == gettoken:
                self.get = item[gettokenlength:]
            elif self.resolve == None and item[:resolvetokenlength] == resolvetoken:
                self.resolve = item[resolvetokenlength:]

def getOwnIpAddr():
    """ Return an A record with external IP address of localhost
    """
    def findOwnIpAddr():
        def gotResult(result):
            return result
        addr_or_d = None
        try:
           s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
           # umm, I know google is never, never, never-ever down right?
           s.connect(('google.com', 0)) 
           addr_or_d = s.getsockname()[0]
        except:
            try:
                addr_or_d = client.getHostByName(socket.getfqdn())
                addr_or_d.addCallback(gotResult)
            except:
                addr_or_d = "127.0.0.1"
        return addr_or_d
    d = deferLater(reactor, 0, findOwnIpAddr)
    return d

def getConfiguration(uri):
    def gotFailure(failure):
        raise Exception("lookup for " + str(failure.value.message.queries[0].name) + " failed")
    def getZone(uri):
        domainlevels = uri.split(".")
        levelcount = len(domainlevels)
        # if the request is comming from an not fully qualied
        # domain name, i.e "localhost", use our own.
        if levelcount < 2:
            domainlevels = socket.getfqdn().split(".")
            levelcount = len(domainlevels)
        registeredDomain = domainlevels[levelcount-2:levelcount]
        subdomain = [domainlevels[:levelcount-2].pop(), "_mammatus"]
        mammatus_key = ".".join(subdomain) 
        root_zone = ".".join(registeredDomain)
        return (mammatus_key, root_zone)
    def getService(failure):
        service_fqdn = ".".join(("_mammatus._tcp", root_zone))
        d = client.lookupService(service_fqdn) 
        def gotSrvRecord(result):
            (answer, authority, additional) = result
            serviceUri = str(answer[0].payload.target)
            d = client.lookupText(".".join(getZone(serviceUri)))
            d.addCallbacks(getConfigFromText, gotFailure)
            return d
        d.addCallbacks(gotSrvRecord, gotFailure)
        return d
    def getConfigFromText(result):
        (answer, authority, additional) = result
        config = MammatusConfiguration()
        if answer:
            for a in answer:
                if hasattr(a.payload,'data'):
                    for d in a.payload.data:
                        config.parseText(d)
        return config
    (mammatus_key, root_zone) = getZone(uri)
    d = client.lookupText(".".join((mammatus_key, root_zone)))
    d.addCallbacks(getConfigFromText, getService)
    return d 

def getHostByName(name):
    """ Look up possible ip addresses for a name,
        return a host on which http is responding
        for the requested domain
    """
    def discover(config):
        def direct(result):
            (answer, authority, additional) = result
            httpAgent = Agent(reactor)
            def responded(response, addr):
                addr_or_defer = None
                if hasattr(response, "code") and response.code == 200:
                    addr_or_defer = addr
                else:
                    addr_or_defer = deferLater(reactor, 0, test)
                return addr_or_defer
            def test():
                try:
                    a = answer.pop()
                except IndexError:
                    return defer.fail(IOError("No hosts available"))
                addr = str(a.payload.dottedQuad())
                location = "://".join(('http', name))
                uri = "://".join(('http', addr))
                d = httpAgent.request(method="HEAD", headers = Headers({"location": location}), uri = uri)
                d.addBoth(responded, addr)
                return d
            d = deferLater(reactor, 0, test)
            return d
        def ipaddr(addr):
            return addr
        d = None
        if config.resolve == "endpoint":
            endpoint = choice(config.endpoints)
            netloc = urlparse.urlparse(endpoint).netloc
            d = client.lookupAddress(netloc)
            d.addCallback(direct)
        else:
            d = getOwnIpAddr()
        d.addCallback(ipaddr)
        return d
    d = deferLater(reactor, 0, getConfiguration, name)
    def gotFailure(failure):
        raise failure
    d.addCallbacks(discover, gotFailure)
    return d

