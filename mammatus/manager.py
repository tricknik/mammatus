"""
Mammatus, a DNS-centric HA platform 

Dmytri Kleiner <dk@telekommunisten.net>, 2010
"""

import socket, urlparse
from twisted.internet import reactor, defer
from twisted.names import client
from random import choice

def __init__():
    pass

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

