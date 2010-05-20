"""
Mammatus, a DNS-centric HA platform 

Dmytri Kleiner <dk@telekommunisten.net>, 2010
"""

import socket, urlparse
from twisted.internet import reactor, defer
from twisted.web import server, resource
from twisted.names import client
from random import choice

class MammatusRedirect(resource.Resource):
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
            CDNs = []
            token = "endpoint:"
            tokenlength = len(token)
            target = ''
            if answer:
                for a in answer:
                    if hasattr(a.payload,'data'):
                        for d in a.payload.data:
                            for e in str(d).split(";"):
                                    item = e.strip()
                                    if item[:tokenlength] == token:
                                        CDNs.append(item[tokenlength:])
                if CDNs:
                    target = choice(CDNs)
            else:
                target = gotError(None)
            return target
        def redirect(url):
            endpoint =  urlparse.urljoin(url, request.uri)
            request.redirect(endpoint)
            request.finish()
            return None
        url = request.getRequestHostname()
        (mammatus_key, root_zone) = getZone(url)
        d = client.lookupText(".".join((mammatus_key, root_zone)))
        d.addCallbacks(getStorageUrls, gotError)
        d.addCallback(redirect)
        return server.NOT_DONE_YET

def getServer():
    return MammatusRedirect()

