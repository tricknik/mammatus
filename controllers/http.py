"""
Mammatus, a DNS-centric HA platform 

Dmytri Kleiner <dk@telekommunisten.net>, 2010
"""

import socket, urlparse
from twisted.internet import reactor, defer
from twisted.internet.task import deferLater
from twisted.web import server, resource
from twisted.names import client
from random import choice

class MammatusRedirect(resource.Resource):
    """ Redirect All GET requests to CDN
    """
    isLeaf = True
    def setModel(self, model):
        self.model = model
    def render_GET(self, request):
        def error(failure):
            failure.printTraceback(request)
            request.finish()
        def redirect(config):
            endpoint = choice(config.endpoints) 
            target =  urlparse.urljoin(endpoint, request.uri)
            request.redirect(target)
            request.finish()
        url = request.getRequestHostname()
        d =  deferLater(reactor, 0, self.model.getConfiguration, url)
        d.addCallbacks(redirect, error)
        return server.NOT_DONE_YET

def getResource(model):
    resource = MammatusRedirect()
    resource.setModel(model)
    return resource
