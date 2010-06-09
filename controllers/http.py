"""
Mammatus, a DNS-centric HA platform 

Dmytri Kleiner <dk@telekommunisten.net>, 2010
"""

import socket, urlparse
from twisted.internet import reactor, defer
from twisted.internet.task import deferLater
from twisted.web import server, resource
from twisted.web.proxy import ReverseProxyResource
from twisted.names import client
from random import choice

class MammatusHttpResource(resource.Resource):
    isLeaf = True
    def setModel(self, model):
        self.model = model
    def setLocalRoot(self, localroot):
        self.localRoot = localroot)
    def render_GET(self, request):
        config = None
        def error(failure):
            failure.printTraceback(request)
            request.finish()
        def direct(config):
            endpoint = choice(config.endpoints) 
            mode = "redirect"
            if endpoint == "local":
                mode = "local"
            elif config.get == "proxy":
                mode = "proxy"
            d = None
            if hasattr(self, mode):
                resource = getattr(self, mode)
                resource(request, endpoint, config)
            else:
                d = defer.fail(NotImplementedError("No Controller for mode %s" % mode))
            return d
        url = request.getRequestHostname()
        d =  deferLater(reactor, 0, self.model.getConfiguration, url)
        d.addCallbacks(direct, error)
        return server.NOT_DONE_YET

def getController(model, localroot="/srv/mammatus"):
    controller = Controller()
    controller.setModel(model)
    controller.setLocalRoot(localroot)
    return controller
