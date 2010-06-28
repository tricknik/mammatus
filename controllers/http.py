"""
Mammatus, a DNS-centric HA platform 

Dmytri Kleiner <dk@telekommunisten.net>, 2010
"""

import socket, urlparse, os
from twisted.internet import reactor, defer
from twisted.internet.task import deferLater
from twisted.web import server, resource, static, script, proxy
from twisted.names import client
from random import choice

class MammatusHttpResource(resource.Resource):
    isLeaf = True
    def setModel(self, model):
        self.model = model
    def setLocalRoot(self, localroot):
        self.localRoot = localroot
    def render_GET(self, request):
        config = None
        def error(failure):
            failure.printTraceback(request)
            request.finish()
        def direct(config):
            endpoint = choice(config.endpoints) 
            mode = "serve"
            if endpoint == "redirect":
                mode = "redirect"
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
    render_POST = render_GET

class Controller(MammatusHttpResource):
    def serve(self, request, endpoint, config):
        path = "".join((self.localRoot, request.path))
        if os.path.exists(path) and not path.endswith(".ma"):
            file = static.File(path)
        else:
            if not path.endswith(".ma"):
                path = ".".join((path, 'ma'))
            if os.path.exists(path):
                context = {'request': request}
                execfile(path, context, context)
            else:
                file = static.File.childNotFound
        file.render(request)
    def proxy(self, request, endpoint, config):
        host  = urlparse.urlparse(endpoint).netloc
        rproxy = proxy.ReverseProxyResource(host, 80, request.uri)
        rproxy.render(request)
    def redirect(self, request, endpoint, config):
        target = urlparse.urljoin(endpoint, request.uri)
        request.redirect(target)
        request.finish()

def getController(model, localroot="/srv/http"):
    controller = Controller()
    controller.setModel(model)
    controller.setLocalRoot(localroot)
    return controller

