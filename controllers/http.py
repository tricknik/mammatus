"""
Mammatus, a DNS-centric HA platform 

Dmytri Kleiner <dk@telekommunisten.net>, 2010
"""

import socket, urlparse, os
from twisted.internet import reactor, defer
from twisted.internet.task import deferLater
from twisted.web import server, resource, static, proxy
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
            mode = "serve"
            if config.endpoints:
                endpoint = choice(config.endpoints) 
                if endpoint == "redirect":
                    mode = "redirect"
                elif config.get == "proxy":
                    mode = "proxy"
            else:
                endpoint = None
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

class MammatusScript:
    def __init__(self, path, config):
        self.path = path
        self.config = config
    def render(self, request):
        context = {'request': request, 'config':self.config}
        execfile(self.path, context, context)

class Controller(MammatusHttpResource):
    def serve(self, request, endpoint, config):
        finished = False
        path = "".join((self.localRoot, request.path))
        if os.path.isdir(path):
            if not path.endswith("/"):
                path = "".join((path,"/"))
            dPath = "".join((path, 'index.ma'))
            if os.path.exists(dPath):
                path = dPath
            else:
                path = "".join((path, 'index.html'))
        if os.path.exists(path) and not path.endswith(".ma"):
            file = static.File(path)
            finished = True
        else:
            if not path.endswith(".ma") and not path.endswith(".html"):
                path = ".".join((path, 'ma'))
            if os.path.exists(path):
                file = MammatusScript(path, config)
            else:
                file = static.File.childNotFound
                finished = True
        file.render(request)
        if finished:
            request.finish()
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

