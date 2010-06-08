"""
Mammatus, a DNS-centric HA platform 

Dmytri Kleiner <dk@telekommunisten.net>, 2010

The "Petit-bourgeois Reformist" edition

Mammatus exploits the ubiquity of DNS fail-over to implement
hight availablity http service.  

This implementation includes support for resolving the endpoint
IP as well as redirecting to a target domain.

This is selected by specifying resolve=self or resolve=endpoint in
the _mammatus TXT record for the subdomain.

USAGE:
    sudo twistd -[n]y mammatus.tac

"""

from twisted.application import service, internet
from twisted.internet import reactor
from twisted.internet.task import deferLater
from twisted.web import server as web_server
from twisted.names import server as names_server, dns as names_dns
from controllers import dns, http
import model

#########
# Attach method called by tac
##
def expose(application):
    def attachResolver(resolver):
        #########
        # Mammatus is the giver of names, on TCP and UDP.
        ##
        verbosity = 0
        tcpFactory = names_server.DNSServerFactory(clients=[resolver], verbose=verbosity)
        udpFactory = names_dns.DNSDatagramProtocol(tcpFactory)
        tcpFactory.noisy = udpFactory.noisy = verbosity
        dns_service = service.MultiService()
        internet.TCPServer(53, tcpFactory).setServiceParent(dns_service)
        internet.UDPServer(53, udpFactory).setServiceParent(dns_service)
        dns_service.setServiceParent(application)
    def attachServer(server):
        #########
        # Mammatus feeds you, over HTTP.
        ##
        httpFactory = web_server.Site(server)
        web_service = internet.TCPServer(80, httpFactory)
        web_service.setServiceParent(application)

    #########
    # Expose Mammia
    ##
    deferResolver = deferLater(reactor, 0, dns.getResolver, model)
    deferResolver.addCallback(attachResolver)
    deferServer = deferLater(reactor, 0, http.getResource, model)
    deferServer.addCallback(attachServer)

