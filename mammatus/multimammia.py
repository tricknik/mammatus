"""
Mammatus, a DNS-centric HA platform 

Dmytri Kleiner <dk@telekommunisten.net>, 2010

The "Petit-bourgeois Reformist" edition

Mammatus exploits the ubiquity of DNS fail-over to implement
hight availablity http service.  

This implementation includes support for resolving the endpoint
IP as well as redirecting to a target domain.

This is selected by specifying get=redirect or get=resolve in
the _mammatus TXT record for the subdomain.
"""

from twisted.application import service, internet
from twisted.web import server as web_server
from twisted.names import server as names_server, dns


#########
# On the surface of it, a DNS resolver which only responds
# with it's own IP address may seem to be the most idiotic class
# ever written. But HAHA! For us, the only thing better than
# a dummy resolver is a useful idiot.
##
from mammatus.resolvers import resolveself
resolver = resolveself.getResolver()

#########
# What could be a better companion to a DNS Resolver
# that always claims to be whatever domain you are looking for
# Whatever you want: It's OVER HERE!!! 
# Umm, No it's not. Try over there.
##
from mammatus.servers import redirect
server = redirect.getServer()

#########
# Attach method called by tac
##
def attach(application):
    #########
    # Mammatus is the giver of names, on TCP and UDP.
    ##
    verbosity = 0
    tcpFactory = names_server.DNSServerFactory(clients=[resolver], verbose=verbosity)
    udpFactory = dns.DNSDatagramProtocol(tcpFactory)
    tcpFactory.noisy = udpFactory.noisy = verbosity
    dns_service = service.MultiService()
    internet.TCPServer(53, tcpFactory).setServiceParent(dns_service)
    internet.UDPServer(53, udpFactory).setServiceParent(dns_service)

    #########
    # Mammatus feeds you, over HTTP.
    ##
    httpFactory = web_server.Site(server)
    web_service = internet.TCPServer(80, httpFactory)

    #########
    # Expose Mammia
    ##
    web_service.setServiceParent(application)
    dns_service.setServiceParent(application)

