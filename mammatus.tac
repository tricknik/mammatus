"""
Mammatus, a DNS-centric HA platform 

Dmytri Kleiner <dk@telekommunisten.net>, 2010
"""
from twisted.application import service
try:
    from mammatus import useful_idiot
except ImportError:
    import sys
    sys.path.append('.')
    from mammatus import useful_idiot

#########
# All Hail Mammatus, the many breasted goddess
# of commodity cloud computing! Ween at her
# numerous nipples, and hunger no more.
##
application = service.Application("Mammatus Tracker")
useful_idiot.attach(application)

