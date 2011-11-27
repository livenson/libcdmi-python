import urllib2

from libcdmi.blob_operations import BlobOperations
from libcdmi.container_operations import ContainerOperations
from libcdmi.common import CDMIErrorProcessor
from libcdmi.multistep_operations import MultistepOperations


class CDMIConnection():
    
    credentials = None
    endpoint = None
    blob_proxy = None
    container_proxy = None
    mq_proxy = None
    multistep = None
    
    def __init__(self, endpoint, credentials, tre_client_endpoint = None):
        self.credentials = credentials
        self.endpoint = endpoint
        self.tre_client_endpoint = tre_client_endpoint
        
        # install authenticated opener for all of the urllib2 calls
        auth_handler = urllib2.HTTPDigestAuthHandler()
        urllib2.HTTPPasswordMgrWithDefaultRealm()
        auth_handler.add_password(realm=None,
                          uri=endpoint,
                          user=credentials['user'],
                          passwd=credentials['password'])
        # TODO: add required server-side and client-side certificate validation for https connections 
        opener = urllib2.build_opener(auth_handler, urllib2.HTTPSHandler(), CDMIErrorProcessor()) 
        urllib2.install_opener(opener)
        
        self.blob_proxy = BlobOperations(endpoint)
        self.container_proxy = ContainerOperations(endpoint)
        self.multistep = MultistepOperations(self.endpoint, self.blob_proxy, self.container_proxy)
