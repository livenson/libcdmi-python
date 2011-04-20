from libcdmi.blob_operations import BlobOperations

import urllib2
from libcdmi.container_operations import ContainerOperations
from libcdmi.common import CDMIErrorProcessor

class CDMIConnection():
    
    credentials = None
    endpoint = None
    blob_proxy = None
    container_proxy = None
    mq_proxy = None
    
    def __init__(self, endpoint, credentials):
        self.credentials = credentials
        self.endpoint = endpoint
        
        # install authenticated opener for all of the urllib2 calls
        auth_handler = urllib2.HTTPDigestAuthHandler()
        auth_handler.add_password(realm=None,
                          uri=endpoint,
                          user=credentials['user'],
                          passwd=credentials['password'])
        opener = urllib2.build_opener(auth_handler, urllib2.HTTPSHandler(), CDMIErrorProcessor()) 
        urllib2.install_opener(opener)
        
        self.blob_proxy = BlobOperations(endpoint)
        self.container_proxy = ContainerOperations(endpoint)
        