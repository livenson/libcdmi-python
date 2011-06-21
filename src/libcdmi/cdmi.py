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
    
        
    def get_container_files(self, remote_container, local_folder):
        """Download blobs from a specified remote_container to a local_folder"""
        container = self.container_proxy.read(remote_container)
        import os
        if not os.path.exists(local_folder):
            os.makedirs(local_folder)
        for c in container['children']:
            if not c.endswith("/"):
                cf = open(os.path.join(local_folder, c), 'w')              
                fnm = remote_container + "/" + os.path.basename(c)
                cf.write(self.blob_proxy.read(fnm, False))