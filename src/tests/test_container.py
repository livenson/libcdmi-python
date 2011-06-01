from __future__ import with_statement
import unittest
from connection_wrapper import ConnectionWrapper
from libcdmi import cdmi
import random
from urllib2 import HTTPError

class TestContainerOperations(ConnectionWrapper):

    def setUp(self):
        self.base = '/'
        self.remote_container = 'test_container_%s' % random.randint(1, 10000000000)
 
    def testBasicContainerOperations(self):
        """Run through a scenario testing all blob functions."""
        conn = cdmi.CDMIConnection(self.endpoint, self.credentials)             
        conn.container_proxy.create(self.base + self.remote_container, {'hard work': 'success'})
        conn.container_proxy.update(self.base + self.remote_container, {'luck': 'success'})
        print conn.container_proxy.read(self.base + self.remote_container)
        conn.container_proxy.delete(self.base + self.remote_container)
        self.assertRaises(HTTPError, conn.container_proxy.delete, self.base + self.remote_container + "_non_existing")        
    
if __name__ == "__main__":
    unittest.main()