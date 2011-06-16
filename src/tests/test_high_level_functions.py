from __future__ import with_statement
import unittest
from connection_wrapper import ConnectionWrapper
from libcdmi import cdmi
import random

class TestHighLevelFunctions(ConnectionWrapper):

    def setUp(self):
        self.base = '/'
        self.remote_container = 'test_container_%s' % random.randint(1, 10000000000)
        self.local_container = 'test_folder_%s' % random.randint(1, 10000000000)

    def testGetContainerFiles(self):
        conn = cdmi.CDMIConnection(self.endpoint, self.credentials)
        # positive
        conn.container_proxy.create(self.base + self.remote_container, {'hard work': 'success'})
        conn.get_container_files(self.base, self.local_container)
        import shutil
        shutil.rmtree(self.local_container)
        
if __name__ == "__main__":
    unittest.main()