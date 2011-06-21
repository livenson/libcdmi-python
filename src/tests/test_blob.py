from __future__ import with_statement
import unittest
import tempfile
from connection_wrapper import ConnectionWrapper
from libcdmi import cdmi
import random
import os
from urllib2 import HTTPError

class TestBlobOperations(ConnectionWrapper):

    def setUp(self):
        self.remote_container = '/'
        self.remote_blob = 'test_blob_%s' % random.randint(1, 10000000000)
 
    def testBasicBlobOperations(self):
        """Run through a scenario testing all blob functions."""
        conn = cdmi.CDMIConnection(self.endpoint, self.credentials)             
        lf_1, localfile_1 = tempfile.mkstemp()
        lf_2, localfile_2 = tempfile.mkstemp()
        os.write(lf_1, "# Test data #")
        os.write(lf_2, "# More test data #")
        os.close(lf_1)
        os.close(lf_2)
        for cdmi_object in [True, False]: 
            conn.blob_proxy.create_from_file(localfile_1, self.remote_container + self.remote_blob, 
                                                   mimetype='text/plain', cdmi_object=cdmi_object)        
            conn.blob_proxy.read(self.remote_container + self.remote_blob, cdmi_object=cdmi_object)
            conn.blob_proxy.head(self.remote_container + self.remote_blob, cdmi_object=cdmi_object)
            conn.blob_proxy.update_from_file(localfile_1, self.remote_container + self.remote_blob, 
                                             mimetype='text/plain', cdmi_object=cdmi_object, metadata={'desired_backend':'office_1'})
            conn.blob_proxy.delete(self.remote_container + self.remote_blob)
            # check that we get an error when deleting non-existing file
            self.assertRaises(HTTPError, conn.blob_proxy.delete, self.remote_container + self.remote_blob + "_non_existing")
        
        os.unlink(localfile_1)    
        os.unlink(localfile_2)
    
if __name__ == "__main__":
    unittest.main()