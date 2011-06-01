import unittest
import test_blob, test_container

if __name__ == '__main__':
    blob_operations = unittest.TestLoader().loadTestsFromTestCase(test_blob.TestBlobOperations)
    container_operations = unittest.TestLoader().loadTestsFromTestCase(test_container.TestContainerOperations)
    unittest.TextTestRunner(verbosity=2).run(
                                             unittest.TestSuite([blob_operations, container_operations]))