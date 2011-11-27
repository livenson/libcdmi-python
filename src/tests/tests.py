import unittest
import test_blob, test_container, test_multistep 


if __name__ == '__main__':
    blob_operations = unittest.TestLoader().loadTestsFromTestCase(test_blob.TestBlobOperations)
    container_operations = unittest.TestLoader().loadTestsFromTestCase(test_container.TestContainerOperations)
    toplevel_operations = unittest.TestLoader().loadTestsFromTestCase(test_multistep.TestMultistepOperations)
    unittest.TextTestRunner(verbosity=2).run(
                                             unittest.TestSuite([blob_operations, container_operations, toplevel_operations]))
