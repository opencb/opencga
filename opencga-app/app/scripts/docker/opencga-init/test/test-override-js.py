import subprocess
from io import StringIO
import sys
import os
import unittest

os.chdir(sys.path[0])

class Test_init_script(unittest.TestCase):
    def test_end_2_end(self):

        print("> Running Js overrides")

        res = subprocess.run(["python3", "../override-js.py",
                    "--iva-config-path", "./test-config.js",
                    "--cellbase-rest-urls", "http://test-cellbase-host1,http://test-cellbase-host2"],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT, check=True,
                    env={**os.environ, "INIT_REST_HOST": "test-rest-host"}) #Test that the auto import of environment vars is working)

        iva_config = res.stdout.decode("utf-8")
        configAsFile = StringIO(iva_config)

        print("> Testing results")

        # Not efficent but shouldn't be an issue as only a test
        foundCellbase = False
        foundOpenCGA = False
        for line in configAsFile:
            if foundCellbase == False and '"http://test-cellbase-host1", "http://test-cellbase-host2"' in line:
                foundCellbase = True
            if foundOpenCGA == False and 'test-rest-host' in line:
                foundOpenCGA = True
            if foundOpenCGA == True and foundCellbase == True:
                break

        configAsFile.close()
        
        self.assertTrue(foundCellbase)
        self.assertTrue(foundOpenCGA)
        print("PASS: Js configuration overrides successful")

if __name__ == '__main__':
    unittest.main()