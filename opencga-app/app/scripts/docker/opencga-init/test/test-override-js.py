import subprocess
from io import StringIO
import sys
import os

os.chdir(sys.path[0])

print("> Running Js overrides")

res = subprocess.run(["python3", "../override-js.py",
               "--iva-config-path", "./test-config.js",
               "--cellbase-hosts", "test-cellbase-host1,test-cellbase-host2",
               "--rest-host", "test-rest-host"],
               stdout=subprocess.PIPE,
               stderr=subprocess.STDOUT, check=True)

iva_config = res.stdout.decode("utf-8")
configAsFile = StringIO(iva_config)

print("> Testing results")

# Not efficent but shouldn't be an issue as only a test
foundCellbase = False
foundOpenCGA = False
for line in configAsFile:
    if foundCellbase == False and '"test-cellbase-host1", "test-cellbase-host2"' in line:
        foundCellbase = True
    if foundOpenCGA == False and 'test-rest-host' in line:
        foundOpenCGA = True
    if foundOpenCGA == True and foundCellbase == True:
        break

configAsFile.close()

assert(foundCellbase)
assert(foundOpenCGA)
print("PASS: Js configuration overrides successful")