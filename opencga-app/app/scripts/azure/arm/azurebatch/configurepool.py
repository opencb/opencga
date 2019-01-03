import subprocess
import sys
import urllib.request


def run_az_command():
    try:
        print("Attempt set config on pool {}".format(pool_id))
        subprocess.check_call(
            ["az", "batch", "pool", "create", "--json-file", "pool.complete.json"]
        )
        print("Install completed successfully")
    except subprocess.CalledProcessError as e:
        print("Failed config set on pool: {} error: {}".format(pool_id, e))
        exit(4)

if len(sys.argv) != 7:
    print(
        "Expected 'poolid', 'vm_size', 'mount_args', 'artifact_location', 'artifact_sas', 'subnet_id'"
    )
    exit(1)

pool_id = str(sys.argv[1])
vm_size = str(sys.argv[2])
mount_args = str(sys.argv[3])
artifact_location = str(sys.argv[4])
artifact_sas = str(sys.argv[5])
subnet_id = str(sys.argv[6])

url = "{0}/azurebatch/pool.json{1}".format(artifact_location, artifact_sas)
response = urllib.request.urlopen(url)
data = response.read()  # a `bytes` object
text = data.decode("utf-8")  #

# Replace the target string
text = text.replace("ID_HERE", pool_id)
text = text.replace("VM_SIZE_HERE", vm_size)
text = text.replace("MOUNT_ARGS_HERE", mount_args)
text = text.replace("ARTIFACT_LOCATION_HERE", artifact_location)
text = text.replace("ARTIFACT_SAS_HERE", artifact_sas)
text = text.replace("SUBNET_ID_HERE", subnet_id)

# Write the file out again
with open("pool.complete.json", "w") as file:
    file.write(text)

run_az_command()

