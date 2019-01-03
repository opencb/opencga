import subprocess
import sys
from mount import mount_share


def run_az_command(account_name, account_key, account_endpoint, pool_id):
    try:
        print("Attempt set config on pool {}".format(pool_id))
        command = "-v $PWD:/localdir microsoft/azure-cli:2.0.54 az batch pool set --account-endpoint={0} --account-key={1} --account-name={2} --pool-id={3} --json-file=/localdir/pool.json".format(
            account_endpoint, account_key, account_name, pool_id
        )
        subprocess.check_call(["docker", "run", command])
        print("Install completed successfully")
    except subprocess.CalledProcessError as e:
        print("Failed config set on pool: {} error: {}".format(pool_id, e))
        exit(4)


if len(sys.argv) < 7:
    print(
        "Expected 'batchAccountName', 'batchAccountKey', 'accountEndpoint', 'poolId', 'mountType', 'mountArgs'"
    )
    exit(1)

account_name = str(sys.argv[1])
account_key = str(sys.argv[2])
account_endpoint = str(sys.argv[3])
pool_id = str(sys.argv[4])

run_az_command(account_name, account_key, account_endpoint, pool_id)

mount_type = str(sys.argv[5])
mount_data = str(sys.argv[6])

mount_share(mount_type, mount_data)
