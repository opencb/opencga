import argparse
import sys

parser = argparse.ArgumentParser()
parser.add_argument("--iva-config-path", help="path to iva config.js file", default="/opt/opencga/conf/iva/config.js")
parser.add_argument("--cellbase-hosts", required=True)
parser.add_argument("--rest-host", required=True)
parser.add_argument("--save", help="save update to source configuration files (default: false)", default=False, action='store_true')
args = parser.parse_args()

# Unescape parameter strings
cellbase_hosts = args.cellbase_hosts.replace('\"', '')
rest_host = args.rest_host.replace('\"', '')

# Load IVA configuration JS (hack: wouldn't need to change source file ideally)
f = open(args.iva_config_path, "rt")

cellbase_hosts_arr = cellbase_hosts.split(",")
cellbase_hosts_formatted = (', '.join('"' + cellbase_host + '"' for cellbase_host in cellbase_hosts_arr))

out = ""
for line in f:
    line = line.replace('"http://bioinfo.hpc.cam.ac.uk/cellbase"', '{}'.format(cellbase_hosts_formatted))
    line = line.replace('http://bioinfodev.hpc.cam.ac.uk/opencga-test', '{}'.format(rest_host))
    out += line

f.close()

if args.save == False:
    print(out)
else:
    with open(args.iva_config_path, "wt") as f:
        f.write(out)

