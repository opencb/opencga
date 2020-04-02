import sys
import configargparse

parser = configargparse.ArgumentParser(auto_env_var_prefix="INIT_")
parser.add_argument("--iva-config-path", help="path to iva config.js file", default="/opt/opencga/ivaconf/config.js")
parser.add_argument("--cellbase-rest-urls", required=True)
parser.add_argument("--rest-host", required=True)
parser.add_argument("--save", help="save update to source configuration files (default: false)", default=False, action='store_true')
args = parser.parse_args()

# Unescape parameter strings
cellbase_hosts = args.cellbase_rest_urls.replace('\"', '')
rest_host = args.rest_host.replace('\"', '')

# Load IVA configuration JS (hack: wouldn't need to change source file ideally)
f = open(args.iva_config_path, "rt")

cellbase_hosts_arr = cellbase_hosts.split(",")
cellbase_hosts_formatted = (', '.join('"' + cellbase_host + '"' for cellbase_host in cellbase_hosts_arr))

out = ""
for line in f:
    line = line.replace('"CELLBASE_HOST_URL"', '{}'.format(cellbase_hosts_formatted))
    line = line.replace('<OPENCGA_HOST_URL>', '{}'.format(rest_host))
    out += line

f.close()

if args.save == False:
    print(out)
else:
    with open(args.iva_config_path, "wt") as f:
        f.write(out)

