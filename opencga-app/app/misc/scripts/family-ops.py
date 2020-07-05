#!/usr/bin/env python3

from pyopencga.opencga_config import ClientConfiguration
from pyopencga.opencga_client import OpencgaClient
import argparse
import getpass
from pprint import pprint
import json

def qc(families):
    print('Executing qc...')
    for family in families:
        if len(family['members']) > 1:
            data = {'family': family['id']}
            if args.relatednessMaf is not None:
                data['relatednessMaf'] = args.relatednessMaf
            print("Calculate QC " + sample['id'])
            # oc.variants..index_family_genotype(data, study=args.study)

def index(families):
    print('Executing index...')
    for family in families:
        if len(family['members']) > 1:
            family_list = [family['id']]
            data = {'family': family_list}
            print("Index " + family['id'])
            oc.variant_operations.index_family_genotype(data, study=args.study)

# Define parameters
parser = argparse.ArgumentParser()
parser.add_argument('action', help="Action to execute", choices=["qc", "index"], default="")
parser.add_argument('-s', '--study', help="Study ID or fqn")
parser.add_argument('--id', help="comma separated list of family ID")
parser.add_argument('--phenotypes', help="Comma-separated list of phenotype ID, e.g. hp:12345")
parser.add_argument('--disorders', help="Comma-separated list of disorder ID, e.g. hp:12345")
parser.add_argument('--relatednessMaf', help="Populatioon MAF, e.g. cohort:ALL>0.05")
parser.add_argument('--conf', help="Load client-configuration.yml")
parser.add_argument('--url', default="https://bioinfo.hpc.cam.ac.uk/opencga-prod", help="Default https://bioinfo.hpc.cam.ac.uk/opencga-prod")
parser.add_argument('-u', '--user', help="Username to login to OpenCGA")
parser.add_argument('-p', '--password', help="Password to login to OpenCGA")

args = parser.parse_args()

# Ask for password
if args.password is None:
    args.password = getpass.getpass()

# Create OpencgaClient config object from file or dictionary
if args.conf is not None:
    config = ClientConfiguration(args.conf)
else:
    config_dict = {
        "rest": {
            "host": args.url
        }
    }
    config = ClientConfiguration(config_dict)

# Create OpenCGA client and login
oc = OpencgaClient(config)
oc.login(args.user, args.password)  # If done this way, password will be prompted to the user so it is not displayed or..

# Fetch selected families
query = {}
if args.study is not None:
    query['study'] = args.study
if args.id is not None:
    query['id'] = args.id
if args.phenotypes is not None:
    query['phenotypes'] = args.phenotypes
if args.disorders is not None:
    query['disorders'] = args.disorders
family_resp = oc.families.search(**query)

# Execute action
if args.action == "qc":
    qc(family_resp.get_results())
elif args.action == "index":
    index(family_resp.get_results())
