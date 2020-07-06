#!/usr/bin/env python3

from pyopencga.opencga_config import ClientConfiguration
from pyopencga.opencga_client import OpencgaClient
import argparse
import getpass
from pprint import pprint
import json


def qc(samples):
    print('Executing qc...')
    for sample in samples:
        if sample['internal']['status']['name'] == 'READY' \
                and ('qualityControl' not in sample or len(sample['qualityControl']['metrics']) == 0):
            data = {'sample': sample['id']}
            if args.bamFile is not None:
                data['bamFile'] = args.bamFile
            print("Calculate QC " + sample['id'])
            oc.variants.run_sample_qc(data, study=args.study)


def index(samples):
    print('Executing index...')
    for sample in samples:
        if len(sample['members']) > 1:
            sample_list = [sample['id']]
            data = {'sample': sample_list}
            print("Index " + sample['id'])
            oc.variant_operations.index_sample_genotype(data, study=args.study)


# Define parameters
parser = argparse.ArgumentParser()
parser.add_argument('action', help="Action to execute", choices=["qc", "index"], default="")
parser.add_argument('-s', '--study', help="Study ID or fqn")
parser.add_argument('--id', help="Comma-separated list of sample ID")
parser.add_argument('--individual-id', help="Comma-separated list of individual ID")
parser.add_argument('--phenotypes', help="Comma-separated list of phenotype ID, e.g. hp:12345")
parser.add_argument('--bamFile', help="BAM file to be used for some stats")
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
sample_resp = oc.samples.search(**query)

# Execute action
if args.action == "qc":
    qc(sample_resp.get_results())
elif args.action == "index":
    index(sample_resp.get_results())
