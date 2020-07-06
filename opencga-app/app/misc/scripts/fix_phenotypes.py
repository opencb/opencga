#!/usr/bin/env python3

from pyopencga.opencga_config import ClientConfiguration
from pyopencga.opencga_client import OpencgaClient
import argparse
import getpass
from pprint import pprint
import json


def fix_phenotypes(phenotypes):
    pprint(phenotypes)
    return []


def fix_sample_phenotypes(query):
    print('Executing sample...')
    sample_resp = oc.samples.search(**query)
    for sample in sample_resp.get_results():
        print("Fix HPO " + sample['id'])
        if 'phenotypes' in sample and len(sample['phenotypes']) > 0:
            phenotypes = fix_phenotypes(sample['phenotypes'])
            # oc.samples.update(data, study=args.study)


def fix_individual_phenotypes(query):
    print('Executing sample...')
    individual_resp = oc.individuals.search(**query)
    for individual in individual_resp.get_results():
        print("Fix HPO " + individual['id'])
        if 'phenotypes' in individual and len(individual['phenotypes']) > 0:
            phenotypes = fix_phenotypes(individual['phenotypes'])
            # oc.individuals.update(data, study=args.study)


# Define parameters
parser = argparse.ArgumentParser()
parser.add_argument('action', help="Action to execute", choices=['sample', 'individual'], default="")
parser.add_argument('-s', '--study', help="Study ID or fqn")
parser.add_argument('--id', help="Comma-separated list of sample ID")
parser.add_argument('--phenotypes', help="Comma-separated list of phenotype ID, e.g. hp:12345")
parser.add_argument('--hpo', required=True, help="Load HPO ontology terms")
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

# Load HPO Ontology terms
if args.hpo is not None:
    print('load terms')
else:
    print('error and exit')

# Fetch selected families
query = {}
if args.study is not None:
    query['study'] = args.study
if args.id is not None:
    query['id'] = args.id
if args.phenotypes is not None:
    query['phenotypes'] = args.phenotypes

# Execute action
if args.action == "sample":
    fix_sample_phenotypes(query)
elif args.action == "individual":
    fix_individual_phenotypes(query)
