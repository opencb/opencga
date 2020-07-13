#!/usr/bin/env python3

import sys
import argparse
import getpass

from pyopencga.opencga_config import ClientConfiguration
from pyopencga.opencga_client import OpencgaClient


def fix_phenotypes(phenotypes, hpo_info):
    for phenotype in phenotypes:
        if 'id' in phenotype and phenotype['id'].startswith('HP:') and phenotype['id'] in hpo_info:
            if 'name' not in phenotype or not phenotype['name'] or phenotype['name'] == phenotype['id']:
                phenotype['name'] = hpo_info[phenotype['id']]
            if 'source' not in phenotype or not phenotype['source']:
                phenotype['source'] = 'HPO'
    return phenotypes


def fix_sample_phenotypes(oc, query, hpo_info):
    sys.stderr.write('Processing samples...\n')
    sample_resp = oc.samples.search(include='id,phenotypes', **query)
    for sample in sample_resp.result_iterator():
        print(sample['id'])
        sys.stderr.write('Fixing HPOs for sample "{}"\n'.format(sample['id']))
        if 'phenotypes' in sample and sample['phenotypes']:
            phenotypes = fix_phenotypes(sample['phenotypes'], hpo_info)
            print(phenotypes)
            oc.samples.update(study=query['study'], samples=sample['id'],
                              data={'phenotypes': phenotypes})


def fix_individual_phenotypes(oc, query, hpos):
    sys.stderr.write('Processing individuals...\n')
    individual_resp = oc.individuals.search(include='id,phenotypes', **query)
    for individual in individual_resp.result_iterator():
        sys.stderr.write('Fixing HPOs for individual "{}"\n'.format(individual['id']))
        if 'phenotypes' in individual and individual['phenotypes']:
            phenotypes = fix_phenotypes(individual['phenotypes'], hpos)
            oc.individuals.update(study=query['study'], individuals=individual['id'],
                                  data={'phenotypes': phenotypes})


def parse_hpo_file(fpath):
    fhand = open(fpath, 'r')

    # Getting header
    header = None
    for line in fhand:
        if line.startswith('#Format: '):
            header = line[9:].strip().split('<tab>')
        else:
            break
    if header is None:
        raise ValueError('No header found in file "{}"'.format(fpath))

    # Getting ID position in file
    hpo_id_index = None
    hpo_id_fields = [
        'HPO-id',  # phenotype_to_genes.txt
        'HPO-Term-Name',  # genes_to_phenotype.txt
        'HPO-ID'  # diseases_to_genes_to_phenotypes.txt
    ]
    for field in hpo_id_fields:
        if field in header:
            hpo_id_index = header.index(field)
    if hpo_id_index is None:
        raise ValueError('No field in "{}" found in file "{}"'.format(hpo_id_fields, fpath))

    # Getting name position in file
    hpo_name_index = None
    hpo_name_fields = [
        'HPO label',  # phenotype_to_genes.txt
        'HPO-Term-ID',  # genes_to_phenotype.txt
        'HPO-term-name'  # diseases_to_genes_to_phenotypes.txt
    ]
    for field in hpo_name_fields:
        if field in header:
            hpo_name_index = header.index(field)
    if hpo_name_index is None:
        raise ValueError('No field in "{}" found in file "{}"'.format(hpo_name_fields, fpath))

    # Getting HPOs
    fhand.seek(0)  # start of file
    hpo_info = {}
    for line in fhand:
        if not line.startswith('#'):
            line = line.strip().split('\t')
            hpo_info[line[hpo_id_index]] = line[hpo_name_index]
    fhand.close()

    return hpo_info


def main():
    # Define parameters
    parser = argparse.ArgumentParser()
    parser.add_argument('action', help='Action to execute', choices=['sample', 'individual'])
    parser.add_argument('--hpo', required=True, help='Load HPO ontology terms (download from http://compbio.charite.de/jenkins/job/hpo.annotations/lastSuccessfulBuild/artifact/util/annotation/phenotype_to_genes.txt)')
    parser.add_argument('-s', '--study', required=True, help='Study ID or fqn')
    parser.add_argument('--id', help='Comma-separated list of sample ID')
    parser.add_argument('--phenotypes', help='Comma-separated list of phenotype ID, e.g. hp:12345')
    parser.add_argument('--conf', help='Load client-configuration.yml')
    parser.add_argument('--url', help='Default https://bioinfo.hpc.cam.ac.uk/opencga-prod',
                        default='https://bioinfo.hpc.cam.ac.uk/opencga-prod')
    parser.add_argument('-u', '--user', required=True, help='Username to login to OpenCGA')
    parser.add_argument('-p', '--password', help='Password to login to OpenCGA')

    args = parser.parse_args()

    # Ask for password
    if args.password is None:
        args.password = getpass.getpass()

    # Create OpencgaClient config object from file or dictionary
    if args.conf is not None:
        config = ClientConfiguration(args.conf)
    else:
        config_dict = {
            'rest': {
                'host': args.url
            }
        }
        config = ClientConfiguration(config_dict)

    # Create OpenCGA client and login
    oc = OpencgaClient(config)
    oc.login(args.user, args.password)  # Password will be prompted, not displayed

    # Load HPO Ontology terms
    hpo_info = parse_hpo_file(args.hpo)

    # Fetch selected families
    query = {}
    if args.study is not None:
        query['study'] = args.study
    if args.id is not None:
        query['id'] = args.id
    if args.phenotypes is not None:
        query['phenotypes'] = args.phenotypes

    # Execute action
    if args.action == 'sample':
        fix_sample_phenotypes(oc, query, hpo_info)
    elif args.action == 'individual':
        fix_individual_phenotypes(oc, query, hpo_info)


if __name__ == '__main__':
    sys.exit(main())
