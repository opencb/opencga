#!/usr/bin/python3

import argparse
import urllib.request
import json
import datetime

######  USAGE    #######################
##  ./fetch_panels.py -o OUTPUT_DIR
########################################

# configure the CLI options
parser = argparse.ArgumentParser()
parser.add_argument("-o", "--output", help="Output directory for the panels [CURRENT_DIR]")
args = parser.parse_args()

if args.output is None:
    args.output = '.'


# fetch and parse all PanelApp panels
for page in [1, 2, 3]:
    req = urllib.request.Request('https://panelapp.genomicsengland.co.uk/api/v1/panels/?page=' + str(page))
    response = urllib.request.urlopen(req)
    panels = json.loads(response.read().decode('utf-8'))
    for panel in panels['results']:
        # PanelApp panels with version 0.x are not ready for interpretation
        if panel['version'].startswith("0"):
            print("Panel is not ready for interpretation: '" + panel['name'] + "', version: '" + panel['version'] + "'")
        else:
            print("Processing " + panel['name'] + " " + str(panel['id']) + "...")
            req = urllib.request.Request('https://panelapp.genomicsengland.co.uk/api/v1/panels/' + str(panel['id']) + '/')
            response = urllib.request.urlopen(req)
            panel_info = json.loads(response.read().decode('utf-8'))

            # store categories
            categories = []
            categories.append({'name': panel_info['disease_group'], 'level': 1})
            categories.append({'name': panel_info['disease_sub_group'], 'level': 2})

            # store all phenotypes from the Panel
            phenotypes = []
            for disorder in panel_info['relevant_disorders']:
                if disorder != "":
                    phenotypes.append({'id': '', 'name': disorder, 'source': ''})

            # retrieve and store all genes
            genes = []
            for gene in panel_info['genes']:
                ensemblGeneId = ''
                xrefs = []
                publications = []

                # read the first Ensembl gene ID if exists
                if 'GRch37' in gene['gene_data']['ensembl_genes']:
                    ensemblGeneId = gene['gene_data']['ensembl_genes']['GRch37']['82']['ensembl_id']
                elif 'GRch38' in gene['gene_data']['ensembl_genes']:
                    ensemblGeneId = gene['gene_data']['ensembl_genes']['GRch38']['90']['ensembl_id']

                # read OMIM ID
                if gene['gene_data']['omim_gene']:
                    for omim in gene['gene_data']['omim_gene']:
                        xrefs.append({
                            'id': omim,
                            'dbName': 'OMIM'
                        })
                xrefs.append({
                    'id': gene['gene_data']['gene_name'],
                    'dbName': 'GeneName'
                })

                # read the publications
                if gene['publications']:
                    publications = gene['publications']

                # add panel gene
                genes.append({
                    'id': ensemblGeneId,
                    'name': gene['gene_data']['hgnc_symbol'],
                    'xrefs': xrefs,
                    'modeOfInheritance': gene['mode_of_inheritance'],
                    'confidence': gene['confidence_level'],
                    'evidences': gene['evidence'],
                    'publications': publications
                })

            # prepare OpenCGA Panel data model
            opencga_panel = {
                'id': str(panel_info['id']),
                'name': panel_info['name'],
                'categories': categories,
                'phenotypes': phenotypes,
                'tags': [],
                'stats': {
                    'numberOfVariants': 0,
                    'numberOfGenes': len(genes),
                    'numberOfRegions': 0
                },
                'variants': [],
                'genes': genes,
                'regions': [],
                'version': 1,
                'source': {
                    'id': panel_info['id'],
                    'name': panel_info['name'],
                    'version': panel_info['version'],
                    'author': '',
                    'project': 'PanelApp (GEL)'
                },
                'creationDate': datetime.date.today().isoformat(),
                'modificationDate': datetime.date.today().isoformat(),
                'description': panel_info['disease_sub_group'] + ' (' + panel_info['disease_group'] + ')',
                'attributes': {
                    'PanelAppInfo': panel
                }
            }

            with open(args.output + '/' + str(opencga_panel['id']) + '.PanelApp.json', 'w') as file:
                file.write(json.dumps(opencga_panel, indent = 4))
