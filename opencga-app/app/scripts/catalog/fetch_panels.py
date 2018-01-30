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


# fetch and parse PanelApp panels
req = urllib.request.Request('https://panelapp.genomicsengland.co.uk/WebServices/list_panels/')
response = urllib.request.urlopen(req)
panels = json.loads(response.read().decode('utf-8'))
for panel in panels['result']:
    # PanelApp panels with version 0.x are not ready for interpretation
    if panel['CurrentVersion'].startswith("0"):
        print("Panel is not ready for interpretation: '" + panel['Name'] + "', version: '" + panel['CurrentVersion'] + "'")
    else:
        print("Processing " + panel['Name'] + "...")
        req = urllib.request.Request('https://panelapp.genomicsengland.co.uk/WebServices/get_panel/' + panel['Panel_Id'] + '/')
        response = urllib.request.urlopen(req)
        panel_info = json.loads(response.read().decode('utf-8'))['result']

        # store all phenotypes from the Panel
        phenotypes = []
        for disorder in panel['Relevant_disorders']:
            if disorder != "":
                phenotypes.append({'id': '', 'name': disorder, 'source': ''})

        # retrieve and store all genes
        genes = []
        for gene in panel_info['Genes']:
            if not gene['EnsembleGeneIds']:
                genes.append({'id': '', 'name': gene['GeneSymbol'], 'confidence': gene['LevelOfConfidence']})
            else:
                genes.append({'id': gene['EnsembleGeneIds'][0], 'name': gene['GeneSymbol'], 'confidence': gene['LevelOfConfidence']})

        # prepare OpenCGA Panel data model
        opencga_panel = {
            'id': panel['Panel_Id'],
            'name': panel['Name'],
            'version': 1,
            'author': 'PanelApp (GEL)',
            'status': 'READY',
            'date': datetime.date.today().isoformat(),
            'description': '',
            'phenotypes': phenotypes,
            'variants': [],
            'genes': genes,
            'regions': [],
            'attributes': {
                'DiseaseGroup': panel_info['DiseaseGroup'],
                'DiseaseSubGroup': panel_info['DiseaseSubGroup'],
                'panelAppVersion': panel_info['version']
            }
        }

        with open(args.output + '/' + opencga_panel['id'] + '.PanelApp.json', 'w') as file:
            file.write(json.dumps(opencga_panel, indent = 4))
