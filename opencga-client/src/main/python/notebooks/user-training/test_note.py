#%% md

# *pyopencga* Basic Variant and Interpretation Usage

------


**[NOTE]** The server methods used by pyopencga client are defined in the following swagger URL:
- https://ws.opencb.org/opencga-prod/webservices

**[NOTE]** Current implemented methods are registered at the following spreadsheet:
- https://docs.google.com/spreadsheets/d/1QpU9yl3UTneqwRqFX_WAqCiCfZBk5eU-4E3K-WVvuoc/edit?usp=sharing

## Overview

add here

#%% md

## 1. Setup the Client and Login into *pyopencga*

**Configuration and Credentials**

Let's assume we already have *pyopencga* installed in our python setup (all the steps described on [001-pyopencga_first_steps.ipynb](https://github.com/opencb/opencga/tree/develop/opencga-client/src/main/python/notebooks/user-training)).

You need to provide **at least** a host server URL in the standard configuration format for OpenCGA as a python dictionary or in a json file.


#%%

from pyopencga.opencga_config import ClientConfiguration # import configuration module
from pyopencga.opencga_client import OpencgaClient # import client module
from pprint import pprint
import json

####################################
## Configuration parameters  #######
####################################
# OpenCGA host
host = 'https://ws.opencb.org/opencga-prod'

# User credentials
user = 'demouser'
passwd = 'demouser' ## you can skip this, see below.
study = 'demo@family:platinum'
####################################

# Creating ClientConfiguration dict
config_dict = {'rest': {
                       'host': host
                    }
               }
print('Config information:\n',config_dict)

# Pass the config_dict dictionary to the ClientConfiguration method
config = ClientConfiguration(config_dict)

# Create the client
oc = OpencgaClient(config)

# Pass the credentials to the client
# (here we put only the user in order to be asked for the password interactively)
# oc.login(user)

# or you can pass the user and passwd
oc.login(user, passwd)


#%% md

Once we have defined a variable with the client configuration and credentials, we can access to all the methods defined for the client. These methods implement calls to query different data models in *OpenCGA*.


#%%

## Define variables to query different data models though the web services

## Jacobo thinks better oc.variants.query....

variant_client = oc.variants   # Variant Client
user_client = oc.users
project_client = oc.projects
study_client = oc.studies
sample_client = oc.samples
individual_client = oc.individuals
file_client = oc.files
cohort_client = oc.cohorts



#%% md

## 2. Querying Variants

#%% md

Let's get the variant client to query OpenCGA server

#%%

# Let's use corpasome study
study = 'family:corpasome'

#%% md

### Query variants by gene

We can use the `.query()` server to query for variants in a specific gene:

#%%

# Define the gene or genes we want to query

genes=['BRCA2']
limit=2

variants = variant_client.query(study=study, gene=genes, limit=limit) # Other params: exclude='annotation'


variants.print_results('id')
#pprint(variants.get_result(0))

#%% md

## 3. Aggegation Files

#%% md

## 4. Common queries

#%% md

### Filtering by region/gene
Which variants in cohort A are in genomic region B?


#%% md


Which variants in RD38 fall on chromosome 15 between 21,242,091 and 23,226,874bp?

#%%

# Perform the query
variants = variant_client.query(study=study, region='15:21242091-23226874', include='id')

#Print the results
#variants.print_results('id')


#%% md

The new function `.to_data_frame()` added in the *pyopencga* release 2.0.1.1, allows you store the results as a *pandas* dataframe object:

#%%

df = variants.to_data_frame()
formated_df = df.drop(['names', 'studies'], 1)
print('The results can be stored and printed as a pandas DF:\n\n', formated_df.head())

#%%

oc.disease_panels.
variants = variant_client.query(study=study, chromosome=15, include='id')

pprint(variants.get_result(0))
#variants.print_results('id')

#%%

variants = variant_client.query(study=study, region='15:21242091-23226874', include='id')

pprint(variants.get_result(4))
#variants.print_results('id')

#%%


