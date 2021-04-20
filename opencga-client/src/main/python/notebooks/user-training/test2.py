#%% md

# Overview
------

This notebook is intended to provide guidance for querying variants in the OpenCGA server through *pyopencga* from the studies that your user has access to; you can combine the filtration of variants with the exploration of their related clinical data (Samples, Individuals Genotypes etc.).<br>

It is also recommended to get a taste of the clinical data we are encountering in the study: How many samples and individuals does the study have? Is there any defined cohorts? Can we get some statistics about the genotypes of the samples in the Sudy?

For guidance on how to loggin and get started with *opencga* you can refer to : [pyopencga_first_steps.ipynb](https://github.com/opencb/opencga/tree/develop/opencga-client/src/main/python/notebooks/user-training)<br>

A good first step when start working with OpenCGA is to explore **Catalog**, which holds: information about our user, the projects and studies our user has permission to access and the clinical data from the studies. For guidance you can refer to : [pyopencga_catalog.ipynb](https://github.com/opencb/opencga/tree/develop/opencga-client/src/main/python/notebooks/user-training)<br>

**[NOTE]** The server methods used by *pyopencga* client are defined in the following swagger URL:
- https://ws.opencb.org/opencga-prod/webservices/


#%% md

## Setup the Client and Login into *pyopencga*

**Configuration and Credentials**

Let's assume we already have *pyopencga* installed in our python setup (all the steps described on [pyopencga_first_steps.ipynb](https://github.com/opencb/opencga/tree/develop/opencga-client/src/main/python/notebooks/user-training)).

You need to provide **at least** a host server URL in the standard configuration format for OpenCGA as a python dictionary or in a json file.


#%%

# Step 1. Import pyopencga dependecies
from pyopencga.opencga_config import ClientConfiguration # import configuration module
from pyopencga.opencga_client import OpencgaClient # import client module
from pprint import pprint
import json
import matplotlib.pyplot as plt
import seaborn as sns
import datetime
#import pyjq

# Step 2. OpenCGA host
host = 'https://ws.opencb.org/opencga-prod'

# Step 3. User credentials
user = 'demouser'
passwd = 'demouser' ## you can skip this, see below.
####################################

# Step 4. Create the ClientConfiguration dict
config_dict = {'rest': {
                       'host': host
                    }
               }

# Step 5. Create the ClientConfiguration and OpenCGA client
config = ClientConfiguration(config_dict)
oc = OpencgaClient(config)

# Step 6. Login to OpenCGA using the OpenCGA client
# Pass the credentials to the client
# (here we put only the user in order to be asked for the password interactively)
# oc.login(user)

# or you can pass the user and passwd
oc.login(user, passwd)

print('Logged succesfuly to {}, {} well done!'.format(host, oc.token))


#%% md

## Setup OpenCGA Variables

#%% md

Once we have defined a variable with the client configuration and credentials, we can access to all the methods defined for the client. These methods implement calls to query different data models in *OpenCGA*.


#%%

## Define the OpenCGA variables to query data
study = 'family:corpasome'

# You can define list using string with commas
genes='BRCA2'

# or you can use python lists
transcripts=['ENST00000530893']

RESULT_SEPARATOR='\n'

#%% md

# Querying Variants: Filter by Variant Annotation
------

#%% md

In this section you will learn how to query variants filtering by **Variant Annotation**, this is one of the most useful and rich web service with many filter parameters.

You can query variants using the following function:

`
  oc.variants.query()
`

#%% md

## Filter by Region

The format used for referring to genomic coordinates in *OpenCGA* is as follows:

**1:10000-20000 : Chromosome 1, from position 10000 to 200000 bp**

#%%

# Perform the query
variants = oc.variants.query(study=study, region='15:21242091-23226874', include='id')

# Print the results
#variants.print_results('id')
assert jq.compile(".").input(variants).text()

#%% md

The new function `.to_data_frame()` added in the *pyopencga* release 2.0.1.1, allows you store the results as a *pandas* dataframe object:

#%%

df = variants.to_data_frame()
formatted_df = df.drop(['names', 'studies'], 1)
print('The results can be stored and printed as a pandas DF:\n\n', formatted_df.head())

#%% md

With the *pandas dataframe* object, you can create plots using *maplotlib* or *seaborn* python libraries to show the results:

#%%

# Create a plot with the results:

sns.set_theme(style="darkgrid")
sns.color_palette('deep')
sns.displot(data=df, hue='type', hue_order=['INDEL','SNP','SNV'], x="start", bins=50) # multiple= "dodge", 'stack'
plt.title('Variants in Region of Chromosome 15', fontsize=20, fontweight='bold' )
plt.xlabel('Chr15:21242091-23226874 bp', fontsize=12)
plt.ylabel('Variant counts', fontsize=12)
sns.despine(left=False)
plt.show()

#%% md

## Filter by Gene

We can filter variants by gene using the parameters `xref` or `gene`:
* **xref**: you can filter by different IDs including gene, transcrit, dbSNP, ...
* **gene**: only accepts gene IDs

Remember you can pass different IDs using comma as separator.

#%%

# Filter by xref
variants = oc.variants.query(study=study, xref=transcripts, limit=5) # Other params: exclude='annotation'
variants.print_results(fields='id,annotation.consequenceTypes.geneName,annotation.displayConsequenceType') # metadata=False
print(RESULT_SEPARATOR)

# Filter by gene
variants = oc.variants.query(study=study, gene=genes, limit=5) # Other params: exclude='annotation'
variants.print_results(fields='id,annotation.consequenceTypes.geneName,annotation.displayConsequenceType')


#%% md

## Filter by Consequence Type

...

#%%



#%% md

## Filter by Variant Type

values accepted...

#%%

# Filter by SNV
variants = oc.variants.query(study=study, type='SNV', limit=5) # Other params: exclude='annotation'
variants.print_results(fields='id,annotation.consequenceTypes.geneName,annotation.displayConsequenceType') # metadata=False
print(RESULT_SEPARATOR)

# Filter by ...
variants = oc.variants.query(study=study, type='SNV,INDEL', limit=5) # Other params: exclude='annotation'
variants.print_results(fields='id,annotation.consequenceTypes.geneName,annotation.displayConsequenceType')

# Filter by ...
variants = oc.variants.query(study=study, type='DELETION', limit=5) # Other params: exclude='annotation'
variants.print_results(fields='id,annotation.consequenceTypes.geneName,annotation.displayConsequenceType')

#%% md

## Advanced Filters

#%%



#%% md

# Querying Variants: Filter by Sample and VCF params
-------

#%%

# resp = oc.samples.search(limi=1)
# sample= resp.

#%%



#%%



#%% md

# Aggregation Stats
-------

#%%



#%% md

# Use Cases
---------

#%% md

## Fetch all samples per Variant

1. choose one variant
2. get samples
3. go to catalog

#%% md

**Situation:** I am interested in getting a list of all participants in the study, that have:
(A) SNVs in NOD2 gene that have MAF of <0.01 in gnomad_NFE AND are missense, start_lost, stop_gained, or stop_lost
split by hets and homs.

- How I can submit this query but instead of NOD2, for a list of 10 genes (example list: IL3, IL31, IL32, IL34, IL6, IL6R, IL10RA, IL10RB,  IL7, IL7R) to the openCGA server

#%%

# Define the parameters of the variants we want to query

genes = ['NOD2','IL3']
limit = 5
type = ['SNV']
ct = ['missense_variant','start_lost','stop_gained','stop_lost'] #List of SO consequence types, e.g. missense_variant,stop_lost or SO:0001583,SO:0001578
populationFrequencyMaf='gnomAD:ALL<0.01'



#%%

variants = variant_client.query(study=study, gene=genes, type=type, ct=ct,
                                populationFrequencyMaf=populationFrequencyMaf, limit=limit)


variants.print_results('id')
variants.
#pprint(variants.get_result(0))


#%% md

## Sample Variant Stats

1. Choose one random sample from catalog
2. sample stats query

#%%
