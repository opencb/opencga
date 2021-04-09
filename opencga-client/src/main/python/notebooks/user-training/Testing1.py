

from pyopencga.opencga_config import ClientConfiguration # import configuration module
from pyopencga.opencga_client import OpencgaClient # import client module
from pprint import pprint
import json
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use( 'tkAgg' )
import matplotlib.pyplot as plt
import seaborn as sns
#%matplotlib inline
#%%

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

#%%
# Define variables to query different data models though the web services

variant_client = oc.variants   # Variant Client
user_client = oc.users
project_client = oc.projects
study_client = oc.studies
sample_client = oc.samples
individual_client = oc.individuals
file_client = oc.files
cohort_client = oc.cohorts

#%% Let's use corpasome study

study = 'family:corpasome'

#%% md Which variants in RD38 fall on chromosome 15 between 21,242,091 and 23,226,874bp?
variants = variant_client.query(study=study, region='15:21242091-23226874', include='id')

# variants.print_results('id')
df = variants.to_data_frame()
formated_df = df.drop(['names', 'studies'], 1)

#%%
print(formated_df.head)

#%% Which variants in RD38 fall on chromosome 15?

# variants = variant_client.query(study=study, region='15', include='id')
# result = variants.print_results('id')
# df = variants.to_data_frame(include='id')

#%%
formated_df = df.drop(['names', 'studies'], 1)

#%% Select variables based on type
snp_df = formated_df[formated_df.type == 'SNP']
indel_df = formated_df[formated_df.type == 'INDEL']

#%% Set a background
sns.set_theme(style="darkgrid")
sns.color_palette('deep')
#sns.set_style("whitegrid")

#%% Plot all the variants
#sns.histplot(formated_df['start'], bins=50, kde=True)


#%%
sns.set_theme(style="darkgrid")
sns.color_palette('deep')
sns.displot(data=df, hue='type', hue_order=['INDEL','SNP','SNV'], x="start", bins=50) # multiple= "dodge", 'stack'
#plt.legend(prop={'size': 12})
#plt.legend()
plt.title('Variants in Region of Chromosome 15', fontsize=20, fontweight='bold' )
plt.xlabel('Chr15:21242091-23226874 bp', fontsize=12)
plt.ylabel('Variant counts', fontsize=12)
sns.despine(left=False)
plt.show()

#%% Plot formatting
plt.close()

#%%



