# Public Demo

## Overview

We have installed a public _**demo**_ at the University of Cambridge to facilitate the testing and development for all users. We have loaded and indexed **five different datasets** organised in 3 _projects_ and 5 _studies_, these cover the most typical data use cases today such as multi-sample VCF, family exomes, and genomes; or cancer somatic data. All documentation examples and tutorials use this _demo_ installation.

## Connecting to the public _demo_

OpenCGA public _demo_ REST URL is available at [http://bioinfo.hpc.cam.ac.uk/opencga-prod/](http://bioinfo.hpc.cam.ac.uk/opencga-prod/). You can check REST API and documentation at [http://bioinfo.hpc.cam.ac.uk/opencga-prod/webservices/](http://bioinfo.hpc.cam.ac.uk/opencga-prod/webservices/).

### Credentials

We have created a read-only user called _**demouser**_ with password _**demouser**_. As in most OpenCGA installations where normal users are not the owners of the data, _demouser_ has been given VIEW access to all _demo_ user data, this is a very common configuration in OpenCGA  where the owner of the data grant access to other users. In this _demo_ installation the owner of the data is _demo_ user, while _demouser_ user is the public  user created to query data.

## Datasets

### Genomic Data <a href="usingopencga-genomicdata" id="usingopencga-genomicdata"></a>

In this _demo_ we have indexed 5 different genomic datasets. Data has been organised in three _projects_ and five _studies_. These represents different assemblies and data types such as multi sample VCF, aggregated VCF or family genome or exome. The data is organised in 3 _projects_ and 5 studies. You can find some useful information in this table:

| Project ID - Name                                                                                                    | Study ID - Name                                                                                                                 | VCF File Type          | Samples | Variants       |
| -------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------- | ---------------------- | ------- | -------------- |
| <p><em><strong>population</strong></em></p><p><strong></strong></p><p><strong>Population Studies GRCh38</strong></p> | <p><em><strong>1000g</strong></em></p><p><strong></strong></p><p><strong>1000 Genomes phase 3</strong><br><strong></strong></p> | WGS Multisample        | 2,504   | **82,587,763** |
| ****                                                                                                                 | <p><em><strong>uk10k</strong></em></p><p><strong></strong></p><p><strong>UK10K</strong><br><strong></strong></p>                | WGS Aggregated         | 10,000  | **46,624,127** |
| <p><em><strong>family</strong></em></p><p><strong></strong></p><p><strong>Family Studies GRCh37</strong></p>         | <p><em><strong>corpasome</strong></em></p><p><strong></strong></p><p><strong>Corpas Family</strong></p>                         | WES Family Multisample | 4       | **300,711**    |
| ****                                                                                                                 | <p><em><strong>platinum</strong></em></p><p><strong></strong></p><p><strong>Illumina Platinum</strong></p>                      | GWS Family Multisample | 17      | **12,263,246** |

### Clinical Data <a href="usingopencga-clinicaldata" id="usingopencga-clinicaldata"></a>

In order to make this _demo_ more useful to users we have loaded or simulated some clinical data, this allows to exploit OpenCGA analysis such as GWAS or clinical interpretation. You can find clinical data for each study in the following sections.

#### 1000g <a href="usingopencga-1000g" id="usingopencga-1000g"></a>

We loaded the 1000 Genomes pedigree file, you can find a copy at [http://resources.opencb.org/opencb/opencga/templates/demo/20130606\_g1k.ped](http://resources.opencb.org/opencb/opencga/templates/demo/20130606\_g1k.ped) 

#### uk10k <a href="usingopencga-uk10k" id="usingopencga-uk10k"></a>

There is no possible clinical data in this study. This is a _WGS aggregated_ dataset so no samples or genotypes were present in the dataset and, therefore, no _Individuals_ or _Samples_ have been created.  

#### corpasome <a href="usingopencga-corpasome" id="usingopencga-corpasome"></a>

We simulated two different disorders and few phenotypes for the different members of the family. To be documented soon.

#### platinum <a href="usingopencga-platinum" id="usingopencga-platinum"></a>

To be documented soon.

#### rams_cml <a href="usingopencga-rams_cml" id="usingopencga-rams_cml"></a>

To be documented soon.
