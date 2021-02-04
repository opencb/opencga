# Public Demo

## Overview

We have installed a public _**demo**_ at the University of Cambridge to facilitate the testing and development for all users. We have loaded and indexed **five different datasets** organised in 3 _projects_ and 5 _studies_, these cover the most typical data use cases today such as multi-sample VCF, family exomes, and genomes; or cancer somatic data. All documentation examples and tutorials use this _demo_ installation.

## Connecting to the public _demo_

OpenCGA public _demo_ REST URL is available at [http://bioinfo.hpc.cam.ac.uk/opencga-prod/](http://bioinfo.hpc.cam.ac.uk/opencga-prod/). You can check REST API and documentation at [http://bioinfo.hpc.cam.ac.uk/opencga-prod/webservices/](http://bioinfo.hpc.cam.ac.uk/opencga-prod/webservices/).

### Credentials

We have created a read-only user called _**demouser**_ with password _**demouser**_. As in most OpenCGA installations where normal users are not the owners of the data, _demouser_ has been given VIEW access to all _demo_ user data, this is a very common configuration in OpenCGA where the owner of the data grant access to other users. In this _demo_ installation the owner of the data is _demo_ user, while _demouser_ user is the public user created to query data.

## Datasets

### Genomic Data <a id="UsingOpenCGA-GenomicData"></a>

In this _demo_ we have indexed 5 different genomic datasets. Data has been organised in three _projects_ and five _studies_. These represents different assemblies and data types such as multi sample VCF, aggregated VCF or family genome or exome. The data is organised in 3 _projects_ and 5 studies. You can find some useful information in this table:

<table>
  <thead>
    <tr>
      <th style="text-align:left">Project ID - Name</th>
      <th style="text-align:left">Study ID - Name</th>
      <th style="text-align:left">VCF File Type</th>
      <th style="text-align:left">Samples</th>
      <th style="text-align:left">Variants</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td style="text-align:left">
        <p><em><b>population</b></em>
        </p>
        <p>&lt;b&gt;&lt;/b&gt;</p>
        <p><b>Population Studies GRCh38</b>
        </p>
      </td>
      <td style="text-align:left">
        <p><em><b>1000g</b></em>
        </p>
        <p>&lt;b&gt;&lt;/b&gt;</p>
        <p><b>1000 Genomes phase 3<br /></b>
        </p>
      </td>
      <td style="text-align:left">WGS Multisample</td>
      <td style="text-align:left">2,504</td>
      <td style="text-align:left"><b>82,587,763</b>
      </td>
    </tr>
    <tr>
      <td style="text-align:left">&lt;b&gt;&lt;/b&gt;</td>
      <td style="text-align:left">
        <p><em><b>uk10k</b></em>
        </p>
        <p>&lt;b&gt;&lt;/b&gt;</p>
        <p><b>UK10K<br /></b>
        </p>
      </td>
      <td style="text-align:left">WGS Aggregated</td>
      <td style="text-align:left">10,000</td>
      <td style="text-align:left"><b>46,624,127</b>
      </td>
    </tr>
    <tr>
      <td style="text-align:left">
        <p><em><b>family</b></em>
        </p>
        <p>&lt;b&gt;&lt;/b&gt;</p>
        <p><b>Family Studies GRCh37</b>
        </p>
      </td>
      <td style="text-align:left">
        <p><em><b>corpasome</b></em>
        </p>
        <p>&lt;b&gt;&lt;/b&gt;</p>
        <p><b>Corpas Family</b>
        </p>
      </td>
      <td style="text-align:left">WES Family Multisample</td>
      <td style="text-align:left">4</td>
      <td style="text-align:left"><b>300,711</b>
      </td>
    </tr>
    <tr>
      <td style="text-align:left">&lt;b&gt;&lt;/b&gt;</td>
      <td style="text-align:left">
        <p><em><b>platinum</b></em>
        </p>
        <p>&lt;b&gt;&lt;/b&gt;</p>
        <p><b>Illumina Platinum</b>
        </p>
      </td>
      <td style="text-align:left">GWS Family Multisample</td>
      <td style="text-align:left">17</td>
      <td style="text-align:left"><b>12,263,246</b>
      </td>
    </tr>
  </tbody>
</table>

### Clinical Data <a id="UsingOpenCGA-ClinicalData"></a>

In order to make this _demo_ more useful to users we have loaded or simulated some clinical data, this allows to exploit OpenCGA analysis such as GWAS or clinical interpretation. You can find clinical data for each study in the following sections.

#### 1000g <a id="UsingOpenCGA-1000g"></a>

We loaded the 1000 Genomes pedigree file, you can find a copy at [http://resources.opencb.org/opencb/opencga/templates/demo/20130606\_g1k.ped](http://resources.opencb.org/opencb/opencga/templates/demo/20130606_g1k.ped)

#### uk10k <a id="UsingOpenCGA-uk10k"></a>

There is no possible clinical data in this study. This is a _WGS aggregated_ dataset so no samples or genotypes were present in the dataset and, therefore, no _Individuals_ or _Samples_ have been created.

#### corpasome <a id="UsingOpenCGA-corpasome"></a>

We simulated two different disorders and few phenotypes for the different members of the family. To be documented soon.

#### platinum <a id="UsingOpenCGA-platinum"></a>

To be documented soon.

#### rams\_cml <a id="UsingOpenCGA-rams_cml"></a>

To be documented soon.

