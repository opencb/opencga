# Using OpenCGA

## RESTful Web Services <a id="UsingOpenCGA-RESTfulWebServices"></a>

OpenCGA implements a comprehensive and well-designed REST web service API, this consists of more than 200 web services to allow querying and operating data in OpenCGA. You can get more info at [RESTful Web Services](http://docs.opencb.org/display/opencga/RESTful+Web+Services) page.

We have implemented **three different ways** to query and operate OpenCGA through the REST web services API:

* [REST Client Libs](http://docs.opencb.org/display/opencga/RESTful+Web+Services#RESTfulWebServices-ClientLibraries): four different client libraries have been implemented to ease the use of REST web services, This allows bioinformaticians to easily integrate OpenCGA in any pipeline. The four libraries are equally functional and fully maintained, these are [_Java_](http://docs.opencb.org/display/opencga/Java)_,_ [_Python_](http://docs.opencb.org/display/opencga/Python) \(available at [PyPI](https://pypi.org/project/pyopencga/)\), [_R_](http://docs.opencb.org/display/opencga/R) and [_JavaScript_](http://docs.opencb.org/display/opencga/JavaScript)
* [Command Line](http://docs.opencb.org/display/opencga/Command+Line): users and administrators can use _**opencga.sh**_ command line to query and operate OpenCGA. 
* [IVA Web Application](http://docs.opencb.org/display/opencga/IVA+Web+App): an interactive web application called IVA has been developed to query and visualisation OpenCGA data.

## OpenCGA Demo <a id="UsingOpenCGA-OpenCGADemo"></a>

We have deployed a public _**demo**_ installation to facilitate the testing and development for all users. We have loaded and indexed **five different datasets** organised in 3 _projects_ and 5 _studies_, these cover the most typical data use cases today such as multi-sample VCF, family exomes and genomes; or cancer somatic data. All documentation examples and tutorials use this _demo_ installation.

### Connecting to _demo_ installation <a id="UsingOpenCGA-Connectingtodemoinstallation"></a>

OpenCGA _demo_ REST URL is available at [http://bioinfo.hpc.cam.ac.uk/opencga-prod/](http://bioinfo.hpc.cam.ac.uk/opencga-prod/). You can check REST API and documentation at [http://bioinfo.hpc.cam.ac.uk/opencga-prod/webservices/](http://bioinfo.hpc.cam.ac.uk/opencga-prod/webservices/).

We have created a read-only user called _**demouser**_ with password _**demouser**_. As in most OpenCGA installations where normal users are not the owners of the data, _demouser_ has been given VIEW access to all _demo_ user data, this is a very common configuration in OpenCGA where the owner of the data grant access to other users. In this _demo_ installation the owner of the data is _demo_ user, while _demouser_ user is the public user created to query data.

### Genomic Data <a id="UsingOpenCGA-GenomicData"></a>

In this _demo_ we have indexed 5 different genomic datasets. Data has been organised in three _projects_ and five _studies_. These represents different assemblies and data types such as multi sample VCF, aggregated VCF or family genome or exome. The data is organised in 3 _projects_ and 5 studies. You can find some useful information in this table:

<table>
  <thead>
    <tr>
      <th style="text-align:left">Project ID and Name</th>
      <th style="text-align:left">Study ID - Name</th>
      <th style="text-align:left">VCF File Type</th>
      <th style="text-align:left">Samples</th>
      <th style="text-align:left">Variants</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td style="text-align:left">
        <p><b>population</b>
        </p>
        <p><b>Population Studies GRCh38</b>
        </p>
      </td>
      <td style="text-align:left"><b>1000g - 1000 Genomes phase 3<br /></b>
      </td>
      <td style="text-align:left">WGS Multi sample</td>
      <td style="text-align:left">2,504</td>
      <td style="text-align:left"><b>82,587,763</b>
      </td>
    </tr>
    <tr>
      <td style="text-align:left"><b>uk10k - UK10K<br /></b>
      </td>
      <td style="text-align:left">WGS Aggregated</td>
      <td style="text-align:left">10,000</td>
      <td style="text-align:left"><b>46,624,127</b>
      </td>
      <td style="text-align:left"></td>
    </tr>
    <tr>
      <td style="text-align:left">
        <p><b>family</b>
        </p>
        <p><b>Family Studies GRCh37</b>
        </p>
      </td>
      <td style="text-align:left"><b>corpasome - Corpas Family</b>
      </td>
      <td style="text-align:left">WES Family Multi sample</td>
      <td style="text-align:left">4</td>
      <td style="text-align:left"><b>300,711</b>
      </td>
    </tr>
    <tr>
      <td style="text-align:left"><b>platinum - Illumina Platinum</b>
      </td>
      <td style="text-align:left">GWS Family Multi sample</td>
      <td style="text-align:left">17</td>
      <td style="text-align:left"><b>12,263,246</b>
      </td>
      <td style="text-align:left"></td>
    </tr>
    <tr>
      <td style="text-align:left">
        <p><b>cancer</b>
        </p>
        <p><b>Cancer Studies GRCh37</b>
        </p>
      </td>
      <td style="text-align:left">
        <p><b>rams_cml - RAMS_CML<br /></b>
        </p>
        <p>Chronic Myeloid Leukemia - Russian Academy of Medical Sciences<b><br /></b>
        </p>
      </td>
      <td style="text-align:left">Somatic</td>
      <td style="text-align:left">11</td>
      <td style="text-align:left"><b>121,384</b>
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

#### Test

test

#### Test2 <a id="UsingOpenCGA-rams_cml"></a>

test2

#### Test 3

aaa





