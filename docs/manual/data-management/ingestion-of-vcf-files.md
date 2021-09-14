---
description: >-
  This page describes the process that an operator follows to load their VCF
  files into an OpenCGA Variant Store.
---

# Loading of VCF files

## **Introduction**

This page describes the typical process that an operator will follow to load their VCF files into an OpenCGA using the OpenCGA command line tools. Loading of VCF files can be done either before \(normally\) or after the loading of sample and clinical metadata. This document assumes OpenCGA version 2.0 or later

It typically takes a few minutes to load a VCF from a single exome but it can take several days \(or even weeks\) to load many thousands of whole genomes. For more information on data load times see \[Data Load Benchmarks\].

The process is divided into 5 steps:

1. "Register" the source VCF files with OpenCGA; this creates basic File, Sample and Individual entries in Catalog. 
2. "Index" each VCF file; this loads data into the Variant Store
3. "Annotate" all newly-created variants against the CellBase knowledge base.
4. "Summarise"; re-calculate all variant statistics.
5. "Secondary Index" to include annotations and summaries. 

\[TODO: update the figure below to follow the process described above\]

![](https://lh4.googleusercontent.com/O1IDTz7z5AGUjYe0wuugmEIJNlle5gkO-wt9wc2xjTJKmlfVbBE3HWNLTQglVlGSPXGN1NlHGEfC5TtZbtRHIuFMOE93QnZTU_Z34l4n9jrAQ2mPC99ltfZJ5b7hu2D2w0sO6ih6)

## **Prerequisites**

This document assumes that:

* The source VCF files are accessible \(e.g. via shared filesystem\) on the target OpenCGA server. 
* The operator has access to a workstation with network access to the web services on the OpenCGA server.
* Compatible OpenCGA client software is installed on the workstation. Find [here](ingestion-of-vcf-files.md) the instructions on how to install the client software.
* The destination Study has been created on the OpenCGA server. Find [here](projects-and-studies.md) instructions for creating Projects and Studies. 
* The operator has login credentials on the OpenCGA server with appropriate permissions; i.e. write access to the destination Study. 

### **Catalog file register**

This step presents the data to OpenCGA and registers the new files into the system. Samples will be created automatically after linking the file, by reading the VCF Header. This step can be further extended with extra annotations, defining individuals, creating cohorts or even families.

It is important to note that this step is a synchronous operation that does not upload the genomic data \(e.g:VCFs\) into OpenCGA, instead, the files will only be “linked” \(registered\) with OpenCGA. Therefore, the files to link must be in a location that is accessible by the OpenCGA server \(REST servers and the Master service\).

#### **Catalog Path Structure**

**I**nternally, the Catalog metadata holds a logical tree view of the linked files that can easily be explored or listed. Try using:

```text
$ ./opencga.sh files tree --study <study> --folder <folder>
```

New folders can be created with this command:

```text
$ ./opencga files create --study <study> --path <catalog-logical-path>
```

Being `<catalog-logical-path>` the directory that you’d like to create within catalog.

#### **Linking files synchronously \(less than 5000 samples\)**

{% hint style="warning" %}
Note that for files with more than 5000 samples, the linking step needs to be performed as an asynchronous job using the command described below.
{% endhint %}

Each file needs to be “linked” into OpenCGA using this command line:

```text
$ ./opencga.sh files link --study <study> 
                           --path <catalog-logical-path> 
                           --input </path/to/data>
```

Multiple files can be linked using the same command typing multiple input files separated by space or comma.

#### **Linking files asynchronously \(more than 5000 samples\)**

For VCFs containing more than 5000 samples, the linking steps needs to be performed as an asynchronous job. In this case, a different command needs to be run:

```text
$ ./opencga.sh files link-run --study <study> 
                              --path <catalog-logical-path> 
                              --input </path/to/data>
```

**Full example:**

```text
## Create one folder “data/” in study “myStudy”
$ ./opencga.sh files create --study <owner@project:myStudy> --path <data> 

## Link the files “myFirstFile.vcf.gz” and “mySecondFile.vcf.gz” in the newly created folder
$ ./opencga.sh files link  --study <owner@project:myStudy> 
                           --path <data/> 
                           -i /data/myFirstFile.vcf.gz /data/mySecondFile.vcf.gz
```

### **Variant storage index**

This operation will read the content of the file, run some simple validations to detect possible errors or data corruptions, and ingest the variants into the Hadoop system, building some high performance indexes.

Each file index operation will be run by an asynchronous job, to be executed by the OpenCGA Master service.

Contrary to the Catalog File Register step, only one file should be provided as input in the Variant storage index command line. This will create separate asynchronous indexing jobs for each one of the files. This is important in order to avoid failure of the jobs.

Use this command to launch a variant index job:

```text
$ ./opencga.sh operations variant-index --study <study>
                                 --file <catalog-logical-path>
```

All the jobs along with their current status can be either inspected from IVA, or running this command line:

```text
$ ./opencga.sh jobs top ---study <study>
```

**Special scenarios**

* **Samples data split by chromosome or region**

By default, OpenCGA doesn’t allow you to index a VCF file if any of its samples is already indexed as part of another VCF file. This restriction is to avoid accidental data duplications. In case of having one dataset split by chromosome or region, this restriction can be bypassed by adding the param `--load-split-data <chromosome|region>` to the variant index command line.

* **Multiple files for the same samples**

Similarly to the previous scenario, a dataset may contain multiple files from the same set of samples that may want to be indexed together, for example, when using multiple VCF callers for the same sample. In this case, you can bypass the restriction by adding the param `--load-multi-file-data.`

* **Family or Somatic callers**

When using special callers it is important to specify it in the command line with either`--family / --somatic.`

{% hint style="danger" %}
Note: Be aware that the misuse of this parameters may lead to data corruption.
{% endhint %}

### **Variant Annotation**

Once all the data is loaded, we need to run the Variant Annotation. This is a key enrichment operation that will attach CellBase Variant Annotations with the loaded data, allowing filtering by a large number of fields.

Find more information at**:** [**http://docs.opencb.org/display/cellbase/Variant+Annotation**](http://docs.opencb.org/display/cellbase/Variant+Annotation)

The Variant Storage Engine will run the annotation just for the new variants, being able to reuse the existing annotations to save time and disk usage. This operation is executed at the project level, so shared variants between studies won’t need to be annotated twice.

```text
$ ./opencga.sh operations variant-annotation-index --project<project> 
                                                   --study <study>
```

Similar to the variant-index process, this command line will queue an asynchronous job to be executed by the OpenCGA Master service.

### **Variant Statistics calculation**

The second enrichment operation is the Variant Statistics Calculation. After defining a cohort, you might decide to compute the Variant Stats for that cohort. These statistics include the most typical values like allele and genotype frequencies, MAF, QUAL average, FILTER count...

```text
$ ./opencga.sh operations variant-stats-index --study <study> 
                                  --cohort <coh1>,..,<cohN>
```

For updating the stats of all the cohorts, or when there are no cohorts in the study apart from the default `ALL cohort`:

```text
$ ./opencga.sh operations variant-stats-index --study <study> --cohort ALL
```

#### **Aggregated VCFs**

{% hint style="warning" %}
**This section is under current development.**
{% endhint %}

In case of having computed stats codified in the INFO column of a VCF using standard or non-standard keys, these values can be converted into `VariantStats` models, and be used for filtering.

To extract the statistics, you need to create a mapping file between the INFO keys containing the information, and it’s meaning. Each line will have this format:  
**&lt;COHORT&gt;.&lt;KNOWN\_KEY&gt;=&lt;INFO\_KEY&gt;**

Then, this file needs to be linked in catalog, and referred when computing the stats.

OpenCGA supports 3 different “ways” of codifying the information, known as “aggregation method”. Some of these are named after public studies that started using them. Each one defines a set of known keys that will be used to parse the statistics.

* **BASIC : Using standard vcf-spec INFO keys.**
  * **AN : Total number of alleles in called genotypes**
  * **AC : Total number of alternate alleles in called genotypes**
  * **AF : Allele Frequency, for each ALT allele, in the same order as listed**
* **EXAC**
  * **HET: Count of HET genotypes. For multi allelic variants, the genotype order is 0/1, 0/2, 0/3, 0/4... 1/2, 1/3, 1/4... 2/3, 2/4... 3/4...**
  * **HOM : Count of HOM genotypes. For multi allelic variants, the genotype order is 1/1, 2/2, …**
* **EVS**
  * **GTS: List of Genotypes**
  * **GTC: Genotypes count, ordered according to “GTS”**
  * **GROUPS\_ORDER: Order of cohorts for key “MAF”**
  * **MAF: Minor allele frequency value for each cohort, ordered according to “GROUPS\_ORDER”**

**e.g. Single cohort variant stats  
custom\_mapping.properties**

<table>
  <thead>
    <tr>
      <th style="text-align:left">
        <p><b>ALL.AC =AC</b>
        </p>
        <p><b>ALL.AN =AN</b>
        </p>
        <p><b>ALL.AF =AF</b>
        </p>
        <p><b>ALL.HET=AC_Het</b>
        </p>
        <p><b>ALL.HOM=AC_Hom/2</b>
        </p>
        <p><b>#Key &#x201C;HEMI&#x201D; is not supported</b>
        </p>
        <p><b>#ALL.HEMI=AC_Hemi</b>
        </p>
      </th>
    </tr>
  </thead>
  <tbody></tbody>
</table>

```text
$ ./opencga.sh operations variant-stats-index --study <study> 
                                 --cohort <ALL>
                                 --aggregation EXAC
                                 --aggregation-mapping-file custom_mapping.properties
```

### **Variant Secondary Index Build**

Secondary indexes are built using the search engine Apache Solr for improving the performance of some queries and aggregations, allowing full text search and faceted queries to the Variant database.

This secondary index will include the Variant Annotation and all computed Variant Stats. Therefore, this step needs to be executed only once all annotations and statistics are finished.

```text
$ ./opencga.sh operations variant-secondary-index --project <project> 
                                                  --study <study>
```

## **Enrichment Operations**

This steps are optional operations, that can be indexed to enrich the data displayed at the IVA web application:

### **Sample Variant Stats**

Sample Variant Stats will contain a set of aggregated statistics values for each sample.

```text
$ ./opencga.sh variant sample-stats-run --study <STUDY> --sample all
```

These aggregated values can be computed across all variants from each sample, or using a subset of variants using a variant filter query. e.g:

```text
$ ./opencga.sh variant sample-stats-run --study <STUDY>
                                        --sample all
                                        --variant-query ct=missense_variant
                                        --variant-query biotype=protein_coding
```

By default, this analysis will produce a file, and optionally, the result can be indexed in the catalog metadata store, given an ID.

```text
./opencga.sh variant sample-stats-run --study <STUDY>
                                      --sample all
                                      --index
                                      --index-id missense_variants
                                      --variant-query ct=missense_variant
                                      --variant-query biotype=protein_coding
```

The ID ALL can only be used if without any variant query filter.

```text
$ ./opencga.sh variant sample-stats-run --study <STUDY>
                                        --sample all
                                        --index
                                        --index-id ALL
```

### **Cohort Variant Stats**

{% hint style="warning" %}
**This section is under current development.**
{% endhint %}

### **Family Index**

{% hint style="warning" %}
**This section is under current development.**
{% endhint %}

