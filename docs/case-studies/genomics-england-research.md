# Genomics England Research

## Background

One of the goals of [The 100,000 Genomes Project](https://www.genomicsengland.co.uk/about-genomics-england/the-100000-genomes-project/) from [Genomics England](https://www.genomicsengland.co.uk) is to enable new medical research. Researchers will study how best to use genomics in healthcare and how best to interpret the data to help patients. The causes, diagnosis and treatment of disease will also be investigated. This is currently the largest national sequencing project of its kind in the world.

To achieve this goal Genomics England set up a _Research_ environment for researchers and clinicians. OpenCGA, CellBase and IVA from OpenCB were installed as data platform. We loaded **64,078 whole genomes** in OpenCGA, in total **about 1 billion unique variants** were loaded and indexed in [OpenCGA Variant Storage](http://docs.opencb.org/display/opencga/Variant+Storage+Engine), and all the metadata and clinical data for samples and patients were loaded in [OpenCGA Catalog](http://docs.opencb.org/display/opencga/Catalog+and+Security). **OpenCGA was able to load and index about 6,000 samples a day**, executing the variant annotation and computing different cohort stats for the all the data run in less than a week. In summary, all data was loaded, indexed, annotated and stats calculated in less than 2 weeks. Genomic variants were annotated using [CellBase](http://docs.opencb.org/display/cellbase/CellBase+Home) and the [IVA](http://docs.opencb.org/display/iva/Welcome+to+IVA) front-end was installed for researchers and clinicians to analyse and visualise the data. In this document you can find a full report of about the loading and analysis of the 64,078 genomes.

## Genomic and Clinical Data

Clinical data and genomic variants of **64,078 genomes** were loaded and indexed in OpenCGA. In total we loaded more than **30,000 VCF** files accounting for about 40TB of compressed disk space. Data was organised in four different datasets depending on the genome assembly _(GRCh37_ or _GRCh38)_ and the type of study _(germline_ or _somatic)_, and this was organised in OpenCGA in three different _Projects_ and four _Studies:_

| Project             | Study ID and Name                                         | Samples | VCF Files | VCF File Type | Samples/File  | Variants        |
| ------------------- | --------------------------------------------------------- | ------- | --------- | ------------- | ------------- | --------------- |
| **GRCh37 Germline** | <p><strong>RD37</strong></p><p>Rare Disease GRCh37</p>    | 12,142  | 5,329     | Multi sample  | 2.28          | **298,763,059** |
| **GRCh38 Germline** | <p><strong>RD38</strong></p><p>Rare Disease GRCh38</p>    | 33,180  | 16,591    | Multi sample  | 2.00          | **437,740,498** |
| **GRCh38 Germline** | <p><strong>CG38</strong></p><p>Cancer Germline GRCh38</p> | 9,167   | 9,167     | Single sample | 1.00          | **286,136,051** |
| **GRCh38 Somatic**  | <p><strong>CS38</strong></p><p>Cancer Somatic GRCh38</p>  | 9,589   | 9,589     | Somatic       | 1.00          | **398,402,166** |
| **Total**           | 64,078                                                    | 40,676  |           |               | 1,421,041,774 |                 |

[OpenCGA Catalog](http://docs.opencb.org/display/opencga/Catalog+and+Security) stores all the metadata and clinical data of **files, samples**, **individuals** and **cohorts**. Rare Disease studies also include pedigree metadata by defining **families**. Also, a **Clinical Analysis** were defined for each family. Several [Variable Sets](http://docs.opencb.org/display/opencga/AnnotationSets+1.4.0) have been defined to store GEL custom data for all these entities.

## Platform

For the Research environment we have used **OpenCGA v1.4** using the new Hadoop Variant Storage that use [**Apache HBase**](https://hbase.apache.org) as back-end because of the huge amount of data and analysis needed. We have also used [**CellBase**](http://docs.opencb.org/display/cellbase/CellBase+Home)** v4.6** for the variant annotation. Finally we set up a [**IVA**](http://docs.opencb.org/display/iva/Welcome+to+IVA)** v1.0** web-based variant analysis tool.

The **Hadoop cluster** consists of about 30 nodes running [**Hortonworks**](https://hortonworks.com)** HDP 2.6.5** (which comes with **HBase 1.1.2**) and a LSF queue for loading all the VCF files, see this table for more detail:

| Node                  | Nodes | Cores | Memory (GB) | Storage (TB)   |
| --------------------- | ----- | ----- | ----------- | -------------- |
| **Hadoop Master**     | 5     | 28    | 216         | 7.2 (6x1.2)    |
| **Hadoop Worker**     | 30    | 28    | 216         | 7.2 (6x1.2)    |
| **LSF Loading Queue** | 10    | 12    | 364         | Isilon storage |

## Genomic Data Load

In order to improve the **loading performance,** we set up a small LSF queue of ten computing nodes. This configuration allowed us to load multiple files at the same time. We configured LSF to load up to 6 VCF files per node resulting in 60 files being loaded in HBase in parallel without any incidence, by doing this we observed a **50x in loading throughput**. This resulted in an average of 125 VCF files loaded per hour in studies RD37 and RD38, which is about **2 files per minute**. In the study CG38 the performance was 240 VCF files per hour or about **4 files per minute**.

### Rare Disease Loading Performance

The files from Rare Disease studies (RD38 & RD37) contain 2 samples per file on average. This results in larger files, increasing the loading time compared with single-sample files. As mentioned above the loading performance was about 125 files per hour or 3,000 files per day. In terms of number of samples it is about **250 samples per hour or 6,000 samples a day**.

The loading performance always depend on the number of variants and concurrent files being loaded, the performance was quite stable during the load and performance degradation was observed as can be seen here:

![](http://docs.opencb.org/download/attachments/15598206/image2019-4-16\_16-9-3.png?version=1\&modificationDate=1555423744015\&api=v2\&effects=border-simple,blur-border)

| Concurrent files loaded       | 60       |
| ----------------------------- | -------- |
| Average files loaded per hour | 125.72   |
| Load time per file            | 00:28:38 |

#### Saturation Study

As part of the data loading process we decided to study the number of unique variants added in each batch of 500 samples. We generated this saturation plot for RD38:  

![](http://docs.opencb.org/download/attachments/15598206/image.png?version=1\&modificationDate=1560250247113\&api=v2)

### Cancer Loading Performance

The files from Cancer Germline studies (CG38) contain one sample per file. Compared with the Rare Disease, these files are smaller in size, therefore, as expected the file load was almost 2x faster. As mentioned above, the loading performance was about 240 genomes per hour or 5,800 files per day. In terms of number of samples it is about **5,800 samples a day**, which is consistent with Rare Disease performance.

![](http://docs.opencb.org/download/attachments/15598206/image2019-4-16\_16-9-10.png?version=1\&modificationDate=1555423750111\&api=v2\&effects=border-simple,blur-border)

| Concurrent files loaded       | 60       |
| ----------------------------- | -------- |
| Average files loaded per hour | 242.05   |
| Load time per file            | 00:14:52 |

## Analysis Benchmark

In this section you can find information about the performance of main variant storage operations and most common queries and clinical analysis. Please, for data loading performance information go to section **Genomic Data Load** above.

### Variant Storage Operations

Variant Storage operations take care of preparing the data for executing queries and analysis. There are two main operations: **Variant Annotation** and **Cohort Stats Calculation**.

#### Variant Annotation

This operation uses the [**CellBase**](http://docs.opencb.org/display/cellbase/CellBase+Home) to annotate each unique variant in the database, this annotation include consequence types, population frequencies, conservation scores clinical info, ... and will be typically used for variant queries and clinical analysis. Variant annotation of the 585 million unique variants of project** GRCh38 Germline** took about 3 days, **about 200 million variants were annotated per day**.

#### Cohort Stats Calculation

Cohort Stats are used for filtering variants in a similar way as the population frequencies. A set of cohorts were defined in each study.

* **ALL** with all samples in the study
* **PARENTS **with all parents in the study (only for Rare Disease studies)
* **UNAFF_PARENTS** with all unaffected parents in the study (only for Rare Disease studies)

Pre-computing stats for different cohort and ten of thousands of samples is a high-performance operation that run in **less than 2 hours** for each study.

### Query and Aggregation Stats

To study the performance we used **RD38** which the largest study with 438 million variants and 33,000 samples. We first run some queries to the aggregated data filtering by variant annotation and cohort stats. We were interested in the different index performance so we limit the results to be returned the first 10 variants excluding the genotypic data of the 33,000 samples, by doing this we remove the effect of reading from disk or transferring data through the network which is very variable across different clusters. For queries using patient data go to the next section. Here you can find some of the common queries executed.

| Filters                                                                                                                                               | Results | Total Results | Time (sec) |
| ----------------------------------------------------------------------------------------------------------------------------------------------------- | ------- | ------------- | ---------- |
| **consequence type** = LoF + missense_variant                                                                                                         | 10      | 3704626       | 0.189      |
| <p><strong>consequence type</strong> = LoF + missense_variant</p><p><strong>biotype</strong> = protein_coding</p>                                     | 10      | 3576472       | 0.260      |
| **panel** = with 200 genes                                                                                                                            | 10      | 3882902       | 0.299      |
| **gene** = BMPR2                                                                                                                                      | 10      | 37244         | 0.344      |
| <p><strong>gene</strong> = BMPR2</p><p><strong>consequence type</strong> = LoF</p>                                                                    | 10      | 189           | 0.443      |
| **type** = INDEL                                                                                                                                      | 10      | 79597426      | 0.802      |
| <p><strong>type</strong> = INDEL</p><p><strong>biotype</strong> = protein_coding</p>                                                                  | 10      | 38799454      | 0.358      |
| <p><strong>type</strong> = INDEL</p><p><strong>biotype</strong> = protein_coding</p><p><strong>consequence type</strong> = LoF + missense_variant</p> | 10      | 240443        | 0.556      |
| <p><strong>consequence type</strong> = LoF + missense_variant</p><p><strong>population frequency</strong> = 1000G ALL &#x3C; 0.005</p>                | 10      | 3157533       | 5.96       |

As can be observed most queries run below 1 second, you can combine as many filters as wanted. 

### Clinical Analysis

We also use here **RD38** which is the largest study. Clinical queries, or sample queries, enforces queries to return variants of a specific set of samples. These queries can use all the filters from the general queries. The result here also includes a **pathogenic prediction** for each variant, which determines possible conditions associated to the variant.

| Filters                                                                                                                                                                                                      | Results | Total Results | Time (sec) |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------- | ------------- | ---------- |
| <p><strong>mode of inheritance</strong> = recessive</p><p><strong>filter</strong> = PASS</p>                                                                                                                 | 10      | 211787        | 0.420      |
| <p><strong>mode of inheritance</strong> = recessive</p><p><strong>consequence type</strong> = LoF + missense_variant</p>                                                                                     | 10      | 710           | 1.95       |
| <p><strong>mode of inheritance</strong> = recessive</p><p><strong>consequence type</strong> = LoF + missense_variant</p><p><strong>filter</strong> = PASS</p>                                                | 10      | 656           | 1.92       |
| <p><strong>mode of inheritance</strong> = recessive</p><p><strong>consequence type</strong> = LoF + missense_variant</p><p><strong>filter</strong> = PASS</p><p><strong>panel</strong> = with 58 genes</p>   | 10      | 7             | 2.11       |
| <p><em><strong>de novo</strong></em><strong> Analysis</strong></p><p><strong>filter</strong> = PASS</p><p><strong>consequence type</strong> = LoF + missense_variant</p>                                     | 24      | 24            | 0.680      |
| <p><strong>Compound Heterozygous Analysis</strong></p><p><strong>filter</strong> = PASS</p><p><strong>biotype</strong> = protein_coding</p><p><strong>consequence type</strong> = LoF + missense_variant</p> | 417     | 417           | 10.930     |

As it can be observed most of the family clinical analysis run in less than 2 seconds in the largest study with 33,000 samples.

## User Interfaces

Several user interfaces have been developed to query and analyse data from OpenCGA: IVA web-based tool, Python and R clients, and a command line.

### IVA

[IVA](http://docs.opencb.org/display/iva/Welcome+to+IVA) v1.0.3 was installed to provide a friendly web-based analysis tool to browse variants and execute clinical analysis.   

![](http://docs.opencb.org/download/attachments/15598206/GEL_IVA_RD38\_grid.png?version=1\&modificationDate=1560259726188\&api=v2)

### Command line

You can also query variants efficiently using the command line built in. Performance depends on the number of samples fetched and the RPC used _(REST_ or _gRPC),_ in the best scenario you can fetch few thousands variants per second. You can see a simple example here producing a VCF file:

|   |
| - |

## Support

OpenCB team is setting up **Zetta Genomics**, a start-up to offer support, consultancy and custom feature development. We have partnered with Microsoft Azure to ensure OpenCB Suite runs efficiently in **Microsoft Azure** cloud. We are running a proof-of-concept at the moment with GEL data to benchmark and test Azure.

## Acknowledgements

We would like to thank Genomics England very much for their support and for trusting in OpenCGA and the rest of OpenCB Suite for this amazing release. In particular, we would like to thank Augusto Rendon, Anna Need, Carolyn Tregidgo, Frank Nankivell and Chris Odhams for their support, test and valuable feedback.
