# Variant Storage Engine

## Overview <a id="VariantStorageEngine-Overview"></a>

OpenCGA Variant Storage Engine provides a centralized solution to storage Genomic Variants. It provides a source of data for analysis and visualization in compatible viewers like [GenomeMaps](http://www.genomemaps.org/). Allowing a fast reading and filtering for variants will speed up analysis, with fastest and more accurate results.

There is an increasing number of biological formats supported by OpenCGA related with a common NGS pipeline. Within these formats, we focus on Genomic Variants due to the complexity and analysis capabilities

## Operations <a id="VariantStorageEngine-Operations"></a>

There is an extensive list of operations that can be executed with the Variant Storage Engine. Their operations are:

* [Sample Genotype Aggregation](http://docs.opencb.org/display/opencga/Sample+Genotype+Aggregation)
* [Variant Annotation](http://docs.opencb.org/display/opencga/Variant+Annotation)
* [Secondary Index](http://docs.opencb.org/display/opencga/Secondary+Index)
* [Export / Import](http://docs.opencb.org/pages/viewpage.action?pageId=15597876)

## Study oriented <a id="VariantStorageEngine-Studyoriented"></a>

The OpenCGA Variant Storage will create an independent database for each project. This database, same way as the projects in Catalog, is divided by studies. This allows distributing the data into independent studies. Allows queries across multiple studies. Reduces the disk space consumption and the required time to generate the variant annotation by using the same variant annotation across the same database.

## VCF types <a id="VariantStorageEngine-VCFtypes"></a>

* **Aggregated VCFs**: Variant files with no sample-specific values. Just aggregated data
* **Merged VCFs**: Variant files with a batch of samples with specific samples data.
* **gVCFs**: Single sample files with information for all the positions.



## Data Model





