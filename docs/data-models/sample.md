# Sample
## Overview
Sample data model hosts information about any biological material, normally extracted from an _Individual_, that is used for a particular analysis. This is the main data model, it stores the most basic and important information.
### Summary 
| Field | Create | Update | Unique | Required|
| :--- | :---: | :---: |:---: |:---: |
| uuid | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| release | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| version | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| internal | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| id | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |
| source | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| processing | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| collection | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| qualityControl | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| creationDate | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| modificationDate | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| description | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| somatic | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| phenotypes | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| individualId | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| fileIds | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| cohortIds | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| status | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| attributes | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |

## Data Model

### Sample
You can find the Java code [here](https://github.com/opencb/opencga/tree/issue-1806/opencga-coresrc/main/java/org/opencb/opencga/core/models/sample/Sample.java).

| Field | Description |
| :---  | :--- |
| **uuid**<br>*String* <br> | <p>Unique 32-character identifier assigned automatically by OpenCGA.</p> |
| **release**<br>*int* <br> | <p>An integer describing the current data release.</p> |
| **version**<br>*int* <br> | <p>Autoincremental version assigned to the registered entry. By default, updates does not create new versions. To enable versioning, users must set the `incVersion` flag from the /update web service when updating the document.</p> |
| **internal**<br>*<a href="sample.md#SampleInternal"><em>SampleInternal</em></a>* <br><br>_since_: 2.0 | <p>Sample internal information</p> |
| **id**<br>*String* <br> | <p>Sample data model hosts information about any biological material, normally extracted from an _Individual_, that is used for a particular analysis. This is the main data model, it stores the most basic and important information.</p> |
| **source**<br>*<a href="sample.md#ExternalSource"><em>ExternalSource</em></a>* <br><br>_since_: 2.2 | <p>The external source from where the example was imported</p> |
| **processing**<br>*<a href="sample.md#SampleProcessing"><em>SampleProcessing</em></a>* <br><br>_since_: 2.0 | <p>Describes how the sample was processed in the lab.</p> |
| **collection**<br>*<a href="sample.md#SampleCollection"><em>SampleCollection</em></a>* <br><br>_since_: 2.0 | <p>Describes how the sample was collected.</p> |
| **qualityControl**<br>*<a href="sample.md#SampleQualityControl"><em>SampleQualityControl</em></a>* <br><br>_since_: 2.0 | <p>Contains different metrics to evaluate the quality of the sample.</p> |
| **creationDate**<br>*String* <br><br>_since_: 1.0 | <p>Autogenerated date following the format YYYYMMDDhhmmss containing the date when the entry was first registered.</p> |
| **modificationDate**<br>*String* <br><br>_since_: 1.0 | <p>Autogenerated date following the format YYYYMMDDhhmmss containing the date when the entry was last modified.</p> |
| **description**<br>*String* <br> | <p>Users may provide a description for the entry.</p> |
| **somatic**<br>*boolean* <br> | <p>Describes if the sample is somatic or not </p> |
| **phenotypes**<br>*List<<a href="sample.md#Phenotype"><em>Phenotype</em></a>>* <br> | <p>List of phenotypes </p> |
| **individualId**<br>*String* <br> | <p>Individual id of the sample</p> |
| **fileIds**<br>*List* <br> | <p>File ids of the sample</p> |
| **cohortIds**<br>*List* <br> | <p>Cohort ids of the sample</p> |
| **status**<br>*<a href="sample.md#CustomStatus"><em>CustomStatus</em></a>* <br><br>_since_: 2.0 | <p>Cohort ids of the sample</p> |
| **attributes**<br>*Map* <br><br>_since_: 1.0 | <p>Dictionary that can be customised by users to store any additional information users may require..</p> |

### ExternalSource
You can find the Java code [here](https://github.com/opencb/opencga/tree/issue-1806/opencga-coresrc/main/java/org/opencb/opencga/core/models/common/ExternalSource.java).

| Field | Description |
| :---  | :--- |
| **id**<br>*String* <br> | <p>Source ID...</p> |
| **name**<br>*String* <br> | <p>Source name...</p> |
| **description**<br>*String* <br> | <p>Source description...</p> |
| **source**<br>*String* <br> | <p>Source ...</p> |
| **url**<br>*String* <br> | <p>Source ID</p> |

### SampleQualityControl
You can find the Java code [here](https://github.com/opencb/opencga/tree/issue-1806/opencga-coresrc/main/java/org/opencb/opencga/core/models/sample/SampleQualityControl.java).

| Field | Description |
| :---  | :--- |
| **SampleQualityControl.files**<br>*List* <br> | <p>Files used for the quality control of the sample</p> |
| **SampleQualityControl.comments**<br>*List<<a href="sample.md#ClinicalComment"><em>ClinicalComment</em></a>>* <br> | <p>Comments for the quality control of the sample</p> |
| **SampleQualityControl.variant**<br>*<a href="sample.md#SampleVariantQualityControlMetrics"><em>SampleVariantQualityControlMetrics</em></a>* <br> | <p>Describes variant quality control</p> |

### ClinicalComment
You can find the Java code [here](https://github.com/opencb/biodata/tree/develop/biodata-models/src/main/java/org/opencb/biodata/models/clinical/ClinicalComment.java).

| Field | Description |
| :---  | :--- |
| **author**<br>*String* <br> | <p>Clinical comment author</p> |
| **message**<br>*String* <br> | <p>Clinical comment message</p> |
| **tags**<br>*List* <br> | <p>List of tags for the clinical comment</p> |
| **date**<br>*String* <br> | <p>Date of the clinical comment</p> |

### SampleVariantQualityControlMetrics
You can find the Java code [here](https://github.com/opencb/opencga/tree/issue-1806/opencga-coresrc/main/java/org/opencb/opencga/core/models/sample/SampleVariantQualityControlMetrics.java).

| Field | Description |
| :---  | :--- |
| **SampleVariantQualityControlMetrics.variantStats**<br>*List<<a href="sample.md#SampleQcVariantStats"><em>SampleQcVariantStats</em></a>>* <br> | <p>Variant stats for the quality control of the sample</p> |
| **SampleVariantQualityControlMetrics.signatures**<br>*List<<a href="sample.md#Signature"><em>Signature</em></a>>* <br> | <p>Signature for the quality control of the sample</p> |
| **SampleVariantQualityControlMetrics.genomePlot**<br>*<a href="sample.md#GenomePlot"><em>GenomePlot</em></a>* <br> | <p>Genome plot for the quality control of the sample</p> |
| **SampleVariantQualityControlMetrics.files**<br>*List* <br> | <p>File for the quality control metrics of the sample</p> |

### SampleQcVariantStats
You can find the Java code [here](https://github.com/opencb/biodata/tree/develop/biodata-models/src/main/java/org/opencb/biodata/models/clinical/qc/SampleQcVariantStats.java).

| Field | Description |
| :---  | :--- |
| **id**<br>*String* <br> | <p>Id to identify the object</p> |
| **description**<br>*String* <br> | <p>Users may provide a description for the entry.</p> |
| **query**<br>*Map* <br> | <p>Map for query</p> |
| **stats**<br>*SampleVariantStats* <br> | <p>Stats result set</p> |

### GenomePlot
You can find the Java code [here](https://github.com/opencb/biodata/tree/develop/biodata-models/src/main/java/org/opencb/biodata/models/clinical/qc/GenomePlot.java).

| Field | Description |
| :---  | :--- |
| **id**<br>*String* <br> | <p>Id to identify the object</p> |
| **description**<br>*String* <br> | <p>Users may provide a description for the entry.</p> |
| **config**<br>*<a href="sample.md#GenomePlotConfig"><em>GenomePlotConfig</em></a>* <br> | <p>Config of the genomePlot</p> |
| **file**<br>*String* <br> | <p>File of the genomePlot</p> |

### GenomePlotConfig
You can find the Java code [here](https://github.com/opencb/biodata/tree/develop/biodata-models/src/main/java/org/opencb/biodata/models/clinical/qc/GenomePlotConfig.java).

| Field | Description |
| :---  | :--- |
| **title**<br>*String* <br> | <p>Title of the genome plot configuration</p> |
| **density**<br>*String* <br> | <p>Density of the genome plot configuration</p> |
| **generalQuery**<br>*Map* <br> | <p>Map for the general query of the genome plot configuration</p> |
| **tracks**<br>*List<<a href="sample.md#GenomePlotTrack"><em>GenomePlotTrack</em></a>>* <br> | <p>List of GenomePlotTrack</p> |

### GenomePlotTrack
You can find the Java code [here](https://github.com/opencb/biodata/tree/develop/biodata-models/src/main/java/org/opencb/biodata/models/clinical/qc/GenomePlotTrack.java).

| Field | Description |
| :---  | :--- |
| **type**<br>*String* <br> | <p>Genome Plot Track Type</p> |
| **description**<br>*String* <br> | <p>Genome Plot Track description</p> |
| **query**<br>*Map* <br> | <p>Genome Plot Track map for query</p> |

### Signature
You can find the Java code [here](https://github.com/opencb/biodata/tree/develop/biodata-models/src/main/java/org/opencb/biodata/models/clinical/qc/Signature.java).

| Field | Description |
| :---  | :--- |
| **id**<br>*String* <br> | <p>Id to identify the object</p> |
| **description**<br>*String* <br> | <p>Users may provide a description for the entry.</p> |
| **query**<br>*ObjectMap* <br> | <p>Map for query</p> |
| **type**<br>*String* <br> | <p>Signature type SNV, INDEL...</p> |
| **counts**<br>*List<<a href="sample.md#GenomeContextCount"><em>GenomeContextCount</em></a>>* <br> | <p>List of GenomeContextCount</p> |
| **files**<br>*List* <br> | <p>List of files of signature</p> |
| **fitting**<br>*<a href="sample.md#SignatureFitting"><em>SignatureFitting</em></a>* <br> | <p>Signature fitting</p> |

### GenomeContextCount
You can find the Java code [here](https://github.com/opencb/biodata/tree/develop/biodata-models/src/main/java/org/opencb/biodata/models/clinical/qc/Signature/GenomeContextCount.java).

| Field | Description |
| :---  | :--- |
| **context**<br>*String* <br> | <p>Genome context to count</p> |
| **total**<br>*int* <br> | <p>Counted integer</p> |

### SignatureFitting
You can find the Java code [here](https://github.com/opencb/biodata/tree/develop/biodata-models/src/main/java/org/opencb/biodata/models/clinical/qc/SignatureFitting.java).

| Field | Description |
| :---  | :--- |
| **method**<br>*String* <br> | <p>Method used to fit the signature</p> |
| **signatureSource**<br>*String* <br> | <p>Source of the fitting signature</p> |
| **signatureVersion**<br>*String* <br> | <p>Signature version of the fitting signature</p> |
| **scores**<br>*List* <br> | <p>Scores of the fitting signature</p> |
| **coeff**<br>*double* <br> | <p>Coefficient of the fitting signature</p> |
| **file**<br>*String* <br> | <p>Files of the fitting signature</p> |

### CustomStatus
You can find the Java code [here](https://github.com/opencb/opencga/tree/issue-1806/opencga-coresrc/main/java/org/opencb/opencga/core/models/common/CustomStatus.java).

| Field | Description |
| :---  | :--- |
| **CustomStatus.name**<br>*String* <br> | <p>Name of the status</p> |
| **CustomStatus.description**<br>*String* <br> | <p>Users may provide a description for the entry.</p> |
| **CustomStatus.date**<br>*String* <br> | <p>Users may provide a description for the entry.</p> |

### SampleInternal
You can find the Java code [here](https://github.com/opencb/opencga/tree/issue-1806/opencga-coresrc/main/java/org/opencb/opencga/core/models/sample/SampleInternal.java).

| Field | Description |
| :---  | :--- |
| **RgaIndex.rga**<br>*<a href="sample.md#RgaIndex"><em>RgaIndex</em></a>* <br> | <p>Rga index for Sample internal</p> |

### RgaIndex
You can find the Java code [here](https://github.com/opencb/opencga/tree/issue-1806/opencga-coresrc/main/java/org/opencb/opencga/core/models/common/RgaIndex.java).

| Field | Description |
| :---  | :--- |
| **RgaIndex.status**<br>*Status* <br> | <p>Status of the Rga index NOT_INDEXED, INDEXED, INVALID_PERMISSIONS, INVALID_METADATA, INVALID</p> |
| **RgaIndex.date**<br>*String* <br> | <p>Date of Rga index</p> |

### SampleProcessing
You can find the Java code [here](https://github.com/opencb/opencga/tree/issue-1806/opencga-coresrc/main/java/org/opencb/opencga/core/models/sample/SampleProcessing.java).

| Field | Description |
| :---  | :--- |
| **SampleProcessing.product**<br>*List<<a href="sample.md#OntologyTermAnnotation"><em>OntologyTermAnnotation</em></a>>* <br> | <p>Describes which product was used to process the sample in the lab.</p> |
| **SampleProcessing.preparationMethod**<br>*String* <br> | <p>Describes which preparation method was used to process the sample in the lab.</p> |
| **SampleProcessing.preparationMethod**<br>*String* <br> | <p>Describes which extraction method was used to process the samplein the lab.</p> |
| **SampleProcessing.labSampleId**<br>*String* <br> | <p>Original id has the sample in the lab.</p> |
| **SampleProcessing.quantity**<br>*String* <br> | <p>Number of process has done the sample.</p> |
| **SampleProcessing.date**<br>*String* <br> | <p>Date when the sample was processed in the lab.</p> |
| **SampleProcessing.attributes**<br>*Map* <br> | <p>Attributes of the processing.</p> |

### OntologyTermAnnotation
You can find the Java code [here](https://github.com/opencb/biodata/tree/develop/biodata-models/src/main/java/org/opencb/biodata/models/core/OntologyTermAnnotation.java).

| Field | Description |
| :---  | :--- |
| **id**<br>*String* <br> | <p>Id to identify the object</p> |
| **name**<br>*String* <br> | <p>Object name</p> |
| **description**<br>*String* <br> | <p>Users may provide a description for the entry.</p> |
| **description**<br>*String* <br> | <p>Ontology source</p> |
| **description**<br>*String* <br> | <p>Ontology url</p> |
| **attributes**<br>*Map* <br> | <p>Dictionary that can be customised by users to store any additional information users may require..</p> |

### Phenotype
You can find the Java code [here](https://github.com/opencb/biodata/tree/develop/biodata-models/src/main/java/org/opencb/biodata/models/clinical/Phenotype.java).

| Field | Description |
| :---  | :--- |
| **ageOfOnset**<br>*String* <br> | <p>Indicates the age of on set of the phenotype</p> |
| **status**<br>*Status* <br> | <p>Status of phenotype OBSERVED, NOT_OBSERVED, UNKNOWN</p> |

### SampleCollection
You can find the Java code [here](https://github.com/opencb/opencga/tree/issue-1806/opencga-coresrc/main/java/org/opencb/opencga/core/models/sample/SampleCollection.java).

| Field | Description |
| :---  | :--- |
| **SampleCollection.from**<br>*List<<a href="sample.md#OntologyTermAnnotation"><em>OntologyTermAnnotation</em></a>>* <br> | <p>OntologyTermAnnotation list</p> |
| **SampleCollection.type**<br>*String* <br> | <p>Type of the sample collection</p> |
| **SampleCollection.quantity**<br>*String* <br> | <p>Quantity collected for the sample.</p> |
| **SampleCollection.method**<br>*String* <br> | <p>Describes which method was used to collect the sample</p> |
| **SampleCollection.date**<br>*String* <br> | <p>Date when the sample was collected.</p> |
| **SampleCollection.attributes**<br>*Map* <br> | <p>Attributes of the sample collection.</p> |

### OntologyTermAnnotation
You can find the Java code [here](https://github.com/opencb/biodata/tree/develop/biodata-models/src/main/java/org/opencb/biodata/models/core/OntologyTermAnnotation.java).

| Field | Description |
| :---  | :--- |
| **id**<br>*String* <br> | <p>Id to identify the object</p> |
| **name**<br>*String* <br> | <p>Object name</p> |
| **description**<br>*String* <br> | <p>Users may provide a description for the entry.</p> |
| **description**<br>*String* <br> | <p>Ontology source</p> |
| **description**<br>*String* <br> | <p>Ontology url</p> |
| **attributes**<br>*Map* <br> | <p>Dictionary that can be customised by users to store any additional information users may require..</p> |
## Example
This is a full JSON example:
```javascript
{
  id: "ISDBM322015",
  uuid: "eba13afe-0172-0004-0001-d4c92fd95e0a",
  individualId: "ISDBM322015",
  fileIds: [
    "data:quartet.variants.annotated.vcf.gz",
    "SonsAlignedBamFile.bam"
  ],
  annotationSets: [],
  description: "",
  somatic: false,
  qualityControl: {
    fileIds: [],
    comments: [],
    alignmentMetrics: [
      {
        bamFileId: SonsAlignedBamFile.bam,
        fastQc: {13 items},
        samtoolsFlagstats: {14 items},
        geneCoverageStats: [2 items]
      }
    ],
    variantMetrics: {
      variantStats: [1 item],
      signatures: [],
      vcfFileIds: []
    }
  },
  release: 1,
  version: 5,
  creationDate: "20200625131831",
  modificationDate: "20200709003738",
  phenotypes: [
    {
      id: "HP:0000545",
      name: "Myopia",
      source: "HPO"
    }
  ],
  status: {
    name: "",
    description: "",
    date: ""
  },
  internal: {
    status: {
      name: "READY",
      date: "20200625131831",
      description: ""
    }
  },
  attributes: {
    OPENCGA_INDIVIDUAL: {
      id: "ISDBM322015",
      name: "ISDBM322015",
      uuid: "eba13738-0172-0006-0001-283471b7ae69",
      father: {4 items},
      mother: {4 items},
      location: {},
      qualityControl: {4 items},
      sex: "MALE",
      karyotypicSex: "XY",
      ethnicity: "",
      population: {},
      release: 1,
      version: 6,
      creationDate: "20200625131830",
      modificationDate: "20201027004616",
      lifeStatus: "ALIVE",
      phenotypes: [2 items],
      disorders: [1 item],
      parentalConsanguinity: false,
      status: {3 items},
      internal: {1 item},
      attributes: {}
    }
  }
}

```