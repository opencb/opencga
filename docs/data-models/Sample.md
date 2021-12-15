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
You can find the Java code [here](src/main/java/org/opencb/opencga/core/models/sample/Sample.java).

| Field | Description |
| :---  | :--- |
| **uuid**<br>*String* <br> | <p>Unique 32-character identifier assigned automatically by OpenCGA.</p> |
| **release**<br>*int* <br> | <p>An integer describing the current data release.</p> |
| **version**<br>*int* <br> | <p>Autoincremental version assigned to the registered entry. By default, updates does not create new versions. To enable versioning, users must set the `incVersion` flag from the /update web service when updating the document.</p> |
| **internal**<br>*<a href="Sample.md#SampleInternal"><em>SampleInternal</em></a>* <br><br>_since_: 2.0 | <p>Sample internal information</p> |
| **id**<br>*String* <br> | <p>Sample data model hosts information about any biological material, normally extracted from an _Individual_, that is used for a particular analysis. This is the main data model, it stores the most basic and important information.</p> |
| **source**<br>*<a href="Sample.md#ExternalSource"><em>ExternalSource</em></a>* <br><br>_since_: 2.2 | <p>The external source from where the example was imported</p> |
| **processing**<br>*<a href="Sample.md#SampleProcessing"><em>SampleProcessing</em></a>* <br><br>_since_: 2.0 | <p>Describes how the sample was processed in the lab.</p> |
| **collection**<br>*<a href="Sample.md#SampleCollection"><em>SampleCollection</em></a>* <br><br>_since_: 2.0 | <p>Describes how the sample was collected.</p> |
| **qualityControl**<br>*<a href="Sample.md#SampleQualityControl"><em>SampleQualityControl</em></a>* <br><br>_since_: 2.0 | <p>Contains different metrics to evaluate the quality of the sample.</p> |
| **creationDate**<br>*String* <br><br>_since_: 1.0 | <p>Autogenerated date following the format YYYYMMDDhhmmss containing the date when the entry was first registered.</p> |
| **modificationDate**<br>*String* <br><br>_since_: 1.0 | <p>Autogenerated date following the format YYYYMMDDhhmmss containing the date when the entry was last modified.</p> |
| **description**<br>*String* <br> | <p>Users may provide a description for the entry.</p> |
| **somatic**<br>*boolean* <br> | <p>Describes if the sample is somatic or not </p> |
| **phenotypes**<br>*List<<a href="Sample.md#Phenotype"><em>Phenotype</em></a>>* <br> | <p>List of phenotypes </p> |
| **individualId**<br>*String* <br> | <p>Individual id of the sample</p> |
| **fileIds**<br>*List* <br> | <p>File ids of the sample</p> |
| **cohortIds**<br>*List* <br> | <p>Cohort ids of the sample</p> |
| **status**<br>*<a href="Sample.md#CustomStatus"><em>CustomStatus</em></a>* <br><br>_since_: 2.0 | <p>Cohort ids of the sample</p> |
| **attributes**<br>*Map* <br><br>_since_: 1.0 | <p>Dictionary that can be customised by users to store any additional information users may require..</p> |

### ExternalSource
You can find the Java code [here](src/main/java/org/opencb/opencga/core/models/common/ExternalSource.java).

| Field | Description |
| :---  | :--- |
| **id**<br>*String* <br> | <p>Source ID...</p> |
| **name**<br>*String* <br> | <p>Source name...</p> |
| **description**<br>*String* <br> | <p>Source description...</p> |
| **source**<br>*String* <br> | <p>Source ...</p> |
| **url**<br>*String* <br> | <p>Source ID</p> |

### SampleQualityControl
You can find the Java code [here](src/main/java/org/opencb/opencga/core/models/sample/SampleQualityControl.java).

| Field | Description |
| :---  | :--- |
| **SampleQualityControl.files**<br>*List* <br> | <p>Files used for the quality control of the sample</p> |
| **SampleQualityControl.comments**<br>*List<<a href="Sample.md#ClinicalComment"><em>ClinicalComment</em></a>>* <br> | <p>Comments for the quality control of the sample</p> |
| **SampleQualityControl.variant**<br>*<a href="Sample.md#SampleVariantQualityControlMetrics"><em>SampleVariantQualityControlMetrics</em></a>* <br> | <p>Describes variant quality control</p> |

### SampleVariantQualityControlMetrics
You can find the Java code [here](src/main/java/org/opencb/opencga/core/models/sample/SampleVariantQualityControlMetrics.java).

| Field | Description |
| :---  | :--- |
| **SampleVariantQualityControlMetrics.variantStats**<br>*List<<a href="Sample.md#SampleQcVariantStats"><em>SampleQcVariantStats</em></a>>* <br> | <p>Variant stats for the quality control of the sample</p> |
| **SampleVariantQualityControlMetrics.signatures**<br>*List<<a href="Sample.md#Signature"><em>Signature</em></a>>* <br> | <p>Signature for the quality control of the sample</p> |
| **SampleVariantQualityControlMetrics.genomePlot**<br>*<a href="Sample.md#GenomePlot"><em>GenomePlot</em></a>* <br> | <p>Genome plot for the quality control of the sample</p> |
| **SampleVariantQualityControlMetrics.files**<br>*List* <br> | <p>File for the quality control metrics of the sample</p> |

### GenomePlot
You can find the Java code [here](src/main/java/org/opencb/biodata/models/clinical/qc/GenomePlot.java).


### CustomStatus
You can find the Java code [here](src/main/java/org/opencb/opencga/core/models/common/CustomStatus.java).

| Field | Description |
| :---  | :--- |
| **CustomStatus.name**<br>*String* <br> | <p>Name of the status</p> |
| **CustomStatus.description**<br>*String* <br> | <p>Users may provide a description for the entry.</p> |
| **CustomStatus.date**<br>*String* <br> | <p>Users may provide a description for the entry.</p> |

### SampleProcessing
You can find the Java code [here](src/main/java/org/opencb/opencga/core/models/sample/SampleProcessing.java).

| Field | Description |
| :---  | :--- |
| **SampleProcessing.product**<br>*List<<a href="Sample.md#OntologyTermAnnotation"><em>OntologyTermAnnotation</em></a>>* <br> | <p>Describes which product was used to process the sample in the lab.</p> |
| **SampleProcessing.preparationMethod**<br>*String* <br> | <p>Describes which preparation method was used to process the sample in the lab.</p> |
| **SampleProcessing.preparationMethod**<br>*String* <br> | <p>Describes which extraction method was used to process the samplein the lab.</p> |
| **SampleProcessing.labSampleId**<br>*String* <br> | <p>Original id has the sample in the lab.</p> |
| **SampleProcessing.quantity**<br>*String* <br> | <p>Number of process has done the sample.</p> |
| **SampleProcessing.date**<br>*String* <br> | <p>Date when the sample was processed in the lab.</p> |
| **SampleProcessing.attributes**<br>*Map* <br> | <p>Attributes of the processing.</p> |

### SampleCollection
You can find the Java code [here](src/main/java/org/opencb/opencga/core/models/sample/SampleCollection.java).

| Field | Description |
| :---  | :--- |
| **SampleCollection.from**<br>*List<<a href="Sample.md#OntologyTermAnnotation"><em>OntologyTermAnnotation</em></a>>* <br> | <p>OntologyTermAnnotation list</p> |
| **SampleCollection.type**<br>*String* <br> | <p>Type of the sample collection</p> |
| **SampleCollection.quantity**<br>*String* <br> | <p>Quantity collected for the sample.</p> |
| **SampleCollection.method**<br>*String* <br> | <p>Describes which method was used to collect the sample</p> |
| **SampleCollection.date**<br>*String* <br> | <p>Date when the sample was collected.</p> |
| **SampleCollection.attributes**<br>*Map* <br> | <p>Attributes of the sample collection.</p> |

### SampleInternal
You can find the Java code [here](src/main/java/org/opencb/opencga/core/models/sample/SampleInternal.java).

| Field | Description |
| :---  | :--- |
| **RgaIndex.rga**<br>*<a href="Sample.md#RgaIndex"><em>RgaIndex</em></a>* <br> | <p>Rga index for Sample internal</p> |

### RgaIndex
You can find the Java code [here](src/main/java/org/opencb/opencga/core/models/common/RgaIndex.java).

| Field | Description |
| :---  | :--- |
| **RgaIndex.status**<br>*<a href="Sample.md#Status"><em>Status</em></a>* <br> | <p>Status of the Rga index</p> |
| **RgaIndex.date**<br>*String* <br> | <p>Date of Rga index</p> |
