# Sample
## Overview
Sample data model hosts information about any biological material, normally extracted from an _Individual_, that is used for a particular analysis. This is the main data model, it stores the most basic and important information.
### Summary 
| Field | Create | Update | Unique | Required|
| :--- | :---: | :---: |:---: |:---: |
| id | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |
| uuid | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
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
| release | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| version | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| internal | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |

## Data Model

### Sample
<<<<<<< HEAD
You can find the Java code [here](https://github.com/opencb/opencga/tree/TASK-521/opencga-core/src/main/java/org/opencb/opencga/core/models/sample/Sample.java).
=======
You can find the Java code [here](https://github.com/opencb/opencga/tree/release-2.2.x/opencga-core/src/main/java/org/opencb/opencga/core/models/sample/Sample.java).
>>>>>>> release-2.2.x

| Field | Description |
| :---  | :--- |
| **id**<br>*String* <br> | <p>Sample data model hosts information about any biological material, normally extracted from an _Individual_, that is used for a particular analysis. This is the main data model, it stores the most basic and important information.</p> |
| **uuid**<br>*String* <br> | <p>Unique 32-character identifier assigned automatically by OpenCGA.</p> |
| **source**<br>*[ExternalSource](https://docs.opencga.opencb.org/data-models/sample#externalsource)* <br><br>_since_: 2.2 | <p>The external source from where the example was imported.</p> |
| **processing**<br>*[SampleProcessing](https://docs.opencga.opencb.org/data-models/sample#sampleprocessing)* <br><br>_since_: 2.0 | <p>Describes how the sample was processed in the lab.</p> |
| **collection**<br>*[SampleCollection](https://docs.opencga.opencb.org/data-models/sample#samplecollection)* <br><br>_since_: 2.0 | <p>Describes how the sample was collected.</p> |
| **qualityControl**<br>*[SampleQualityControl](https://docs.opencga.opencb.org/data-models/sample#samplequalitycontrol)* <br><br>_since_: 2.0 | <p>Contains different metrics to evaluate the quality of the sample.</p> |
| **creationDate**<br>*String* <br><br>_since_: 1.0 | <p>Autogenerated date following the format YYYYMMDDhhmmss containing the date when the entry was first registered.</p> |
| **modificationDate**<br>*String* <br><br>_since_: 1.0 | <p>Autogenerated date following the format YYYYMMDDhhmmss containing the date when the entry was last modified.</p> |
| **description**<br>*String* <br> | <p>Users may provide a description for the entry.</p> |
| **somatic**<br>*boolean* <br> | <p>Describes if the sample is somatic or not .</p> |
| **phenotypes**<br>*List<<a href="https://docs.opencga.opencb.org/data-models/sample#phenotype"><em>Phenotype</em></a>>* <br> | <p>List of phenotypes .</p> |
| **individualId**<br>*String* <br> | <p>Individual id of the sample.</p> |
| **fileIds**<br>*List<<em>String</em>>* <br> | <p>File ids of the sample.</p> |
| **cohortIds**<br>*List<<em>String</em>>* <br> | <p>Cohort ids of the sample.</p> |
| **status**<br>*[Status](https://docs.opencga.opencb.org/data-models/sample#status)* <br><br>_since_: 2.0 | <p>Cohort ids of the sample.</p> |
| **attributes**<br>*Map<String,Object>* <br><br>_since_: 1.0 | <p>You can use this field to store any other information, keep in mind this is not indexed so you cannot search by attributes.</p> |
| **release**<br>*int* <br> | <p>An integer describing the current data release.</p> |
| **version**<br>*int* <br> | <p>Autoincremental version assigned to the registered entry. By default, updates does not create new versions. To enable versioning, users must set the `incVersion` flag from the /update web service when updating the document.</p> |
| **internal**<br>*[SampleInternal](https://docs.opencga.opencb.org/data-models/sample#sampleinternal)* <br><br>_since_: 2.0 | <p>Sample internal information.</p> |

<<<<<<< HEAD
### SampleInternal
You can find the Java code [here](https://github.com/opencb/opencga/tree/TASK-521/opencga-core/src/main/java/org/opencb/opencga/core/models/sample/SampleInternal.java).

| Field | Description |
| :---  | :--- |
| **RgaIndex.rga**<br>*[RgaIndex](https://docs.opencga.opencb.org/data-models/sample#rgaindex)* <br> | <p>Rga index for Sample internal.</p> |
| **status**<br>*[InternalStatus](https://docs.opencga.opencb.org/data-models/sample#internalstatus)* <br> | <p>Status of the internal object.</p> |
| **registrationDate**<br>*String* <br> | <p>Registration date of the internal object.</p> |
| **lastModified**<br>*String* <br> | <p>Date of the last modification of the internal object.</p> |

### SampleCollection
You can find the Java code [here](https://github.com/opencb/opencga/tree/TASK-521/opencga-core/src/main/java/org/opencb/opencga/core/models/sample/SampleCollection.java).
=======
### SampleProcessing
You can find the Java code [here](https://github.com/opencb/opencga/tree/release-2.2.x/opencga-core/src/main/java/org/opencb/opencga/core/models/sample/SampleProcessing.java).

| Field | Description |
| :---  | :--- |
| **product**<br>*[OntologyTermAnnotation](https://docs.opencga.opencb.org/data-models/sample#ontologytermannotation)* <br> | <p>Describes which product was used to process the sample in the lab.</p> |
| **preparationMethod**<br>*String* <br> | <p>Describes which preparation method was used to process the sample in the lab.</p> |
| **preparationMethod**<br>*String* <br> | <p>Describes which extraction method was used to process the samplein the lab.</p> |
| **labSampleId**<br>*String* <br> | <p>Original id has the sample in the lab.</p> |
| **quantity**<br>*String* <br> | <p>Number of process has done the sample.</p> |
| **date**<br>*String* <br> | <p>Date when the sample was processed in the lab.</p> |
| **attributes**<br>*Map<String,Object>* <br> | <p>Attributes of the processing.</p> |

### SampleQualityControl
You can find the Java code [here](https://github.com/opencb/opencga/tree/release-2.2.x/opencga-core/src/main/java/org/opencb/opencga/core/models/sample/SampleQualityControl.java).

| Field | Description |
| :---  | :--- |
| **files**<br>*List<<em>String</em>>* <br> | <p>Files used for the quality control of the sample.</p> |
| **comments**<br>*List<<a href="https://docs.opencga.opencb.org/data-models/sample#clinicalcomment"><em>ClinicalComment</em></a>>* <br> | <p>Comments for the quality control of the sample.</p> |
| **variant**<br>*[SampleVariantQualityControlMetrics](https://docs.opencga.opencb.org/data-models/sample#samplevariantqualitycontrolmetrics)* <br> | <p>Describes variant quality control.</p> |

### ExternalSource
You can find the Java code [here](https://github.com/opencb/opencga/tree/release-2.2.x/opencga-core/src/main/java/org/opencb/opencga/core/models/common/ExternalSource.java).

| Field | Description |
| :---  | :--- |
| **id**<br>*String* <br> | <p>Source ID...</p> |
| **name**<br>*String* <br> | <p>Source name...</p> |
| **description**<br>*String* <br> | <p>Source description...</p> |
| **source**<br>*String* <br> | <p>Source ...</p> |
| **url**<br>*String* <br> | <p>Source ID</p> |

### SampleCollection
You can find the Java code [here](https://github.com/opencb/opencga/tree/release-2.2.x/opencga-core/src/main/java/org/opencb/opencga/core/models/sample/SampleCollection.java).
>>>>>>> release-2.2.x

| Field | Description |
| :---  | :--- |
| **from**<br>*List<<a href="https://docs.opencga.opencb.org/data-models/sample#ontologytermannotation"><em>OntologyTermAnnotation</em></a>>* <br> | <p>OntologyTermAnnotation list.</p> |
| **type**<br>*String* <br> | <p>Type of the sample collection.</p> |
| **quantity**<br>*String* <br> | <p>Quantity collected for the sample.</p> |
| **method**<br>*String* <br> | <p>Describes which method was used to collect the sample.</p> |
| **date**<br>*String* <br> | <p>Date when the sample was collected.</p> |
| **attributes**<br>*Map<String,Object>* <br> | <p>Attributes of the sample collection.</p> |

<<<<<<< HEAD
### SampleProcessing
You can find the Java code [here](https://github.com/opencb/opencga/tree/TASK-521/opencga-core/src/main/java/org/opencb/opencga/core/models/sample/SampleProcessing.java).

| Field | Description |
| :---  | :--- |
| **product**<br>*[OntologyTermAnnotation](https://docs.opencga.opencb.org/data-models/sample#ontologytermannotation)* <br> | <p>Describes which product was used to process the sample in the lab.</p> |
| **preparationMethod**<br>*String* <br> | <p>Describes which preparation method was used to process the sample in the lab.</p> |
| **preparationMethod**<br>*String* <br> | <p>Describes which extraction method was used to process the samplein the lab.</p> |
| **labSampleId**<br>*String* <br> | <p>Original id has the sample in the lab.</p> |
| **quantity**<br>*String* <br> | <p>Number of process has done the sample.</p> |
| **date**<br>*String* <br> | <p>Date when the sample was processed in the lab.</p> |
| **attributes**<br>*Map<String,Object>* <br> | <p>Attributes of the processing.</p> |

=======
>>>>>>> release-2.2.x
### Status
You can find the Java code [here](https://github.com/opencb/biodata/tree/develop/biodata-models/src/main/java/org/opencb/biodata/models/common/Status.java).


<<<<<<< HEAD
### ExternalSource
You can find the Java code [here](https://github.com/opencb/opencga/tree/TASK-521/opencga-core/src/main/java/org/opencb/opencga/core/models/common/ExternalSource.java).
=======
### SampleInternal
You can find the Java code [here](https://github.com/opencb/opencga/tree/release-2.2.x/opencga-core/src/main/java/org/opencb/opencga/core/models/sample/SampleInternal.java).
>>>>>>> release-2.2.x

| Field | Description |
| :---  | :--- |
| **RgaIndex.rga**<br>*[RgaIndex](https://docs.opencga.opencb.org/data-models/sample#rgaindex)* <br> | <p>Rga index for Sample internal.</p> |
| **status**<br>*[InternalStatus](https://docs.opencga.opencb.org/data-models/sample#internalstatus)* <br> | <p>Status of the internal object.</p> |
| **registrationDate**<br>*String* <br> | <p>Registration date of the internal object.</p> |
| **lastModified**<br>*String* <br> | <p>Date of the last modification of the internal object.</p> |

### Phenotype
You can find the Java code [here](https://github.com/opencb/biodata/tree/develop/biodata-models/src/main/java/org/opencb/biodata/models/clinical/Phenotype.java).

| Field | Description |
| :---  | :--- |
| **id**<br>*String* <br> | <p>Id to identify the object</p> |
| **ageOfOnset**<br>*String* <br> | <p>Indicates the age of on set of the phenotype</p> |
| **status**<br>*Status* <br> | <p>Status of phenotype OBSERVED, NOT_OBSERVED, UNKNOWN</p> |
| **name**<br>*String* <br> | <p>Object name</p> |
| **description**<br>*String* <br> | <p>Users may provide a description for the entry.</p> |
| **source**<br>*String* <br> | <p>Ontology source</p> |
| **url**<br>*String* <br> | <p>Ontology url</p> |
| **attributes**<br>*Map<String,String>* <br> | <p>Dictionary that can be customised by users to store any additional information users may require..</p> |

<<<<<<< HEAD
### SampleQualityControl
You can find the Java code [here](https://github.com/opencb/opencga/tree/TASK-521/opencga-core/src/main/java/org/opencb/opencga/core/models/sample/SampleQualityControl.java).

| Field | Description |
| :---  | :--- |
| **files**<br>*List<<em>String</em>>* <br> | <p>Files used for the quality control of the sample.</p> |
| **comments**<br>*List<<a href="https://docs.opencga.opencb.org/data-models/sample#clinicalcomment"><em>ClinicalComment</em></a>>* <br> | <p>Comments for the quality control of the sample.</p> |
| **variant**<br>*[SampleVariantQualityControlMetrics](https://docs.opencga.opencb.org/data-models/sample#samplevariantqualitycontrolmetrics)* <br> | <p>Describes variant quality control.</p> |

### OntologyTermAnnotation
You can find the Java code [here](https://github.com/opencb/biodata/tree/develop/biodata-models/src/main/java/org/opencb/biodata/models/core/OntologyTermAnnotation.java).

| Field | Description |
| :---  | :--- |
| **id**<br>*String* <br> | <p>Id to identify the object</p> |
| **name**<br>*String* <br> | <p>Object name</p> |
| **description**<br>*String* <br> | <p>Users may provide a description for the entry.</p> |
| **source**<br>*String* <br> | <p>Ontology source</p> |
| **url**<br>*String* <br> | <p>Ontology url</p> |
| **attributes**<br>*Map<String,String>* <br> | <p>Dictionary that can be customised by users to store any additional information users may require..</p> |

### InternalStatus
You can find the Java code [here](https://github.com/opencb/opencga/tree/TASK-521/opencga-core/src/main/java/org/opencb/opencga/core/models/common/InternalStatus.java).


### RgaIndex
You can find the Java code [here](https://github.com/opencb/opencga/tree/TASK-521/opencga-core/src/main/java/org/opencb/opencga/core/models/common/RgaIndex.java).
=======
### RgaIndex
You can find the Java code [here](https://github.com/opencb/opencga/tree/release-2.2.x/opencga-core/src/main/java/org/opencb/opencga/core/models/common/RgaIndex.java).
>>>>>>> release-2.2.x

| Field | Description |
| :---  | :--- |
| **status**<br>*Status* <br> | <p>Status of the Rga index NOT_INDEXED, INDEXED, INVALID_PERMISSIONS, INVALID_METADATA, INVALID.</p> |
| **date**<br>*String* <br> | <p>Date of Rga index.</p> |

<<<<<<< HEAD
### ClinicalComment
You can find the Java code [here](https://github.com/opencb/biodata/tree/develop/biodata-models/src/main/java/org/opencb/biodata/models/clinical/ClinicalComment.java).

| Field | Description |
| :---  | :--- |
| **author**<br>*String* <br> | <p>Clinical comment author</p> |
| **message**<br>*String* <br> | <p>Clinical comment message</p> |
| **tags**<br>*List<<em>String</em>>* <br> | <p>List of tags for the clinical comment</p> |
| **date**<br>*String* <br> | <p>Date of the clinical comment</p> |

### SampleVariantQualityControlMetrics
You can find the Java code [here](https://github.com/opencb/opencga/tree/TASK-521/opencga-core/src/main/java/org/opencb/opencga/core/models/sample/SampleVariantQualityControlMetrics.java).
=======
### OntologyTermAnnotation
You can find the Java code [here](https://github.com/opencb/biodata/tree/develop/biodata-models/src/main/java/org/opencb/biodata/models/core/OntologyTermAnnotation.java).

| Field | Description |
| :---  | :--- |
| **id**<br>*String* <br> | <p>Id to identify the object</p> |
| **name**<br>*String* <br> | <p>Object name</p> |
| **description**<br>*String* <br> | <p>Users may provide a description for the entry.</p> |
| **source**<br>*String* <br> | <p>Ontology source</p> |
| **url**<br>*String* <br> | <p>Ontology url</p> |
| **attributes**<br>*Map<String,String>* <br> | <p>Dictionary that can be customised by users to store any additional information users may require..</p> |

### SampleVariantQualityControlMetrics
You can find the Java code [here](https://github.com/opencb/opencga/tree/release-2.2.x/opencga-core/src/main/java/org/opencb/opencga/core/models/sample/SampleVariantQualityControlMetrics.java).
>>>>>>> release-2.2.x

| Field | Description |
| :---  | :--- |
| **variantStats**<br>*List<<a href="https://docs.opencga.opencb.org/data-models/sample#sampleqcvariantstats"><em>SampleQcVariantStats</em></a>>* <br> | <p>Variant stats for the quality control of the sample.</p> |
| **signatures**<br>*List<<a href="https://docs.opencga.opencb.org/data-models/sample#signature"><em>Signature</em></a>>* <br> | <p>Signature for the quality control of the sample.</p> |
| **genomePlot**<br>*[GenomePlot](https://docs.opencga.opencb.org/data-models/sample#genomeplot)* <br> | <p>Genome plot for the quality control of the sample.</p> |
| **files**<br>*List<<em>String</em>>* <br> | <p>File for the quality control metrics of the sample.</p> |

### InternalStatus
You can find the Java code [here](https://github.com/opencb/opencga/tree/release-2.2.x/opencga-core/src/main/java/org/opencb/opencga/core/models/common/InternalStatus.java).


### ClinicalComment
You can find the Java code [here](https://github.com/opencb/biodata/tree/develop/biodata-models/src/main/java/org/opencb/biodata/models/clinical/ClinicalComment.java).

| Field | Description |
| :---  | :--- |
| **author**<br>*String* <br> | <p>Clinical comment author</p> |
| **message**<br>*String* <br> | <p>Clinical comment message</p> |
| **tags**<br>*List<<em>String</em>>* <br> | <p>List of tags for the clinical comment</p> |
| **date**<br>*String* <br> | <p>Date of the clinical comment</p> |

### GenomePlot
You can find the Java code [here](https://github.com/opencb/biodata/tree/develop/biodata-models/src/main/java/org/opencb/biodata/models/clinical/qc/GenomePlot.java).

| Field | Description |
| :---  | :--- |
| **id**<br>*String* <br> | <p>Id to identify the object</p> |
| **description**<br>*String* <br> | <p>Users may provide a description for the entry.</p> |
| **config**<br>*[GenomePlotConfig](https://docs.opencga.opencb.org/data-models/sample#genomeplotconfig)* <br> | <p>Config of the genomePlot</p> |
| **file**<br>*String* <br> | <p>File of the genomePlot</p> |

### Signature
You can find the Java code [here](https://github.com/opencb/biodata/tree/develop/biodata-models/src/main/java/org/opencb/biodata/models/clinical/qc/Signature.java).

| Field | Description |
| :---  | :--- |
| **id**<br>*String* <br> | <p>Id to identify the object</p> |
| **description**<br>*String* <br> | <p>Users may provide a description for the entry.</p> |
| **query**<br>*ObjectMap* <br> | <p>Map for query</p> |
| **type**<br>*String* <br> | <p>Signature type SNV, INDEL...</p> |
| **counts**<br>*List<<a href="https://docs.opencga.opencb.org/data-models/sample#genomecontextcount"><em>GenomeContextCount</em></a>>* <br> | <p>List of GenomeContextCount</p> |
| **files**<br>*List<<em>String</em>>* <br> | <p>List of files of signature</p> |
| **fitting**<br>*[SignatureFitting](https://docs.opencga.opencb.org/data-models/sample#signaturefitting)* <br> | <p>Signature fitting</p> |

<<<<<<< HEAD
=======
### SampleQcVariantStats
You can find the Java code [here](https://github.com/opencb/biodata/tree/develop/biodata-models/src/main/java/org/opencb/biodata/models/clinical/qc/SampleQcVariantStats.java).

| Field | Description |
| :---  | :--- |
| **id**<br>*String* <br> | <p>Id to identify the object</p> |
| **description**<br>*String* <br> | <p>Users may provide a description for the entry.</p> |
| **query**<br>*Map<String,String>* <br> | <p>Map for query</p> |
| **stats**<br>*SampleVariantStats* <br> | <p>Stats result set</p> |
| **sampleId**<br>*String* <br> | <p>Stats result set</p> |

>>>>>>> release-2.2.x
### GenomePlotConfig
You can find the Java code [here](https://github.com/opencb/biodata/tree/develop/biodata-models/src/main/java/org/opencb/biodata/models/clinical/qc/GenomePlotConfig.java).

| Field | Description |
| :---  | :--- |
| **title**<br>*String* <br> | <p>Title of the genome plot configuration</p> |
| **density**<br>*String* <br> | <p>Density of the genome plot configuration</p> |
| **generalQuery**<br>*Map<String,String>* <br> | <p>Map for the general query of the genome plot configuration</p> |
| **tracks**<br>*List<<a href="https://docs.opencga.opencb.org/data-models/sample#genomeplottrack"><em>GenomePlotTrack</em></a>>* <br> | <p>List of GenomePlotTrack</p> |

### SignatureFitting
You can find the Java code [here](https://github.com/opencb/biodata/tree/develop/biodata-models/src/main/java/org/opencb/biodata/models/clinical/qc/SignatureFitting.java).

| Field | Description |
| :---  | :--- |
| **method**<br>*String* <br> | <p>Method used to fit the signature</p> |
| **signatureSource**<br>*String* <br> | <p>Source of the fitting signature</p> |
| **signatureVersion**<br>*String* <br> | <p>Signature version of the fitting signature</p> |
| **scores**<br>*List<<em>Score</em>>* <br> | <p>Scores of the fitting signature</p> |
| **coeff**<br>*double* <br> | <p>Coefficient of the fitting signature</p> |
| **file**<br>*String* <br> | <p>Files of the fitting signature</p> |

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
| **scores**<br>*List<<em>Score</em>>* <br> | <p>Scores of the fitting signature</p> |
| **coeff**<br>*double* <br> | <p>Coefficient of the fitting signature</p> |
| **file**<br>*String* <br> | <p>Files of the fitting signature</p> |

### GenomePlotTrack
You can find the Java code [here](https://github.com/opencb/biodata/tree/develop/biodata-models/src/main/java/org/opencb/biodata/models/clinical/qc/GenomePlotTrack.java).

| Field | Description |
| :---  | :--- |
| **type**<br>*String* <br> | <p>Genome Plot Track Type</p> |
| **description**<br>*String* <br> | <p>Genome Plot Track description</p> |
| **query**<br>*Map<String,String>* <br> | <p>Genome Plot Track map for query</p> |
