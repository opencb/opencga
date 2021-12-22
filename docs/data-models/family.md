# Family
## Overview
Data model details for class: org.opencb.opencga.core.models.family.Family
### Summary 
| Field | Create | Update | Unique | Required|
| :--- | :---: | :---: |:---: |:---: |
| id | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |
| uuid | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| name | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| members | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| phenotypes | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| disorders | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| qualityControl | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| creationDate | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| modificationDate | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| expectedSize | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| description | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| status | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| internal | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| roles | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| attributes | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| release | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| version | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |

## Data Model

### Family
You can find the Java code [here](https://github.com/opencb/opencga/tree/issue-1806/opencga-core/src/main/java/org/opencb/opencga/core/models/family/Family.java).

| Field | Description |
| :---  | :--- |
| **id**<br>*String* <br> | <p>Family is a mandatory parameter when creating a new sample, this ID cannot be changed at the moment.</p> |
| **uuid**<br>*String* <br> | <p>Unique 32-character identifier assigned automatically by OpenCGA.</p> |
| **name**<br>*String* <br> | <p>Family name</p> |
| **members**<br>*List<[Individual](https://docs.opencga.opencb.org/data-models/individual) >* <br> | <p>List of individuals who are family members</p> |
| **phenotypes**<br>*List<[Phenotype](https://docs.opencga.opencb.org/data-models/family#phenotype) >* <br> | <p>List of phenotypes </p> |
| **disorders**<br>*List<[Disorder](https://docs.opencga.opencb.org/data-models/family#disorder) >* <br> | <p>Family disorders</p> |
| **qualityControl**<br>*[FamilyQualityControl](https://docs.opencga.opencb.org/data-models/family#familyqualitycontrol)* <br> | <p>Contains different metrics to evaluate the quality of the individual.</p> |
| **creationDate**<br>*String* <br><br>_since_: 1.0 | <p>Autogenerated date following the format YYYYMMDDhhmmss containing the date when the entry was first registered.</p> |
| **modificationDate**<br>*String* <br><br>_since_: 1.0 | <p>Autogenerated date following the format YYYYMMDDhhmmss containing the date when the entry was last modified.</p> |
| **expectedSize**<br>*int* <br> | <p>Family expected size</p> |
| **description**<br>*String* <br> | <p>Users may provide a description for the entry.</p> |
| **status**<br>*[CustomStatus](https://docs.opencga.opencb.org/data-models/family#customstatus)* <br><br>_since_: 2.0 | <p>Object to set a custom status</p> |
| **internal**<br>*[FamilyInternal](https://docs.opencga.opencb.org/data-models/family#familyinternal)* <br><br>_since_: 2.0 | <p>Internal field for manage the object</p> |
| **roles**<br>*Map<String,Map<String,<a href="https://docs.opencga.opencb.org/data-models/family#familiarrelationship"><em>FamiliarRelationship</em></a>>* <br> | <p>Roles of family members</p> |
| **attributes**<br>*Map<String,Object>* <br><br>_since_: 1.0 | <p>Dictionary that can be customised by users to store any additional information users may require..</p> |
| **release**<br>*int* <br> | <p>An integer describing the current data release.</p> |
| **version**<br>*int* <br> | <p>Autoincremental version assigned to the registered entry. By default, updates does not create new versions. To enable versioning, users must set the `incVersion` flag from the /update web service when updating the document.</p> |

### FamilyQualityControl
You can find the Java code [here](https://github.com/opencb/opencga/tree/issue-1806/opencga-core/src/main/java/org/opencb/opencga/core/models/family/FamilyQualityControl.java).

| Field | Description |
| :---  | :--- |
| **relatedness**<br>*List<[RelatednessReport](https://docs.opencga.opencb.org/data-models/family#relatednessreport) >* <br> | <p>Reports of family relationship</p> |
| **files**<br>*List<[String](https://docs.opencga.opencb.org/data-models/family#string) >* <br> | <p>File IDs related to the quality control</p> |
| **comments**<br>*List<[ClinicalComment](https://docs.opencga.opencb.org/data-models/family#clinicalcomment) >* <br> | <p>Comments related to the quality control</p> |

### Enum FamiliarRelationship
_Enumeration class._
You can find the Java code [here](https://github.com/opencb/opencga/tree/issue-1806/opencga-core/src/main/java/org/opencb/opencga/core/models/family/Family/FamiliarRelationship.java).


### Map
You can find the Java code [here](https://github.com/opencb/opencga/tree/issue-1806/opencga-core/src/main/java/java/util/Map.java).


### String
You can find the Java code [here](https://github.com/opencb/opencga/tree/issue-1806/opencga-core/src/main/java/java/lang/String.java).


### CustomStatus
You can find the Java code [here](https://github.com/opencb/opencga/tree/issue-1806/opencga-core/src/main/java/org/opencb/opencga/core/models/common/CustomStatus.java).

| Field | Description |
| :---  | :--- |
| **CustomStatus.name**<br>*String* <br> | <p>Name of the status</p> |
| **CustomStatus.description**<br>*String* <br> | <p>Users may provide a description for the entry.</p> |
| **CustomStatus.date**<br>*String* <br> | <p>Users may provide a description for the entry.</p> |

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

### Disorder
You can find the Java code [here](https://github.com/opencb/biodata/tree/develop/biodata-models/src/main/java/org/opencb/biodata/models/clinical/Disorder.java).

| Field | Description |
| :---  | :--- |
| **id**<br>*String* <br> | <p>Id to identify the object</p> |
| **name**<br>*String* <br> | <p>Object name</p> |
| **description**<br>*String* <br> | <p>Users may provide a description for the entry.</p> |
| **source**<br>*String* <br> | <p>Ontology source</p> |
| **url**<br>*String* <br> | <p>Ontology url</p> |
| **attributes**<br>*Map<String,String>* <br> | <p>Dictionary that can be customised by users to store any additional information users may require..</p> |

### Object
You can find the Java code [here](https://github.com/opencb/opencga/tree/issue-1806/opencga-core/src/main/java/java/lang/Object.java).


### FamilyInternal
You can find the Java code [here](https://github.com/opencb/opencga/tree/issue-1806/opencga-core/src/main/java/org/opencb/opencga/core/models/family/FamilyInternal.java).

| Field | Description |
| :---  | :--- |
| **status**<br>*[Status](https://docs.opencga.opencb.org/data-models/family#status)* <br> | <p>Status of the internal object</p> |
| **registrationDate**<br>*String* <br> | <p>Registration date of the internal object</p> |
| **lastModified**<br>*String* <br> | <p>Date of the last modification of the internal object</p> |

### RelatednessReport
You can find the Java code [here](https://github.com/opencb/biodata/tree/develop/biodata-models/src/main/java/org/opencb/biodata/models/clinical/qc/RelatednessReport.java).


### ClinicalComment
You can find the Java code [here](https://github.com/opencb/biodata/tree/develop/biodata-models/src/main/java/org/opencb/biodata/models/clinical/ClinicalComment.java).

| Field | Description |
| :---  | :--- |
| **author**<br>*String* <br> | <p>Clinical comment author</p> |
| **message**<br>*String* <br> | <p>Clinical comment message</p> |
| **tags**<br>*List<[String](https://docs.opencga.opencb.org/data-models/family#string) >* <br> | <p>List of tags for the clinical comment</p> |
| **date**<br>*String* <br> | <p>Date of the clinical comment</p> |

### Status
You can find the Java code [here](https://github.com/opencb/opencga/tree/issue-1806/opencga-core/src/main/java/org/opencb/opencga/core/models/common/Status.java).

| Field | Description |
| :---  | :--- |
| **name**<br>*String* <br> | <p>Name of the  Status</p> |
