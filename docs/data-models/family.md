# Family
## Overview
Family data model hosts information about any family.
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
You can find the Java code [here](https://github.com/opencb/opencga/tree/TASK-354/opencga-core/src/main/java/org/opencb/opencga/core/models/family/Family.java).

| Field | Description |
| :---  | :--- |
| **id**<br>*String* <br> | <p>Family is a mandatory parameter when creating a new sample, this ID cannot be changed at the moment.</p> |
| **uuid**<br>*String* <br> | <p>Unique 32-character identifier assigned automatically by OpenCGA.</p> |
| **name**<br>*String* <br> | <p>Family name.</p> |
| **members**<br>*List<<a href="https://docs.opencga.opencb.org/data-models/individual"><em>Individual</em></a>>* <br> | <p>List of individuals who are family members.</p> |
| **phenotypes**<br>*List<<a href="https://docs.opencga.opencb.org/data-models/family#phenotype"><em>Phenotype</em></a>>* <br> | <p>List of phenotypes .</p> |
| **disorders**<br>*List<<a href="https://docs.opencga.opencb.org/data-models/family#disorder"><em>Disorder</em></a>>* <br> | <p>Family disorders.</p> |
| **qualityControl**<br>*[FamilyQualityControl](https://docs.opencga.opencb.org/data-models/family#familyqualitycontrol)* <br> | <p>Contains different metrics to evaluate the quality of the individual.</p> |
| **creationDate**<br>*String* <br><br>_since_: 1.0 | <p>Autogenerated date following the format YYYYMMDDhhmmss containing the date when the entry was first registered.</p> |
| **modificationDate**<br>*String* <br><br>_since_: 1.0 | <p>Autogenerated date following the format YYYYMMDDhhmmss containing the date when the entry was last modified.</p> |
| **expectedSize**<br>*int* <br> | <p>Family expected size.</p> |
| **description**<br>*String* <br> | <p>Users may provide a description for the entry.</p> |
| **status**<br>*[Status](https://docs.opencga.opencb.org/data-models/family#status)* <br><br>_since_: 2.0 | <p>Object to set a custom status.</p> |
| **internal**<br>*[FamilyInternal](https://docs.opencga.opencb.org/data-models/family#familyinternal)* <br><br>_since_: 2.0 | <p>Internal field to manage the object.</p> |
| **roles**<br>*Map<String,Map<String,FamiliarRelationship>* <br> | <p>Map of members ids and enum of roles (FATHER, MOTHER, IDENTICAL_TWIN, SON, UNCLE, PATERNAL_GRANDFATHER.)  .</p> |
| **attributes**<br>*Map<String,Object>* <br><br>_since_: 1.0 | <p>You can use this field to store any other information, keep in mind this is not indexed so you cannot search by attributes.</p> |
| **release**<br>*int* <br> | <p>An integer describing the current data release.</p> |
| **version**<br>*int* <br> | <p>Autoincremental version assigned to the registered entry. By default, updates does not create new versions. To enable versioning, users must set the `incVersion` flag from the /update web service when updating the document.</p> |

### Status
You can find the Java code [here](https://github.com/opencb/biodata/tree/develop/biodata-models/src/main/java/org/opencb/biodata/models/common/Status.java).


### FamilyQualityControl
You can find the Java code [here](https://github.com/opencb/opencga/tree/TASK-354/opencga-core/src/main/java/org/opencb/opencga/core/models/family/FamilyQualityControl.java).

| Field | Description |
| :---  | :--- |
| **relatedness**<br>*List<<a href="https://docs.opencga.opencb.org/data-models/family#relatednessreport"><em>RelatednessReport</em></a>>* <br> | <p>Reports of family relationship.</p> |
| **files**<br>*List<<em>String</em>>* <br> | <p>File IDs related to the quality control.</p> |
| **comments**<br>*List<<a href="https://docs.opencga.opencb.org/data-models/family#clinicalcomment"><em>ClinicalComment</em></a>>* <br> | <p>Comments related to the quality control.</p> |

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

### FamilyInternal
You can find the Java code [here](https://github.com/opencb/opencga/tree/TASK-354/opencga-core/src/main/java/org/opencb/opencga/core/models/family/FamilyInternal.java).

| Field | Description |
| :---  | :--- |
| **status**<br>*[Status](https://docs.opencga.opencb.org/data-models/family#status)* <br> | <p>Status of the internal object.</p> |
| **registrationDate**<br>*String* <br> | <p>Registration date of the internal object.</p> |
| **lastModified**<br>*String* <br> | <p>Date of the last modification of the internal object.</p> |

### RelatednessReport
You can find the Java code [here](https://github.com/opencb/biodata/tree/develop/biodata-models/src/main/java/org/opencb/biodata/models/clinical/qc/RelatednessReport.java).

| Field | Description |
| :---  | :--- |
| **method**<br>*String* <br> | <p>Method of the relatedness report</p> |
| **maf**<br>*String* <br> | <p>Minor allele frequency to filter variants, e.g.: 1kg_phase3:CEU>0.35, cohort:ALL>0.05</p> |
| **scores**<br>*List<<em>RelatednessScore</em>>* <br> | <p>Relatedness scores for pair of samples</p> |
| **files**<br>*List<<em>String</em>>* <br> | <p>List of files of Relatedness Report</p> |

### ClinicalComment
You can find the Java code [here](https://github.com/opencb/biodata/tree/develop/biodata-models/src/main/java/org/opencb/biodata/models/clinical/ClinicalComment.java).

| Field | Description |
| :---  | :--- |
| **author**<br>*String* <br> | <p>Clinical comment author</p> |
| **message**<br>*String* <br> | <p>Clinical comment message</p> |
| **tags**<br>*List<<em>String</em>>* <br> | <p>List of tags for the clinical comment</p> |
| **date**<br>*String* <br> | <p>Date of the clinical comment</p> |
