# Cohort
## Overview
Cohort data model hosts information about any cohort.
### Summary 
| Field | Create | Update | Unique | Required|
| :--- | :---: | :---: |:---: |:---: |
| id | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |
| uuid | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| type | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| creationDate | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| modificationDate | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| description | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| samples | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| numSamples | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| release | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| status | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| internal | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| attributes | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |

## Data Model

### Cohort
You can find the Java code [here](https://github.com/opencb/opencga/tree/TASK-153/opencga-core/src/main/java/org/opencb/opencga/core/models/cohort/Cohort.java).

| Field | Description |
| :---  | :--- |
| **id**<br>*String* <br> | <p>Object ID is a mandatory parameter when creating a new one, this ID cannot be changed at the moment.</p> |
| **uuid**<br>*String* <br> | <p>Unique 32-character identifier assigned automatically by OpenCGA.</p> |
| **type**<br>*CohortType* <br> | <p>Cohort type that can have the values CASE_CONTROL, CASE_SET, CONTROL_SET, PAIRED, PAIRED_TUMOR, AGGREGATE, TIME_SERIES, FAMILY, TRIO and COLLECTION.</p> |
| **creationDate**<br>*String* <br> | <p>Autogenerated date following the format YYYYMMDDhhmmss containing the date when the entry was first registered.</p> |
| **modificationDate**<br>*String* <br><br>_since_: 1.0 | <p>Autogenerated date following the format YYYYMMDDhhmmss containing the date when the entry was last modified.</p> |
| **description**<br>*String* <br> | <p>Users may provide a description for the entry.</p> |
| **samples**<br>*List<<a href="https://docs.opencga.opencb.org/data-models/sample"><em>Sample</em></a>>* <br> | <p>List of cohort samples.</p> |
| **numSamples**<br>*int* <br> | <p>Number of samples.</p> |
| **release**<br>*int* <br> | <p>An integer describing the current data release.</p> |
| **status**<br>*[Status](https://docs.opencga.opencb.org/data-models/cohort#status)* <br> | <p>Object to set a custom status.</p> |
| **internal**<br>*[CohortInternal](https://docs.opencga.opencb.org/data-models/cohort#cohortinternal)* <br> | <p>Internal field for manage the object.</p> |
| **attributes**<br>*Map<String,Object>* <br> | <p>You can use this field to store any other information, keep in mind this is not indexed so you cannot search by attributes.</p> |

### Status
You can find the Java code [here](https://github.com/opencb/biodata/tree/develop/biodata-models/src/main/java/org/opencb/biodata/models/common/Status.java).


### CohortInternal
You can find the Java code [here](https://github.com/opencb/opencga/tree/TASK-153/opencga-core/src/main/java/org/opencb/opencga/core/models/cohort/CohortInternal.java).

| Field | Description |
| :---  | :--- |
| **status**<br>*[Status](https://docs.opencga.opencb.org/data-models/cohort#status)* <br> | <p>Status of the internal object.</p> |
| **registrationDate**<br>*String* <br> | <p>Registration date of the internal object.</p> |
| **lastModified**<br>*String* <br> | <p>Date of the last modification of the internal object.</p> |
