# Project
## Overview
Project data model hosts information about any project.
### Summary 
| Field | Create | Update | Unique | Required|
| :--- | :---: | :---: |:---: |:---: |
| id | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |
| uuid | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| name | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| fqn | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| creationDate | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| modificationDate | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| description | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| organism | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| cellbase | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| currentRelease | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| studies | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| release | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| attributes | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |

## Data Model

### Project
<<<<<<< HEAD
<<<<<<< HEAD
You can find the Java code [here](https://github.com/opencb/opencga/tree/TASK-521/opencga-core/src/main/java/org/opencb/opencga/core/models/project/Project.java).
=======
You can find the Java code [here](https://github.com/opencb/opencga/tree/release-2.2.x/opencga-core/src/main/java/org/opencb/opencga/core/models/project/Project.java).
>>>>>>> release-2.2.x
=======
You can find the Java code [here](https://github.com/opencb/opencga/tree/TASK-1587/opencga-core/src/main/java/org/opencb/opencga/core/models/project/Project.java).
>>>>>>> release-2.2.x

| Field | Description |
| :---  | :--- |
| **id**<br>*String* <br> | <p>Object ID is a mandatory parameter when creating a new one, this ID cannot be changed at the moment.</p> |
| **uuid**<br>*String* <br> | <p>Unique 32-character identifier assigned automatically by OpenCGA.</p> |
| **name**<br>*String* <br> | <p>Name of the .</p> |
| **fqn**<br>*String* <br> | <p>Full Qualified Name (user@projectId).</p> |
| **creationDate**<br>*String* <br> | <p>Autogenerated date following the format YYYYMMDDhhmmss containing the date when the entry was first registered.</p> |
| **modificationDate**<br>*String* <br><br>_since_: 1.0 | <p>Autogenerated date following the format YYYYMMDDhhmmss containing the date when the entry was last modified.</p> |
| **description**<br>*String* <br> | <p>Users may provide a description for the entry.</p> |
| **organism**<br>*[ProjectOrganism](https://docs.opencga.opencb.org/data-models/project#projectorganism)* <br> | <p>Organism to which the project belongs.</p> |
| **cellbase**<br>*CellBaseConfiguration* <br> | <p>Cellbase configuration.</p> |
| **currentRelease**<br>*int* <br> | <p>An integer describing the current data release.</p> |
| **studies**<br>*List<<a href="https://docs.opencga.opencb.org/data-models/study"><em>Study</em></a>>* <br> | <p>Project study list.</p> |
| **release**<br>*[ProjectInternal](https://docs.opencga.opencb.org/data-models/project#projectinternal)* <br> | <p>An integer describing the current data release.</p> |
| **attributes**<br>*Map<String,Object>* <br> | <p>You can use this field to store any other information, keep in mind this is not indexed so you cannot search by attributes.</p> |

### ProjectOrganism
<<<<<<< HEAD
You can find the Java code [here](https://github.com/opencb/opencga/tree/TASK-521/opencga-core/src/main/java/org/opencb/opencga/core/models/project/ProjectOrganism.java).


### ProjectInternal
<<<<<<< HEAD
You can find the Java code [here](https://github.com/opencb/opencga/tree/TASK-521/opencga-core/src/main/java/org/opencb/opencga/core/models/project/ProjectInternal.java).
=======
You can find the Java code [here](https://github.com/opencb/opencga/tree/release-2.2.x/opencga-core/src/main/java/org/opencb/opencga/core/models/project/ProjectInternal.java).
>>>>>>> release-2.2.x
=======
You can find the Java code [here](https://github.com/opencb/opencga/tree/TASK-1587/opencga-core/src/main/java/org/opencb/opencga/core/models/project/ProjectOrganism.java).


### ProjectInternal
You can find the Java code [here](https://github.com/opencb/opencga/tree/TASK-1587/opencga-core/src/main/java/org/opencb/opencga/core/models/project/ProjectInternal.java).
>>>>>>> release-2.2.x

| Field | Description |
| :---  | :--- |
| **datastores**<br>*Datastores* <br> | <p>Default value is VARIANT.</p> |
<<<<<<< HEAD
=======
| **cellbase**<br>*CellBaseConfiguration* <br> | <p>Cellbase configuration.</p> |
>>>>>>> release-2.2.x
| **status**<br>*[InternalStatus](https://docs.opencga.opencb.org/data-models/project#internalstatus)* <br> | <p>Status of the internal object.</p> |
| **registrationDate**<br>*String* <br> | <p>Registration date of the internal object.</p> |
| **lastModified**<br>*String* <br> | <p>Date of the last modification of the internal object.</p> |

<<<<<<< HEAD
<<<<<<< HEAD
### InternalStatus
You can find the Java code [here](https://github.com/opencb/opencga/tree/TASK-521/opencga-core/src/main/java/org/opencb/opencga/core/models/common/InternalStatus.java).
=======
### ProjectOrganism
You can find the Java code [here](https://github.com/opencb/opencga/tree/release-2.2.x/opencga-core/src/main/java/org/opencb/opencga/core/models/project/ProjectOrganism.java).


### InternalStatus
You can find the Java code [here](https://github.com/opencb/opencga/tree/release-2.2.x/opencga-core/src/main/java/org/opencb/opencga/core/models/common/InternalStatus.java).
>>>>>>> release-2.2.x
=======
### InternalStatus
You can find the Java code [here](https://github.com/opencb/opencga/tree/TASK-1587/opencga-core/src/main/java/org/opencb/opencga/core/models/common/InternalStatus.java).
>>>>>>> release-2.2.x

