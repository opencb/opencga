# Job
## Overview
Data model details for class: org.opencb.opencga.core.models.job.Job
### Summary 
| Field | Create | Update | Unique | Required|
| :--- | :---: | :---: |:---: |:---: |
| id | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |
| uuid | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| description | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| tool | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| userId | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| commandLine | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| params | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| creationDate | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| modificationDate | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| priority | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| release | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| outDir | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| input | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| output | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| tags | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| dependsOn | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| execution | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| stdout | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| stderr | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| visited | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| release | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| study | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| attributes | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |

## Data Model

### Job
You can find the Java code [here](https://github.com/opencb/opencga/tree/issue-1806/opencga-core/src/main/java/org/opencb/opencga/core/models/job/Job.java).

| Field | Description |
| :---  | :--- |
| **id**<br>*String* <br> | <p>Object ID is a mandatory parameter when creating a new one, this ID cannot be changed at the moment.</p> |
| **uuid**<br>*String* <br> | <p>Unique 32-character identifier assigned automatically by OpenCGA.</p> |
| **description**<br>*String* <br> | <p>Users may provide a description for the entry.</p> |
| **tool**<br>*[ToolInfo](https://docs.opencga.opencb.org/data-models/job#toolinfo)* <br> | <p>Job tool info.</p> |
| **userId**<br>*String* <br> | <p>Job user id.</p> |
| **commandLine**<br>*String* <br> | <p>Job command line.</p> |
| **params**<br>*Map<String,Object>* <br> | <p>Job params.</p> |
| **creationDate**<br>*String* <br> | <p>Autogenerated date following the format YYYYMMDDhhmmss containing the date when the entry was first registered.</p> |
| **modificationDate**<br>*String* <br><br>_since_: 1.0 | <p>Autogenerated date following the format YYYYMMDDhhmmss containing the date when the entry was last modified.</p> |
| **priority**<br>*Priority* <br> | <p>Job priority.</p> |
| **release**<br>*[JobInternal](https://docs.opencga.opencb.org/data-models/job#jobinternal)* <br> | <p>An integer describing the current data release.</p> |
| **outDir**<br>*[File](https://docs.opencga.opencb.org/data-models/file)* <br> | <p>Output dir for the job.</p> |
| **input**<br>*List<<a href="https://docs.opencga.opencb.org/data-models/file"><em>File</em></a>>* <br> | <p>List of input files.</p> |
| **output**<br>*List<<a href="https://docs.opencga.opencb.org/data-models/file"><em>File</em></a>>* <br> | <p>List of output files.</p> |
| **tags**<br>*List<<em>String</em>>* <br> | <p>List of tags for the job.</p> |
| **dependsOn**<br>*List<<a href="https://docs.opencga.opencb.org/data-models/job"><em>Job</em></a>>* <br> | <p>List of jobs the current job depends on.</p> |
| **execution**<br>*[ExecutionResult](https://docs.opencga.opencb.org/data-models/job#executionresult)* <br> | <p>Result of the execution.</p> |
| **stdout**<br>*[File](https://docs.opencga.opencb.org/data-models/file)* <br> | <p>Standard out file.</p> |
| **stderr**<br>*[File](https://docs.opencga.opencb.org/data-models/file)* <br> | <p>Standard error file.</p> |
| **visited**<br>*boolean* <br> | <p>Boolean that represents if the job has been visited or not.</p> |
| **release**<br>*int* <br> | <p>An integer describing the current data release.</p> |
| **study**<br>*[JobStudyParam](https://docs.opencga.opencb.org/data-models/job#jobstudyparam)* <br> | <p>Job study.</p> |
| **attributes**<br>*Map<String,Object>* <br> | <p>You can use this field to store any other information, keep in mind this is not indexed so you cannot search by attributes.</p> |

### ToolInfo
You can find the Java code [here](https://github.com/opencb/opencga/tree/issue-1806/opencga-core/src/main/java/org/opencb/opencga/core/models/job/ToolInfo.java).

| Field | Description |
| :---  | :--- |
| **id**<br>*String* <br> | <p>Object ID is a mandatory parameter when creating a new one, this ID cannot be changed at the moment.</p> |
| **description**<br>*String* <br> | <p>Users may provide a description for the entry.</p> |
| **scope**<br>*Scope* <br> | <p>Tool info scope can have the values GLOBAL, PROJECT and STUDY.</p> |
| **type**<br>*Type* <br> | <p>Tool info type can have the values OPERATION and ANALYSIS.</p> |
| **resource**<br>*Resource* <br> | <p>Tool info resource can have the values AUDIT, USER, PROJECT, STUDY, FILE, SAMPLE, JOB, INDIVIDUAL, COHORT, DISEASE_PANEL, FAMILY, CLINICAL_ANALYSIS, INTERPRETATION, VARIANT, ALIGNMENT, CLINICAL, EXPRESSION, RGA and FUNCTIONAL.</p> |

### ExecutionResult
You can find the Java code [here](https://github.com/opencb/opencga/tree/issue-1806/opencga-core/src/main/java/org/opencb/opencga/core/tools/result/ExecutionResult.java).

| Field | Description |
| :---  | :--- |
| **id**<br>*String* <br> | <p>Object ID is a mandatory parameter when creating a new one, this ID cannot be changed at the moment.</p> |
| **executor**<br>*[ExecutorInfo](https://docs.opencga.opencb.org/data-models/job#executorinfo)* <br> | <p>Object describes execution information.</p> |
| **start**<br>*[Date](https://docs.opencga.opencb.org/data-models/job#date)* <br> | <p>Date the execution started.</p> |
| **end**<br>*[Date](https://docs.opencga.opencb.org/data-models/job#date)* <br> | <p>Date the execution was completed.</p> |
| **status**<br>*[Status](https://docs.opencga.opencb.org/data-models/job#status)* <br> | <p>Executor status can have the values PENDING, RUNNING, DONE and ERROR.</p> |
| **externalFiles**<br>*List<<a href="https://docs.opencga.opencb.org/data-models/job#uri"><em>URI</em></a>>* <br> | <p>List of uris to the external files.</p> |
| **steps**<br>*List<<a href="https://docs.opencga.opencb.org/data-models/job#toolstep"><em>ToolStep</em></a>>* <br> | <p>List of ToolStep.</p> |
| **events**<br>*List<<em>Event</em>>* <br> | <p>List of Event.</p> |
| **attributes**<br>*ObjectMap* <br> | <p>You can use this field to store any other information, keep in mind this is not indexed so you cannot search by attributes.</p> |

### JobStudyParam
You can find the Java code [here](https://github.com/opencb/opencga/tree/issue-1806/opencga-core/src/main/java/org/opencb/opencga/core/models/job/JobStudyParam.java).

| Field | Description |
| :---  | :--- |
| **id**<br>*String* <br> | <p>Object ID is a mandatory parameter when creating a new one, this ID cannot be changed at the moment.</p> |
| **others**<br>*List<<em>String</em>>* <br> | <p>List of strings.</p> |

### JobInternal
You can find the Java code [here](https://github.com/opencb/opencga/tree/issue-1806/opencga-core/src/main/java/org/opencb/opencga/core/models/job/JobInternal.java).

| Field | Description |
| :---  | :--- |
| **status**<br>*[ExecutionStatus](https://docs.opencga.opencb.org/data-models/job#executionstatus)* <br> | <p>Job internal status can have the values PENDING, QUEUED, RUNNING, DONE, ERROR, UNKNOWN, REGISTERING, UNREGISTERED, ABORTED, DELETED.</p> |
| **webhook**<br>*[JobInternalWebhook](https://docs.opencga.opencb.org/data-models/job#jobinternalwebhook)* <br> | <p>Job internal Webhook.</p> |
| **events**<br>*List<<em>Event</em>>* <br> | <p>Events of the internal job.</p> |
| **status**<br>*[Status](https://docs.opencga.opencb.org/data-models/job#status)* <br> | <p>Status of the internal object.</p> |
| **registrationDate**<br>*String* <br> | <p>Registration date of the internal object.</p> |
| **lastModified**<br>*String* <br> | <p>Date of the last modification of the internal object.</p> |

### JobInternalWebhook
You can find the Java code [here](https://github.com/opencb/opencga/tree/issue-1806/opencga-core/src/main/java/org/opencb/opencga/core/models/job/JobInternalWebhook.java).

| Field | Description |
| :---  | :--- |
| **webhook**<br>*[URL](https://docs.opencga.opencb.org/data-models/job#url)* <br> | <p>Webhook URL.</p> |
| **status**<br>*Map<String,Status>* <br> | <p>Webhook status map can have the values SUCCESS or ERROR.</p> |

### Status
You can find the Java code [here](https://github.com/opencb/opencga/tree/issue-1806/opencga-core/src/main/java/org/opencb/opencga/core/tools/result/Status.java).


### ExecutorInfo
You can find the Java code [here](https://github.com/opencb/opencga/tree/issue-1806/opencga-core/src/main/java/org/opencb/opencga/core/tools/result/ExecutorInfo.java).

| Field | Description |
| :---  | :--- |
| **id**<br>*String* <br> | <p>Object ID is a mandatory parameter when creating a new one, this ID cannot be changed at the moment.</p> |
| **clazz**<br>*String* <br> | <p>ExecutorInfo class.</p> |
| **params**<br>*ObjectMap* <br> | <p>ExecutorInfo params.</p> |
| **source**<br>*Source* <br> | <p>Executor info source can have the values FILE, PARQUET_FILE, MONGODB,  HBASE, STORAGE.</p> |
| **framework**<br>*Framework* <br> | <p>Executor info framework can have the values LOCAL, MAP_REDUCE, SPARK.</p> |

### ExecutionStatus
You can find the Java code [here](https://github.com/opencb/opencga/tree/issue-1806/opencga-core/src/main/java/org/opencb/opencga/core/models/common/Enums/ExecutionStatus.java).

| Field | Description |
| :---  | :--- |
| **name**<br>*String* <br> | <p>Name of the . Status</p> |
| **date**<br>*String* <br> | <p>Date has setted the status.</p> |
| **description**<br>*String* <br> | <p>Users may provide a description for the entry.</p> |
| **~~message~~**<br>*String* <br><br>_Deprecated_ | <p>Deprecated: Message describing the status.</p> |

### ToolStep
You can find the Java code [here](https://github.com/opencb/opencga/tree/issue-1806/opencga-core/src/main/java/org/opencb/opencga/core/tools/result/ToolStep.java).

| Field | Description |
| :---  | :--- |
| **id**<br>*String* <br> | <p>Object ID is a mandatory parameter when creating a new one, this ID cannot be changed at the moment.</p> |
| **start**<br>*[Date](https://docs.opencga.opencb.org/data-models/job#date)* <br> | <p>Date the execution started.</p> |
| **end**<br>*[Date](https://docs.opencga.opencb.org/data-models/job#date)* <br> | <p>Date the execution was completed.</p> |
| **status**<br>*Type* <br> | <p>Executor status can have the values PENDING, RUNNING, DONE and ERROR.</p> |
| **attributes**<br>*ObjectMap* <br> | <p>You can use this field to store any other information, keep in mind this is not indexed so you cannot search by attributes.</p> |

### Date
You can find the Java code [here](https://github.com/opencb/opencga/tree/issue-1806/opencga-core/src/main/java/java/util/Date.java).


### URI
You can find the Java code [here](https://github.com/opencb/opencga/tree/issue-1806/opencga-core/src/main/java/java/net/URI.java).


### URL
You can find the Java code [here](https://github.com/opencb/opencga/tree/issue-1806/opencga-core/src/main/java/java/net/URL.java).

