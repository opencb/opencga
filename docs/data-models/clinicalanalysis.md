# ClinicalAnalysis
## Overview
ClinicalAnalysis data model hosts information about any analysis.
### Summary 
| Field | Create | Update | Unique | Required|
| :--- | :---: | :---: |:---: |:---: |
| id | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |
| uuid | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| description | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| type | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| disorder | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| files | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| proband | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| family | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| panels | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| panelLock | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| locked | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| interpretation | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| secondaryInterpretations | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| consent | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| analyst | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| report | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| priority | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| flags | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| creationDate | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| modificationDate | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| dueDate | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| release | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| qualityControl | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| comments | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| audit | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| internal | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| attributes | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| status | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |

## Data Model

### ClinicalAnalysis
You can find the Java code [here](https://github.com/opencb/opencga/tree/TASK-153/opencga-core/src/main/java/org/opencb/opencga/core/models/clinical/ClinicalAnalysis.java).

| Field | Description |
| :---  | :--- |
| **id**<br>*String* <br> | <p>ClinicalAnalysis ID is a mandatory parameter when creating a new ClinicalAnalysis, this ID cannot be changed at the moment.</p> |
| **uuid**<br>*String* <br> | <p>Unique 32-character identifier assigned automatically by OpenCGA.</p> |
| **description**<br>*String* <br> | <p>Users may provide a description for the entry.</p> |
| **type**<br>*Type* <br> | <p>Enumeration of the diferent types of clinical analysis SINGLE, FAMILY, CANCER, COHORT, AUTOCOMPARATIVE.</p> |
| **disorder**<br>*[Disorder](https://docs.opencga.opencb.org/data-models/clinicalanalysis#disorder)* <br> | <p>Disorder of the clinical analysis.</p> |
| **files**<br>*List<<a href="https://docs.opencga.opencb.org/data-models/file"><em>File</em></a>>* <br> | <p>List of files (VCF, BAM and BIGWIG).</p> |
| **proband**<br>*[Individual](https://docs.opencga.opencb.org/data-models/individual)* <br> | <p>Individual proband of the clinical analysis.</p> |
| **family**<br>*[Family](https://docs.opencga.opencb.org/data-models/family)* <br> | <p>Family of the clinical analysis.</p> |
| **panels**<br>*List<<a href="https://docs.opencga.opencb.org/data-models/clinicalanalysis#panel"><em>Panel</em></a>>* <br> | <p>List of panels to the clinical analysis.</p> |
| **panelLock**<br>*boolean* <br> | <p>Boolean to set lock panels.</p> |
| **locked**<br>*boolean* <br> | <p>Boolean that indicates if the clinical analysis is locked or not.</p> |
| **interpretation**<br>*[Interpretation](https://docs.opencga.opencb.org/data-models/clinicalanalysis#interpretation)* <br> | <p>Interpretation of the clinical analysis.</p> |
| **secondaryInterpretations**<br>*List<<a href="https://docs.opencga.opencb.org/data-models/clinicalanalysis#interpretation"><em>Interpretation</em></a>>* <br> | <p>List of Interpretations containing the second and consecutive.</p> |
| **consent**<br>*[ClinicalConsentAnnotation](https://docs.opencga.opencb.org/data-models/clinicalanalysis#clinicalconsentannotation)* <br> | <p>Object contains consent annotations of clinical analysis.</p> |
| **analyst**<br>*[ClinicalAnalyst](https://docs.opencga.opencb.org/data-models/clinicalanalysis#clinicalanalyst)* <br> | <p>The analyst of the clinical analysis.</p> |
| **report**<br>*[ClinicalReport](https://docs.opencga.opencb.org/data-models/clinicalanalysis#clinicalreport)* <br> | <p>Report of the clinical analysis.</p> |
| **priority**<br>*[ClinicalPriorityAnnotation](https://docs.opencga.opencb.org/data-models/clinicalanalysis#clinicalpriorityannotation)* <br> | <p>Priority of the clinical analysis.</p> |
| **flags**<br>*List<<a href="https://docs.opencga.opencb.org/data-models/clinicalanalysis#flagannotation"><em>FlagAnnotation</em></a>>* <br> | <p>List of flags for the clinical analysis.</p> |
| **creationDate**<br>*String* <br><br>_since_: 1.0 | <p>Autogenerated date following the format YYYYMMDDhhmmss containing the date when the entry was first registered.</p> |
| **modificationDate**<br>*String* <br><br>_since_: 1.0 | <p>Autogenerated date following the format YYYYMMDDhhmmss containing the date when the entry was last modified.</p> |
| **dueDate**<br>*String* <br><br>_since_: 1.0 | <p>Due date of the clinical analysis.</p> |
| **release**<br>*int* <br> | <p>An integer describing the current data release.</p> |
| **qualityControl**<br>*[ClinicalAnalysisQualityControl](https://docs.opencga.opencb.org/data-models/clinicalanalysis#clinicalanalysisqualitycontrol)* <br> | <p>Contains different metrics to evaluate the quality of the individual.</p> |
| **comments**<br>*List<<a href="https://docs.opencga.opencb.org/data-models/clinicalanalysis#clinicalcomment"><em>ClinicalComment</em></a>>* <br> | <p>List of Clinical Analysis comments.</p> |
| **audit**<br>*List<<a href="https://docs.opencga.opencb.org/data-models/clinicalanalysis#clinicalaudit"><em>ClinicalAudit</em></a>>* <br> | <p>List of Clinical Analysis audits.</p> |
| **internal**<br>*[ClinicalAnalysisInternal](https://docs.opencga.opencb.org/data-models/clinicalanalysis#clinicalanalysisinternal)* <br> | <p>Internal field to manage the object.</p> |
| **attributes**<br>*Map<String,Object>* <br> | <p>You can use this field to store any other information, keep in mind this is not indexed so you cannot search by attributes.</p> |
| **status**<br>*Status* <br> | <p>Object status.</p> |

### ClinicalAudit
You can find the Java code [here](https://github.com/opencb/biodata/tree/develop/biodata-models/src/main/java/org/opencb/biodata/models/clinical/ClinicalAudit.java).

| Field | Description |
| :---  | :--- |
| **author**<br>*String* <br> | <p>Audit author</p> |
| **action**<br>*Action* <br> | <p>Enum action that can have the values  CREATE_CLINICAL_ANALYSIS, CREATE_INTERPRETATION, UPDATE_CLINICAL_ANALYSIS, DELETE_CLINICAL_ANALYSIS, UPDATE_INTERPRETATION, REVERT_INTERPRETATION, CLEAR_INTERPRETATION, MERGE_INTERPRETATION, SWAP_INTERPRETATION and DELETE_INTERPRETATION</p> |
| **message**<br>*String* <br> | <p>Audit message</p> |
| **date**<br>*String* <br> | <p>Date of the audit</p> |

### ClinicalAnalyst
You can find the Java code [here](https://github.com/opencb/biodata/tree/develop/biodata-models/src/main/java/org/opencb/biodata/models/clinical/ClinicalAnalyst.java).

| Field | Description |
| :---  | :--- |
| **id**<br>*String* <br> | <p>Id to identify the object</p> |
| **name**<br>*String* <br> | <p>Object name</p> |
| **email**<br>*String* <br> | <p>Email of the analyst</p> |
| **assignedBy**<br>*String* <br> | <p>Assigned by field</p> |
| **date**<br>*String* <br> | <p>Date of the clinical analyst</p> |

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

### ClinicalComment
You can find the Java code [here](https://github.com/opencb/biodata/tree/develop/biodata-models/src/main/java/org/opencb/biodata/models/clinical/ClinicalComment.java).

| Field | Description |
| :---  | :--- |
| **author**<br>*String* <br> | <p>Clinical comment author</p> |
| **message**<br>*String* <br> | <p>Clinical comment message</p> |
| **tags**<br>*List<<em>String</em>>* <br> | <p>List of tags for the clinical comment</p> |
| **date**<br>*String* <br> | <p>Date of the clinical comment</p> |

### ClinicalReport
You can find the Java code [here](https://github.com/opencb/opencga/tree/TASK-153/opencga-core/src/main/java/org/opencb/opencga/core/models/clinical/ClinicalReport.java).

| Field | Description |
| :---  | :--- |
| **title**<br>*String* <br> | <p>Report title.</p> |
| **overview**<br>*String* <br> | <p>Report overview.</p> |
| **discussion**<br>*String* <br> | <p>Report discussion.</p> |
| **logo**<br>*String* <br> | <p>Report logo.</p> |
| **signedBy**<br>*String* <br> | <p>Indicates who has signed the report.</p> |
| **signature**<br>*String* <br> | <p>Report signature.</p> |
| **date**<br>*String* <br> | <p>Report date.</p> |

### ClinicalPriorityAnnotation
You can find the Java code [here](https://github.com/opencb/opencga/tree/TASK-153/opencga-core/src/main/java/org/opencb/opencga/core/models/study/configuration/ClinicalPriorityAnnotation.java).

| Field | Description |
| :---  | :--- |
| **id**<br>*String* <br> | <p>Object ID is a mandatory parameter when creating a new one, this ID cannot be changed at the moment.</p> |
| **description**<br>*String* <br> | <p>Users may provide a description for the entry.</p> |
| **rank**<br>*int* <br> | <p>ClinicalPriorityAnnotation rank.</p> |
| **date**<br>*String* <br> | <p>ClinicalPriorityAnnotation date.</p> |

### ClinicalAnalysisInternal
You can find the Java code [here](https://github.com/opencb/opencga/tree/TASK-153/opencga-core/src/main/java/org/opencb/opencga/core/models/clinical/ClinicalAnalysisInternal.java).

| Field | Description |
| :---  | :--- |
| **status**<br>*[Status](https://docs.opencga.opencb.org/data-models/clinicalanalysis#status)* <br> | <p>Status of the internal object.</p> |
| **registrationDate**<br>*String* <br> | <p>Registration date of the internal object.</p> |
| **lastModified**<br>*String* <br> | <p>Date of the last modification of the internal object.</p> |

### Interpretation
You can find the Java code [here](https://github.com/opencb/opencga/tree/TASK-153/opencga-core/src/main/java/org/opencb/opencga/core/models/clinical/Interpretation.java).

| Field | Description |
| :---  | :--- |
| **studyUid**<br>*long* <br> | <p>Study identifier.</p> |
| **uid**<br>*long* <br> | <p>Interpretation identifier.</p> |
| **panels**<br>*List<<a href="https://docs.opencga.opencb.org/data-models/clinicalanalysis#panel"><em>Panel</em></a>>* <br> | <p>Interpretation panel list.</p> |
| **internal**<br>*[InterpretationInternal](https://docs.opencga.opencb.org/data-models/clinicalanalysis#interpretationinternal)* <br> | <p>Internal field to manage the object.</p> |
| **release**<br>*int* <br> | <p>An integer describing the current data release.</p> |

### ClinicalAnalysisQualityControl
You can find the Java code [here](https://github.com/opencb/opencga/tree/TASK-153/opencga-core/src/main/java/org/opencb/opencga/core/models/clinical/ClinicalAnalysisQualityControl.java).

| Field | Description |
| :---  | :--- |
| **summary**<br>*QualityControlSummary* <br> | <p>ClinicalAnalysisQualityControl summary that can have the values HIGH, MEDIUM, LOW, DISCARD, NEEDS_REVIEW, UNKNOWN.</p> |
| **comments**<br>*List<<a href="https://docs.opencga.opencb.org/data-models/clinicalanalysis#clinicalcomment"><em>ClinicalComment</em></a>>* <br> | <p>List of ClinicalAnalysisQualityControl comments.</p> |
| **comments**<br>*List<<em>String</em>>* <br> | <p>List of ClinicalAnalysisQualityControl files.</p> |

### ClinicalConsentAnnotation
You can find the Java code [here](https://github.com/opencb/opencga/tree/TASK-153/opencga-core/src/main/java/org/opencb/opencga/core/models/study/configuration/ClinicalConsentAnnotation.java).

| Field | Description |
| :---  | :--- |
| **consents**<br>*List<<a href="https://docs.opencga.opencb.org/data-models/clinicalanalysis#clinicalconsentparam"><em>ClinicalConsentParam</em></a>>* <br> | <p>List of ClinicalConsentParam.</p> |
| **date**<br>*String* <br> | <p>Date of the ClinicalConsentAnnotation.</p> |

### FlagAnnotation
You can find the Java code [here](https://github.com/opencb/opencga/tree/TASK-153/opencga-core/src/main/java/org/opencb/opencga/core/models/common/FlagAnnotation.java).

| Field | Description |
| :---  | :--- |
| **id**<br>*String* <br> | <p>Object ID is a mandatory parameter when creating a new one, this ID cannot be changed at the moment.</p> |
| **description**<br>*String* <br> | <p>Users may provide a description for the entry.</p> |
| **date**<br>*String* <br> | <p>FlagAnnotation date.</p> |

### Panel
You can find the Java code [here](https://github.com/opencb/opencga/tree/TASK-153/opencga-core/src/main/java/org/opencb/opencga/core/models/panel/Panel.java).

| Field | Description |
| :---  | :--- |
| **uuid**<br>*String* <br> | <p>Unique 32-character identifier assigned automatically by OpenCGA.</p> |
| **release**<br>*int* <br> | <p>An integer describing the current data release.</p> |
| **version**<br>*int* <br> | <p>OpenCGA version of this panel, this is incremented when the panel is updated.</p> |
| **~~author~~**<br>*String* <br><br>_Deprecated_ | <p>Author of the panel.</p> |
| **status**<br>*[Status](https://docs.opencga.opencb.org/data-models/clinicalanalysis#status)* <br> | <p>Panel status can have the values READY or DELETED.</p> |
| **studyUid**<br>*long* <br> | <p>Panel reference to study.</p> |
| **uid**<br>*long* <br> | <p>Panel reference to study.</p> |

### ClinicalConsentParam
You can find the Java code [here](https://github.com/opencb/opencga/tree/TASK-153/opencga-core/src/main/java/org/opencb/opencga/core/models/study/configuration/ClinicalConsentParam.java).

| Field | Description |
| :---  | :--- |
| **id**<br>*String* <br> | <p>Object ID is a mandatory parameter when creating a new one, this ID cannot be changed at the moment.</p> |
| **name**<br>*String* <br> | <p>Name of the .</p> |
| **description**<br>*String* <br> | <p>Users may provide a description for the entry.</p> |
| **value**<br>*Value* <br> | <p>Value of the param that can have the values YES, NO and UNKNOWN.</p> |

### InterpretationInternal
You can find the Java code [here](https://github.com/opencb/opencga/tree/TASK-153/opencga-core/src/main/java/org/opencb/opencga/core/models/clinical/InterpretationInternal.java).

| Field | Description |
| :---  | :--- |
| **status**<br>*[InterpretationStatus](https://docs.opencga.opencb.org/data-models/clinicalanalysis#interpretationstatus)* <br> | <p>State of the interpretation that can have the values READY, DELETED, NOT_REVIEWED, UNDER_REVIEW, REVIEWED and REJECTED.</p> |
| **status**<br>*[Status](https://docs.opencga.opencb.org/data-models/clinicalanalysis#status)* <br> | <p>Status of the internal object.</p> |
| **registrationDate**<br>*String* <br> | <p>Registration date of the internal object.</p> |
| **lastModified**<br>*String* <br> | <p>Date of the last modification of the internal object.</p> |

### Status
You can find the Java code [here](https://github.com/opencb/biodata/tree/develop/biodata-models/src/main/java/org/opencb/biodata/models/common/Status.java).


### InterpretationStatus
You can find the Java code [here](https://github.com/opencb/opencga/tree/TASK-153/opencga-core/src/main/java/org/opencb/opencga/core/models/clinical/InterpretationStatus.java).

