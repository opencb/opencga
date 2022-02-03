# File
## Overview
File data model hosts information about any file.
### Summary 
| Field | Create | Update | Unique | Required|
| :--- | :---: | :---: |:---: |:---: |
| id | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |
| uuid | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| name | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| type | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| format | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| bioformat | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| checksum | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| uri | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| path | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| release | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| creationDate | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| modificationDate | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| description | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| external | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| size | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| software | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| experiment | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| sampleIds | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| jobId | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| tags | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| relatedFiles | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| qualityControl | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| stats | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| status | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| internal | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |
| attributes | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png?raw=true"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png?raw=true"> |

## Data Model

### File
You can find the Java code [here](https://github.com/opencb/opencga/tree/issue-1806/opencga-core/src/main/java/org/opencb/opencga/core/models/file/File.java).

| Field | Description |
| :---  | :--- |
| **id**<br>*String* <br> | <p>Object ID is a mandatory parameter when creating a new one, this ID cannot be changed at the moment.</p> |
| **uuid**<br>*String* <br> | <p>Unique 32-character identifier assigned automatically by OpenCGA.</p> |
| **name**<br>*String* <br> | <p>The name of the file.</p> |
| **type**<br>*Type* <br> | <p>The type can have the values FILE or DIRECTORY.</p> |
| **format**<br>*Format* <br> | <p>The format can have the values VCF, BCF, GVCF, TBI, BIGWIG, SAM, BAM, BAI, CRAM, CRAI, FASTQ, FASTA, PED, TAB_SEPARATED_VALUES, COMMA_SEPARATED_VALUES, XML, PROTOCOL_BUFFER, JSON, AVRO, PARQUET, IMAGE, PLAIN, BINARY, NONE and UNKNOWN.</p> |
| **bioformat**<br>*Bioformat* <br> | <p>The bioformat can have the values MICROARRAY_EXPRESSION_ONECHANNEL_AGILENT, MICROARRAY_EXPRESSION_ONECHANNEL_AFFYMETRIX, MICROARRAY_EXPRESSION_ONECHANNEL_GENEPIX, MICROARRAY_EXPRESSION_TWOCHANNELS_AGILENT, MICROARRAY_EXPRESSION_TWOCHANNELS_GENEPIX, DATAMATRIX_EXPRESSION, IDLIST, IDLIST_RANKED, ANNOTATION_GENEVSANNOTATION, OTHER_NEWICK, OTHER_BLAST, OTHER_INTERACTION, OTHER_GENOTYPE, OTHER_PLINK, OTHER_VCF, OTHER_PED, @Deprecated VCF4, VARIANT, ALIGNMENT, COVERAGE, SEQUENCE, PEDIGREE, REFERENCE_GENOME, NONE and UNKNOWN.</p> |
| **checksum**<br>*String* <br> | <p>The checksum of the file.</p> |
| **uri**<br>*[URI](https://docs.opencga.opencb.org/data-models/file#uri)* <br> | <p>The uri of the file.</p> |
| **path**<br>*String* <br> | <p>The path of the file.</p> |
| **release**<br>*int* <br> | <p>An integer describing the current data release.</p> |
| **creationDate**<br>*String* <br> | <p>Autogenerated date following the format YYYYMMDDhhmmss containing the date when the entry was first registered.</p> |
| **modificationDate**<br>*String* <br><br>_since_: 1.0 | <p>Autogenerated date following the format YYYYMMDDhhmmss containing the date when the entry was last modified.</p> |
| **description**<br>*String* <br> | <p>Users may provide a description for the entry.</p> |
| **external**<br>*boolean* <br> | <p>Indicates the file is external or not.</p> |
| **size**<br>*long* <br> | <p>The size of the file.</p> |
| **software**<br>*[Software](https://docs.opencga.opencb.org/data-models/file#software)* <br> | <p>Software related with file.</p> |
| **experiment**<br>*[FileExperiment](https://docs.opencga.opencb.org/data-models/file#fileexperiment)* <br> | <p>File experiment.</p> |
| **sampleIds**<br>*List<<em>String</em>>* <br> | <p>List of sample ids of the file.</p> |
| **jobId**<br>*String* <br> | <p>File job id.</p> |
| **tags**<br>*List<<em>String</em>>* <br> | <p>File tags.</p> |
| **relatedFiles**<br>*List<<a href="https://docs.opencga.opencb.org/data-models/file#filerelatedfile"><em>FileRelatedFile</em></a>>* <br> | <p>List of objects FileRelatedFiles describing related files.</p> |
| **qualityControl**<br>*[FileQualityControl](https://docs.opencga.opencb.org/data-models/file#filequalitycontrol)* <br> | <p>Contains different metrics to evaluate the quality of the individual.</p> |
| **~~stats~~**<br>*Map<String,Object>* <br><br>_Deprecated_ | <p>Stats of the object.</p> |
| **status**<br>*[Status](https://docs.opencga.opencb.org/data-models/file#status)* <br> | <p>Object to set a custom status.</p> |
| **internal**<br>*[FileInternal](https://docs.opencga.opencb.org/data-models/file#fileinternal)* <br> | <p>Internal field for manage the object.</p> |
| **attributes**<br>*Map<String,Object>* <br> | <p>You can use this field to store any other information, keep in mind this is not indexed so you cannot search by attributes.</p> |

### Status
You can find the Java code [here](https://github.com/opencb/biodata/tree/develop/biodata-models/src/main/java/org/opencb/biodata/models/common/Status.java).


### FileExperiment
You can find the Java code [here](https://github.com/opencb/opencga/tree/issue-1806/opencga-core/src/main/java/org/opencb/opencga/core/models/file/FileExperiment.java).


### Software
You can find the Java code [here](https://github.com/opencb/biodata/tree/develop/biodata-models/src/main/java/org/opencb/biodata/models/clinical/interpretation/Software.java).

| Field | Description |
| :---  | :--- |
| **name**<br>*String* <br> | <p>Software name</p> |
| **version**<br>*String* <br> | <p>Software version</p> |
| **repository**<br>*String* <br> | <p>Software repository</p> |
| **commit**<br>*String* <br> | <p>Software commit</p> |
| **website**<br>*String* <br> | <p>Software website</p> |
| **params**<br>*Map<String,String>* <br> | <p>Software params</p> |

### FileQualityControl
You can find the Java code [here](https://github.com/opencb/opencga/tree/issue-1806/opencga-core/src/main/java/org/opencb/opencga/core/models/file/FileQualityControl.java).


### FileRelatedFile
You can find the Java code [here](https://github.com/opencb/opencga/tree/issue-1806/opencga-core/src/main/java/org/opencb/opencga/core/models/file/FileRelatedFile.java).


### URI
You can find the Java code [here](https://github.com/opencb/opencga/tree/issue-1806/opencga-core/src/main/java/java/net/URI.java).


### FileInternal
You can find the Java code [here](https://github.com/opencb/opencga/tree/issue-1806/opencga-core/src/main/java/org/opencb/opencga/core/models/file/FileInternal.java).

| Field | Description |
| :---  | :--- |
| **status**<br>*[FileStatus](https://docs.opencga.opencb.org/data-models/file#filestatus)* <br> | <p>File status can have the values READY, DELETED, TRASHED, STAGE, MISSING, PENDING_DELETE, DELETING, REMOVED and MISSING_SAMPLES.</p> |
| **variant**<br>*[FileInternalVariant](https://docs.opencga.opencb.org/data-models/file#fileinternalvariant)* <br> | <p>File internal variant.</p> |
| **alignment**<br>*[FileInternalAlignment](https://docs.opencga.opencb.org/data-models/file#fileinternalalignment)* <br> | <p>File internal alignment.</p> |
| **sampleMap**<br>*Map<String,String>* <br> | <p>Map of samples.</p> |
| **missingSamples**<br>*[MissingSamples](https://docs.opencga.opencb.org/data-models/file#missingsamples)* <br> | <p>Object describes missing samples.</p> |
| **status**<br>*[Status](https://docs.opencga.opencb.org/data-models/file#status)* <br> | <p>Status of the internal object.</p> |
| **registrationDate**<br>*String* <br> | <p>Registration date of the internal object.</p> |
| **lastModified**<br>*String* <br> | <p>Date of the last modification of the internal object.</p> |

### FileInternalAlignment
You can find the Java code [here](https://github.com/opencb/opencga/tree/issue-1806/opencga-core/src/main/java/org/opencb/opencga/core/models/file/FileInternalAlignment.java).


### MissingSamples
You can find the Java code [here](https://github.com/opencb/opencga/tree/issue-1806/opencga-core/src/main/java/org/opencb/opencga/core/models/file/MissingSamples.java).

| Field | Description |
| :---  | :--- |
| **existing**<br>*List<<em>String</em>>* <br> | <p>List of existing samples.</p> |
| **nonExisting**<br>*List<<em>String</em>>* <br> | <p>List of non existing samples.</p> |

### FileInternalVariant
You can find the Java code [here](https://github.com/opencb/opencga/tree/issue-1806/opencga-core/src/main/java/org/opencb/opencga/core/models/file/FileInternalVariant.java).


### FileStatus
You can find the Java code [here](https://github.com/opencb/opencga/tree/issue-1806/opencga-core/src/main/java/org/opencb/opencga/core/models/file/FileStatus.java).

