 #Sample

 ##Overview

Sample data model hosts information about any biological material, normally extracted from an _Individual_, that is used for a particular analysis.
 This is the main data model, it stores the most basic and important information.

 ## Sample Field Attributes

 There are some attributes that a user can manipulate, while others are reserved for internal OpenCGA use

 ### Fields subjected to Update and Create Operations

 **Create Fields: `id`**

 **Update Fields: `individualId, fileIds, processing, collection, somatic, annotationSets, qualityControl, description, phenotypes, status, attributes`**

 ### Fields for OpenCGA Internal use \(immutable\)

 **`uuid, release, version, creationDate, modificationDate, internal`**
### Fields for Create Operations 
`id* processing collection qualityControl release version modificationDate description somatic phenotypes individualId fileIds status internal attributes `
### Fields for Update Operations
`processing collection qualityControl release version modificationDate description somatic phenotypes individualId fileIds status internal attributes `
### Fields uniques
`uuid creationDate `
## Data Model
###Sample
You can find the Java code [here](https://github.com/opencb/opencga/blob/master/opencga-core/src/main/java/org/opencb/opencga/core/models/sample/Sample.java).

| Field | Tags | Description |
| :--- | :--- | :--- |
| **id**<br>*String*|**`Required, Create, Immutable`** | Unique Sample ID in the study, this can be repeated across different studies. This is a mandatory parameter in the creation and cannot be changed at the moment.</br></br> |
| **uuid**<br>*String*|**`Internal, Unique, Immutable`** | Global unique ID in any study of any OpenCGA installation. This is created during the sample creation and cannot be changed.</br></br> |
| **processing**<br>*<a href="Sample.md#SampleProcessing"><em>SampleProcessing</em></a>*| **`Updatable`** | An object describing how to sample was processed.</br></br> |
| **collection**<br>*<a href="Sample.md#SampleCollection"><em>SampleCollection</em></a>*| **`Updatable`** | An object describing how the sample was collected.</br></br> |
| **qualityControl**<br>*<a href="Sample.md#SampleQualityControl"><em>SampleQualityControl</em></a>*|  | </br></br> |
| **release**<br>*int*|**`Updatable`** | An integer describing the current release.</br></br> |
| **version**<br>*int*|**`Updatable`** | An integer describing the current version.</br></br> |
| **creationDate**<br>*String*|**`Internal, Unique, Immutable`** | An string describing the creation date.</br></br> |
| **modificationDate**<br>*String*|**`Updatable`** | An string describing the last modification date.</br></br> |
| **description**<br>*String*|**`Updatable`** | An string to describe the properties of the sample.</br></br> |
| **somatic**<br>*boolean*| | </br></br> |
| **phenotypes**<br>*List*|**`Updatable`** | A List with related phenotypes.</br></br> |
| **individualId**<br>*String*|**`Updatable`** | A reference to the Individual containing this sample. Notice that samples can exist without and Individual ID, this field is not mandatory..</br></br> |
| **fileIds**<br>*List*|**`Updatable`** | List of File ID containing this sample, eg BAM, VCF, QC images, ...</br></br> |
| **status**<br>*<a href="Sample.md#CustomStatus"><em>CustomStatus</em></a>*| **`Updatable`** | An object describing the status of the Sample.</br></br> |
| **internal**<br>*<a href="Sample.md#SampleInternal"><em>SampleInternal</em></a>*|  | </br></br> |
| **attributes**<br>*Map*| | </br></br> |
## Related data models
###SampleCollection
You can find the Java code [here](https://github.com/opencb/opencga/blob/master/opencga-core/src/main/java/org/opencb/opencga/core/models/sample/SampleCollection.java).

| Field | Tags | Description |
| :--- | :--- | :--- |
| **tissue**<br>*String*| | </br></br> |
| **organ**<br>*String*| | </br></br> |
| **quantity**<br>*String*| | </br></br> |
| **method**<br>*String*| | </br></br> |
| **date**<br>*String*| | </br></br> |
| **attributes**<br>*Map*| | </br></br> |
###CustomStatus
You can find the Java code [here](https://github.com/opencb/opencga/blob/master/opencga-core/src/main/java/org/opencb/opencga/core/models/common/CustomStatus.java).

| Field | Tags | Description |
| :--- | :--- | :--- |
| **name**<br>*String*| | </br></br> |
| **description**<br>*String*| | </br></br> |
| **date**<br>*String*| | </br></br> |
###SampleProcessing
You can find the Java code [here](https://github.com/opencb/opencga/blob/master/opencga-core/src/main/java/org/opencb/opencga/core/models/sample/SampleProcessing.java).

| Field | Tags | Description |
| :--- | :--- | :--- |
| **product**<br>*String*| | </br></br> |
| **preparationMethod**<br>*String*| | </br></br> |
| **extractionMethod**<br>*String*| | </br></br> |
| **labSampleId**<br>*String*| | </br></br> |
| **quantity**<br>*String*| | </br></br> |
| **date**<br>*String*| | </br></br> |
| **attributes**<br>*Map*| | </br></br> |
###SampleInternal
You can find the Java code [here](https://github.com/opencb/opencga/blob/master/opencga-core/src/main/java/org/opencb/opencga/core/models/sample/SampleInternal.java).

| Field | Tags | Description |
| :--- | :--- | :--- |
| **status**<br>*<a href="SampleInternal.md#Status"><em>Status</em></a>*|  | </br></br> |
| **rga**<br>*<a href="SampleInternal.md#RgaIndex"><em>RgaIndex</em></a>*|  | </br></br> |
###Status
You can find the Java code [here](https://github.com/opencb/opencga/blob/master/opencga-core/src/main/java/org/opencb/opencga/core/models/common/Status.java).

| Field | Tags | Description |
| :--- | :--- | :--- |
| **name**<br>*String*| | </br></br> |
| **date**<br>*String*| | </br></br> |
| **description**<br>*String*| | </br></br> |
| **message**<br>*String*| | </br></br> |
| **READY**<br>*String*| | READY name means that the object is being used.</br></br> |
| **DELETED**<br>*String*| | DELETED name means that the object is marked as removed, so it can be completely removed from the database with a clean action.</br></br> |
| **STATUS_LIST**<br>*List*| | </br></br> |
###RgaIndex
You can find the Java code [here](https://github.com/opencb/opencga/blob/master/opencga-core/src/main/java/org/opencb/opencga/core/models/common/RgaIndex.java).

| Field | Tags | Description |
| :--- | :--- | :--- |
| **status**<br>*<a href="RgaIndex.md#Status"><em>Status</em></a>*|  | </br></br> |
| **date**<br>*String*| | </br></br> |
###Status
You can find the Java code [here](https://github.com/opencb/opencga/blob/master/opencga-core/src/main/java/org/opencb/opencga/core/models/common/RgaIndex/Status.java).

| Field | Tags | Description |
| :--- | :--- | :--- |
| **NOT_INDEXED**<br>*<a href="Status.md#Status"><em>Status</em></a>*|  | </br></br> |
| **INDEXED**<br>*<a href="Status.md#Status"><em>Status</em></a>*|  | </br></br> |
| **INVALID_PERMISSIONS**<br>*<a href="Status.md#Status"><em>Status</em></a>*|  | </br></br> |
| **INVALID_METADATA**<br>*<a href="Status.md#Status"><em>Status</em></a>*|  | </br></br> |
| **INVALID**<br>*<a href="Status.md#Status"><em>Status</em></a>*|  | </br></br> |
###SampleQualityControl
You can find the Java code [here](https://github.com/opencb/opencga/blob/master/opencga-core/src/main/java/org/opencb/opencga/core/models/sample/SampleQualityControl.java).

| Field | Tags | Description |
| :--- | :--- | :--- |
| **files**<br>*List*| | </br></br> |
| **comments**<br>*List*| | </br></br> |
| **variantMetrics**<br>*<a href="SampleQualityControl.md#SampleVariantQualityControlMetrics"><em>SampleVariantQualityControlMetrics</em></a>*|  | </br></br> |
###SampleVariantQualityControlMetrics
You can find the Java code [here](https://github.com/opencb/opencga/blob/master/opencga-core/src/main/java/org/opencb/opencga/core/models/sample/SampleVariantQualityControlMetrics.java).

| Field | Tags | Description |
| :--- | :--- | :--- |
| **variantStats**<br>*List*| | </br></br> |
| **signatures**<br>*List*| | </br></br> |
| **genomePlots**<br>*List*| | </br></br> |
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
