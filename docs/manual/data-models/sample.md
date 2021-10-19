# Sample
## Overview
Sample data model hosts information about any biological material, normally extracted from an _Individual_, that is used for a particular
 analysis. This is the main data model, it stores the most basic and important information.
### Summary 
| Field | Create | Update | Unique | Required|
| :--- | :---: | :---: |:---: |:---: |
| id | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png"> |
| uuid | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png"> |
| processing | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png"> |
| collection | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png"> |
| qualityControl | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png"> |
| release | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png"> |
| version | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png"> |
| creationDate | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png"> |
| modificationDate | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png"> |
| description | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png"> |
| somatic | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png"> |
| phenotypes | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png"> |
| individualId | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png"> |
| fileIds | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png"> |
| cohortIds | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png"> |
| status | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png"> |
| internal | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png"> |
| attributes | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/yes.png"> |<img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png"> | <img src="https://github.com/opencb/opencga/blob/develop/docs/data-models/no.png"> |

## Data Model
### Sample
You can find the Java code [here](https://github.com/opencb/opencga/blob/master/opencga-core/src/main/java/org/opencb/opencga/core/models/sample/Sample.java).

| Field | Description |
| :---  | :--- |
| **id**<br> *String* <br> | <p>Sample ID in the study, this must be unique in the study but can be repeated in different studies. This is a mandatory parameter<br> when creating a new sample, this ID cannot be changed at the moment.</p>_Tags_: _required, immutable, unique_ |
| **uuid**<br> *String* <br> | <p>Generic: Unique 32-character identifier assigned automatically by OpenCGA.</p>_Tags_: _immutable, unique_ |
| **processing**<br>*<a href="sample.md#SampleProcessing"><em>SampleProcessing</em></a>* <br> | <p>Describes how the sample was processed in the lab.</p> |
| **collection**<br>*<a href="sample.md#SampleCollection"><em>SampleCollection</em></a>* <br><br>_since_: 2.1 | <p>Describes how the sample was collected.</p>_Note_: _The sample collection is a list of samples_ |
| **qualityControl**<br>*<a href="sample.md#SampleQualityControl"><em>SampleQualityControl</em></a>* <br><br>_since_: 2.1 | <p>Contains different metrics to evaluate the quality of the sample.</p>_Note_: _The sample collection is a list of samples_</br>_More info at_: <a href="https://www.zettagenomics.com">ZetaGenomics</a> |
| **release**<br> *int* <br> | <p>An integer describing the current data release.</p>_Tags_: _immutable_ |
| **version**<br> *int* <br> | <p>Generic: Autoincremental version assigned to the registered entry. By default, updates does not create new versions. To enable<br> versioning, users must set the `incVersion` flag from the /update web service when updating the document.</p>_Tags_: _immutable_ |
| **creationDate**<br> *String* <br> | <p>Generic: Autogenerated date following the format YYYYMMDDhhmmss containing the date when the entry was first registered.</p>_Tags_: _immutable_ |
| **modificationDate**<br> *String* <br> | <p>Generic: Autogenerated date following the format YYYYMMDDhhmmss containing the date when the entry was last modified.</p>_Tags_: _immutable_ |
| **description**<br> *String* <br> | <p>Generic: Users may provide a description for the entry.</p> |
| **somatic**<br> *boolean* <br> | <p>Indicates if the sample is somatic or germline (default)</p> |
| **phenotypes**<br> List<*Phenotype*> <br> | <p></p> |
| **individualId**<br> *String* <br> | <p></p> |
| **fileIds**<br> List<*String*> <br> | <p></p> |
| **cohortIds**<br> List<*String*> <br> | <p></p> |
| **status**<br>*<a href="sample.md#CustomStatus"><em>CustomStatus</em></a>* <br> | <p>Generic: Object to define the status of the entry.</p> |
| **internal**<br>*<a href="sample.md#SampleInternal"><em>SampleInternal</em></a>* <br> | <p>Generic: Field automatically managed by OpenCGA containing relevant information of the entry. This field is used for internal<br> purposes and is visible for users.</p>_Tags_: _immutable_ |
| **attributes**<br> Map<*Object*,*String*> <br> | <p>Dictionary that can be customised by users to store any additional information users may require.</p>_Note_: _This field is not meant to be queried. It should only contain extra information. To store additional information meant to<br> be queried, please use annotationSets._ |
### SampleCollection
You can find the Java code [here](https://github.com/opencb/opencga/blob/master/opencga-core/src/main/java/org/opencb/opencga/core/models/sample/SampleCollection.java).

| Field | Description |
| :---  | :--- |
| **tissue**<br> *String* <br> | <p></p> |
| **organ**<br> *String* <br> | <p></p> |
| **quantity**<br> *String* <br> | <p></p> |
| **method**<br> *String* <br> | <p></p> |
| **date**<br> *String* <br> | <p></p> |
| **attributes**<br> Map<*Object*,*String*> <br> | <p></p> |
### SampleQualityControl
You can find the Java code [here](https://github.com/opencb/opencga/blob/master/opencga-core/src/main/java/org/opencb/opencga/core/models/sample/SampleQualityControl.java).

| Field | Description |
| :---  | :--- |
| **fileIds**<br> List<*String*> <br> | <p></p> |
| **comments**<br> List<*ClinicalComment*> <br> | <p></p> |
| **variant**<br>*<a href="sample.md#SampleVariantQualityControlMetrics"><em>SampleVariantQualityControlMetrics</em></a>* <br> | <p></p> |
### SampleVariantQualityControlMetrics
You can find the Java code [here](https://github.com/opencb/opencga/blob/master/opencga-core/src/main/java/org/opencb/opencga/core/models/sample/SampleVariantQualityControlMetrics.java).

| Field | Description |
| :---  | :--- |
| **variantStats**<br> List<*SampleQcVariantStats*> <br> | <p></p> |
| **signatures**<br> List<*Signature*> <br> | <p></p> |
| **genomePlots**<br> List<*GenomePlot*> <br> | <p></p> |
### CustomStatus
You can find the Java code [here](https://github.com/opencb/opencga/blob/master/opencga-core/src/main/java/org/opencb/opencga/core/models/common/CustomStatus.java).

| Field | Description |
| :---  | :--- |
| **name**<br> *String* <br> | <p>Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed vestibulum aliquet lobortis. Pellentesque venenatis lacus quis nibh<br> interdum finibus.</p>_Tags_: _required, immutable_ |
| **description**<br> *String* <br> | <p>Proin aliquam ante in ligula tincidunt, cursus volutpat urna suscipit. Phasellus interdum, libero at posuere blandit, felis dui<br> dignissim leo, quis ullamcorper felis elit a augue.</p>_Tags_: _required_ |
| **date**<br> *String* <br> | <p>Nullam commodo tortor nec lectus cursus finibus. Sed quis orci fringilla, cursus diam quis, vehicula sapien. Etiam bibendum dapibus<br> lectus, ut ultrices nunc vulputate ac.</p>_Tags_: _internal, unique, immutable_ |
### SampleProcessing
You can find the Java code [here](https://github.com/opencb/opencga/blob/master/opencga-core/src/main/java/org/opencb/opencga/core/models/sample/SampleProcessing.java).

| Field | Description |
| :---  | :--- |
| **product**<br> *String* <br> | <p></p> |
| **preparationMethod**<br> *String* <br> | <p></p> |
| **extractionMethod**<br> *String* <br> | <p></p> |
| **labSampleId**<br> *String* <br> | <p></p> |
| **quantity**<br> *String* <br> | <p></p> |
| **date**<br> *String* <br> | <p></p> |
| **attributes**<br> Map<*Object*,*String*> <br> | <p></p> |
### SampleInternal
You can find the Java code [here](https://github.com/opencb/opencga/blob/master/opencga-core/src/main/java/org/opencb/opencga/core/models/sample/SampleInternal.java).

| Field | Description |
| :---  | :--- |
| **rga**<br>*<a href="sample.md#RgaIndex"><em>RgaIndex</em></a>* <br> | <p></p> |
### RgaIndex
You can find the Java code [here](https://github.com/opencb/opencga/blob/master/opencga-core/src/main/java/org/opencb/opencga/core/models/common/RgaIndex.java).

| Field | Description |
| :---  | :--- |
| **status**<br>*<a href="sample.md#enum-rgaindexstatus"><em>RgaIndex.Status</em></a>* <br> | <p>Nullam commodo tortor nec lectus cursus finibus. Sed quis orci fringilla, cursus diam quis, vehicula sapien. Etiam bibendum dapibus<br> lectus, ut ultrices nunc vulputate ac.</p>_Tags_: _internal, unique, immutable_ |
| **date**<br> *String* <br> | <p>Nullam commodo tortor nec lectus cursus finibus. Sed quis orci fringilla, cursus diam quis, vehicula sapien. Etiam bibendum dapibus<br> lectus, ut ultrices nunc vulputate ac.</p>_Tags_: _internal, unique, immutable_ |
### Enum RgaIndex.Status
_Enumeration class._
You can find the Java code [here](https://github.com/opencb/opencga/blob/master/opencga-core/src/main/java/org/opencb/opencga/core/models/common/RgaIndex/Status.java).

| Field | Description |
| :---  | :--- |
| **NOT_INDEXED** <br> | <p></p> |
| **INDEXED** <br> | <p></p> |
| **INVALID_PERMISSIONS** <br> | <p></p> |
| **INVALID_METADATA** <br> | <p></p> |
| **INVALID** <br> | <p></p> |
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