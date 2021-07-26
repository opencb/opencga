# Sample

## Overview

Sample data model hosts information about any biological material, normally extracted from an _Individual_, that is used for a particular analysis. This is the main data model, it stores the most basic and important information. ![](../../.gitbook/assets/image%20%286%29%20%285%29%20%285%29%20%285%29%20%285%29%20%283%29.png) 

### Summary

| Field | create | update | unique | required |
| :--- | :---: | :---: | :---: | :---: |
| id | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/check.png) |  | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/check.png) | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/check.png) |
| processing | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/check.png) | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/check.png) | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/error.png) | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/error.png) |
| collection | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/check.png) | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/check.png) | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/error.png) | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/error.png) |
| qualityControl | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/check.png) | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/check.png) | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/error.png) | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/error.png) |
| description | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/check.png) | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/check.png) | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/error.png) | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/error.png) |
| somatic | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/check.png) | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/check.png) | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/error.png) | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/error.png) |
| phenotypes | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/check.png) | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/check.png) | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/error.png) | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/error.png) |
| individualId | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/check.png) | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/check.png) | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/error.png) | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/error.png) |
| fileIds | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/check.png) | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/check.png) | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/error.png) | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/error.png) |
| status | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/check.png) | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/check.png) | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/error.png) | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/error.png) |
| attributes | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/check.png) | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/check.png) | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/error.png) | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/error.png) |
| uuid | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/error.png) | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/error.png) | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/check.png) | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/error.png) |
| release | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/error.png) | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/error.png) | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/error.png) | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/error.png) |
| version | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/error.png) | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/error.png) | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/error.png) | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/error.png) |
| creationDate | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/error.png) | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/error.png) | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/error.png) | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/error.png) |
| modificationDate | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/error.png) | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/error.png) | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/error.png) | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/error.png) |
| internal | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/error.png) | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/error.png) | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/error.png) | ![](http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/error.png) |

## Data Model

### Sample

You can find the Java code [here](https://github.com/opencb/opencga/blob/master/opencga-core/src/main/java/org/opencb/opencga/core/models/sample/Sample.java).

| Field | Description |
| :--- | :--- |
| **id**  _String_   | Sample ID in the study, this must be unique in the study but can be repeated in different studies. This is a mandatory parameter  when creating a new sample, this ID cannot be changed at the moment._Tags_: _required, immutable, unique_ |
| **uuid**  _String_   | Global unique ID at the whole OpenCGA installation. This is automatically created during the sample creation and cannot be changed._Tags_: _internal, unique, immutable_ |
| **processing** [_SampleProcessing_](sample.md#SampleProcessing)   | Describes how the sample was processed in the lab. |
| **collection** [_SampleCollection_](sample.md#SampleCollection)   _since_: 2.1 | Describes how the sample was collected._Note_: _The sample collection is a list of samples_ |
| **qualityControl** [_SampleQualityControl_](sample.md#SampleQualityControl)   _since_: 2.1 | Contains different metrics to evaluate the quality of the sample._Note_: _The sample collection is a list of samples_&lt;/br&gt;_More info at_: [ZetaGenomics](https://www.zettagenomics.com) |
| **release**  _int_   | An integer describing the current data release._Tags_: _internal_ |
| **version**  _int_   | An integer describing the current version._Tags_: _internal_ |
| **creationDate**  _String_   | String representing when the sample was created, this is automatically set by OpenCGA._Tags_: _internal_ |
| **modificationDate**  _String_   | String representing when was the last time the sample was modified, this is automatically set by OpenCGA._Tags_: _internal_ |
| **description**  _String_   | An string to describe the properties of the sample. |
| **somatic**  _boolean_   | Indicates if the sample is somatic or germline \(default\) |
| **phenotypes**  List&lt;_Phenotype_&gt;   | A List with related phenotypes. |
| **individualId**  _String_   | A reference to the Individual containing this sample. Notice that samples can exist without and Individual ID, this field is not  mandatory.._More info at_: [ZetaGenomics](https://www.zettagenomics.com) |
| **fileIds**  List&lt;_String_&gt;   _Deprecated_ | List of File ID containing this sample, eg BAM, VCF, QC images, ... |
| **status** [_CustomStatus_](sample.md#CustomStatus)   | An object describing the status of the Sample. |
| **internal** [_SampleInternal_](sample.md#SampleInternal)   | An object describing the internal information of the Sample. This is managed by OpenCGA._Tags_: _internal_ |
| **attributes**  Map&lt;_Object_,_String_&gt;   | You can use this field to store any other information, keep in mind this is not indexed so you cannot search by attributes. |

### SampleInternal

You can find the Java code [here](https://github.com/opencb/opencga/blob/master/opencga-core/src/main/java/org/opencb/opencga/core/models/sample/SampleInternal.java).

| Field | Description |
| :--- | :--- |
| **status** [_Status_](sample.md#Status)   |  |
| **rga** [_RgaIndex_](sample.md#RgaIndex)   |  |

### StudyInternal

You can find the Java code [here](https://github.com/opencb/opencga/blob/master/opencga-core/src/main/java/org/opencb/opencga/core/models/study/StudyInternal.java).

| Field | Description |
| :--- | :--- |
| **status** [_Status_](sample.md#Status)   |  |
| **configuration** [_StudyConfiguration_](sample.md#StudyConfiguration)   |  |

### ProjectInternal

You can find the Java code [here](https://github.com/opencb/opencga/blob/master/opencga-core/src/main/java/org/opencb/opencga/core/models/project/ProjectInternal.java).

| Field | Description |
| :--- | :--- |
| **datastores** [_Datastores_](sample.md#Datastores)   |  |
| **cellbase** [_CellBaseConfiguration_](sample.md#CellBaseConfiguration)   |  |
| **status** [_Status_](sample.md#Status)   |  |

### CohortInternal

You can find the Java code [here](https://github.com/opencb/opencga/blob/master/opencga-core/src/main/java/org/opencb/opencga/core/models/cohort/CohortInternal.java).

| Field | Description |
| :--- | :--- |
| **status** [_CohortStatus_](sample.md#CohortStatus)   |  |

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

