# Sample

## Overview

Sample data model ... 

## Data Models

### Sample

This is the main data model, it stores the most basic and important information. You can find the Java [here](https://github.com/opencb/opencga/blob/master/opencga-core/src/main/java/org/opencb/opencga/core/models/sample/Sample.java).

| Field                                                                                                                                     | Description                                                                                                                                                                                                                      |
| ----------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| <p><strong>id</strong></p><p><em>String</em></p>                                                                                          | <p>Unique Sample ID in the study, this can be repeated across different studies. This is a mandatory parameter in the creation and cannot be changed at the moment.</p><p><em>Constraints: Mandatory, Unique, Immutable</em></p> |
| <p><strong>uuid</strong></p><p><em>String</em></p>                                                                                        | <p>Global unique ID in any study of any OpenCGA installation. This is created during the sample creation and cannot be changed.</p><p><em>Constraints: Internal, Unique, Immutable</em></p>                                      |
| <p><strong>individualId</strong></p><p><em>String</em></p>                                                                                | A reference to the [Individual](individual.md) containing this sample. Notice that samples can exist without and Individual ID, this field is not mandatory.                                                                     |
| <p><strong>fileIds</strong></p><p><em>List&#x3C;String></em></p>                                                                          | List of [File ID](./#file-id) containing this sample, eg BAM, VCF, QC images, ...                                                                                                                                                |
| <p><strong>processing</strong></p><p><em></em><a href="sample.md#sampleprocessing"><em>SampleProcessing</em></a><em></em></p>             | An object describing how to sample was processed.                                                                                                                                                                                |
| <p><strong>collection</strong></p><p><em></em><a href="sample.md#samplecollection"><em>SampleCollection</em></a><em></em></p>             | An object describing how the sample was collected.                                                                                                                                                                               |
| <p><strong>somatic</strong></p><p><em>Boolean</em></p>                                                                                    |                                                                                                                                                                                                                                  |
| <p><strong>annotationSets</strong></p><p><em>List&#x3C;</em><a href="./#annotation-set"><em>AnnotationSet</em></a><em>></em></p>          |                                                                                                                                                                                                                                  |
| <p><strong>qualityControl</strong></p><p><em></em><a href="sample.md#samplequalitycontrol"><em>SampleQualityControl</em></a><em></em></p> |                                                                                                                                                                                                                                  |
| <p><strong>release</strong></p><p><em>Integer</em></p>                                                                                    |                                                                                                                                                                                                                                  |
| <p><strong>version</strong></p><p><em>Integer</em></p>                                                                                    |                                                                                                                                                                                                                                  |
| <p><strong>creationDate</strong></p><p><em>String</em></p>                                                                                | A string representing the creation date in format YYYYMMDDHHmmss                                                                                                                                                                 |
| <p><strong>modificationDate</strong></p><p><em>String</em></p>                                                                            | A string representing the modification date in format YYYYMMDDHHmmss                                                                                                                                                             |
| <p><strong>description</strong></p><p><em>String</em></p>                                                                                 |                                                                                                                                                                                                                                  |
| <p><strong>phenotypes</strong></p><p><em>List&#x3C;</em><a href="./#phenotype"><em>Phenotype</em></a><em>></em></p>                       |                                                                                                                                                                                                                                  |
| <p><strong>status</strong></p><p><em></em><a href="./#status"><em>CustomStatus</em></a><em></em></p>                                      |                                                                                                                                                                                                                                  |
| <p><strong>internal</strong></p><p><em>SampleInternal</em></p>                                                                            |                                                                                                                                                                                                                                  |
| <p><strong>attributes</strong></p><p><em>Map</em></p>                                                                                     |                                                                                                                                                                                                                                  |

### SampleProcessing

This object describes how the sample was processed in the lab.

| Field                                                           | Description                                                    |
| --------------------------------------------------------------- | -------------------------------------------------------------- |
| <p><strong>product</strong></p><p><em>String</em></p>           | Type of product sequenced, this can be DNA or RNA for instance |
| <p><strong>preparationMethod</strong></p><p><em>String</em></p> |                                                                |
| <p><strong>extractionMethod</strong></p><p><em>String</em></p>  |                                                                |
| <p><strong>labSampleId</strong></p><p><em>String</em></p>       |                                                                |
| <p>quantity</p><p><em>String</em></p>                           |                                                                |
| <p>date</p><p><em>String</em></p>                               |                                                                |

### SampleCollection



### SampleQualityControl



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
