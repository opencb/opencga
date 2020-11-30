# Sample

## Overview

Sample data model ...

Legend:

* U - Unique: this field must be unique in the study
* M - Mandatory: it is a mandatory parameter in the create\(\)
* I - Immutable: cannot be changed in the updated\(\)
* O - OpenCGA: managed by OpenCGA, no create\(\) or update\(\) permitted

## Data Model

### Sample

| Field | Type | Constraints | Description |
| :--- | :--- | :--- | :--- |
| **id** | String | U, M, I | Unique ID in the study |
| **uuid** | String | U, I |  |
| **individualId** | String | U |  |
| **fileIds** | List&lt;String&gt; |  |  |
| **processing** | SampleProcessing |  |  |
| **collection** | SampleCollection |  |  |
| **somatic** | Boolean |  |  |
| **annotationSets** | List&lt;[AnnotationSet](./#annotation-set)&gt; |  |  |
| **qualityControl** | SampleQualityControl |  |  |
| **release** | Integer | O |  |
| **version** | Integer | O |  |
| **creationDate** | String | O |  |
| **modificationDate** | String | O |  |
| **description** | String |  |  |
| **phenotypes** | List&lt;Phenotype&gt; |  |  |
| **status** | CustomStatus |  |  |
| **internal** | SampleInternal | O |  |
| **attributes** | Map |  |  |

### SampleProcessing



### SampleCollection



## Example

This is a JSON example:

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

