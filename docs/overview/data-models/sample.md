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

<table>
  <thead>
    <tr>
      <th style="text-align:left">Field</th>
      <th style="text-align:left">Constraints</th>
      <th style="text-align:left">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td style="text-align:left">
        <p><b>id</b>
        </p>
        <p><em>String</em>
        </p>
      </td>
      <td style="text-align:left">U, M, I</td>
      <td style="text-align:left">Unique ID in the study</td>
    </tr>
    <tr>
      <td style="text-align:left">
        <p><b>uuid</b>
        </p>
        <p><em>String</em>
        </p>
      </td>
      <td style="text-align:left">U, I</td>
      <td style="text-align:left"></td>
    </tr>
    <tr>
      <td style="text-align:left">
        <p><b>individualId</b>
        </p>
        <p><em>String</em>
        </p>
      </td>
      <td style="text-align:left">U</td>
      <td style="text-align:left"></td>
    </tr>
    <tr>
      <td style="text-align:left">
        <p><b>fileIds</b>
        </p>
        <p><em>List&lt;String&gt;</em>
        </p>
      </td>
      <td style="text-align:left"></td>
      <td style="text-align:left"></td>
    </tr>
    <tr>
      <td style="text-align:left">
        <p><b>processing</b>
        </p>
        <p><em>SampleProcessing</em>
        </p>
      </td>
      <td style="text-align:left"></td>
      <td style="text-align:left"></td>
    </tr>
    <tr>
      <td style="text-align:left">
        <p><b>collection</b>
        </p>
        <p><em>SampleCollection</em>
        </p>
      </td>
      <td style="text-align:left"></td>
      <td style="text-align:left"></td>
    </tr>
    <tr>
      <td style="text-align:left">
        <p><b>somatic</b>
        </p>
        <p><em>Boolean</em>
        </p>
      </td>
      <td style="text-align:left"></td>
      <td style="text-align:left"></td>
    </tr>
    <tr>
      <td style="text-align:left">
        <p><b>annotationSets</b>
        </p>
        <p><em>List&lt;</em><a href="./#annotation-set"><em>AnnotationSet</em></a><em>&gt;</em>
        </p>
      </td>
      <td style="text-align:left"></td>
      <td style="text-align:left"></td>
    </tr>
    <tr>
      <td style="text-align:left">
        <p><b>qualityControl</b>
        </p>
        <p><em>SampleQualityControl</em>
        </p>
      </td>
      <td style="text-align:left"></td>
      <td style="text-align:left"></td>
    </tr>
    <tr>
      <td style="text-align:left">
        <p><b>release</b>
        </p>
        <p><em>Integer</em>
        </p>
      </td>
      <td style="text-align:left">O</td>
      <td style="text-align:left"></td>
    </tr>
    <tr>
      <td style="text-align:left">
        <p><b>version</b>
        </p>
        <p><em>Integer</em>
        </p>
      </td>
      <td style="text-align:left">O</td>
      <td style="text-align:left"></td>
    </tr>
    <tr>
      <td style="text-align:left">
        <p><b>creationDate</b>
        </p>
        <p><em>String</em>
        </p>
      </td>
      <td style="text-align:left">O</td>
      <td style="text-align:left"></td>
    </tr>
    <tr>
      <td style="text-align:left">
        <p><b>modificationDate</b>
        </p>
        <p><em>String</em>
        </p>
      </td>
      <td style="text-align:left">O</td>
      <td style="text-align:left"></td>
    </tr>
    <tr>
      <td style="text-align:left">
        <p><b>description</b>
        </p>
        <p><em>String</em>
        </p>
      </td>
      <td style="text-align:left"></td>
      <td style="text-align:left"></td>
    </tr>
    <tr>
      <td style="text-align:left">
        <p><b>phenotypes</b>
        </p>
        <p><em>List&lt;Phenotype&gt;</em>
        </p>
      </td>
      <td style="text-align:left"></td>
      <td style="text-align:left"></td>
    </tr>
    <tr>
      <td style="text-align:left">
        <p><b>status</b>
        </p>
        <p><em>CustomStatus</em>
        </p>
      </td>
      <td style="text-align:left"></td>
      <td style="text-align:left"></td>
    </tr>
    <tr>
      <td style="text-align:left">
        <p><b>internal</b>
        </p>
        <p><em>SampleInternal</em>
        </p>
      </td>
      <td style="text-align:left">O</td>
      <td style="text-align:left"></td>
    </tr>
    <tr>
      <td style="text-align:left">
        <p><b>attributes</b>
        </p>
        <p><em>Map</em>
        </p>
      </td>
      <td style="text-align:left"></td>
      <td style="text-align:left"></td>
    </tr>
  </tbody>
</table>

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

