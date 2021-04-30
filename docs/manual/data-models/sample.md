# Sample

## Overview

Sample data model hosts information about any biological material, normally extracted from an _Individual_, that is used for a particular analysis.

## Entity Field Attributes

There are some attributes that a user can manipulate, while others are reserved for internal OpenCGA use

### Sample Fields subjected to Update and Create Operations

**Create Fields: `id`**

**Update Fields: `individualId, fileIds, processing, collection, somatic, annotationSets, qualityControl, description, phenotypes, status, attributes`**

### Fields for OpenCGA Internal use \(immutable\) 

**`uuid, release, version, creationDate, modificationDate, internal`**

## Data Model

### Sample

This is the main data model, it stores the most basic and important information. You can find the Java code [here](https://github.com/opencb/opencga/blob/master/opencga-core/src/main/java/org/opencb/opencga/core/models/sample/Sample.java).

<table>
  <thead>
    <tr>
      <th style="text-align:left"><b>Field</b>
      </th>
      <th style="text-align:left"><b>Description</b>
      </th>
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
      <td style="text-align:left">
        <p><b>Unique</b> Sample ID in the study, this can be repeated across different
          studies. This is a mandatory parameter in the creation and cannot be changed
          at the moment.</p>
        <p><em>Constraints: Required, Create</em>
        </p>
      </td>
    </tr>
    <tr>
      <td style="text-align:left">
        <p><b>uuid</b>
        </p>
        <p><em>String</em>
        </p>
      </td>
      <td style="text-align:left">
        <p>Global unique ID in any study of any OpenCGA installation. This is created
          during the sample creation and cannot be changed.</p>
        <p><em>Constraints: Internal, Unique, Immutable</em>
        </p>
      </td>
    </tr>
    <tr>
      <td style="text-align:left">
        <p><b>individualId</b>
        </p>
        <p><em>String</em>
        </p>
      </td>
      <td style="text-align:left">A reference to the <a href="individual.md">Individual</a> containing this
        sample. Notice that samples can exist without and Individual ID, this field
        is not mandatory.</td>
    </tr>
    <tr>
      <td style="text-align:left">
        <p><b>fileIds</b>
        </p>
        <p><em>List&lt;String&gt;</em>
        </p>
      </td>
      <td style="text-align:left">List of <a href="./#file-id">File ID</a> containing this sample, eg BAM,
        VCF, QC images, ...</td>
    </tr>
    <tr>
      <td style="text-align:left">
        <p><b>processing</b>
        </p>
        <p>&lt;em&gt;&lt;/em&gt;<a href="sample.md#sampleprocessing"><em>SampleProcessing</em></a>&lt;em&gt;&lt;/em&gt;</p>
      </td>
      <td style="text-align:left">An object describing how to sample was processed.</td>
    </tr>
    <tr>
      <td style="text-align:left">
        <p><b>collection</b>
        </p>
        <p>&lt;em&gt;&lt;/em&gt;<a href="sample.md#samplecollection"><em>SampleCollection</em></a>&lt;em&gt;&lt;/em&gt;</p>
      </td>
      <td style="text-align:left">An object describing how the sample was collected.</td>
    </tr>
    <tr>
      <td style="text-align:left">
        <p><b>somatic</b>
        </p>
        <p><em>Boolean</em>
        </p>
      </td>
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
    </tr>
    <tr>
      <td style="text-align:left">
        <p><b>qualityControl</b>
        </p>
        <p>&lt;em&gt;&lt;/em&gt;<a href="sample.md#samplequalitycontrol"><em>SampleQualityControl</em></a>&lt;em&gt;&lt;/em&gt;</p>
      </td>
      <td style="text-align:left"></td>
    </tr>
    <tr>
      <td style="text-align:left">
        <p><b>release</b>
        </p>
        <p><em>Integer</em>
        </p>
      </td>
      <td style="text-align:left"></td>
    </tr>
    <tr>
      <td style="text-align:left">
        <p><b>version</b>
        </p>
        <p><em>Integer</em>
        </p>
      </td>
      <td style="text-align:left"></td>
    </tr>
    <tr>
      <td style="text-align:left">
        <p><b>creationDate</b>
        </p>
        <p><em>String</em>
        </p>
      </td>
      <td style="text-align:left">A string representing the creation date in format YYYYMMDDHHmmss</td>
    </tr>
    <tr>
      <td style="text-align:left">
        <p><b>modificationDate</b>
        </p>
        <p><em>String</em>
        </p>
      </td>
      <td style="text-align:left">A string representing the modification date in format YYYYMMDDHHmmss</td>
    </tr>
    <tr>
      <td style="text-align:left">
        <p><b>description</b>
        </p>
        <p><em>String</em>
        </p>
      </td>
      <td style="text-align:left"></td>
    </tr>
    <tr>
      <td style="text-align:left">
        <p><b>phenotypes</b>
        </p>
        <p><em>List&lt;</em><a href="./#phenotype"><em>Phenotype</em></a><em>&gt;</em>
        </p>
      </td>
      <td style="text-align:left"></td>
    </tr>
    <tr>
      <td style="text-align:left">
        <p><b>status</b>
        </p>
        <p>&lt;em&gt;&lt;/em&gt;<a href="./#status"><em>CustomStatus</em></a>&lt;em&gt;&lt;/em&gt;</p>
      </td>
      <td style="text-align:left"></td>
    </tr>
    <tr>
      <td style="text-align:left">
        <p><b>internal</b>
        </p>
        <p><em>SampleInternal</em>
        </p>
      </td>
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
    </tr>
  </tbody>
</table>

### SampleProcessing

This object describes how the sample was processed in the lab.

<table>
  <thead>
    <tr>
      <th style="text-align:left">Field</th>
      <th style="text-align:left">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td style="text-align:left">
        <p><b>product</b>
        </p>
        <p><em>String</em>
        </p>
      </td>
      <td style="text-align:left">Type of product sequenced, this can be DNA or RNA for instance</td>
    </tr>
    <tr>
      <td style="text-align:left">
        <p><b>preparationMethod</b>
        </p>
        <p><em>String</em>
        </p>
      </td>
      <td style="text-align:left"></td>
    </tr>
    <tr>
      <td style="text-align:left">
        <p><b>extractionMethod</b>
        </p>
        <p><em>String</em>
        </p>
      </td>
      <td style="text-align:left"></td>
    </tr>
    <tr>
      <td style="text-align:left">
        <p><b>labSampleId</b>
        </p>
        <p><em>String</em>
        </p>
      </td>
      <td style="text-align:left"></td>
    </tr>
    <tr>
      <td style="text-align:left">
        <p>quantity</p>
        <p><em>String</em>
        </p>
      </td>
      <td style="text-align:left"></td>
    </tr>
    <tr>
      <td style="text-align:left">
        <p>date</p>
        <p><em>String</em>
        </p>
      </td>
      <td style="text-align:left"></td>
    </tr>
  </tbody>
</table>

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

