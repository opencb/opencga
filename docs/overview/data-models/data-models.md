# Sample

## Overview

Sample data model ...

Legend

* U: unique
* M: mandatory, must be provided in the creation
* I: immutable, cannot be updated

## Data Model

### Sample

| Field | Type | Constraints | Description |
| :--- | :--- | :--- | :--- |
| **id** | String | U, M, I | Unique ID in the study |
| **uuid** | String | U |  |
| **individualId** | String |  |  |
| **fileIds** | List&lt;String&gt; |  |  |
| **processing** | SampleProcessing |  |  |
| **collection** | SampleCollection |  |  |
| **somatic** | Boolean |  |  |
| **qualityControl** | SampleQualityControl |  |  |
| **release** | Integer |  |  |
| **version** | Integer |  |  |
| **creationDate** | String |  |  |
| **modificationDate** | String |  |  |
| **description** | String |  |  |
| **phenotypes** | List&lt;Phenotype&gt; |  |  |
| **status** | CustomStatus |  |  |
| **internal** | SampleInternal |  |  |
| **attributes** | Map |  |  |

### SampleProcessing



### SampleCollection



## Example

This is a JSON example:

