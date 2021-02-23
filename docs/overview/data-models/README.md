---
description: >-
  Remarkable data models from openCGA resources that can be relevant for
  operating with OpenCGA through their web services.
---

# Data Models

## Design Principles

All OpenCGA Data Models have been designed to follow some principles:

1. Parent-Child List - Parent resources have a list of Child resources objects in their data models.
2. Chid-Parent reference - String ids from Parents referenced in Child data model.
3. Annotation Sets
4. String id is mandatory for any resource. A unique uuid is generated for each instance of a resource created, which is immutable. ​
5. Each resource has a version, release, creationDate and modificationDate attribute which is immutable.



## Implementation

### Diagram

![](../../.gitbook/assets/catalog_data_models_v13.png)

## Common Data Models

### Annotation Set

| Field | Description |
| :--- | :--- |
|  |  |

### Phenotype

Describe a phenotype following an OBO ontology.

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
        <p><b>id</b>
        </p>
        <p><em>String</em>
        </p>
      </td>
      <td style="text-align:left"></td>
    </tr>
    <tr>
      <td style="text-align:left">
        <p><b>name</b>
        </p>
        <p><em>String</em>
        </p>
      </td>
      <td style="text-align:left"></td>
    </tr>
    <tr>
      <td style="text-align:left">
        <p><b>source</b>
        </p>
        <p><em>String</em>
        </p>
      </td>
      <td style="text-align:left"></td>
    </tr>
    <tr>
      <td style="text-align:left">
        <p><b>ageOfOnset</b>
        </p>
        <p><em>String</em>
        </p>
      </td>
      <td style="text-align:left"></td>
    </tr>
    <tr>
      <td style="text-align:left">
        <p><b>status</b>
        </p>
        <p><em>Status</em>
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

### Creation and Modification Date

### Status

### File ID

File IDs contain the path using `:`

