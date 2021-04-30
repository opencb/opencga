# Templates / Manifest

Since OpenCGA v2.1.x, the OpenCGA users with administration roles are offered the possibility to work with templates. Templates are a set of files with a defined specification that allow the user to perform a series of different operations, related to the ingestion of metadata in OpenCGA, e.g: define the samples, individuals,  permission groups, etc. For more information on how OpenCGA stores metadata in Catalog refer to \[ADD\_SECTION\]

Templates are defined at a study level and could be provided in different formats accordingly to the user's needs. The file format and some common use cases are illustrated in the following section.

Remember that OpenCGA is highly configurable, and the use of templates constitutes a useful resource to reduce some common artifacts on the ingestion of metadata, but you can always use the clients \[TO\_ADD\] or WebServices to perform different operations in OpenCGA. 

## How it Works

The templates define a way to easily ingest metadata into OpenCGA. You need different things:

* **manfiest**: There is only one required file that you'd need to provide to use the template-related operations. This is a config `json` OR `yml` file named after the `studyId` where the template is to be applied. This file will define the root \(i.e: the study where you will perform the operation\)
* **metadata and clinical data**:   
* ```text
  ---------- OP 3 ----------------------- fileName == ruta (a partir de study)
  manifest.yml
  RD37/      
            individuals.txt
            individuals.phenotypes.txt
  ```



### JSON/YAML Files

You have to provide a single JSON or YAML file per entity. In the case of using JSON you should write one JSON per line, if YAML is used then you can just concat them separating by '---'.

The following entities are supported:

* individuals.{json \| yaml}  
* samples
* files
* families
* cohorts
* clinical\_analysis



### TAB Files

You can load data for individuals, samples, ... using TAB files.

The columns are configurable...

Rules:

* First line starting with \# symbol containing the exact name of the corresponding data model
* the order of the columns are not relevant

```text
individuals.txt:
#id         name        sex         status.name     (phenotypes.id)
NA001       pepe        yes         READY           
NA002       ...


individuals.phenotypes.txt
#individualId   id              name        description
NA001           HP:1234         klsalksa    sdasdasda
NA002           HP:1234         klsalksa    sdasdasda
study.groups.txt
```

