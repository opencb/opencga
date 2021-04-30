# Templates / Manifest

Since OpenCGA v2.1.x, the OpenCGA users with administration roles are offered the possibility to work with templates. Templates are a set of files with a defined specification that allow the user to perform a series of different operations, related to the ingestion of metadata in OpenCGA, e.g: define the samples, individuals,  permission groups, etc. For more information on how OpenCGA stores metadata in Catalog refer to [Catalog](../../components-1/catalog/)

Templates are defined at a study level and could be provided in different formats accordingly to the user's needs. The file format and some common use cases are illustrated in the following section.

Remember that OpenCGA is highly configurable, and the use of templates constitutes a useful resource to reduce some common artifacts on the ingestion of metadata, but you can always use the OpenCGA clients \([Client Libraries](../../using-opencga/client-libraries/)\), command line \([Command Line](../../using-opencga/command-line.md)\) or [REST Web Service API](../../using-opencga/restful-web-service-api.md) to perform different operations in OpenCGA. 

## How it Works

The templates define a way to easily ingest metadata into OpenCGA. You need different things:

* **manfiest**: There is only one required file that you'd need to provide to use the template-related operations. This is a  `json` OR `yml` file named  `manifest.{json|yaml}` containing the specific configuration applied to the template. This file will define the root \(i.e: the study where you will perform the operation\). An example is provided below

```text
manifest.yaml

```

* **metadata and clinical data**: You might need to provide a file per entity, where entities corresponds to one of the different comprehensive data models supported by OpenCGA Catalog \(**individuals, samples, files, families, cohorts, clinical\_analysis**\). Each file will contain the entity-related information that you want to ingest into Catalog. For usability purposes two main specifications will be accepted. You can find the file structures accepted below:

### JSON/YAML Files

You might want to provide a single JSON or YAML file per entity. In the case of using JSON you should write one JSON per line, if YAML is used then you can just concat them separating by '---'.

The following entities are supported:

* For  ndividuals.{json \| yaml}  
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

