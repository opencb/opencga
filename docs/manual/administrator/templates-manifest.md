# Templates / Manifest

Since OpenCGA v2.X.X, the OpenCGA users with administration roles are offered the possibility to work with templates. Templates are a set of files with a defined specification that allow the user to perform a series of different operations, related to the ingestion of metadata in OpenCGA, e.g: define the samples, individuals,  permission groups etc. For more information on how OpenCGA stores metadata in Catalog refer to \[ADD\_SECTION\]

The templates are defined at a study level and could be provided in different formats accordingly to the user needs. The files format and some common user cases are illustrated in the following section.

Remember that OpenCGA is highly configurable, and the use of templates constitutes a useful resource to reduce some common artefacts on the ingestion of metadata, but you can always use the clients \[TO\_ADD\] or WebServices to perform different operations in OpenCGA. 

## Procedure

The templates define a way to easily ingest metadata into OpenCGA:

* There is only one required file that you'd need to provide to use the templates-related operations. This is a config `json` OR `yml` file named after the `studyId` where the template is to be applied. This file will define the root \(i.e: the study where you will perform the operation\)

```text
---------- OP 3 ----------------------- fileName == ruta (a partir de study)
RD37.yml
RD37/      
          groups.txt
          individuals.txt
          individuals.phenotypes.txt
```

