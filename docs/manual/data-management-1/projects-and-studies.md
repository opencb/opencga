# Working with Projects and Studies

## Introduction

The project/study organisation is key in order to optimise the usability of the data in OpenCGA.

**Projects** provide physical separation of data into different database tables.  
**Studies** provide logical separation of data within a Project.

### Guidelines

* You MUST store your data in different  projects when they are on different genome assemblies \(e.g you should create a project for data from _GRCh37_ and other for data from _GRCh38_\)
* You CAN store your data in different projects when there is no foreseeable  need to process them jointly.
* You may divide your data in studies corresponding to different independent datasets that may be used together in some analysis, with the aim of having homogeneous datasets for each study.

### Owner user

The owner is the user who creates the project/study where the new data will be loaded. The users with permission to perform data ingestion into a concrete study in OpenCGA are the owner user, and other users with admin privileges for the specific study \(provided by the owner\).

After deciding structure, the new projects and studies may need to be created. This step must be performed by the owner of the new created elements.

## **Creating new projects**

The first step is [login](../login.md) into OpenCGA with a `FULL` account \(See [Data Management](projects-and-studies.md)\). Then, a Project can be created using the next command:

```text
$ ./opencga.sh projects create --id <short-project-id> 
                                -n <full-project-name> 
                                --organism-scientific-name hsapiens 
                                --organism-assembly <GRCh37|GRCh38>
```

Optionally, you can add other parameters like `--description` . You can get the full list of parameters by adding to the command.

## **Creating new studies**

Similar to the project creation, studies are created with this command:

```text
$ ./opencga.sh studies create --project <project-id> 
                              --id <short-study-id> 
                              -n <full-study-name>
```

{% hint style="info" %}
You don’t need to provide the organism assembly again, as it’s inherited from the project. Remember that all studies from the same project will share the same assembly.
{% endhint %}

To get the list of all projects and studies belonging to one specific user, run:

```text
$ ./opencga.sh users info
```

### \*\*\*\*

