# Sharing and Permissions

## Overview

OpenCGA _Catalog_ provides an **authenticated** **environment** to manage data. It counts with a powerful mechanism of custom-built and secure permission system defined by a mechanism of **Access Control Lists \(ACLs\).**

The system enables to define a list of permissions at any entity level. The permissions are granted to members, being a member either a isolate user or a defined group of users.

## Users <a id="DataManagement-Users"></a>

A [_User_](https://github.com/opencb/opencga/blob/develop/opencga-core/src/main/java/org/opencb/opencga/core/models/User.java) is generally a person who will be using OpenCGA. The idea in OpenCGA is that every single person have its corresponding user created in OpenCGA. Every user should be authenticated \(see [Authentication](http://docs.opencb.org/display/opencga/Authentication) section\) to be able to perform any action. However, to be able to perform any actions, users will need to be granted some specific permissions or to have a specific category within the _Study_ \(see [Sharing and Permissions](http://docs.opencb.org/display/opencga/Sharing+and+Permissions) section\).

There are two default types of user accounts:

* **full**: these users have permission to create _projects_ and _studies._
* **guest**: users that will not have the possibility to create their own _projects_ and _studies._ Despite this, these users will still be able to collaborate \(view, write...\) in other user's studies as long as they have been granted the proper permissions.

### Groups <a id="DataManagement-Groups"></a>

You can create _group_ of users, this will simplify data permission management. Groups are defined at _study_ level, i.e. each _study_ contains different groups. _Groups_ can only be created by the study _owner_ or the study _admins._ A [_Group_](https://github.com/opencb/opencga/blob/develop/opencga-core/src/main/java/org/opencb/opencga/core/models/Group.java) of users will generally bring together users that have something in common. Groups are strongly related to permissions in Catalog \(see [Sharing and Permissions](http://docs.opencb.org/display/opencga/Sharing+and+Permissions) section\). For example, let's imagine that we have 5 different departments in our institution and each department requires different permissions to the data. In that case, we could think of creating as many groups of users as different departments we have in our institution and give the specific permissions to those groups \(not to the users\) that have been created in OpenCGA. Doing it this way have lots of benefits:

* A user belonging to different departments \(groups\) will have the permissions from all the groups he/she belongs to.
* If one user leaves the department, we would just need to remove that user from the corresponding group. That user will automatically lose the permissions the group has\*.
* If one user starts in the department, we would just need to add that user to the corresponding group. That user will automatically gain the permissions the group has.

  \* Unless the user had some or all the permissions granted to the group defined in a different group he/she might still belong to or assigned directly to the user.

All _studies_ have always two administrative groups that cannot be deleted or renamed, these are _admins_ and _members_.

## Projects <a id="DataManagement-Projects"></a>

Any _**full**_ user can create any number of _projects_ \(and _studies\)._ A [_Project_](https://github.com/opencb/opencga/blob/develop/opencga-core/src/main/java/org/opencb/opencga/core/models/Project.java) is _a piece of planned work or an activity that is finished over a period of time and intended to achieve a particular purpose_ \([Cambridge dictionary](https://dictionary.cambridge.org/dictionary/english/project) definition\). A _Project_ in Catalog is understood as a scientific project for one concrete species. Any project in Catalog will contain at least a name, an alias \(project identifier\) and the species organism. But it can also contain the organisation and a description of the project.

Projects are used as the central piece for variants storage.

## Studies <a id="DataManagement-Studies"></a>

Projects are composed by a set of studies. A [_Study_](https://github.com/opencb/opencga/blob/develop/opencga-core/src/main/java/org/opencb/opencga/core/models/Study.java) is _the activity of examining a subject in detail in order to discover new information_ \([Cambridge dictionary](https://dictionary.cambridge.org/dictionary/english/study) definition\) Any project owner can create as many studies as necessary. Most of the Catalog data models, except for _User_ and _Project_ belong to a particular _Study,_ so it can be seen as the central piece in OpenCGA Catalog. A _Study_ contains, similarly to _Project_, a name and an alias \(study identifier\). Optionally, it can have a description as well.

### Groups in a Study <a id="DataManagement-Groups.1"></a>

Despite the explanation of _Groups_ from a previous section, _Groups_ are actually defined within a _Study_. Different studies can have different groups of users that are basically defined by the _Study_ owner or administrators. By default, every _Study_ is created with two reserved groups \(_admins_ and _members_\). The roles of these two groups is described in [Sharing and Permissions](http://docs.opencb.org/display/opencga/Sharing+and+Permissions) section.

### Variable Sets and Annotation Sets <a id="DataManagement-VariableSetsandAnnotationSets"></a>

One of the most outstanding features of OpenCGA Catalog is the ability to not only store any type of data in the database, but also filter and query by any of the values populated by the researchers. This can be achieved with what we have called _Variable Sets_ and _Annotation Sets._

A [_Variable Set_](https://github.com/opencb/opencga/blob/develop/opencga-core/src/main/java/org/opencb/opencga/core/models/VariableSet.java) is a set of [_Variables_](https://github.com/opencb/opencga/blob/develop/opencga-core/src/main/java/org/opencb/opencga/core/models/Variable.java), understanding as a _Variable_ the complete definition of a field that need to be populated. In other words, a _Variable Set_ could be seen as a template of a form that is given to the patient containing the points the patient should fill in. A _Variable Set_ will look similar to the table shown above. That _Variable Set_ is composed of four well described _Variables_:

| _**Variable**_ | Sex | Categorical | Yes | MALE, FEMALE, UNKNOWN |
| :--- | :--- | :--- | :--- | :--- |
| _**Variable**_ | Age | Integer | Yes | NA |
| _**Variable**_ | Mother name | Text | Yes | NA |
| _**Variable**_ | Affected | Boolean | Yes | NA |

Every _Study_ can have as many different _Variable Set_ definitions as necessary.

The values defined for each of the _Variables_ are called [_Annotations_](https://github.com/opencb/opencga/blob/develop/opencga-core/src/main/java/org/opencb/opencga/core/models/Annotation.java), and the population of a whole _Variable Set_ is called [_Annotation Set_](https://github.com/opencb/opencga/blob/develop/opencga-core/src/main/java/org/opencb/opencga/core/models/AnnotationSet.java)_._ This means that an _Annotation Set_ only makes sense and is always related to one concrete _Variable Set._

There are four _Annotable_ data models: _Sample, Individual, Family_ and _Cohort_. Each entry from these data models can have _Annotation Sets_ as can be seen in the diagram in the right margin. An _Annotation Set_ will look to something similar to:

| _Annotation_ | Sex | MALE |
| :--- | :--- | :--- |
| _Annotation_ | Age | 60 |
| _Annotation_ | Mother name | Jane |
| _Annotation_ | Affected | Yes |

OpenCGA allows querying by any of these key-value pairs.

### Files <a id="DataManagement-Files"></a>

OpenCGA Catalog keeps track of all the files and folders containing the relevant data. Every [_File_](https://github.com/opencb/opencga/blob/develop/opencga-core/src/main/java/org/opencb/opencga/core/models/File.java) registry contains the physical path where the files/folders are stored in the file system \(uri\). Besides this, Catalog creates a virtual file structure so no matter what the real location of the files are, users can organise and work with those files differently. Everything related to _Files_ can be found in the [File Management](http://docs.opencb.org/display/opencga/File+Management) section.

### Individuals and Families <a id="DataManagement-IndividualsandFamilies"></a>

We understand an [_Individual_](https://github.com/opencb/opencga/blob/develop/opencga-core/src/main/java/org/opencb/opencga/core/models/Individual.java) as a subject \(typically a person\) for which some analysis will be made. A group of _Individuals_ with any parental or blood relationship is called [_Family_](https://github.com/opencb/opencga/blob/develop/opencga-core/src/main/java/org/opencb/opencga/core/models/Individual.java). Any of these two data models can have _Annotation Sets_ defined.

### Samples and Cohorts <a id="DataManagement-SamplesandCohorts"></a>

A [_Sample_](https://github.com/opencb/opencga/blob/develop/opencga-core/src/main/java/org/opencb/opencga/core/models/Sample.java) is any biological material, normally extracted from an _Individual_, that is used for a particular analysis. [_Cohorts_](https://github.com/opencb/opencga/blob/develop/opencga-core/src/main/java/org/opencb/opencga/core/models/Cohort.java) contains groups of samples sharing some particular conditions such as "healthy" vs "infected". Any of these two data models can also have _Annotation Sets_ defined.

### Clinical Analysis <a id="DataManagement-ClinicalAnalysis"></a>

A [_Clinical Analysis_](https://github.com/opencb/opencga/blob/develop/opencga-core/src/main/java/org/opencb/opencga/core/models/ClinicalAnalysis.java) contains all the information of the _Individuals_ and _Samples_ involved to perform a real clinical analysis. It also allows storing the interpretations derived from the results.

### Jobs <a id="DataManagement-Job"></a>

OpenCGA Catalog allows running different tools. This tools can be any of the ones built in OpenCGA, but also any external tool the user might need to use. Every time the user calls to a analysis web service to run anything, a new [_Job_](https://github.com/opencb/opencga/blob/develop/opencga-core/src/main/java/org/opencb/opencga/core/models/Job.java) is created. This jobs contain the essential information of the task that needs to be run. A daemon is in charge of checking whether there are any prepared, queued or running jobs and update the information.

{% hint style="info" %}
OpenCGA supports SGE \(Sun Grid Engine\) that accepts, schedules, dispatches, and manages the remote and distributed execution of large numbers of standalone or parallel jobs.
{% endhint %}

