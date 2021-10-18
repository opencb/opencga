# Study ACLs

{% hint style="info" %}
Remember that you can always check the Catalog code that implements OpenCGA's ACLs permission system in our official publicly available [GitHub repository](https://github.com/opencb/opencga/blob/9b00edc7b556898d6b65527a333ecdd62aea3791/opencga-core/src/main/java/org/opencb/opencga/core/models/study/StudyAclEntry.java).
{% endhint %}

Here you will find the list of permissions that can be granted to a **member** (user or group) in OpenCGA. Thanks to the **ACLs** system, permissions can be assigned to almost any entry level (except for _User _and _Project)._

## How it works

A list of the basic permissions and their explanations can be found in the list below:

* VIEW_: Give permission to access in read-only mode to the entry (study, file, sample...). WRITE: Give permission to create and update that kind of entries within the study. This do not include permissions to modify annotation and/or annotation sets. Those actions will need additional permissions._
* _DELETE:_ Give permission to delete that kind of entries. 
* ANNOTATIONS: In Sample, Individual, Family and Cohort we have three additional permissions to deal with annotations.

 Files deserve a special treatment as they not only exist in the database, but also physically in the file system. The special permissions added for files are the following:

_VIEW_FILE_HEADER_: Give permission to retrieve just the header of a file. DOWNLOAD_FILES or _DOWNLOAD_: Give permission to download the whole file.

{% tabs %}
{% tab title="SAMPLES" %}
* **VIEW_SAMPLES**
* **WRITE_SAMPLES** _(implies: VIEW_SAMPLES)_
* **DELETE_SAMPLES** _(implies:VIEW_SAMPLES, WRITE_SAMPLES)_
* **VIEW_SAMPLE_ANNOTATIONS** _(implies: VIEW_SAMPLES)_
* **WRITE_SAMPLE_ANNOTATIONS** _(implies: VIEW_SAMPLES, VIEW_SAMPLE_ANNOTATIONS)_
* **DELETE_SAMPLE_ANNOTATIONS** _(implies: VIEW_SAMPLES, VIEW_SAMPLE_ANNOTATIONS, WRITE_SAMPLE_ANNOTATIONS)_
* **VIEW_AGGREGATED_VARIANTS**
* **VIEW_SAMPLE_VARIANTS** _(implies: VIEW_SAMPLES, VIEW_SAMPLE_ANNOTATIONS, VIEW_AGGREGATED_VARIANTS)_
{% endtab %}

{% tab title="INDIVIDUALS" %}
* **VIEW_INDIVIDUALS**
* **WRITE_INDIVIDUALS** _(implies: VIEW_INDIVIDUALS)_
* **DELETE_INDIVIDUALS** _(implies:VIEW_INDIVIDUALS, WRITE_INDIVIDUALS)_
* **VIEW_INDIVIDUAL_ANNOTATIONS** _(implies: VIEW_INDIVIDUALS)_
* **WRITE_INDIVIDUAL_ANNOTATIONS** _(implies: VIEW_INDIVIDUALS, VIEW_INDIVIDUAL_ANNOTATIONS)_
* **DELETE_INDIVIDUAL_ANNOTATIONS** _(implies: VIEW_INDIVIDUALS, VIEW_INDIVIDUAL_ANNOTATIONS, WRITE_INDIVIDUAL_ANNOTATIONS)_
{% endtab %}

{% tab title="FILES" %}
* **VIEW_FILES**
* **VIEW_FILE_HEADER **_(implies: VIEW_FILES)_
* **VIEW_FILE_CONTENT **_(implies: VIEW_FILES) _
* **WRITE_FILES** (_implies: VIEW_FILES_)
* **DELETE_FILES**_** **(implies: VIEW_FILES, WRITE_FILES)_
* **DOWNLOAD_FILES **_(implies: VIEW_FILES)_
* **UPLOAD_FILES**_** **(implies: WRITE_FILES, VIEW_FILES) _
* **VIEW_FILE_ANNOTATIONS**_ (implies: VIEW_FILES)_
* **WRITE_FILE_ANNOTATIONS** _(implies: VIEW_FILE_ANNOTATIONS, VIEW_FILES)_
* **DELETE_FILE_ANNOTATIONS **_(implies: WRITE_FILE_ANNOTATIONS, VIEW_FILE_ANNOTATIONS, VIEW_FILES)_
{% endtab %}

{% tab title="JOBS" %}
* **EXECUTE_JOBS**
* **VIEW_JOBS**
* **WRITE_JOBS** _(implies: VIEW_JOBS)_
* **DELETE_JOBS** _(implies: VIEW_JOBS, WRITE_JOBS)_
{% endtab %}

{% tab title="FAMILIES" %}
* **VIEW_FAMILIES**
* **WRITE_FAMILIES** _(implies: VIEW_FAMILIES)_
* **DELETE_FAMILIES**_ (implies: VIEW_FAMILIES, WRITE_FAMILIES)_
* **VIEW_FAMILY_ANNOTATIONS** _(implies: VIEW_FAMILIES)_
* **WRITE_FAMILY_ANNOTATIONS** _(implies:VIEW_FAMILIES, VIEW_FAMILY_ANNOTATIONS)_
* **DELETE_FAMILY_ANNOTATIONS** _(implies: VIEW_FAMILIES, VIEW_FAMILY_ANNOTATIONS, WRITE_FAMILY_ANNOTATIONS)_
{% endtab %}

{% tab title="COHORTS" %}
* **VIEW_COHORTS**
* **WRITE_COHORTS**_ (implies: VIEW_COHORTS)_
* **DELETE_COHORTS** _(implies: VIEW_COHORTS, WRITE_COHORTS)_
* **VIEW_COHORT_ANNOTATIONS** _(implies: VIEW_COHORTS)_
*  **WRITE_COHORT_ANNOTATIONS** _(implies: VIEW_COHORTS, VIEW_COHORT_ANNOTATIONS)_
* **DELETE_COHORT_ANNOTATIONS** _(implies: VIEW_COHORTS, VIEW_COHORT_ANNOTATIONS, WRITE_COHORT_ANNOTATIONS)_
{% endtab %}

{% tab title="PANELS" %}
* **VIEW_PANELS**
* **WRITE_PANELS** _(implies: VIEW_PANELS)_
* **DELETE_PANELS** _(implies: VIEW_PANELS, WRITE_PANELS)_
{% endtab %}

{% tab title="CLINICAL" %}
* **VIEW_CLINICAL_ANALYSIS**
* **WRITE_CLINICAL_ANALYSIS** _(implies: VIEW_CLINICAL_ANALYSIS)_
* **DELETE_CLINICAL_ANALYSIS** _(implies: VIEW_CLINICAL_ANALYSIS, WRITE_CLINICAL_ANALYSIS)_
{% endtab %}
{% endtabs %}

## Permissions Templates <a href="sharingandpermissions-specialcases" id="sharingandpermissions-specialcases"></a>

OpenCGA Catalog implements two Permissions templates:  predefined generic roles that capture a list of defined permissions. The permission templates can be granted to either users or groups.

* **analyst**: The member (user or group) will be given full READ and WRITE (not DELETE) permissions for all the entries related to the study. These users will be able to view and do modifications on all the data that is related to the study. 
* **view_only**: The member (user or group) will be given full READ permissions.

## Special cases <a href="sharingandpermissions-specialcases" id="sharingandpermissions-specialcases"></a>

Permissions can be given to any concrete entity (file, sample, cohort...) to deny or grant access to just one concrete entry. This is always true except for a few exceptions in which we might propagate those same permissions to other entries:

### Files <a href="sharingandpermissions-files" id="sharingandpermissions-files"></a>

File entry might be of type file or folder (directory). Permissions assigned in folders are propagated to all the children (files and folders) recursively.

### Individuals/Samples <a href="sharingandpermissions-individuals-samples" id="sharingandpermissions-individuals-samples"></a>

Individuals are really strongly related with samples. So every time permissions are given to an individual, the same permissions can be applied to all the related samples if the user sets the 'propagate' field to True, and vice-versa.

### Give public access to non-existing users <a href="sharingandpermissions-givepublicaccesstonon-existingusers" id="sharingandpermissions-givepublicaccesstonon-existingusers"></a>

Catalog has one special user for this purpose represented with _\*_ symbol. Anytime a user tries to fetch anything and no session id is provided, Catalog will treat that user as _\*_. By default, only authorised users will have access to data. However, study managers can still define permissions for non-authenticated users assigning permissions to the "user" _\*._
