# Study ACLs

{% hint style="info" %}
Remember that you can always check the Catalog code that implements OpenCGA's ACLs permission system in our official publicly available [GitHub repository](https://github.com/opencb/opencga/blob/9b00edc7b556898d6b65527a333ecdd62aea3791/opencga-core/src/main/java/org/opencb/opencga/core/models/study/StudyAclEntry.java).
{% endhint %}

Here you will find the list of permissions that can be granted to a **member** \(user or group\) in OpenCGA. Thanks to the **ACLs** system, permissions can be assigned to almost any entry level \(except for _User_ and _Project\)._

## How it works

A list of the basic permissions and their explanations can be found in the list below:

* VIEW_: Give permission to access in read-only mode to the entry \(study, file, sample...\). WRITE: Give permission to create and update that kind of entries within the study. This do not include permissions to modify annotation and/or annotation sets. Those actions will need additional permissions._
* _DELETE:_ Give permission to delete that kind of entries. 
* ANNOTATIONS: In Sample, Individual, Family and Cohort we have three additional permissions to deal with annotations.

  Files deserve a special treatment as they not only exist in the database, but also physically in the file system. The special permissions added for files are the following:

_VIEW\_FILE\_HEADER_: Give permission to retrieve just the header of a file. DOWNLOAD\_FILES or _DOWNLOAD_: Give permission to download the whole file.

{% tabs %}
{% tab title="SAMPLES" %}
* **VIEW\_SAMPLES**
* **WRITE\_SAMPLES** _\(implies: VIEW\_SAMPLES\)_
* **DELETE\_SAMPLES** _\(implies:VIEW\_SAMPLES, WRITE\_SAMPLES\)_
* **VIEW\_SAMPLE\_ANNOTATIONS** _\(implies: VIEW\_SAMPLES\)_
* **WRITE\_SAMPLE\_ANNOTATIONS** _\(implies: VIEW\_SAMPLES, VIEW\_SAMPLE\_ANNOTATIONS\)_
* **DELETE\_SAMPLE\_ANNOTATIONS** _\(implies: VIEW\_SAMPLES, VIEW\_SAMPLE\_ANNOTATIONS, WRITE\_SAMPLE\_ANNOTATIONS\)_
* **VIEW\_AGGREGATED\_VARIANTS**
* **VIEW\_SAMPLE\_VARIANTS** _\(implies: VIEW\_SAMPLES, VIEW\_SAMPLE\_ANNOTATIONS, VIEW\_AGGREGATED\_VARIANTS\)_
{% endtab %}

{% tab title="INDIVIDUALS" %}
* **VIEW\_INDIVIDUALS**
* **WRITE\_INDIVIDUALS** _\(implies: VIEW\_INDIVIDUALS\)_
* **DELETE\_INDIVIDUALS** _\(implies:VIEW\_INDIVIDUALS, WRITE\_INDIVIDUALS\)_
* **VIEW\_INDIVIDUAL\_ANNOTATIONS** _\(implies: VIEW\_INDIVIDUALS\)_
* **WRITE\_INDIVIDUAL\_ANNOTATIONS** _\(implies: VIEW\_INDIVIDUALS, VIEW\_INDIVIDUAL\_ANNOTATIONS\)_
* **DELETE\_INDIVIDUAL\_ANNOTATIONS** _\(implies: VIEW\_INDIVIDUALS, VIEW\_INDIVIDUAL\_ANNOTATIONS, WRITE\_INDIVIDUAL\_ANNOTATIONS\)_
{% endtab %}

{% tab title="FILES" %}
* **VIEW\_FILES**
* **VIEW\_FILE\_HEADER** _\(implies: VIEW\_FILES\)_
* **VIEW\_FILE\_CONTENT** _\(implies: VIEW\_FILES\)_ 
* **WRITE\_FILES** \(_implies: VIEW\_FILES_\)
* **DELETE\_FILES** _\*\*\(implies: VIEW\_FILES, WRITE\_FILES\)_
* **DOWNLOAD\_FILES** _\(implies: VIEW\_FILES\)_
* **UPLOAD\_FILES** _\*\*\(implies: WRITE\_FILES, VIEW\_FILES\)_ 
* **VIEW\_FILE\_ANNOTATIONS** _\(implies: VIEW\_FILES\)_
* **WRITE\_FILE\_ANNOTATIONS** _\(implies: VIEW\_FILE\_ANNOTATIONS, VIEW\_FILES\)_
* **DELETE\_FILE\_ANNOTATIONS** _\(implies: WRITE\_FILE\_ANNOTATIONS, VIEW\_FILE\_ANNOTATIONS, VIEW\_FILES\)_
{% endtab %}

{% tab title="JOBS" %}
* **EXECUTE\_JOBS**
* **VIEW\_JOBS**
* **WRITE\_JOBS** _\(implies: VIEW\_JOBS\)_
* **DELETE\_JOBS** _\(implies: VIEW\_JOBS, WRITE\_JOBS\)_
{% endtab %}

{% tab title="FAMILIES" %}
* **VIEW\_FAMILIES**
* **WRITE\_FAMILIES** _\(implies: VIEW\_FAMILIES\)_
* **DELETE\_FAMILIES** _\(implies: VIEW\_FAMILIES, WRITE\_FAMILIES\)_
* **VIEW\_FAMILY\_ANNOTATIONS** _\(implies: VIEW\_FAMILIES\)_
* **WRITE\_FAMILY\_ANNOTATIONS** _\(implies:VIEW\_FAMILIES, VIEW\_FAMILY\_ANNOTATIONS\)_
* **DELETE\_FAMILY\_ANNOTATIONS** _\(implies: VIEW\_FAMILIES, VIEW\_FAMILY\_ANNOTATIONS, WRITE\_FAMILY\_ANNOTATIONS\)_
{% endtab %}

{% tab title="COHORTS" %}
* **VIEW\_COHORTS**
* **WRITE\_COHORTS** _\(implies: VIEW\_COHORTS\)_
* **DELETE\_COHORTS** _\(implies: VIEW\_COHORTS, WRITE\_COHORTS\)_
* **VIEW\_COHORT\_ANNOTATIONS** _\(implies: VIEW\_COHORTS\)_
* **WRITE\_COHORT\_ANNOTATIONS** _\(implies: VIEW\_COHORTS, VIEW\_COHORT\_ANNOTATIONS\)_
* **DELETE\_COHORT\_ANNOTATIONS** _\(implies: VIEW\_COHORTS, VIEW\_COHORT\_ANNOTATIONS, WRITE\_COHORT\_ANNOTATIONS\)_
{% endtab %}

{% tab title="PANELS" %}
* **VIEW\_PANELS**
* **WRITE\_PANELS** _\(implies: VIEW\_PANELS\)_
* **DELETE\_PANELS** _\(implies: VIEW\_PANELS, WRITE\_PANELS\)_
{% endtab %}

{% tab title="CLINICAL" %}
* **VIEW\_CLINICAL\_ANALYSIS**
* **WRITE\_CLINICAL\_ANALYSIS** _\(implies: VIEW\_CLINICAL\_ANALYSIS\)_
* **DELETE\_CLINICAL\_ANALYSIS** _\(implies: VIEW\_CLINICAL\_ANALYSIS, WRITE\_CLINICAL\_ANALYSIS\)_
{% endtab %}
{% endtabs %}

## Permissions Templates <a id="SharingandPermissions-Specialcases"></a>

OpenCGA Catalog implements two Permissions templates: predefined generic roles that capture a list of defined permissions. The permission templates can be granted to either users or groups.

* **analyst**: The member \(user or group\) will be given full READ and WRITE \(not DELETE\) permissions for all the entries related to the study. These users will be able to view and do modifications on all the data that is related to the study. 
* **view\_only**: The member \(user or group\) will be given full READ permissions.

## Special cases <a id="SharingandPermissions-Specialcases"></a>

Permissions can be given to any concrete entity \(file, sample, cohort...\) to deny or grant access to just one concrete entry. This is always true except for a few exceptions in which we might propagate those same permissions to other entries:

### Files <a id="SharingandPermissions-Files"></a>

File entry might be of type file or folder \(directory\). Permissions assigned in folders are propagated to all the children \(files and folders\) recursively.

### Individuals/Samples <a id="SharingandPermissions-Individuals/Samples"></a>

Individuals are really strongly related with samples. So every time permissions are given to an individual, the same permissions can be applied to all the related samples if the user sets the 'propagate' field to True, and vice-versa.

### Give public access to non-existing users <a id="SharingandPermissions-Givepublicaccesstonon-existingusers"></a>

Catalog has one special user for this purpose represented with _\*_ symbol. Anytime a user tries to fetch anything and no session id is provided, Catalog will treat that user as _\*_. By default, only authorised users will have access to data. However, study managers can still define permissions for non-authenticated users assigning permissions to the "user" _\*._

