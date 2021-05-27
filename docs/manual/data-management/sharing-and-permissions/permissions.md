# Permissions

{% hint style="info" %}
Remember that you can always check the Catalog code that implements OpenCGA's ACLs permission system in our official publicly available [GitHub repository](https://github.com/opencb/opencga/blob/9b00edc7b556898d6b65527a333ecdd62aea3791/opencga-core/src/main/java/org/opencb/opencga/core/models/study/StudyAclEntry.java).
{% endhint %}

Here you will find the list of permissions that can be granted to a **member** \(user or group\) in OpenCGA. Thanks to the **ACLs** system, permissions can be assigned to almost any entry level \(except for _User_ and _Project\)._

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
* **DELETE\_FILES** _****\(implies: VIEW\_FILES, WRITE\_FILES\)_
* **DOWNLOAD\_FILES** _\(implies: VIEW\_FILES\)_
* **UPLOAD\_FILES** _****\(implies: WRITE\_FILES, VIEW\_FILES\)_ 
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
{% endtabs %}

```text


COHORTS
    VIEW_COHORTS(Collections.emptyList(), CohortAclEntry.CohortPermissions.VIEW.name(), COHORT),
    WRITE_COHORTS(Collections.singletonList(VIEW_COHORTS), CohortAclEntry.CohortPermissions.WRITE.name(), COHORT),
    DELETE_COHORTS(Arrays.asList(VIEW_COHORTS, WRITE_COHORTS), CohortAclEntry.CohortPermissions.DELETE.name(), COHORT),
    VIEW_COHORT_ANNOTATIONS(Collections.singletonList(VIEW_COHORTS), CohortAclEntry.CohortPermissions.VIEW_ANNOTATIONS.name(), COHORT),
    WRITE_COHORT_ANNOTATIONS(Arrays.asList(VIEW_COHORTS, VIEW_COHORT_ANNOTATIONS),
            CohortAclEntry.CohortPermissions.WRITE_ANNOTATIONS.name(), COHORT),
    DELETE_COHORT_ANNOTATIONS(Arrays.asList(VIEW_COHORTS, VIEW_COHORT_ANNOTATIONS, WRITE_COHORT_ANNOTATIONS),
            CohortAclEntry.CohortPermissions.DELETE_ANNOTATIONS.name(), COHORT),

DISEASE PANELS
    VIEW_PANELS(Collections.emptyList(), PanelAclEntry.PanelPermissions.VIEW.name(), DISEASE_PANEL),
    WRITE_PANELS(Collections.singletonList(VIEW_PANELS), PanelAclEntry.PanelPermissions.WRITE.name(), DISEASE_PANEL),
    DELETE_PANELS(Arrays.asList(VIEW_PANELS, WRITE_PANELS), PanelAclEntry.PanelPermissions.DELETE.name(), DISEASE_PANEL),

CLINICAL ANALYSIS
    VIEW_CLINICAL_ANALYSIS(Collections.emptyList(), ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions.VIEW.name(),
            CLINICAL_ANALYSIS),
    WRITE_CLINICAL_ANALYSIS(Collections.singletonList(VIEW_CLINICAL_ANALYSIS),
            ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions.WRITE.name(), CLINICAL_ANALYSIS),
    DELETE_CLINICAL_ANALYSIS(Arrays.asList(VIEW_CLINICAL_ANALYSIS, WRITE_CLINICAL_ANALYSIS),
            ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions.DELETE.name(), CLINICAL_ANALYSIS);
```

## Special cases <a id="SharingandPermissions-Specialcases"></a>

Permissions can be given to any concrete entity \(file, sample, cohort...\) to deny or grant access to just one concrete entry. This is always true except for a few exceptions in which we might propagate those same permissions to other entries:

### Files <a id="SharingandPermissions-Files"></a>

File entry might be of type file or folder. Permissions assigned in folders are propagated to all the children \(files and folders\) recursively.

 All permissions that might have had files and folders under the folder being given permissions will be modified according to the action being performed in the parent folder. In other words, if we are setting new permissions for the folder, any possible permissions the files and folders under the parent folder might have had will be completely replaced by the parent folder's permissions. However, if the action being performed is just adding a new permission to the parent folder, children files and folders will keep their old permissions plus the new one\(s\) added to the parent folder.

### Individuals/Samples <a id="SharingandPermissions-Individuals/Samples"></a>

Individuals are really strong related with samples. So every time permissions are given to an individual, the same permissions can be applied to all the related samples if the user sets the 'propagate' field to True, and vice-versa.

## Use cases <a id="SharingandPermissions-Usecases"></a>

### Give public access to non-existing users <a id="SharingandPermissions-Givepublicaccesstonon-existingusers"></a>

Catalog has one special user for this purpose represented with _\*_ symbol. Anytime a user tries to fetch anything and no session id is provided, Catalog will treat that user as _\*_. By default, only authorised users will have access to data. However, study managers can still define permissions for non-authenticated users assigning permissions to the "user" _\*._



