# Adding Custom Metadata

## Variables and Annotations

Clinical Data in OpenCGA can be enriched with custom annotations _Variable Sets_ and _Annotation Sets_.

### Variable Set

A [_Variable Set_](https://github.com/opencb/opencga/blob/develop/opencga-core/src/main/java/org/opencb/opencga/core/models/VariableSet.java) is a free modelled data model. The fields of a _Variable Set_ are explained below:

* **id:** Unique String that can be used to identify the defined _Variable Set_.
* **unique:** Boolean indicating whether there can only exist one single _Annotation Set_ annotating the _Variable Set_ per each _Annotable\*_ entry or not. If false, many _Annotation Sets_ annotating the same _Variable Set_ per _Annotable_ entry will be allowed.
* **confidential:** Boolean indicating whether the _Variable Set_ as well as the _Annotation Sets_ annotating the _Variable Set_ are confidential or not. In case of confidentiality, only the users with that CONFIDENTIAL permission will be able to access it. 
* **description:** String containing a description of the _Variable Set_ defined.
* **variables:** List containing all the different _Variables_ that will form the _Variable Set._ Explained in detail below.

**Annotable**: We consider an entry to be _Annotable_ if the entry can have _Annotation Sets._ At this stage, only [_File_](https://github.com/opencb/opencga/blob/develop/opencga-core/src/main/java/org/opencb/opencga/core/models/File.java)_,_ [_Sample_](https://github.com/opencb/opencga/blob/develop/opencga-core/src/main/java/org/opencb/opencga/core/models/Sample.java)_,_ [_Individual_](https://github.com/opencb/opencga/blob/develop/opencga-core/src/main/java/org/opencb/opencga/core/models/Individual.java)_,_ [_Cohort_](https://github.com/opencb/opencga/blob/develop/opencga-core/src/main/java/org/opencb/opencga/core/models/Cohort.java) and [_Family_](https://github.com/opencb/opencga/blob/develop/opencga-core/src/main/java/org/opencb/opencga/core/models/Family.java) __are _Annotable._

{% hint style="warning" %}
Confidential: Explained in Sharing and Permissions section !
{% endhint %}

A _Variable Set_ is composed of a set of [_Variables_](https://github.com/opencb/opencga/blob/develop/opencga-core/src/main/java/org/opencb/opencga/core/models/Variable.java)_._ A _Variable_ can be understood as a user-defined field that can be of any type \(Boolean, String, Integer, Float, Object, List...\). The different fields of a _Variable_ are:

* **id:** String containing the unique identifier of the field \(_Variable_\) defined by the user.
* **name:**Nice identifier of the _name_. This field is intended to be used in a web application to show the field _name_ in a nicer way.
* **category:** Free String that can contain anything useful for the user to group and categorise _Variables_ of a same _Variable Set._
* **type:** Type of the field \(_Variable_\) defined. It can be one of BOOLEAN, CATEGORICAL\*, INTEGER, DOUBLE, TEXT, OBJECT.
* **defaultValue:** Object containing the default value of the _Variable_ in case the user has not given any value when creating the _Annotation Set_.
* **required:** Boolean indicating whether the field is mandatory to be filled in or not.
* **multivalue:** Boolean indicating whether the field being annotated is a List of type _type_ or it will only contain a single value.
* **allowedValues:** A list containing all the possible values a field could have.
* **rank:** Integer containing the order in which the annotations will be shown \(only for web purposes\).
* **dependsOn:** String containing the _Variable_ the current _Variable_ would depend on. Let's say we have defined two different _Variables_ in a _Variable Set_ called _country_ and _city._ We can decide that we could only give a value to _city_ once the _country_ have been filled in, so _city_ would depend on _country._
* **description:** String containing a description for the _Variable._
* **variableSet:** List of _Variables_ that would only be used if the _Variable_ being modelled is of type _Object._ Every _Variable_ from the list will have the fields explained in this list.
* **Categorical**: A _Categorical_ variable can be understood as an _Enum_ object where the possible values that can be assigned are already known. Example of some categorical  _Variables_ are: _month,_ that can only contain values from January to December, _gender,_ that could only contain values from MALE, FEMALE, UNKNOWN; etc.

#### Examples

We are going to create two different _Variable Sets_, remember that the Variable Sets are defined at _study_ level. The first one will be used to properly identify every single _Individual_ created in OpenCGA. 

```text
{
    "id": "individual_private_details",
    "unique": true,
    "confidential": true,
    "description": "Private details of the individual",
    "variables": [
    {
        "id": "full_name",
        "name": "Full name",
        "category": "Personal",
        "type": "TEXT",
        "defaultValue": "",
        "required": true,
        "multiValue": false,
        "allowedValues": [],
        "rank": 1,
        "dependsOn": "",
        "description": "Individual full name",
        "attributes": {}
    },
    {
        "id": "age",
        "name": "Age",
        "category": "Personal",
        "type": "INTEGER",
        "required": true,
        "multiValue": false,
        "allowedValues": [
        "0:120"
        ],
        "rank": 2,
        "dependsOn": "",
        "description": "Individual age",
        "attributes": {}
    }]
}
```

The next one will be used to store some additional metadata from the _Samples_ extracted from the _Individuals._ 

```text
{
  "unique": true,
  "confidential": false,
  "id": "sample_metadata",
  "description": "Sample origin",
  "variables": [
{
      "id": "cell_line",
      "name": "Cell line",
      "category": "string",
      "type": "TEXT",
      "required": false,
      "multiValue": false,
      "allowedValues": [],
      "rank": 2,
      "dependsOn": "",
      "description": "Sample cell line",
      "attributes": {}
    },
      {
      "id": "cell_type",
      "name": "Cell type",
      "category": "string",
      "type": "TEXT",
      "required": false,
      "multiValue": false,
      "allowedValues": [],
      "rank": 3,
      "dependsOn": "",
      "description": "Sample cell type",
      "attributes": {}
    }
}
```

### Annotations

An [_Annotation Set_](https://github.com/opencb/opencga/blob/develop/opencga-core/src/main/java/org/opencb/opencga/core/models/AnnotationSet.java) is the set of [_Annotations_](https://github.com/opencb/opencga/blob/develop/opencga-core/src/main/java/org/opencb/opencga/core/models/Annotation.java) __given for a concrete _Annotable_ entry using a particular _Variable Set_ template. The most important fields of an _Annotation Set_ are:

* **id:** Unique name to identify the annotation set created.
* **variableSetId:**Unique value identifying the _Variable Set_ the _Annotation Set_ is using to define the _Annotations._
* **annotations:** List of _Annotations_ or, in other words, values assigned for each _Variable_ defined in the _Variable Set_ corresponding to the _variableSetId._

The _Annotations_ are just key-value objects where each key need to match any of the _Variable names_ defined in the _Variable Set,_ and the values will correspond to the actual _Annotation_ of the _Variable._

Every time an annotation is made, OpenCGA will make, at least, the following checks:

* The data types of the _Annotations_ match the types defined for the _Variables._
* No mandatory _Variable_ is missing an _Annotation_.
* The value for a particular _Variable_ matches any of the allowed values if this array is provided and non empty.

#### Examples

An _Annotation_ example for both _Variable Sets_ examples can be found below:

```text
{
  "id": "annotation_set_id",
  "variableSetId": "individual_private_details",
  "annotations": {
    "full_name": "John Smith",
    "age": 60,
    "gender": "MALE",
    "hpo": ["HP:0000118", "HP:0000220"]
  }
}
```

```text
{
  "id": "annotation_set_id",
  "variableSetId": "sample_metadata",
  "annotations": {
    "tissue": "	umbilical cord blood",
    "cell_type": "multipotent progenitor",
    "preparation": "100 (or less, if 100 were not available) highly purified Haematopoietic stem and progenitor cells..."
  }
}
```



