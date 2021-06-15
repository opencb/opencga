# SampleCollection
## Overview
Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed vestibulum aliquet lobortis. Pellentesque venenatis lacus quis nibh interdum
 finibus.
### Fields tags 
| Field | unique | required | immutable| internal|
| :--- | :---: | :---: |:---: |:---: |
| tissue | <img src="http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/check.png" width="16px" heigth="16px"> | <img src="http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/error.png" width="16px" heigth="16px"> |<img src="http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/error.png" width="16px" heigth="16px"> | <img src="http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/check.png" width="16px" heigth="16px"> |
| organ | <img src="http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/check.png" width="16px" heigth="16px"> | <img src="http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/error.png" width="16px" heigth="16px"> |<img src="http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/error.png" width="16px" heigth="16px"> | <img src="http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/check.png" width="16px" heigth="16px"> |
| quantity | <img src="http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/check.png" width="16px" heigth="16px"> | <img src="http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/error.png" width="16px" heigth="16px"> |<img src="http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/error.png" width="16px" heigth="16px"> | <img src="http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/check.png" width="16px" heigth="16px"> |
| method | <img src="http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/error.png" width="16px" heigth="16px"> | <img src="http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/check.png" width="16px" heigth="16px"> |<img src="http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/check.png" width="16px" heigth="16px"> | <img src="http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/error.png" width="16px" heigth="16px"> |
| date | <img src="http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/check.png" width="16px" heigth="16px"> | <img src="http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/error.png" width="16px" heigth="16px"> |<img src="http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/error.png" width="16px" heigth="16px"> | <img src="http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/check.png" width="16px" heigth="16px"> |
| attributes | <img src="http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/error.png" width="16px" heigth="16px"> | <img src="http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/check.png" width="16px" heigth="16px"> |<img src="http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/check.png" width="16px" heigth="16px"> | <img src="http://docs.opencb.org/s/en_GB/7101/4f8ce896bdf903a209ab02696e335acf844f5c2c/_/images/icons/emoticons/error.png" width="16px" heigth="16px"> |
### Fields without tags 
``
### Fields for Create Operations 
`method* attributes* `
### Fields for Update Operations
`attributes `
### Fields uniques
`tissue organ quantity date `
## Data Model
### SampleCollection
You can find the Java code [here](https://github.com/opencb/opencga/blob/master/opencga-core/src/main/java/org/opencb/opencga/core/models/sample/SampleCollection.java).

| Field | Tags | Description |
| :--- | :--- | :--- |
| **tissue**<br>*String* <br> |**`Internal, Unique, Immutable`** | <p>Nullam commodo tortor nec lectus cursus finibus. Sed quis orci fringilla, cursus diam quis, vehicula sapien. Etiam bibendum dapibus<br> lectus, ut ultrices nunc vulputate ac.</p></br> |
| **organ**<br>*String* <br>since: 2.1 |**`Internal, Unique, Immutable`** | <p>Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed vestibulum aliquet lobortis. Pellentesque venenatis lacus quis nibh<br> interdum finibus.</p>**`The sample collection is a list of samples`**</br><a href="https://www.zettagenomics.com">ZetaGenomics</a> |
| **~~quantity~~ <br> Deprecated**<br>*String* <br> |**`Internal, Unique, Immutable`** | <p>Nullam commodo tortor nec lectus cursus finibus. Sed quis orci fringilla, cursus diam quis, vehicula sapien. Etiam bibendum dapibus<br> lectus, ut ultrices nunc vulputate ac.</p></br> |
| **method**<br>*String* <br> |**`Required, Immutable`** | <p>Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed vestibulum aliquet lobortis. Pellentesque venenatis lacus quis nibh<br> interdum finibus.</p></br> |
| **date**<br>*String* <br> |**`Internal, Unique, Immutable`** | <p>Nullam commodo tortor nec lectus cursus finibus. Sed quis orci fringilla, cursus diam quis, vehicula sapien. Etiam bibendum dapibus<br> lectus, ut ultrices nunc vulputate ac.</p></br> |
| **attributes**<br>*Map* <br> |**`Required`** | <p>Proin aliquam ante in ligula tincidunt, cursus volutpat urna suscipit. Phasellus interdum, libero at posuere blandit, felis dui<br> dignissim leo, quis ullamcorper felis elit a augue.</p></br> |
## Related data models
