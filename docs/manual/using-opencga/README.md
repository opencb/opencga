---
description: >-
  You can interact with your data in OpenCGA in several different ways, find the
  one that suits you most and get started!
---

# Using OpenCGA

## Overview <a id="UsingOpenCGA-RESTfulWebServices"></a>

OpenCGA is implemented to provide users with multiple resources to manage and query variant and phenotypic information. 

Each method comes with specific advantages: whilst ones are more versatile and allow users to do almost anything, others are less limited in the use but quicker to configure and use. 

OpenCGA is a very versatile piece of software and thus we've dedicated special care to allow the final user to explore every possibility. Anyone can explore and choose the client methods which adapts better to their own specific use case.

### REST Web Services

OpenCGA implements a comprehensive and well-designed REST web service API, this consists of more than 200 web services to allow querying and operating data in OpenCGA. You can get more info at [RESTful Web Services](http://docs.opencb.org/display/opencga/RESTful+Web+Services) page.

We have implemented **three different ways** to query and operate OpenCGA through the REST web services API:

* [REST Client Libs](http://docs.opencb.org/display/opencga/RESTful+Web+Services#RESTfulWebServices-ClientLibraries): four different client libraries have been implemented to ease the use of REST web services, This allows bioinformaticians to easily integrate OpenCGA in any pipeline. The four libraries are equally functional and fully maintained, these are [_Java_](http://docs.opencb.org/display/opencga/Java)_,_ [_Python_](http://docs.opencb.org/display/opencga/Python) \(available at [PyPI](https://pypi.org/project/pyopencga/)\), [_R_](http://docs.opencb.org/display/opencga/R) and [_JavaScript_](http://docs.opencb.org/display/opencga/JavaScript)
* [Command Line](http://docs.opencb.org/display/opencga/Command+Line): users and administrators can use _**opencga.sh**_ command line to query and operate OpenCGA. 
* [IVA Web Application](http://docs.opencb.org/display/opencga/IVA+Web+App): an interactive web application called IVA has been developed to query and visualisation OpenCGA data.

### gRPC



