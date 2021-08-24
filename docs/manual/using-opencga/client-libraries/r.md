---
description: >-
  If you're a R user and you're keen of running OpenCGA through R, you're in the
  right place!
---

# opencgaR - R  library

**opencgaR** is a R library that interacts with OpenCGA REST API to execute any operation supported by the web services through R code.

The client offers programmatic access to the implemented REST web services, facilitating the access and analysis of data stored in OpenCGA. From version 2.0.0 data is returned in a new _RestResponse_ object which contains metadata and the results. The client also implements some handy methods to return information from this object.

{% hint style="info" %}
**opencgaR code** has been implemented by _Marta Bleda._ It's open-source and can be found at [https://github.com/opencb/opencga/tree/develop/opencga-client/src/main/R](https://github.com/opencb/opencga/tree/develop/opencga-client/src/main/R). It can be installed easily by downloading the pre-build package. Please, find more details on how to use the R library at [Using the R client](http://docs.opencb.org/display/opencga/Using+the+R+client).
{% endhint %}

## Installation <a id="R-Installation"></a>

### Requisites

* An operating machine with R installed and functional. **opencgaR** requires at least **R version 3.4 \(**although most of the code is fully compatible with earlier versions\). 
* Have the pre-build R package stored in your local machine. Alternatively you can also provide the URL to the pre-build R package file.

 The pre-build R package of **opencgaR** can be downloaded from the OpenCGA v2.0.0 GitHub Release at [https://github.com/opencb/opencga/releases](https://github.com/opencb/opencga/releases).

Once requirements have been fulfilled, installing opencgaR becomes as easy as use the `install.packages` function in R. `install.packages` can also install a source package from a remote    `.tar.gz` file by providing the URL to such file.

### Installation from the R terminal

```
## Install opencgaR by providing the URL to the package
> install.packages("opencgaR_2.0.0.tar.gz", repos=NULL, type="source")
```

