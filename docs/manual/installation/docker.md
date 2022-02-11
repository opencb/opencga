# Running Docker

## Overview <a id="Docker-Overview"></a>

OpenCGA produces **four** different Docker images for runnning OpenCGA in two main different modes: _cluster_ and _local_. All of them run Java 8 and can be found in [OpenCB Docker Hub](https://hub.docker.com/u/opencb), the docker images are:

* \*\*\*\*[opencga-base](https://hub.docker.com/r/opencb/opencga-base): base image for building the other images, this image contains the OpenCGA basic installation folder.
* [opencga-init](https://hub.docker.com/r/opencb/opencga-init): image running REST web services
* [opencga-demo](https://hub.docker.com/r/opencb/opencga-demo): all-in-one image with all OpenCGA components running
* [opencga-ext-tools](https://hub.docker.com/r/opencb/opencga-ext-tools): image that contains external tools (e.g., samtools, fastqc, R, ...) used by OpenCGA analysis

You can use OpenCGA Docker image to run a complete OpenCGA platform locally. You can use the other images to deploy a whole cluster solution in a cloud environment using Kubernetes. Docker Images are deployed in [Docker Hub OpenCB organisation](https://hub.docker.com/orgs/opencb).

### Implementation <a id="Docker-Implementation"></a>

OpenCGA publishes a number of images into [DockerHub](https://hub.docker.com/u/opencb) for user ease. These images are based on _Alpine JRE_ images to keep sizes as small as possible, contains OpenCGA binaries, for complete contents of image, please have a look at Dockerfile in github. A typical image name will follow the following structure :

* opencga:{_OPENCGA\_VERSION\_NUMBER_}-{_VARIANT\_STORAGE\_FLAVOUR_}
  * OPENCGA\_VERSION\_NUMBER \_\_ will be like 1.4.0, 1.4.2, 2.0 etc
  * VARIANT\_STORAGE\_FLAVOUR can be mongoDB 4.0, hdinshigh, emr etc
  * e.g. **opencga:1.4.0-mongo4.0**

OpenCGA has published a docker image for quick testing and playing without going through hassle to learn, wait and install each and every OpenCGA components. The docker image is available at the public docker registry under the repository _**opencb/opencga-demo**._

The OpenCGA demo docker image contains the following components:

* OpenCGA binaries
* MongoDB 4.0
* Solr 6.6 \(_default_\) 
* _**init.sh,**_ a bash script to :
  1. Install OpenCGA catalog 
  2. Populate data \(optional\)

To download OpenCGA demo image, use the command [_docker pull_](https://docs.docker.com/engine/reference/commandline/pull/) with the OpenCB enterprise, opencga-demo repository and tag.

## Run OpenCGA Cluster

Kubernetes, Ansible, OpenStack, ....

## Run OpenCGA Local

This mode is not intended for production but for a user demo.

## Build Docker Images

