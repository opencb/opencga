# Running Docker

## Overview <a id="Docker-Overview"></a>

OpenCGA produces **four** different Docker images for different use cases, these are:

* _opencga-base_: base image for building the other images, this image contains the OpenCGA basic installation folder  
* _opencga-rest_: image running REST web services
* _opencga-master_: image running _master_ service
* _opencga_: all-in-one image with all OpenCGA components running

You can use _opencga_ Docker image to run a complete OpenCGA platform locally. You can use the other images to deploy a whole cluster solution in a cloud environment using Kubernetes. Docker Images are deployed in [Docker Hub OpenCB organisation](https://hub.docker.com/orgs/opencb).

## Design and Implementation <a id="Docker-DesignandImplementation"></a>

### Images <a id="Docker-Images"></a>

All images run with the user _**opencga**_ and run **Java 8**. Docker images are deployed in Docker Hub.

#### opencga-base <a id="Docker-opencga-base"></a>

This image contains the basic installation in directory /opt

More info at [https://hub.docker.com/repository/docker/opencb/opencga-base](https://hub.docker.com/repository/docker/opencb/opencga-base)

#### opencga-rest <a id="Docker-opencga-rest"></a>

This image is based in _opencga-base_ and runs REST web services using the REST server command-line.

#### opencga-master <a id="Docker-opencga-master"></a>

This image runs the master service

#### opencga <a id="Docker-opencga"></a>

all-in-one image with all OpenCGA components running 

### Implementation <a id="Docker-Implementation"></a>

OpenCGA publishes a number of images into [DockerHub](https://hub.docker.com/u/opencb) for user ease. These images are based on _Alpine JRE_ images to keep sizes as small as possible, contains OpenCGA binaries, for complete contents of image, please have a look at Dockerfile in github. A typical image name will follow the following structure :

* opencga:{_OPENCGA\_VERSION\_NUMBER_}-{_VARIANT\_STORAGE\_FLAVOUR_}
  * OPENCGA\_VERSION\_NUMBER __ will be like 1.4.0, 1.4.2, 2.0 etc
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

To download OpenCGA demo image, use the command [_docker pull_](https://docs.docker.com/engine/reference/commandline/pull/) with the OpenCB enterprise, opencga-demo repository and tag

