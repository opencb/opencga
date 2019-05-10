# Dockerized OpenCGA
OpenCGA can be packaged as a Docker container by leveraging the included Dockerfiles.
These Dockerfiles contain:
 - **opencga** : A base image including the OpenCGA build output directory
 - **opencga-daemon** : An image to run the OpenCGA daemon
 - **opencga-init**: An image to manipulate OpenCGA configuration files with overrides
 - **opencga-app**: An image to host the OpenCGA web service on Tomcat
 - **iva**: An image to run IVA on Apache HTTP

# Make
We have provided a Makefile to make it simple to work with the included Dockerfiles as part of a local development process or continous integration system.

## Usage
Building all the OpenCGA docker images can be achieved by running the following command in the `opencga-app/app/scripts/docker/` directory:

`make`

If you have an existing OpenCGA build in the `build` directory, this command will containerize it and build the related container images. If you haven't already built OpenCGA this command will build the current OpenCGA repository inside a container and write the output to the `build` directory.

If you wish to run this command from any other directory, simply use the command:

`make -f path/to/Makefile`

You can provide parameters to the build by passing environment variables into the command:

`make DOCKER_USERNAME="user" DOCKER_PASSWORD="pass" PUBLISH=true TAG="v0.0.1"`

So that you don't have to keep tweaking this command, you can also provide parameters by writing a `.env` file. The default location for the file is `opencga-app/app/scripts/docker/make_env`. If you want to override this location, please provide the command line variable `ENVFILE=/path/to/envfile`.

`ENVFILE=/opencga-app/app/scripts/docker/customenv make`

The available parameters that can be set on the command line or in your env file are:
 - DOCKER_USERNAME=''   : Username to login to the docker registry
 - DOCKER_PASSWORD=''   : Password to login to the docker registry
 - DOCKER_SERVER=''     : Docker registry server (default: docker.io)
 - DOCKER_REPO=''       : Docker registry repository (default: docker username for docker.io)
 - DOCKER_BUILD_ARGS='' : Additional build arguments to pass to the docker build command
 - PUBLISH=''           : Set to 'true' to publish the docker images to a container registry
 - TAG=''               : Set to override the default Git commit SHA docker image tag

If you want to perform a specific action, you can do so by using one of the following make commands:
 - `make login`
 - `make APP_NAME=opencga DOCKER_BUILD_ARGS=--no-cache build`
 - `make APP_NAME=opencga tag`
 - `make APP_NAME=opencga publish`

As you can see, you can still provide arguments to the `make` targets by defining them on the command line. Using a specific target usually means targetting a specific app, this requires you to pass the app name to make using the `APP_NAME` paramter.

> Please note: Parameters defined in the `.env` file will take precedent over command line parameters.

# Remote Docker Daemon
Some developers use a workflow that uses a remote docker daemon, such as, Docker for Windows or Docker for Mac.
In this scenario you won't be able to build OpenCGA using the default `make` target or `build` target. This is because these targets expect to be able to mount the source code from a host directory. Docker doesn't support doing this to a remote daemon. There are 2 possible work arounds for this scenario.

 * Build OpenCGA on the host first. If you have an existing build, `make` will pick it up at run time.
 * Create a Docker volume, run a temporary busybox container, use `docker cp` to copy the entire OpenCGA source into it, kill and remove the temporary container, edit the `build-all.sh` script to mount the docker volume at th expected `/src/` path.