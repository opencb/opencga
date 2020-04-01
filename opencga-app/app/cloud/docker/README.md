# Dockerized OpenCGA
OpenCGA can be packaged as a Docker container by leveraging the included Dockerfiles.
These Dockerfiles contain:
 - **opencga-base** : A base image including the OpenCGA build output directory
 - **opencga-init** : An image to manipulate OpenCGA configuration files with overrides
 - **opencga-r**    : An image to run R based analysis

# Build
We have provided a python script to make it simple to work with the included Dockerfiles as part of a local development process or continous integration system.

## Usage
You need to first compile OpenCGA with maven.
Then, building all the OpenCGA docker images can be achieved by running the following command in the `build/cloud/docker/` directory:

`./docker-build.py`

This command will containerize the `build` folder and build the related container images.

You can provide parameters to the build by passing environment variables into the command:

`./docker-build.py --username user --password "pass" --tag v0.0.1 push`

The available parameters that can be set on the command line or in your env file are:
```
usage: docker-build.py [-h] [--images IMAGES] [--tag TAG]
                       [--build-folder BUILD_FOLDER] [--org ORG]
                       [--username USERNAME] [--password PASSWORD]
                       [--docker-build-args DOCKER_BUILD_ARGS]
                       {build,push,delete}

positional arguments:
  {build,push,delete}   Action to execute

optional arguments:
  -h, --help            show this help message and exit
  --images IMAGES       comma separated list of images to be made, e.g.
                        base,init,r
  --tag TAG             the tag for this code, e.g. v2.0.0-hdp3.1
  --build-folder BUILD_FOLDER
                        the location of the build folder, if not default
                        location
  --org ORG             Docker organization
  --username USERNAME   Username to login to the docker registry (REQUIRED if
                        deleting from DockerHub)
  --password PASSWORD   Password to login to the docker registry (REQUIRED if
                        deleting from DockerHub)
  --docker-build-args DOCKER_BUILD_ARGS
                        Additional build arguments to pass to the docker build
                        command. Usage: --docker-build-args='ARGS' e.g:
                        --docker-build-args='--no-cache'
```

If you want to perform a specific action, you can do so by using one of the following commands:
 - `docker-build.py push`
 - `docker-build.py build --org jcoll --tag ${git log -1 --pretty=%h}`
 - `docker-build.py build --docker-build-args "--no-cache"`

# Remote Docker Daemon
Some developers use a workflow that uses a remote docker daemon, such as, Docker for Windows or Docker for Mac.
In this scenario you won't be able to build OpenCGA using the default `docker-build.py build` target. This is because these targets expect to be able to mount the source code from a host directory. Docker doesn't support doing this to a remote daemon. There are 2 possible work arounds for this scenario.

 * Build OpenCGA on the host first. If you have an existing build, `docker-build.py` will pick it up at run time.
 * Create a Docker volume, run a temporary busybox container, use `docker cp` to copy the entire OpenCGA source into it, kill and remove the temporary container, edit the `docker-build.py` script to mount the docker volume at the expected path.