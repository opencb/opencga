#!make

# Usage:
# ---
# `make` : Will invoke the default build-all target to build all docker images
# `make APP_NAME=opencga-app build` : will build a specific docker image
# `ENVFILE /path/to/env/file make` : will build the default target using a custom `make_env` file
# `make login` : Logs into the specified docker registry
# `make APP_NAME=opencga-app tag` : will tag the specified app image should it exist
# `make APP_NAME=opencga-app publish` : will push the specified app image should it exist

# Override the `make_env` file default location by setting ENVFILE env var
ENVFILE ?= make_env
ifneq ("$(wildcard $(make_env))","")
    include $(ENVFILE)
    export
endif


# Check required variables are set for target build
ifeq ($(MAKECMDGOALS), build)
ifndef APP_NAME
$(error APP_NAME is not set)
endif
endif

# Check required variables are set for target tag
ifeq ($(MAKECMDGOALS), tag)
ifndef APP_NAME
$(error APP_NAME is not set)
endif
endif

# Check required variables are set for target login
ifeq ($(MAKECMDGOALS), login)
ifndef DOCKER_PASSWORD
$(error DOCKER_PASSWORD is not set)
endif
ifndef DOCKER_USERNAME
$(error DOCKER_USERNAME is not set)
endif
endif

# Check required variables are set for target publish
ifeq ($(MAKECMDGOALS), publish)
ifndef APP_NAME
$(error APP_NAME is not set)
endif
endif

# Set defaults for optional variables
ifneq ($(DOCKER_SERVER),"")	# If docker server set, add trailing slash
	DOCKER_SERVER := $(DOCKER_SERVER)/
endif
DOCKER_REPO ?= $(DOCKER_USERNAME)
TAG ?= $(shell git rev-parse --verify HEAD)
PUBLISH ?= false
PATH_PREFIX = opencga-app/app/scripts/docker/$(APP_NAME)

# Set default make target
.DEFAULT_GOAL := build-all

# Run a script to build all docker images
.PHONY: build-all
build-all:
	./build-all.sh $(TAG) $(DOCKER_REPO) $(DOCKER_USERNAME) $(PUBLISH)

# Builds and tags the container
.PHONY: build
build:
	(cd $(shell git rev-parse --show-toplevel); docker build $(DOCKER_BUILD_ARGS) -t $(APP_NAME) -f $(PATH_PREFIX)/Dockerfile .)

# Tags a docker image with git commit SHA
.PHONY: tag
tag:
	docker tag $(APP_NAME) $(DOCKER_SERVER)$(DOCKER_REPO)/$(APP_NAME):$(TAG)

# Logs in to a docker registry
.PHONY: login
login:
	echo $(DOCKER_PASSWORD) | docker login -u $(DOCKER_USERNAME) --password-stdin $(DOCKER_SERVER)

# Publishes a docker image to docker registry
.PHONY: publish
publish: login tag
	docker push $(DOCKER_SERVER)$(DOCKER_REPO)/$(APP_NAME):$(TAG)

.PHONY: default
default: build-all;
