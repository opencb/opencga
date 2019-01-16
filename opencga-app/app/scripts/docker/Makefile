#!make

# Override the `make_env` file default
# location by setting ENVFILE env var
ENVFILE ?= make_env
include $(ENVFILE)
export

# Check required variables are set
ifndef DOCKER_USERNAME
$(error DOCKER_USERNAME is not set)
endif
ifndef DOCKER_PASSWORD
$(error DOCKER_PASSWORD is not set)
endif

# Set defaults for optional variables
DOCKER_REPO ?= $(DOCKER_USERNAME)
ifdef DOCKER_SERVER
	DOCKER_SERVER := $(DOCKER_SERVER)/
endif
TAG ?= $(shell git rev-parse --verify HEAD)
PATH_PREFIX ?= "."
PUBLISH ?= false

# Set default make target
.DEFAULT_GOAL := build-all

# Run a script to build all docker images
.PHONY: build-all
build-all:
	./build-all.sh $(TAG) $(DOCKER_REPO) $(DOCKER_USERNAME) $(PUBLISH)

# Builds and tags the container
.PHONY: build
build:
	docker build $(DOCKER_BUILD_ARGS) -t $(APP_NAME) -f $(PATH_PREFIX)/Dockerfile .

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
