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
ifneq ("$(wildcard $(ENVFILE))","")
    include $(ENVFILE)
    export
endif

# Make sure MAKECMDGOALS is set to default target if empty
ifeq ($(MAKECMDGOALS),)
MAKECMDGOALS := build-all
endif

# Check required variables are set for target build-all
ifeq ($(MAKECMDGOALS), build-all)
ifeq ($(PUBLISH), true)
ifeq ($(DOCKER_USERNAME),)
$(error DOCKER_USERNAME is not set. It is required when PUBLISH is true.)
endif
ifeq ($(DOCKER_PASSWORD),)
$(error DOCKER_PASSWORD is not set. It is required when PUBLISH is true.)
endif
endif
endif

# Check required variables are set for target build
ifeq ($(MAKECMDGOALS), build)
ifeq ($(APP_NAME),)
$(error APP_NAME is not set)
endif
endif

# Check required variables are set for target tag
ifeq ($(MAKECMDGOALS), tag)
ifeq ($(APP_NAME),)
$(error APP_NAME is not set)
endif
ifeq ($(DOCKER_USERNAME),)
$(error DOCKER_USERNAME is not set)
endif
endif

# Check required variables are set for target login
ifeq ($(MAKECMDGOALS), login)
ifeq ($(DOCKER_PASSWORD),)
$(error DOCKER_PASSWORD is not set)
endif
ifeq ($(DOCKER_USERNAME),)
$(error DOCKER_USERNAME is not set)
endif
endif

# Check required variables are set for target publish
ifeq ($(MAKECMDGOALS), publish)
ifeq ($(APP_NAME),)
$(error APP_NAME is not set)
endif
endif

# Set defaults for optional variables
$(info $(DOCKER_SERVER))
ifneq ($(DOCKER_SERVER),)
DOCKER_SERVER := $(DOCKER_SERVER)/
endif

DOCKER_USERNAME ?= <not set>
DOCKER_REPO ?= $(DOCKER_USERNAME)
TAG ?= $(shell git rev-parse --verify HEAD)
PUBLISH ?= false
PATH_PREFIX = opencga-app/app/scripts/docker/$(APP_NAME)
BRANCH = $(shell git branch | grep \* | cut -d ' ' -f2)
COMMIT = $(shell git rev-parse --verify HEAD)

# Set default make target
.DEFAULT_GOAL := build-all

# Print useful metadata about the build
.PHONY: metadata
metadata:
	@echo
	@echo "---------------------"
	@echo "Build Metadata"
	@echo "---------------------"
	@echo "Branch:          $(BRANCH)"
	@echo "Commit:          $(COMMIT)"
	@echo "Docker Tag:      $(TAG)"
	@echo "Docker Repo:     $(DOCKER_REPO)"
	@echo "Docker Username: $(DOCKER_USERNAME)"
	@echo "Docker Server:   $(DOCKER_SERVER)"
	@echo "Publish:         $(PUBLISH)"
	@echo "---------------------"
	@echo

# Run a script to build all docker images
.PHONY: build-all
build-all: metadata
	./build-all.sh

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
