#!make

# Override default location by
# setting ENVFILE
ENVFILE ?= make_env
include $(ENVFILE)
export

# Set defaults
DOCKER_REPO ?= $(DOCKER_USERNAME)
ifdef DOCKER_SERVER
	DOCKER_SERVER := $(DOCKER_SERVER)/
endif
TAG ?= $(shell git rev-parse --verify HEAD)	# Default to git commit SHA
SEMVER ?= 0.0.0
PATH_PREFIX ?= "."

.PHONY: build test image publish run stop login tag version release

version:
	echo "Commit SHA: $(TAG)\nVersion: $(SEMVER)"

# Builds the container
build:
	docker build $(DOCKER_BUILD_ARGS) -f "${PATH_PREFIX}/Dockerfile" .

# Builds and locally tags the container
image:
	docker build $(DOCKER_BUILD_ARGS) -t $(APP_NAME) -f "${PATH_PREFIX}/Dockerfile" .

# Tag a docker image with git commit
tag: image
	docker tag $(APP_NAME) $(DOCKER_SERVER)$(DOCKER_REPO)/$(APP_NAME):$(TAG)

# Tag a docker image with semantic version
semver: tag
	docker tag $(APP_NAME) $(DOCKER_SERVER)$(DOCKER_REPO)/$(APP_NAME):$(SEMVER)

# Logs in to docker repository
login:
	echo $(DOCKER_PASSWORD) | docker login -u $(DOCKER_USERNAME) --password-stdin $(DOCKER_SERVER)

# Publishes an image to docker repository
publish: login tag
	docker push $(DOCKER_SERVER)$(DOCKER_REPO)/$(APP_NAME):$(TAG)

# Publishes a releasable image to docker repository
release: login semver publish
	docker push $(DOCKER_SERVER)$(DOCKER_REPO)/$(APP_NAME):$(SEMVER)

# Stops a running image
stop:
	docker stop $(APP_NAME); docker rm $(APP_NAME)

default: build
