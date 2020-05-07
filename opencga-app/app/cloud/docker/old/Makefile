#!make

# Usage:
# ---
# Please refer to README.md

PREFIX = opencga-app/app/cloud/docker/
ENVFILE ?= $(PREFIX)/make_env
BUILD_PATH ?= ./build
MAKE_ENV := $(shell echo '$(.VARIABLES)' | awk -v RS=' ' '/^[a-zA-Z0-9]+$$/' > /dev/null 2>&1)
SHELL_EXPORT := $(foreach v,$(MAKE_ENV),$(v)='$($(v))')

# Set default make target
.DEFAULT_GOAL := build-all

# Make all vars available to sub Makefiles
export

# Run a script to build all docker images
.PHONY: build-all
build-all:
	(cd $(shell git rev-parse --show-toplevel); $(SHELL_EXPORT) $(PREFIX)/build-all.sh $(ENVFILE) $(BUILD_PATH))

.PHONY: build
build:
	(cd $(shell git rev-parse --show-toplevel); $(MAKE) build -f $(PREFIX)/Makefile.docker)

# Tags a docker image with git commit SHA
.PHONY: tag
tag:
	(cd $(shell git rev-parse --show-toplevel); $(MAKE) tag -f $(PREFIX)/Makefile.docker)

# Logs in to a docker registry
.PHONY: login
login:
	(cd $(shell git rev-parse --show-toplevel); $(MAKE) login -f $(PREFIX)/Makefile.docker)

# Publishes a docker image to docker registry
.PHONY: publish
publish: tag login
	(cd $(shell git rev-parse --show-toplevel); $(MAKE) publish -f $(PREFIX)/Makefile.docker)

.PHONY: default
default: build-all;
