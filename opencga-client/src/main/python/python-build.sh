#!/bin/bash
set -e

## The command line Usage
display_help() {
    echo "Usage: {build|push|push-test}" >&2
    echo
    echo "   clean            Clean PyPI build packages"
    echo "   build            Build pyOpenCGA python PyPI package"
    echo "   push             Push pyOpenCGA to PyPI"
    echo "   push-test        Push pyOpenCGA to test PyPI"
    echo ""
    exit 1
}

## check mandatory parameters 'action' and 'tag' exist
if [ -z "$1" ]; then
  echo "Error: action is required"
  echo ""
  display_help
else
  ACTION=$1
fi

## Get Python base directory and move there
BASEDIR=`dirname $0`
cd $BASEDIR

## Get OpenCGA version from Python setup file
VERSION=`grep version setup.py | cut -d '=' -f 2 | tr \' \" | sed 's/[", ]//g'`


build () {
  echo "****************************"
  echo "Building PyPI package ..."
  echo "***************************"
  python3 -m pip install --user --upgrade setuptools wheel
  python3 setup.py sdist bdist_wheel
  echo ""
}

if [ $ACTION = "clean" ]; then
  echo "******************************"
  echo "Deleting build, dist and pyopencga.egg-info ..."
  echo "******************************"
  rm -rfv build dist pyopencga.egg-info
fi

if [ $ACTION = "build" ]; then
  build
fi

if [ $ACTION = "push" ]; then
  build

  echo "******************************"
  echo "Pushing to PyPI ..."
  echo "******************************"
  python3 -m pip install --user --upgrade twine
  python3 -m twine upload dist/*
fi

if [ $ACTION = "push-test" ]; then
  build

  echo "******************************"
  echo "Pushing to test PyPI..."
  echo "******************************"
  ## Get HTTP response code, if already exists then response is 200
  STATUS=$(curl -s --head -w %{http_code} https://test.pypi.org/project/pyopencga/$VERSION/ -o /dev/null)
  if [ $STATUS = "200" ]; then
    echo "Version $VERSION already exists in test PyPI!"
  else
    python3 -m pip install --user --upgrade twine
    python3 -m twine upload --repository-url https://test.pypi.org/legacy/ dist/*
  fi
fi
