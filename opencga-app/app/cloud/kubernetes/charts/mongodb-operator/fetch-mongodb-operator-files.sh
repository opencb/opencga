#!/bin/sh

set -e -x

# Don't move the PWD until we found out the realpath. It could be a relative path.
cd "$(dirname "$0")"

git clone --branch v0.5.1 https://github.com/mongodb/mongodb-kubernetes-operator.git


mkdir -p templates/operator
cp -r mongodb-kubernetes-operator/config/crd/bases/mongodbcommunity.mongodb.com_mongodbcommunity.yaml \
      mongodb-kubernetes-operator/config/rbac/ \
      mongodb-kubernetes-operator/config/manager/manager.yaml \
      templates/operator/

rm templates/operator/rbac/kustomization.yaml

rm -rf mongodb-kubernetes-operator
#kubectl apply -f config/crd/bases/mongodbcommunity.mongodb.com_mongodbcommunity.yaml
#kubectl apply -k config/rbac/
#kubectl create -f config/manager/manager.yaml
