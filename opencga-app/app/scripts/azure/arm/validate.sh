#!/bin/bash
cd $(dirname "$0")

set -e
npx armval "**/azuredeploy.json"