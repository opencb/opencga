#!/bin/bash
cd $(dirname "$0")

set -e
npx --ignore-existing armval "**/azuredeploy.json"
