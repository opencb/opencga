#!/bin/bash
cd $(dirname "$0")

set -e
npm install armval && node node_modules/.bin/armval "**/azuredeploy.json"