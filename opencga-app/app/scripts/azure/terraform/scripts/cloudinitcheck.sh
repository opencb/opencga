#!/bin/bash
set -e

# Block until cloud-init completes
cloud-init status --wait  > /dev/null 2>&1

# Did cloud init fail?
[ $? -ne 0 ] && echo 'Cloud-init failed' && cloud-init status --long && exit 1

echo 'Cloud-init succeeded at ' `date -R`
cloud-init status --long

