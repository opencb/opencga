#!/bin/bash
set -e

# Block until cloud-init completes
cloud-init status --wait  > /dev/null 2>&1

# Did cloud init fail?
[ $? -ne 0 ] && echo 'Cloud-init failed' && cat /var/log/cloud-init-output.log && cloud-init status --long && exit 1

echo 'Cloud-init succeeded at ' `date -R`
cloud-init status --long
cat /var/log/cloud-init-output.log 

