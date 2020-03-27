# Cloud init binary data

The cloud init file uses a base64 encoded version of the two script files within this folder (i.e. `postdeploy.sh` and `renew_mongo_cert.sh`). If you update these files, you then need to update `azuredeploy.sh` in this folder with the updated base64 encoded representation.

Once a script is update the following commands can be used to re-generate the binary data chunk. You then need to take the base64 output and update the ARM template `azuredeploy.json`.

| source script         | command to regenerate (linux)                               | command to regenerate (mac)                            | variable to update in `azuredeploy.json` |
| --------------------- | ----------------------------------------------------------- | ------------------------------------------------------ | ---------------------------------------- |
| `postdeploy.sh`       | `cat ./mongodb/postdeploy.sh | gzip -f| base64 -w 0`        | `cat ./mongodb/postdeploy.sh | gzip -f | base64`       | `postDeployShEncoded`                    |
| `renew_mongo_cert.sh` | `cat ./mongodb/renew_mongo_cert.sh | gzip -f | base64 -w 0` | `cat ./mongodb/renew_mongo_cert.sh | gzip -f | base64` | `renewMongoCertShEncoded`                |

