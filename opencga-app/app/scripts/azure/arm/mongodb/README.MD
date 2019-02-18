# Cloud init binary data

The cloud init file uses the binary data from the two script files. 

Once a script is update the following command can be used to re-generate the binary data chunk

```
cat ./mongodb/postdeploy.sh | gzip -f | base64 -w 0
```

This should then be update in the ARM template `azuredeploy.json`