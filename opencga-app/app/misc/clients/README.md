# Multi-language client generator

Need to compile and start a rest server to generate the clients.
```
mvn clean install -DskipTests -Dopencga.war.name=opencga
./build/bin/opencga-admin.sh server rest --start --port 9123
```

Then, execute the client generator for each language
## Java
```
python3 build/misc/clients/java_client_generator.py http://localhost:9123/opencga opencga-client/src/main/java/org/opencb/opencga/client/rest/clients
```

## Python
```
python3 build/misc/clients/python_client_generator.py http://localhost:9123/opencga opencga-client/src/main/python/pyopencga/rest_clients
```

## R
```
python3 build/misc/clients/r_client_generator.py http://localhost:9123/opencga opencga-client/src/main/R/R/
```

## JS
```
python3 build/misc/clients/js_client_generator.py http://localhost:9123/opencga <target>
```

