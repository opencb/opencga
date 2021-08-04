# Command Line Code Generator

Need to compile and start a rest server to generate the command line.

```
mvn clean install -DskipTests -Dopencga.war.name=opencga
./build/bin/opencga-admin.sh server rest --start --port 9123
```

Then, execute the command line generator

## Java

```
python3 build/misc/cli/java_cli_generator.py http://localhost:9123/opencga opencga-app/src/main/java/org/opencb/opencga/app/cli/main
```

