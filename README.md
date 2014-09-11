OpenCGA
=======

Big data processing and analysis solution for genomic data.


## Prerequisites
OpenCGA depends on some other projects:

* datastore: https://github.com/opencb/datastore (branch 'develop')
* biodata: https://github.com/opencb/biodata (branch 'develop')
* java-common-libs: https://github.com/opencb/java-common-libs (branch 'develop')
* cellbase: https://github.com/opencb/cellbase (branch 'develop')
* variant: https://github.com/opencb/variant (branch 'develop')

## Building
The repositories listed above must be cloned and compiled in the specified order before building OpenCGA. The build process is managed by Maven scripts. Please run the following command for each repository, including OpenCGA's:

mvn clean compile install

If this command would crash in any project due to tests failures (see "Testing"), please use:

mvn clean compile install -DskipTests

## Testing
Should you want to run the unit tests, you can use Maven or your favorite IDE. Just take note that some tests require of certain database back-ends (MongoDB, Hadoop HBase) and may fail if they are not available.


## AES encryption

For AES encryption please download UnlimitedJCEPolicyJDK7.zip from http://www.oracle.com/technetwork/java/javase/downloads/jce-7-download-432124.html .
Then unzip the file into $JAVA_HOME/jre/lib/security