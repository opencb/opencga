# Overview
OpenCGA is an open-source project that aims to provide a Big Data storage engine and analysis framework for genomic scale data analysis of hundreds of terabytes or even petabytes. OpenCGA provides a scalable and high-performance **Storage Engine** framework to index biological data such as BAM or VCF files using different NoSQL databases, currently only MongoDB has been fully developed. A data analytics and genomic **Analysis** layer interface has been implemented over this big data storage index. A metadata **Catalog** has also been developed to provide authentification and ACLs and to keep track all of files and sample annotation. All these can be queried through a comprehensive RESTful web services API or using the command line interface.

OpenCGA constitutes the big data analysis component of [OpenCB](http://www.opencb.org/) initiative. It is used by other projects such as [EMBL-EBI EVA](https://www.ebi.ac.uk/eva/), [Babelomics](http://www.babelomics.org/) or [BierApp](http://bierapp.babelomics.org/).

### Documentation
You can find OpenCGA documentation and tutorials at: https://github.com/opencb/opencga/wiki.

### Issues Tracking
You can report bugs or request new features at [GitHub issue tracking](https://github.com/opencb/opencga/issues).

### Release Notes and Roadmap
Releases notes are available at [GitHub releases](https://github.com/opencb/opencga/releases).

Roadmap is available at [GitHub milestones](https://github.com/opencb/opencga/milestones). You can report bugs or request new features at [GitHub issue tracking](https://github.com/opencb/opencga/issues).

### Versioning
OpenCGA is versioned following the rules from [Semantic versioning](https://semver.org/).

### Maintainers
We recommend to contact OpenCGA developers by writing to OpenCB mailing list opencb@googlegroups.com. Current main developers and maintainers are:
* Ignacio Medina (im411@cam.ac.uk) (_Founder and Project Leader_)
* Jacobo Coll (jacobo.coll-moragon@genomicsengland.co.uk)
* Pedro Furio (pedro.furio@genomicsengland.co.uk)

##### Former Contributors
* Matthias Haimel (mh719@cam.ac.uk)
* Asuncion Gallego (agallego@cipf.es)
* Cristina Y. Gonzalez (cyenyxe@ebi.ac.uk)
* Jose M. Mut (jmmut@ebi.ac.uk)
* Roberto Alonso (ralonso@cipf.es)
* Alejandro Aleman (aaleman@cipf.es)
* Franscisco Salavert (fsalavert@cipf.es)

##### Contributing
OpenCGA is an open-source and collaborative project. We appreciate any help and feedback from users, you can contribute in many different ways such as simple bug reporting and feature request. Dependending on your skills you are more than welcome to develop client tools, new features or even fixing bugs.


# How to build
OpenCGA is mainly developed in Java and it uses [Apache Maven](https://maven.apache.org/) as building tool. OpenCGA requires Java 8, in particular **JDK 1.8.0_60+**, and other OpenCB dependencies that can be found in [Maven Central Repository](https://search.maven.org/).

Stable releases are merged and tagged at **_master_** branch, you are encourage to use latest stable release for production. Current active development is carried out at **_develop_** branch and need **Java 8**, in particular **JDK 1.8.0_60+**, only compilation is guaranteed and bugs are expected, use this branch for development or for testing new functionalities. Dependencies of **_master_** branch are ensured to be deployed at [Maven Central Repository](https://search.maven.org/), but dependencies for **_develop_** branch may require users to download and install the following git repositories from OpenCB:
* _java-common-libs_: https://github.com/opencb/java-common-libs (branch 'develop')
* _biodata_: https://github.com/opencb/biodata (branch 'develop')
* _cellbase_: https://github.com/opencb/cellbase (branch 'develop')
* _oskar_: https://github.com/opencb/oskar (branch 'develop')

### Cloning
OpenCGA is an open-source and free project, you can download default **_develop_** branch by executing:

    imedina@ivory:~$ git clone https://github.com/opencb/opencga.git
    Cloning into 'opencga'...
    remote: Counting objects: 20267, done.
    remote: Compressing objects: 100% (219/219), done.
    remote: Total 20267 (delta 105), reused 229 (delta 35)
    Receiving objects: 100% (20267/20267), 7.23 MiB | 944.00 KiB/s, done.
    Resolving deltas: 100% (6363/6363), done.

Latest stable release at **_master_** branch can be downloaded executing:

    imedina@ivory:~$ git clone -b master https://github.com/opencb/opencga.git
    Cloning into 'opencga'...
    remote: Counting objects: 20267, done.
    remote: Compressing objects: 100% (219/219), done.
    remote: Total 20267 (delta 105), reused 229 (delta 35)
    Receiving objects: 100% (20267/20267), 7.23 MiB | 812.00 KiB/s, done.
    Resolving deltas: 100% (6363/6363), done.


### Build
You can build OpenCGA by executing the following command from the root of the cloned repository:

    $ mvn clean install -DskipTests

For changing particular settings during buildings you can create a profile in _~/.m2/settings.xml_ in the \<*profiles*\> section using this template:

        <?xml version="1.0" encoding="UTF-8"?>
        <settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"
          xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
          xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0 http://maven.apache.org/xsd/settings-1.0.0.xsd">
            <profiles>
                <profile>
                    <id>custom-config</id>
                    <activation>
                        <activeByDefault>true</activeByDefault>
                    </activation>
                    <properties>
                        <opencga.war.name>opencga-${project.version}</opencga.war.name>

                        <!-- General -->
                        <OPENCGA.INSTALLATION.DIR>/opt/opencga</OPENCGA.INSTALLATION.DIR>
                        <OPENCGA.USER.WORKSPACE>file:///opt/opencga/sessions/</OPENCGA.USER.WORKSPACE>
                        <OPENCGA.JOBS.DIR>${OPENCGA.USER.WORKSPACE}/jobs/</OPENCGA.JOBS.DIR>
                        <OPENCGA.DB.PREFIX>opencga</OPENCGA.DB.PREFIX>
                        <OPENCGA.EXECUTION.MODE>LOCAL</OPENCGA.EXECUTION.MODE>

                        <!-- Client -->
                        <OPENCGA.CLIENT.REST.HOST>http://localhost:8080/${opencga.war.name}</OPENCGA.CLIENT.REST.HOST>
                        <OPENCGA.CLIENT.GRPC.HOST>http://localhost:9091</OPENCGA.CLIENT.GRPC.HOST>
                        <OPENCGA.CLIENT.ORGANISM.SCIENTIFIC_NAME>Homo sapiens</OPENCGA.CLIENT.ORGANISM.SCIENTIFIC_NAME>
                        <OPENCGA.CLIENT.ORGANISM.COMMON_NAME>human</OPENCGA.CLIENT.ORGANISM.COMMON_NAME>
                        <OPENCGA.CLIENT.ORGANISM.TAXONOMY_CODE>9606</OPENCGA.CLIENT.ORGANISM.TAXONOMY_CODE>
                        <OPENCGA.CLIENT.ORGANISM.ASSEMBLY></OPENCGA.CLIENT.ORGANISM.ASSEMBLY>

                        <OPENCGA.SERVER.REST.PORT>9090</OPENCGA.SERVER.REST.PORT>
                        <OPENCGA.SERVER.GRPC.PORT>9091</OPENCGA.SERVER.GRPC.PORT>
                        <OPENCGA.MONITOR.PORT>9092</OPENCGA.MONITOR.PORT>

                        <!-- Catalog -->
                        <OPENCGA.CATALOG.DB.HOSTS>localhost:27017</OPENCGA.CATALOG.DB.HOSTS>
                        <OPENCGA.CATALOG.DB.USER></OPENCGA.CATALOG.DB.USER>
                        <OPENCGA.CATALOG.DB.PASSWORD></OPENCGA.CATALOG.DB.PASSWORD>
                        <OPENCGA.CATALOG.DB.AUTHENTICATION_DATABASE></OPENCGA.CATALOG.DB.AUTHENTICATION_DATABASE>
                        <OPENCGA.CATALOG.DB.CONNECTIONS_PER_HOST>20</OPENCGA.CATALOG.DB.CONNECTIONS_PER_HOST>

                        <!-- Storage -->
                        <OPENCGA.STORAGE.DEFAULT_ENGINE>mongodb</OPENCGA.STORAGE.DEFAULT_ENGINE>
                        <OPENCGA.STORAGE.CACHE.HOST>localhost:6379</OPENCGA.STORAGE.CACHE.HOST>
                        <OPENCGA.STORAGE.SEARCH.HOST>http://localhost:8983/solr/</OPENCGA.STORAGE.SEARCH.HOST>
                        <OPENCGA.STORAGE.STUDY_METADATA_MANAGER></OPENCGA.STORAGE.STUDY_METADATA_MANAGER>

                        <!-- Storage Variants general -->
                        <OPENCGA.STORAGE.VARIANT.DB.HOSTS>localhost:27017</OPENCGA.STORAGE.VARIANT.DB.HOSTS>
                        <OPENCGA.STORAGE.VARIANT.DB.USER></OPENCGA.STORAGE.VARIANT.DB.USER>
                        <OPENCGA.STORAGE.VARIANT.DB.PASSWORD></OPENCGA.STORAGE.VARIANT.DB.PASSWORD>

                        <!-- Storage Alignments general -->
                        <OPENCGA.STORAGE.ALIGNMENT.DB.HOSTS>localhost:27017</OPENCGA.STORAGE.ALIGNMENT.DB.HOSTS>
                        <OPENCGA.STORAGE.ALIGNMENT.DB.USER></OPENCGA.STORAGE.ALIGNMENT.DB.USER>
                        <OPENCGA.STORAGE.ALIGNMENT.DB.PASSWORD></OPENCGA.STORAGE.ALIGNMENT.DB.PASSWORD>

                        <!-- Storage-mongodb -->
                        <OPENCGA.STORAGE.MONGODB.VARIANT.DB.AUTHENTICATION_DATABASE></OPENCGA.STORAGE.MONGODB.VARIANT.DB.AUTHENTICATION_DATABASE>
                        <OPENCGA.STORAGE.MONGODB.VARIANT.DB.CONNECTIONS_PER_HOST>20</OPENCGA.STORAGE.MONGODB.VARIANT.DB.CONNECTIONS_PER_HOST>

                        <!-- Storage-hadoop -->
                        <!--If empty, will use the ZOOKEEPER_QUORUM read from the hbase configuration files-->
                        <OPENCGA.STORAGE.HADOOP.VARIANT.DB.HOSTS></OPENCGA.STORAGE.HADOOP.VARIANT.DB.HOSTS>
                        <OPENCGA.STORAGE.HADOOP.VARIANT.DB.USER></OPENCGA.STORAGE.HADOOP.VARIANT.DB.USER>
                        <OPENCGA.STORAGE.HADOOP.VARIANT.DB.PASSWORD></OPENCGA.STORAGE.HADOOP.VARIANT.DB.PASSWORD>
                        <OPENCGA.STORAGE.HADOOP.VARIANT.HBASE.NAMESPACE></OPENCGA.STORAGE.HADOOP.VARIANT.HBASE.NAMESPACE>

                        <!-- Email server -->
                        <OPENCGA.MAIL.HOST></OPENCGA.MAIL.HOST>
                        <OPENCGA.MAIL.PORT></OPENCGA.MAIL.PORT>
                        <OPENCGA.MAIL.USER></OPENCGA.MAIL.USER>
                        <OPENCGA.MAIL.PASSWORD></OPENCGA.MAIL.PASSWORD>

                        <!-- cellbase -->
                        <OPENCGA.CELLBASE.VERSION>v4</OPENCGA.CELLBASE.VERSION>
                        <OPENCGA.CELLBASE.REST.HOST>http://ws.opencb.org/cellbase/</OPENCGA.CELLBASE.REST.HOST>
                        <OPENCGA.CELLBASE.DB.HOST>localhost:27017</OPENCGA.CELLBASE.DB.HOST>
                        <OPENCGA.CELLBASE.DB.USER></OPENCGA.CELLBASE.DB.USER>
                        <OPENCGA.CELLBASE.DB.PASSWORD></OPENCGA.CELLBASE.DB.PASSWORD>
                        <OPENCGA.CELLBASE.DB.AUTHENTICATION_DATABASE></OPENCGA.CELLBASE.DB.AUTHENTICATION_DATABASE>
                        <OPENCGA.CELLBASE.DB.READ_PREFERENCE>secondaryPreferred</OPENCGA.CELLBASE.DB.READ_PREFERENCE>
                    </properties>
                </profile>
            </profiles>
        </settings>

See the description of each property in https://github.com/opencb/opencga/wiki/OpenCGA-installation.

Remember that **_develop_** branch dependencies are not ensured to be deployed at Maven Central, you may need to clone and install **_develop_** branches from OpenCB _biodata_, _datastore_ and _cellbase_ repositories. After this you should have this file structure in **_opencga/build_**:

    build/
    ├── analysis
    ├── bin
    ├── clients
    ├── cloud
    ├── conf
    ├── libs
    ├── LICENSE
<<<<<<< HEAD
    ├── opencga-2.0.0-rc3.war
=======
    ├── opencga-2.0.0.war
>>>>>>> release-2.0.0
    ├── README.md
    ├── misc
    └── test


You can copy the content of the _build_ folder into the installation directory such as _/opt/opencga_.

### Testing
You can run the unit tests using Maven or your favorite IDE. Just notice that some tests may require of certain database back-ends such as MongoDB or Apache HBase and may fail if they are not available.

### Command Line Interface (CLI)
If the build process has gone well you should get an integrated help by executing:

    ./bin/opencga.sh --help

You can find more detailed documentation and tutorials at: https://github.com/opencb/opencga/wiki.

### Other Dependencies
We try to improve the admin experience by making the installation and build as simple as possible. Unfortunately, for some OpenCGA components and functionalities other dependencies are required.

##### Loading data
At this moment there are two variant storage engines available: [MongoDB](https://www.mongodb.org/) and [Apache HBase](http://hbase.apache.org/)

