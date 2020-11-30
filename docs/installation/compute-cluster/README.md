# On-Premise HPC Cluster

You can install OpenCGA on a local HPC cluster.  Installing and configuring OpenCGA consists of different steps, as you will see on this page you must first make sure that the server\(s\) have all dependencies installed, then you can configure and complete the installation.

## Installation and Configuration

### Step 1 - Configuring the Server

OpenCGA requires Java 8, Tomcat or MongoDB. We try to keep dependencies to the minimum to ease development, installation and administration, and divide dependencies into _required_ and _optional._ You can learn about all the dependencies and how to install them at [**Installation Guide &gt; Server Configuration**](http://docs.opencb.org/display/opencga/Server+Configuration).

### Step 2 - Getting OpenCGA

There are two main ways to get OpenCGA for installation:

* You can download binaries from [OpenCGA GitHub Releases](https://github.com/opencb/opencga/releases), notice that **only** _stable_ and _pre-releases_ are tagged and _built_, if you want to test a development version see next point.
* Or You can download the source code from GitHub and use Apache Maven to compile and build it.

Please visit [Getting OpenCGA](building-from-source.md) to learn more about these two options.

### Step 3 - Install OpenCGA Binaries

These instructions assume that you have already downloaded or built openCGA binaries as described on [Using Binaries](http://docs.opencb.org/pages/createpage.action?spaceKey=opencga&title=Using+Binaries&linkCreation=true&fromPageId=327810) and [Building from Source Code](http://docs.opencb.org/display/opencga/Building+from+Source+Code).

Create an installation directory called _/opt/opencga_ and copy the contents of opencga into this :

**Note:** In case of reinstallation, you must clean the installation directory \(_/opt/opencga_\)

```bash
mkdir /opt/opencga
cp -r build/* /opt/opencga
```

### Step 4 - Configure OpenCGA

Execute the following command line to install and initialise Catalog database:

```bash
cd /opt/opencga
./bin/opencga-admin.sh catalog install <<< admin_P@ssword
```

### Step 5 - Deploy WAR file

This is the main interface to perform any action with OpenCGA. User has two different options to start web services:

* You can deploy OpenCGA in a Web Server.
* Or you can use the OpenCGA admin command line.

Next you can learn more about these two options.

#### Deploying OpenCGA in a Web Server

Install [Apache Tomcat](https://tomcat.apache.org/download-80.cgi) and copy the deploy `opencga.war`. To do this, just copy it from the compilation directory \(where you downloaded the OpenCGA repository\) into the Tomcat `webapps` directory:

`cp /opt/opencga/opencga.war $(path_to_tomcat)/webapps`

`path_to_tomcat` is where you downloaded it, or probably`/var/lib/tomcat8` if you installed via apt-get. Then, you should be able to see the swagger page at [`http://localhost:8080/opencga/`](http://localhost:8080/opencga/). See [Using RESTful web services](https://github.com/opencb/opencga/wiki/Using-RESTful-web-services) for a tutorial.

Tomcat server will look for the configuration files in the installation directory, which can be changed in compilation time changing the property OPENCGA.INSTALLATION.DIR.

If the installation directory is empty at compilation time, the web services will search for the environment variable `OPENCGA_HOME`. If none of this is properly set, the web services will not work.

#### Using the OpenCGA admin command line

The OpenCGA admin command line allows users to run an embedded REST server. These web services will be served with Jetty as follows:

`/opencga-admin.sh server rest --start -p`

Warning: This method is still under development.

