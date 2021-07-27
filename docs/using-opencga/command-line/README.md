# Command Line

## Overview <a id="CommandLine-Overview"></a>

**opencga.sh** is the officially recommended command line tool for users. It implements most of the functionality with many different _commands_ and _subcommands._ These _commands_ are a one-to-one mapping of _Resources_ from __REST web services and _subcommands_ are mapping to end-points. All the operations that can be performed using the command line internally create one or several REST calls, so access to REST machine/cluster is required.

## Installation

OpenCGA command line can be downloaded from the main GitHub repository:

[https://github.com/opencb/opencga/releases/](https://github.com/opencb/opencga/releases/)

For a detailed description of all the steps required to download and configure the CLI, refer to the xxx section of the USER MANUAL.

## Correlation Between REST and CLI

In the following URL, "_samples"_ is the resource and "_search"_ is the endpoint:

[https://ws.opencb.org/opencga-demo/webservices/rest/v1/**samples**/**search**](https://ws.opencb.org/opencga-demo/webservices/rest/v1/samples/search)

the corresponding command in the command line is :

| `./opencga.sh samples` |
| :--- |


and the corresponding subcommand is : 

| `./opencga.sh samples search` |
| :--- |


## CLI Session Management

Generally, unless we are pointing to a public OpenCGA installation, users will first need to log in using the "users login" command line. Once the user has successfully logged in, a session file will be generated in their home folder:

| `~/.opencga/session.json` |
| :--- |


This session file contains the following information:

This makes it easier for users to login only once and execute any number of commands till the session token is expired. Session expiration is set by OpenCGA server independently from the client. Once the token is expired, the user has to login again and can perform desired operations as normal.

