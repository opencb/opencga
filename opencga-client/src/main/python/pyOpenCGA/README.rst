.. contents::

PyOpenCGA
==========

This Python client package makes use of the comprehensive RESTful web services API implemented for the `OpenCGA`_ framework.

OpenCGA is an open-source project that aims to provide a Big Data storage engine and analysis framework for genomic scale data analysis of hundreds of terabytes or even petabytes. 
OpenCGA provides a scalable and high-performance Storage Engine framework to index biological data such as BAM or VCF files using different NoSQL databases, currently only MongoDB has been fully developed. 
A data analytics and genomic Analysis layer interface has been implemented over this big data storage index. 
A metadata Catalog has been alse developed to provide authentification and ACLs and to keep track all of files and sample annotation.

More info about this project in the `OpenCGA Wiki`_

Installation
------------

Cloning
```````
PyOpenCGA can be cloned in your local machine by executing in your terminal::

   $ git clone https://github.com/opencb/opencga.git

Once you have downloaded the project you can install the library. We recommend to install it inside a `virtual environment`_::

   $ cd opencga/tree/develop/opencga-client/src/main/python
   $ python setup.py install

Pip install
```````````
Run the following command in the shell::

   $ pip install pyopencga

Usage
-----

Import pyOpenCGA package
````````````````````````

The first step is to import the ConfigClient and OpenCGAClient from pyOpenCGA:

.. code-block:: python

    >>> from pyopencga.opencga_config import ConfigClient
    >>> from pyopencga.opencga_client import OpenCGAClient

Setting up server host configuration
````````````````````````````````````

The second step is to set up the OpenCGA host server configuration you can get a basic configuration dictionary specifying your OpenCGA server host:

.. code-block:: python

    >>> host = 'http://bioinfodev.hpc.cam.ac.uk/opencga-test' # Use a server host where you have an account
    >>> cc = ConfigClient()
    >>> config_dict = cc.get_basic_config_dict(host)
    >>> print(config_dict)
    {'version': 'v1', 'rest': {'hosts': ['http://bioinfodev.hpc.cam.ac.uk/opencga-test']}} 

Log in to OpenCGA host server
`````````````````````````````

With this configuration you can initialize the OpenCGAClient and log into an OpenCGA user account, specifying a user and password:

.. code-block:: python

    >>> oc = OpenCGAClient(configuration=config_dict,user='user_id',pwd='user_password')

For scripting or using Jupyter Notebooks is preferable to load user credentials from an external JSON file.

Once you are logged in, it is mandatory to use the token of the session to propagate the access of the clients to the host server:

.. code-block:: python

    >>> token = oc.session_id
    >>> print(token)
    eyJhbGciOi...

    >>> oc = OpenCGAClient(configuration=config_dict, session_id=token)

Examples
````````

The next step is to create the specific client for the data we want to query:

.. code-block:: python

    >>> projects = oc.projects # Initialize Projects client
    >>> studies = oc.studies # Initialize Studies client
    >>> cohorts = oc.cohorts  # Initialize Cohorst client

Now you can start asking to the OpenCGA RESTful service with pyOpenCGA:

.. code-block:: python

    >>> for project in projects.search(owner=user).results(): 
    ...    print(project['id'])
    project1
    project2
    [...]

There are four different ways to access to the query response data:

.. code-block:: python

    >>> foo_client.method().first() # Returns the first QueryResult 
    >>> foo_client.method().result(position=0) # Returns the result from all QueryResults in a given position
    >>> foo_client.method().results() # Iterates over all the results of all the QueryResults
    >>> foo_client.method().response # Returns the raw response of the QueryResponse

Data can be accessed specifying comma-separated IDs or a list of IDs:

.. code-block:: python

    >>> samples = 'NA12877,NA12878,NA12879'
    >>> samples_list = ['NA12877','NA12878','NA12879']
    >>> sc = oc.samples

    >>> for result in sc.info(query_id=samples, study='user@project1:study1').results():
    ...     print(result['id'], result['attributes']['OPENCGA_INDIVIDUAL']['disorders'])
    NA12877 [{'id': 'OMIM6500', 'name': "Chron's Disease"}]
    NA12878 []
    NA12879 [{'id': 'OMIM6500', 'name': "Chron's Disease"}]

    >>> for result in sc.info(query_id=samples_list, study='user@project1:study1').results():
    ...     print(result['id'], result['attributes']['OPENCGA_INDIVIDUAL']['disorders'])
    NA12877 [{'id': 'OMIM6500', 'name': "Chron's Disease"}]
    NA12878 []
    NA12879 [{'id': 'OMIM6500', 'name': "Chron's Disease"}]

Optional filters and extra options can be added as key-value parameters (where the values can be a comma-separated string or a list).

What can I ask for?
```````````````````
The best way to know which data can be retrieved for each client check `OpenCGA web services`_ swagger.


.. _OpenCGA: https://github.com/opencb/opencga
.. _OpenCGA Wiki: https://github.com/opencb/opencga/wiki
.. _virtual environment: https://help.dreamhost.com/hc/en-us/articles/115000695551-Installing-and-using-virtualenv-with-Python-3 
.. _OpenCGA web services: http://bioinfodev.hpc.cam.ac.uk/opencga-test/webservices/
