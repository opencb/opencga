.. contents::

PyOpenCGA
==========

This Python client package makes use of the comprehensive RESTful web services API implemented for the `OpenCGA`_ platform.
OpenCGA is an open-source project that implements a high-performance, scalable and secure platform for Genomic data analysis and visualisation

OpenCGA implements a secure and high performance platform for Big Data analysis and visualisation in current genomics.
OpenCGA uses the most modern and advanced technologies to scale to petabytes of data. OpenCGA is designed and implemented to work with
few million genomes. It is built on top of three main components: Catalog, Variant and Alignment Storage and Analysis.

More info about this project in the `OpenCGA Docs`_

Installation
------------

Cloning
```````
PyOpenCGA can be cloned in your local machine by executing in your terminal::

   $ git clone https://github.com/opencb/opencga.git

Once you have downloaded the project you can install the library. We recommend to install it inside a `virtual environment`_::

   $ cd opencga/tree/develop/opencga-client/src/main/python/pyOpenCGA
   $ python setup.py install

Pip install
```````````
Run the following command in the shell::

   $ pip install pyopencga

Usage
-----

Import pyOpenCGA package
````````````````````````

The first step is to import the ClientConfiguration and OpenCGAClient from pyOpenCGA:

.. code-block:: python

    >>> from pyopencga.opencga_config import ClientConfiguration
    >>> from pyopencga.opencga_client import OpenCGAClient

Setting up server host configuration
````````````````````````````````````

The second step is to generate a ClientConfiguration instance by passing a configuration dictionary containing the host to point to or a client-configuration.yml file:

.. code-block:: python

    >>> config = ClientConfiguration('/opt/opencga/conf/client-configuration.yml')
    >>> config = ClientConfiguration({
            "rest": {
                    "host": "http://bioinfo.hpc.cam.ac.uk/opencga-demo"
            }
        })

Log in to OpenCGA host server
`````````````````````````````

With this configuration you can initialize the OpenCGAClient, and log in:

.. code-block:: python

    >>> oc = OpenCGAClient(config)
    >>> oc.login('user')

For scripting or using Jupyter Notebooks is preferable to load user credentials from an external JSON file.

Once you are logged in, it is mandatory to use the token of the session to propagate the access of the clients to the host server:

.. code-block:: python

    >>> token = oc.token
    >>> print(token)
    eyJhbGciOi...

    >>> oc = OpenCGAClient(configuration=config_dict, token=token)

Examples
````````

The next step is to get an instance of the clients we may want to use:

.. code-block:: python

    >>> projects = oc.projects # Project client
    >>> studies = oc.studies   # Study client
    >>> samples = oc.samples # Sample client
    >>> cohorts = oc.cohorts # Cohort client

Now you can start asking to the OpenCGA RESTful service with pyOpenCGA:

.. code-block:: python

    >>> for project in projects.search(owner=user).get_results():
    ...    print(project['id'])
    project1
    project2
    [...]

There are two different ways to access to the query response data:

.. code-block:: python

    >>> foo_client.method().get_results() # Iterates over all the results of all the QueryResults
    >>> foo_client.method().get_responses() # Iterates over all the responses

Data can be accessed specifying comma-separated IDs or a list of IDs:

.. code-block:: python

    >>> samples = 'NA12877,NA12878,NA12879'
    >>> samples_list = ['NA12877','NA12878','NA12879']
    >>> sc = oc.samples

    >>> for result in sc.info(query_id=samples, study='user@project1:study1').get_results():
    ...     print(result['id'], result['attributes']['OPENCGA_INDIVIDUAL']['disorders'])
    NA12877 [{'id': 'OMIM6500', 'name': "Chron's Disease"}]
    NA12878 []
    NA12879 [{'id': 'OMIM6500', 'name': "Chron's Disease"}]

    >>> for result in sc.info(query_id=samples_list, study='user@project1:study1').get_results():
    ...     print(result['id'], result['attributes']['OPENCGA_INDIVIDUAL']['disorders'])
    NA12877 [{'id': 'OMIM6500', 'name': "Chron's Disease"}]
    NA12878 []
    NA12879 [{'id': 'OMIM6500', 'name': "Chron's Disease"}]

Optional filters and extra options can be added as key-value parameters (where the values can be a comma-separated string or a list).

What can I ask for?
```````````````````
The best way to know which data can be retrieved for each client check `OpenCGA web services`_ swagger.


.. _OpenCGA: https://github.com/opencb/opencga
.. _OpenCGA Docs: http://docs.opencb.org/display/opencga
.. _virtual environment: https://help.dreamhost.com/hc/en-us/articles/115000695551-Installing-and-using-virtualenv-with-Python-3 
.. _OpenCGA web services: http://bioinfodev.hpc.cam.ac.uk/opencga/webservices/
