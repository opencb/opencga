.. contents::

PyXetabase
====================

This Python client package makes use of the comprehensive RESTful web services API implemented for the `OpenCGA`_ platform.
OpenCGA is an open-source project that implements a high-performance, scalable and secure platform for Genomic data analysis and visualisation.

OpenCGA implements a secure and high performance platform for Big Data analysis and visualisation in current genomics.
OpenCGA uses the most modern and advanced technologies to scale to petabytes of data. OpenCGA is designed and implemented to work with few million genomes. It is built on top of three main components: Catalog, Variant and Alignment Storage and Analysis.

More info about this project in `OpenCGA Docs`_

Installation
------------

PyXetabase can be installed from the Pypi repository. Make sure you have pip available in your machine. You can check this by running::

   $ python3 -m pip --version


If you don't have Python or pip, please refer to https://packaging.python.org/en/latest/tutorials/installing-packages/

To install pyXetabase, run the following command in the shell::

   $ pip install pyxetabase


Usage
-----

Import pyXetabase package
```````````````````````````````````

The first step is to import the ClientConfiguration and OpencgaClient from pyXetabase:

.. code-block:: python

    >>> from pyxetabase.opencga_config import ClientConfiguration
    >>> from pyxetabase.opencga_client import OpencgaClient

Setting up server host configuration
````````````````````````````````````

The second step is to generate a ClientConfiguration instance by passing a configuration dictionary containing the opencga host OR a client-configuration.yml file with that information:

.. code-block:: python

    >>> config = ClientConfiguration('/opt/opencga-enterprise/conf/client-configuration.yml')
    >>> config = ClientConfiguration({
            "rest": {
                    "host": "https://demo.app.zettagenomics.com/opencga"
            }
        })

Log in to OpenCGA host server
`````````````````````````````

With this configuration you can initialize the OpencgaClient, and log in:

.. code-block:: python

    >>> oc = OpencgaClient(config)
    >>> oc.login(user='user', password='pass', organization='organization')

Examples
````````

The first step is to get an instance of the clients we may want to use:

.. code-block:: python

    >>> projects = oc.projects  # Project client
    >>> studies = oc.studies  # Study client
    >>> samples = oc.samples  # Sample client
    >>> individuals = oc.individuals  # Individual client
    >>> cohorts = oc.cohorts  # Cohort client

Now you can start querying with pyXetabase:

.. code-block:: python

    >>> for project in projects.search(owner=user).get_results():
    ...    print(project['id'])
    project1
    project2
    [...]

There are two different ways to access query response data:

.. code-block:: python

    >>> foo_client.method().get_responses()  # Iterates over all the responses
    >>> foo_client.method().get_results()  # Iterates over all the results of the first response

Data can be accessed specifying comma-separated IDs or a list of IDs.

e.g. Retrieving individual karyotypic sex for a list of individuals:

.. code-block:: python

    >>> for result in oc.samples.info(samples='NA12877,NA12878,NA12889', study='platinum').get_results():
    ...     print(result['id'], result['karyotypicSex'])
    NA12877 XY
    NA12878 XX
    NA12889 XY

    >>> for result in oc.samples.info(samples=['NA12877', 'NA12878', 'NA12889'], study='platinum').get_results():
    ...     print(result['id'], result['karyotypicSex'])
    NA12877 XY
    NA12878 XX
    NA12889 XY

Optional filters and extra options can be added as key-value parameters (where the values can be a comma-separated string or a list).

What can I ask for?
```````````````````
The best way to know which data can be retrieved for each client, log into `OpenCGA Demo`_ and check the **OpenCGA REST API** in the **About** section (at the top right corner of the screen).

.. _OpenCGA: https://github.com/opencb/opencga
.. _OpenCGA Docs: http://docs.opencb.org/display/opencga
.. _OpenCGA REST API: https://demo.app.zettagenomics.com/
.. _OpenCGA Demo: https://demo.app.zettagenomics.com/
