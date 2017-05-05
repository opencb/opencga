.. contents::

PyCGA
==========

- This Python package makes use of the exhaustive RESTful Web service API that has been implemented for the `OpenCGA`_ database.

- It provides easy access to OpenCGA, an open-source project that aims to provide a Big Data storage engine and analysis framework for genomic scale data analysis of hundreds of terabytes or even petabytes.

- More info about this project in the `OpenCGA Wiki`_

Installation
------------

Cloning
```````
PyCGA can be cloned in your local machine by executing in your terminal::

   $ git clone https://github.com/opencb/opencga.git

Once you have downloaded the project you can install the library::

   $ cd opencga/tree/develop/opencga-client/src/main/python
   $ python setup.py install

Usage
-----

Getting started
```````````````
The first step is to set up the OpenCGA server configuration:

.. code-block:: python

    >>> configuration = {
            "version": "v1",
            "rest": {
                "hosts": ["http://100.15.26.35:8080/opencga"]
            }
        }

The configuration can be stored in a JSON or YML file as well:

.. code-block:: python

    >>> configuration = '/path/to/config/opencga_configuration.json'

The second step is to import the module and initialize the OpenCGAClient. Configuration, user and password must be specified:

.. code-block:: python

    >>> from pyCGA.opencgarestclients import OpenCGAClient
    >>> oc = OpenCGAClient(configuration=configuration, user='user_example', pwd='pass_example')

If user and password are not desired to be written down in a script, session id can be used instead:

.. code-block:: python

    >>> from pyCGA.opencgarestclients import OpenCGAClient
    >>> oc = OpenCGAClient(configuration=configuration, user='user_example', pwd='pass_example')  # Remove after getting session id
    >>> print oc.session_id  # Remove after getting session id
    "I4MG3fXJIZARl1LhwZ"
    >>> oc = OpenCGAClient(configuration=configuration, session_id='I4MG3fXJIZARl1LhwZ')

The next step is to create the specific client for the data we want to query:

.. code-block:: python

   >>> samples = oc.samples()  # Query for samples
   >>> files = oc.files()  # Query for files
   >>> cohorts = oc.cohorts()  # Query for cohorts

Now you can start asking to the OpenCGA RESTful service by providing a query ID:

.. code-block:: python

   >>> sample_search = samples.search(study='study1', name='sample1').get()
   >>> print sample_search
   "[{'acl': [{'member': '@gel', u'permissions': ['VIEW', 'VIEW_ANNOTATIONS']}..."

Responses are retrieved as JSON formatted data. Therefore, fields can be queried by key:

.. code-block:: python

    >>> creation_date = oc.samples.search(study='study1', name='sample1').get()[0]['creationDate']
    "20170204822738"

First levels in the JSON output can be accessed as attributes:

.. code-block:: python

    >>> creation_date = samples.search(study='study1', name='sample1').get().creationDate
    "20170204122738"

    >>> annotation = cohorts.search(study='study1', name='cohort1').get().annotationSets
    >>> print annotation[0]['annotations'][0]['value']['sex']
    "F"

Regex are allowed in some fields. This is specially useful when searching by name:

.. code-block:: python

    >>> cohort_name = cohorts.search(study=study_id, name='~LP3000506-DNA_J01').get().name
    >>> print cohort_name
    "LP3000506-DNA_J01_LP3000924-DNA_Z02_0"

Data can be accessed specifying comma-separated IDs or a list of IDs:

.. code-block:: python

    >>> creation_date = oc.samples.search(study='study1', name='sample1').get()[0]['creationDate']
    "20170204822738"

    >>> creation_date = oc.samples.search(study='study1', name='sample1').get()[1]['creationDate']
    "20170204822738"

    >>> creation_date = samples.search(study='study1', name='sample1,sample2').get().creationDate
    ["20170204122738", "20170204123049"]

Optional filters and extra options can be added as key-value parameters (value can be a comma-separated string or a list):

.. code-block:: python

    >>> # e.g. "exclude" parameter
    >>> attributes = oc.files.search(study='study1', name='~sample', bioformat='VARIANT', status='READY', exclude='attributes').get().attributes
    >>> print attributes
    [{}, {}, {}, {}, {}, {}, {}, {}]

    >>> # e.g. "limit" parameter
    >>> files = oc.files.search(study='study1', name='~sample', bioformat='VARIANT', status='READY', limit=1).get()
    >>> print len(files)
    1

Special mention for "analysis_variant" endpoint, which returns an iterator:

.. code-block:: python

    >>> variant_iterator = oc.analysis_variant.query(pag_size=100, data={'studies': 'study1', 'gene': 'BRCA2'}, limit=1)
    >>> for variant in var_iterator:
    >>>     print v.get().type
    "SNV"

What can I ask for?
```````````````````
The best way to know which data can be retrieved for each client is either checking out the `RESTful web services`_ section of the OpenCGA Wiki or the `OpenCGA web services`_


.. _OpenCGA: https://github.com/opencb/opencga
.. _OpenCGA Wiki: https://github.com/opencb/opencga/wiki
.. _RESTful web services: https://github.com/opencb/opencga/wiki/RESTful-Web-Services
.. _OpenCGA web services: http://bioinfodev.hpc.cam.ac.uk/opencga/webservices/
