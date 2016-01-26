Welcome to pyCGA
================

`OpenCGA`_  is an open-source project that aims to provide a Big Data storage engine and analysis
framework for genomic scale data analysis of hundreds of terabytes or even petabytes. For users,
its main features will include uploading and downloading files to a repository, storing their
information in a generic way (non-dependant of the original file-format) and retrieving this information
efficiently.

The aim of this python package is provide an interface using the OpenCGA web services from python, and make
easy and comprehensive the use of OpenCGA. And also provide a CLI usable from

The code is open source, and `available on github`_.

.. _OpenCGA: https://github.com/opencb/opencga/wiki
.. _available on github: https://github.com/opencb/


The main documentation for the project is organized into a couple sections:

* :ref:`getting-started`
* :ref:`api-docs`
* :ref:`cli-docs`
* :ref:`dev-docs`

.. _getting-started:

Getting Started
---------------
.. toctree::
    :maxdepth: 2

    installation
    quick_start
    ids

.. _api-docs:

API Documentation
-----------------
.. toctree::
    :maxdepth: 2

    web_services
    queries_from_python


.. _cli-docs:

CLI Documentation
-----------------
.. toctree::
    :maxdepth: 2

    pyCGA
    pyVariantFetcher

.. _dev-docs:

Developer Documentation
-----------------------
.. toctree::
    :maxdepth: 2

    new_web_services




