PyCGA
=====

If you are using PyCGA you have to know the first you need is authenticate yourself in the system, so you will be recognize
by the application and you will be granted to perform the queries you are allowed to. Once you are finished to work with
the application you should log out. Both actions can be done using a simple command line application: PyCGALogin.


PyCGALogin
----------

.. argparse::
    :module: OptionParsers
    :func: get_options_login
    :prog: pyCGALogin


If you have already your session Id, you can start to query the Catalog. The aim of pyCGA is provide very simple command
lines to perform complex actions in Catalog. In this guide you are going to find how to call these programs from the linux
terminal and several example to use cases. PyCGA application it is compounded of 12 tools distributed in 5 sections:

* :ref:`users-docs`
* :ref:`files-docs`
* :ref:`samples-docs`
* :ref:`individuals-docs`
* :ref:`variablesets-docs`

.. _users-docs:

User Tools
----------
.. argparse::
    :module: OptionParsers
    :func: get_options_pycga
    :prog: pyCGA
    :path: users


.. _files-docs:

File Tools
----------

.. argparse::
    :module: OptionParsers
    :func: get_options_pycga
    :prog: pyCGA
    :path: files

Use case example
~~~~~~~~~~~~~~~~

Get the files
    pyCGAIdConverter [sid] --host [hostname] --studyID [studyID] --inputIDType SampleName --outputIDType IndividualName --Id [sampleName]

.. _samples-docs:

Sample Tools
------------
.. argparse::
    :module: OptionParsers
    :func: get_options_pycga
    :prog: pyCGA
    :path: samples


.. _individuals-docs:

Individuals Tools
-----------------
.. argparse::
    :module: OptionParsers
    :func: get_options_pycga
    :prog: pyCGA
    :path: individuals


.. _variablesets-docs:

Variable Sets Tools
-------------------
.. argparse::
    :module: OptionParsers
    :func: get_options_pycga
    :prog: pyCGA
    :path: variables


