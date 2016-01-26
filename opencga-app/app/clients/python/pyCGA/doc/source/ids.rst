.. _how-to-use-ids:

Id Considerations in OpenCGA
============================

Ids in OpenCGA
--------------

Before you make queries to OpenCGA using this API, you should understand how ID of samples and and individuals are
stored in the database.

Each individual has associated:
    1. Unique id in the DB. This id was created by the application. It is an internal id and not used except if you know very well the internal structure of OpenCGA.
    2. Individual Name. This id was created by the administrator of the DB, usually it comes from a PED file.

Each sample has associated:
    1. Unique id in the DB. This id was created by the application. It is an internal id and not used except if you know very well the internal structure of OpenCGA.
    2. Sample name. This name was created using the information in the VCF/BAM/PED file, this one will be the identifier that users should use to refer one sample.
    3. Individual id. This is the internal id for one individual, it is used to links one sample with one individual.

As you can see there are several Ids which you may not necessarily know, this API provide you a tool to perform the conversions in a simple and easy way:


pyCGAIdConverter
----------------

.. |call| replace:: pyCGAIdConverter [sid] --host [hostname] --studyID [studyID]

The output is a 2 column table (tab separated) with the provided id in the first column and the requested id in the second one.

CommandLine Options
-------------------

.. argparse::
    :module: OptionParsers
    :func: get_options_id_converter
    :prog: pyCGAIdConverter

Common use examples
-------------------

Get the Individual Name using the Sample Name::

    pyCGAIdConverter [sid] --host [hostname] --studyID [studyID] --inputIDType SampleName --outputIDType IndividualName --Id [sampleName]

Get the Sample Name using the Individual Name::

    pyCGAIdConverter [sid] --host [hostname] --studyID [studyID] --inputIDType IndividualName --outputIDType SampleName --Id [IndividualName]


.. note::
    Please note that although a sample can only have one individual associated, one individual can have more than one
    sample, (i,e the last example can retrieve more than line per each given Id).