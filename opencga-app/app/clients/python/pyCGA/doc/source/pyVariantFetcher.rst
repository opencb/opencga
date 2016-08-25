pyCGAVariantFetcher
===================

.. |call| replace:: pyCGAVariantFetcher [sid] --host [hostname] --studyID [studyID] <query options>

CommandLine Options
-------------------

.. argparse::
    :module: OptionParsers
    :func: get_options_fetcher
    :prog: pyCGAVariantFetcher

Examples
--------

Get all variants in chromosome 1 with a maf in the database less than 0.05::

    pyCGAVariantFetcher [sid] --host [hostname] --studyID [studyID] --chromosome 1 --maf < 0.05

Get all nonsense variants in a certain gene panel with a phylop conservation score more than 0.2::

    panel=PAX6,B3GALTL,SOX2,MFRP,RAX,BCOR,OTX2,SIX6,BMP4,ALDH1A3,COL4A1,BMPR1A,HCCS,CYP1B1,RAB3GAP1,SHH,VSX2,FOXC1,VAX1,FOXE3,FRAS1,FREM1,XPA,HDAC6,STRA6,FREM2,ERCC3,SMOC1,GRIP1,XPC,RAB18,ERCC2,DDB1,RAB3GAP2,DDB2,ABCB6,ERCC6,ERCC4,ERCC5,PITX2,POLH,GDF3,PITX3,ERCC8,GTF2H5,GDF6,ERCC1,MPLKIP,PRSS56,RARB,TENM3,MAB21L2,TBC1D20
    pyCGAVariantFetcher [sid] --host [hostname] --studyID [studyID] --consequenceType SO:1000062 --gene $panel --conservation phylop>0.2

Get all variants in a certain region with an alternate frequency in AMR population for 1000 Genomes less than 0.1::

    pyCGAVariantFetcher [sid] --host [hostname] --studyID [studyID] --region 1:1100000-1300000 --alternate_frequency






.. note::
    If you have doubts of how to get and use the ids in OpenCGA, pleas read :ref:`how-to-use-ids`

.. note::
    Please notice this program is fetching variants from a DB which can contain millions of variants.
    Although the DB is indexed to obtain the best performance, several general queries are not supported to be
    fast. In this way, for example,  if you try to fetch variants filtering only by type of variants (i,e SNVs),
    the result will be very slow, but if you query for a type of variant in a specific region will be very fast.

    So we recommend, use at least one of these filters in your queries:
        * ids
        * chromosome
        * region
        * gene
        * genotype
        * consequenceType
        * xref
        * alternate_frequency
        * reference_frequency
        * maf
        * mgf



