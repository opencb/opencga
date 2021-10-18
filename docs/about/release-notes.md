# Release Notes

You can find more detailed information at [GitHub Issues](https://github.com/opencb/opencga/issues).

## 2.0.0-RC1 (June 2020)

#### Catalog

* \[**FEATURE**] Improve audit ([#1322](https://github.com/opencb/opencga/issues/1322), [#1483](https://github.com/opencb/opencga/issues/1483))
* \[**FEATURE**] Allow queries based on permissions ([#1486](https://github.com/opencb/opencga/issues/1486))
* \[**FEATURE**] Enable possibility of creating asynchronous tasks support ([#1408](https://github.com/opencb/opencga/issues/1408), [#1509](https://github.com/opencb/opencga/issues/1509))
* \[**FEATURE**] Support ACID transactions in Catalog !! ([#1338](https://github.com/opencb/opencga/issues/1338))
* \[**FEATURE**] Implement an automatic client generator valid for R, Java, Python and JS OpenCGA libraries ([#1464](https://github.com/opencb/opencga/issues/1464))
* \[FEATURE] Add new permission to execute jobs ([#1445](https://github.com/opencb/opencga/issues/1445))
* \[FEATURE] Improve job data model ([#1447](https://github.com/opencb/opencga/issues/1447))
* \[FEATURE] Enable job dependencies ([#1475](https://github.com/opencb/opencga/issues/1475))
* \[FEATURE] Support multistudy jobs ([#1549](https://github.com/opencb/opencga/issues/1549))
* \[FEATURE] Add webhooks ([#1448](https://github.com/opencb/opencga/issues/1448))
* \[FEATURE] Create jobs top webservice and command line ([#1460](https://github.com/opencb/opencga/issues/1460))
* \[FEATURE] Create new **opencga **administrator user with a default project and study ([#1425](https://github.com/opencb/opencga/issues/1425), [#1491](https://github.com/opencb/opencga/issues/1491))
* \[FEATURE] Improve count functionality ([#1448](https://github.com/opencb/opencga/issues/1448))
* \[FEATURE] Add a new webservice to download files from external sources ([#1453](https://github.com/opencb/opencga/issues/1453))
* \[FEATURE] Add dynamic parameter types in VariableSets ([#1478](https://github.com/opencb/opencga/issues/1478))
* \[FEATURE] Add new _allowedKey_ field to VariableSet ([#1554](https://github.com/opencb/opencga/issues/1554))
* \[FEATURE] Add new /head and /tail web services in file ([#1497](https://github.com/opencb/opencga/issues/1497))
* \[FEATURE] Add new jobs/log/head and job/logs/tail web services to see job logs ([#1495](https://github.com/opencb/opencga/issues/1495))
* \[FEATURE] Load annotations from TSV files ([#1488](https://github.com/opencb/opencga/issues/1488))
* \[FEATURE] Allow passing a map of sample ids when linking VCF files ([#1527](https://github.com/opencb/opencga/issues/1527))
* \[FEATURE] Give users the option to set their own statuses  ([#1545](https://github.com/opencb/opencga/issues/1545))
* \[FEATURE] Add new permission to view variants ([#1559](https://github.com/opencb/opencga/issues/1559))
* \[FEATURE] Assign implicit permissions automatically ([#1561](https://github.com/opencb/opencga/issues/1561))
* \[FEATURE] Remove base64 codification from UUIDs ([#1569](https://github.com/opencb/opencga/issues/1569))
* \[FEATURE] Improve Solr queries by annotation ([#1484](https://github.com/opencb/opencga/issues/1484))
* \[FEATURE] Add new webservice to fetch individual relatives ([#1552](https://github.com/opencb/opencga/issues/1552))
* \[FEATURE] Add new webservice to fetch base64 content of images ([#1584](https://github.com/opencb/opencga/issues/1584))
* \[FEATURE] Keep individual references in samples ([#1346](https://github.com/opencb/opencga/issues/1346))
* \[CHANGE-FEATURE] Improve panel operations ([#1577](https://github.com/opencb/opencga/issues/1577))
* \[CHANGE] Configuration file changes ([#1415](https://github.com/opencb/opencga/issues/1415))
* \[CHANGE] DataResponse data model changes ([#1424](https://github.com/opencb/opencga/issues/1424))
* \[CHANGE] Rename variable type in VariableSet ([#1479](https://github.com/opencb/opencga/issues/1479))
* \[CHANGE] Remove group name ([#1513](https://github.com/opencb/opencga/issues/1513))
* \[CHANGE] Rename a few ACLs ([#1601](https://github.com/opencb/opencga/issues/1601))
* \[CHANGE] Data model changes ([#1538](https://github.com/opencb/opencga/issues/1538))
* \[CHANGE] Main login endpoint changes ([#1568](https://github.com/opencb/opencga/issues/1568))
* \[CHANGE] Change webservice to change user password ([#1586](https://github.com/opencb/opencga/issues/1586))
* \[CHANGE] Ensure all REST webservices return on OpenCGAResult ([#1569](https://github.com/opencb/opencga/issues/1569))
* \[BUGFIX] Sync users CLI didn't work for AD ([#1297](https://github.com/opencb/opencga/issues/1297))
* \[BUGFIX] Unable to update "relatedFiles" list ([#1451](https://github.com/opencb/opencga/issues/1451))
* \[PERFORMANCE] Move deleted documents to different collection ([#1369](https://github.com/opencb/opencga/issues/1369))

#### Analysis

* \[CLINICAL] Tiering interpretation analysis for cancer ([#1300](https://github.com/opencb/opencga/issues/1300))
* \[VARIANT] Implement Fisher Test Analysis MapReduce ([#1361](https://github.com/opencb/opencga/issues/1361))
* \[VARIANT] Implement VariantStats OpenCGA Analysis ([#1376](https://github.com/opencb/opencga/issues/1376))
* \[VARIANT] Implement Gwas OpenCGA Analysis ([#1386](https://github.com/opencb/opencga/issues/1386))
* \[VARIANT] Complex sample query by variant to enable cohort creation for clinical trials ([#1474](https://github.com/opencb/opencga/issues/1474))
* \[CLINICAL] Implement mutational signature analysis ([#1490](https://github.com/opencb/opencga/issues/1490))
* \[CORE] Create a Dockerfile with the R packages used by OpenCGA analysis ([#1493](https://github.com/opencb/opencga/issues/1493))
* \[CLINICAL] Implement the inferred sex analysis ([#1544](https://github.com/opencb/opencga/issues/1544))
* \[ALIGNMENT] Implement statistics analysis for alignment coverage ([#1588](https://github.com/opencb/opencga/issues/1588))
* \[CLINICAL] Implement the relatedness analysis based on IBD/IBS ([#1521](https://github.com/opencb/opencga/issues/1521))
* \[CLINICAL] Implement genetic checks to compare with the reported results ([#1522](https://github.com/opencb/opencga/issues/1522))

#### Variant Storage

* \[**FEATURE**] Support Hadoop3.x and HBase2.x ([#925](https://github.com/opencb/opencga/issues/925))
* \[**FEATURE**] Divide opencga-storage-hadoop-deps in submodules ([#1333](https://github.com/opencb/opencga/issues/1333))
* \[FEATURE] Store custom variant scores ([#708](https://github.com/opencb/opencga/issues/708))
* \[FEATURE] Allow load VCFs split by region in Hadoop ([#1471](https://github.com/opencb/opencga/issues/1471))
* \[FEATURE] Copy MapReduce jobs result submitted through an ssh connection ([#1432](https://github.com/opencb/opencga/issues/1432))
* \[FEATURE] Add FilterCount and MeanQuality to VariantStats ([#1502](https://github.com/opencb/opencga/issues/1502))
* \[FEATURE] Allow to configure the variant storage from REST ([#1518](https://github.com/opencb/opencga/issues/1518))
* \[FEATURE] Allow skip sample index when loading variant files ([#1530](https://github.com/opencb/opencga/issues/1530))
* \[FEATURE] Allow indexing multiple files per sample in StorageHadoop ([#1542](https://github.com/opencb/opencga/issues/1542))
* \[FEATURE] Return MendelianError code as an IssueEntry ([#1547](https://github.com/opencb/opencga/issues/1547))
* \[FEATURE] Extend sample filter functionality  ([#1567](https://github.com/opencb/opencga/issues/1567))
* \[CHANGE] Add specific permissions to view variants ([#1559](https://github.com/opencb/opencga/issues/1559))
* \[CHANGE] Rename VariantQueryParams 'format', 'includeFormat' and 'info' ([#1556](https://github.com/opencb/opencga/issues/1556))
* \[CHANGE] Remove SAMPLE_ID and FILE_IDX from Format. Add INCLUDE_SAMPLE_ID ([#1555](https://github.com/opencb/opencga/issues/1555))
* \[CHANGE] Transform endpoint analysis/variants/sample/query into an Analysis ([#1435](https://github.com/opencb/opencga/issues/1435))
* \[CHANGE] Improve variant storage functionality for returning samples ([#1353](https://github.com/opencb/opencga/issues/1353))
* \[CHANGE] Change variant.id and variant.names content ([#1514](https://github.com/opencb/opencga/issues/1514))
* \[CHANGE] Make gene/id/xref query params more strict ([#1515](https://github.com/opencb/opencga/issues/1515))
* \[PERFORMANCE] Add extended clinical index to SampleIndex ([#1454](https://github.com/opencb/opencga/issues/1454))
* \[PERFORMANCE] Add Biotype+Ct combination to SampleInde  ([#1364](https://github.com/opencb/opencga/issues/1364))
* \[PERFORMANCE] Count numTotalResults from covered SampleIndex queries asyncrhonously. ([#1352](https://github.com/opencb/opencga/issues/1352))
* \[PERFORMANCE] Improve SampleIndex File ([#1343](https://github.com/opencb/opencga/issues/1343))
* \[BUGFIX] Remove Jetty9.4 from Hadoop MapReduce classpath ([#1504](https://github.com/opencb/opencga/issues/1504))

## 1.4.2 (June 2019)

#### Catalog

* \[CHANGE] Rename web services from _/stats _to _/aggregationStats_  ([#1253](https://github.com/opencb/opencga/issues/1253))
* \[BUGFIX] Index fails when passing ":" instead of "/" ([#1241](https://github.com/opencb/opencga/issues/1241))
* \[BUGFIX] Alignment index daemon fails ([#1232](https://github.com/opencb/opencga/issues/1232))
* \[BUGFIX] Migration script issue ([#1226](https://github.com/opencb/opencga/issues/1226))
* \[BUGFIX] _Upload_ web service fails when uploading to root folder ([#1276](https://github.com/opencb/opencga/issues/1276))
* \[BUGFIX] Missing _id_ query parameter in _studies/{studies}/groups _web service ([#1275](https://github.com/opencb/opencga/issues/1275))
* \[BUGFIX] Versioning issues ([#1270](https://github.com/opencb/opencga/issues/1270))
* \[BUGFIX] Web service _studies/{studies}/groups/create _not working ([#1250](https://github.com/opencb/opencga/issues/1250))
* \[BUGFIX] Order not respected when querying lists of ids ([#1246](https://github.com/opencb/opencga/issues/1246))
* \[FEATURE] Support passing relatedFiles object to File during _link_ ([#1295](https://github.com/opencb/opencga/issues/1295))
* \[FEATURE] Support _htsget_ protocol for data streaming ([#1277](https://github.com/opencb/opencga/issues/1277))
* \[FEATURE] Add new user category "_application"_ ([#1268](https://github.com/opencb/opencga/issues/1268))
* \[PERFORMANCE] Analysis queries take too much time ([#1245](https://github.com/opencb/opencga/issues/1245))\
  \


| <p>1.4.2. contains a small series of internal changes requiring running a small migration. To do so, please head to the main OpenCGA source folder and run the following:</p><ul><li><strong>mongo_port</strong>: Typically 27017</li><li><strong>database_name</strong>: Typically opencga_catalog</li></ul> |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |

#### Variant Storage

* \[CHANGE] Rename filter "`transcriptionFlag`" to "`transcriptFlag`" ([#1256](https://github.com/opencb/opencga/issues/1256))
* \[BUGFIX] HashMap$Node cannot be cast to java.util.HashMap$TreeNode ([#1323](https://github.com/opencb/opencga/issues/1323))
* \[BUGFIX] Wrong usage of SampleIndex on invalid queries ([#1274](https://github.com/opencb/opencga/issues/1274))
* \[BUGFIX] Fix RowKey generation for non symbolic structural variants #1259
* \[FEATURE] Add chromDensity query filtering by sample genotype ([#1249](https://github.com/opencb/opencga/issues/1249))
* \[FEATURE] Accept nested fields in chromDensity by sample genotype ([#1263](https://github.com/opencb/opencga/issues/1263))
* \[FEATURE] Use SamplingSize field at SampleIndex query executor ([#1339](https://github.com/opencb/opencga/issues/1339))
* \[FEATURE] Add new variant filters: "`cohortStatsAlt`", "`cohortStatsRef`" ([#1239](https://github.com/opencb/opencga/issues/1239))
* \[FEATURE] Query by compound heterozygous ([#1247](https://github.com/opencb/opencga/issues/1247))
* \[FEATURE] Export variants given a file of variant Ids ([#1254](https://github.com/opencb/opencga/issues/1254))
* \[FEATURE] Accept keyword "LoF" in consequence type filter ([#1262](https://github.com/opencb/opencga/issues/1262))
* \[FEATURE] Implement a MR to extract number of variants per file #1287
* \[PERFORMANCE] Remove unneeded FileMetadata reads from HBaseToStudyEntryConverter ([#1350](https://github.com/opencb/opencga/issues/1350))
* \[PERFORMANCE] Improve SampleIndex read performance ([#1319](https://github.com/opencb/opencga/issues/1319))
* \[PERFORMANCE] Add annotation counters to SampleIndex ([#1258](https://github.com/opencb/opencga/issues/1258))
* \[PERFORMANCE] Reduce size of SampleIndex table ([#1252](https://github.com/opencb/opencga/issues/1252))
* \[PERFORMANCE] Include parents genotype in the SampleIndex ([#1244](https://github.com/opencb/opencga/issues/1244))
* \[PERFORMANCE] Skip join SampleIndex with Variants table when possible #1242
* \[IMPROVEMENT] Improve approximate count of CompoundHeterozygous in StorageHadoop #1299
* \[IMPROVEMENT] Improve DeNovo variants definition ([#1340](https://github.com/opencb/opencga/issues/1340))
* \[IMPROVEMENT] Improve Genotype filter to include by default phased genotypes ([#1273](https://github.com/opencb/opencga/issues/1273))
* \[IMPROVEMENT] Make /sampleData support somatic studies ([#1329](https://github.com/opencb/opencga/issues/1329))
* \[IMPROVEMENT] Add VarinatStats to SampleData result ([#1286](https://github.com/opencb/opencga/issues/1286))
* \[IMPROVEMENT] Native implementation of sampleData endpoint in storage-hadoop ([#1285](https://github.com/opencb/opencga/issues/1285))
* \[IMPROVEMENT] Extend panel filter to include regions and single variants #1272
* \[IMPROVEMENT] Extract variant query executor from VariantStorageEngine #1240
* \[COMMAND LINE] Add missing SampleIndex command line operations ([#1282](https://github.com/opencb/opencga/issues/1282))

## 1.4.0-rc1 (August 2018)

#### **Catalog**

* \[IMPROVEMENT] Recognise bigwig files automatically. ([#283](https://github.com/opencb/opencga/issues#283))
* \[IMPROVEMENT] **Major improvements in annotationSets** ([#635](https://github.com/opencb/opencga/issues#635), [#772](https://github.com/opencb/opencga/issues#772), [#849](https://github.com/opencb/opencga/issues#849)):\

  * Queries can be performed by any of the fields. ([#772](https://github.com/opencb/opencga/issues#772))
  * All the fields are indexed in the database, supporting really fast queries. ([#772](https://github.com/opencb/opencga/issues#772))
  * Projections can be added so only specific fields of an annotationSet are included. ([#635](https://github.com/opencb/opencga/issues#635), [#772](https://github.com/opencb/opencga/issues#772))
  * Annotation set web services have been changed. ([#849](https://github.com/opencb/opencga/issues#849))
  * See [AnnotationSets 1.4.0](http://docs.opencb.org/display/opencga/AnnotationSets+1.4.0) section from the OpenCGA documentation for the whole description of features.
* \[FEATURE] Implement new "Permission rules" feature ([#745](https://github.com/opencb/opencga/issues#745))
* \[FEATURE] Add new /admin web services to be able to perform administrative operations through REST ([#759](https://github.com/opencb/opencga/issues#759))
* \[IMPROVEMENT] Improve /user web services. Remove duplicated _configs_ web services ([#661](https://github.com/opencb/opencga/issues#661))
* \[FEATURE] Implement _delete_ operations for most of the entries ([#792](https://github.com/opencb/opencga/issues#792))
* \[IMPROVEMENT] Store _creationDate_ field as an actual date object to improve queries ([#752](https://github.com/opencb/opencga/issues#752))
* \[CHANGE] Important _id_ changes ([#819](https://github.com/opencb/opencga/issues#819)):\

  * Data model _ids_ have been redefined to contain any user-defined _id_ for every entry. Migrations produced in every entity are described in the ticket.
  * Old numeric _ids_ are now lost and only used for internal purposes. Users cannot query by those fields anymore.
  * Update REST web services to support _id_ changes ([#834](https://github.com/opencb/opencga/issues#834))
* \[IMPROVEMENT] Stop creating empty directories when linking _Files_ ([#865](https://github.com/opencb/opencga/issues#865))
* \[FEATURE] Add new _{entry}Action_ field to some update web services to support adding, setting or removing single entries to arrays of them. (Example: _samplesAction_ to add, set or remove samples from an individual, file or cohort) ([#850](https://github.com/opencb/opencga/issues#850))
* \[IMPROVEMENT] Deprecate _VariableSet_ web services and move them to _Study_ web services. Improve and clean old _Study_ web services ([#846](https://github.com/opencb/opencga/issues#846))
* \[IMPROVEMENT] General data model improvements described in [#823](https://github.com/opencb/opencga/issues#823):\

  * Add new inmutable _**uuid**_ field to all entries.
* \[FEATURE] Create catalog solr sync mechanism and enable facet queries [#875](https://github.com/opencb/opencga/issues#875)

#### Variant Storage

* \[FEATURE] Aggregate operation for all samples from a study in the Variant ([#757](https://github.com/opencb/opencga/issues/757))
  * \[IMPROVEMENT] Move HBase write step to a separated MR for fill-missing operation ([#815](https://github.com/opencb/opencga/issues/815))
  * \[IMPROVEMENT] Mark already processed variants in fill missing operation ([#803](https://github.com/opencb/opencga/issues/803))
  * \[IMPROVEMENT] Do not iterate over all VcfRecords when filling gaps or missing variants ([#794](https://github.com/opencb/opencga/issues/794))
  * \[IMPROVEMENT] Fill missing prepare step: copy variants to fill into archive table ([#793](https://github.com/opencb/opencga/issues/793))
  * \[IMPROVEMENT] Split VcfSlice from archive table in REF and NON_REF columns ([#778](https://github.com/opencb/opencga/issues/778))
  * \[IMPROVEMENT] Split archive table in file batches ([#777](https://github.com/opencb/opencga/issues/777))
  * \[CHANGE] Remove merge=advanced from storage-hadoop ([#796](https://github.com/opencb/opencga/issues/796))
* \[FEATURE] Index Sample Genotypes in HBase ([#838](https://github.com/opencb/opencga/issues/838))
  * \[FEATURE] Genotype index intersect in HBase ([#862](https://github.com/opencb/opencga/issues/862))
  * \[FEATURE] Integrate Variants export using MapReduce ([#867](https://github.com/opencb/opencga/issues/867))
  * \[FEATURE] Support enriched genotypes when querying to SampleIndex ([#870](https://github.com/opencb/opencga/issues/870))
  * \[IMPROVEMENT] Use variants SampleIndex when reading from MapReduce ([#868](https://github.com/opencb/opencga/issues/868))
* \[FEATURE] New Variant query filter "clinicalSignificance" ([#872](https://github.com/opencb/opencga/issues/872))
* \[FEATURE] Add new filters for INFO and FORMAT fields other than GT ([#863](https://github.com/opencb/opencga/issues/863))
* \[FEATURE] Add filter by QUAL in Storage Variants ([#809](https://github.com/opencb/opencga/issues/809))
* \[FEATURE] Accept enriched genotypes at GENOTYPE filter ([#750](https://github.com/opencb/opencga/issues/750))
* \[FEATURE] Variant filtering in a samples group ([#578](https://github.com/opencb/opencga/issues/578))
* \[CHANGE] Automatic INCLUDE_STUDY if possible ([#878](https://github.com/opencb/opencga/issues/878))
* \[CHANGE] Automatic INCLUDE_SAMPLE if GENOTYPE VariantQueryParam is present ([#814](https://github.com/opencb/opencga/issues/814))
* \[IMPROVEMENT] Match Consequence type with the specified Gene filter (if any) for Storage Hadoop ([#874](https://github.com/opencb/opencga/issues/874))
* \[CHANGE] Untie internal Catalog and Storage IDs ([#859](https://github.com/opencb/opencga/issues/859))
  * \[FIX] Synchronize Catalog ID to UID changes in Storage Managers catalog ([#861](https://github.com/opencb/opencga/issues/861))
* \[FEATURE] Create a ProjectMetadata object for Variant Storage ([#832](https://github.com/opencb/opencga/issues/832))
* \[IMPROVEMENT] Move storage-hadoop metadata to a separated table ([#781](https://github.com/opencb/opencga/issues/781))
* \[FEATURE] Support and load BREAKENDS in Variant Storage ([#760](https://github.com/opencb/opencga/issues/760))
  * \[FEATURE] Support of Symbolic and Structural variants in Hadoop Variant Storage ([#857](https://github.com/opencb/opencga/issues/857))
* \[IMPROVEMENT] Add direct loader for the first file in a chromosome to load directly to the variants collection ([#354](https://github.com/opencb/opencga/issues/354))
* \[FEATURE] Move removed variants into a trash bin ([#831](https://github.com/opencb/opencga/issues/831))
* \[CHANGE] By default, load all FORMAT fields in Variant Storage MongoDB ([#824](https://github.com/opencb/opencga/issues/824))
* \[CHANGE] Change storage-hadoop table naming policy ([#782](https://github.com/opencb/opencga/issues/782))
* \[IMPROVEMENT] Review and improve Variant Annotation in OpenCGA Storage ([#805](https://github.com/opencb/opencga/issues/805))
* \[IMPROVEMENT] Create one Stage Collection per study in Variants Storage MongoDB ([#801](https://github.com/opencb/opencga/issues/801))
* \[IMPROVEMENT] Filter out overlapping files in variants in storage-hadoop ([#779](https://github.com/opencb/opencga/issues/779))
* \[IMPROVEMENT] Improve remove file in variant-storage-hadoop ([#776](https://github.com/opencb/opencga/issues/776))
* \[IMPROVEMENT] Load annotation using BufferedMutator ([#775](https://github.com/opencb/opencga/issues/775))
* \[FIX] Do not store phoenix primary key columns in HBase as separated columns ([#802](https://github.com/opencb/opencga/issues/802))
* \[FIX] Adapt remove file to new archive schema ([#800](https://github.com/opencb/opencga/issues/800))
* \[FIX] Do not map namespace for VIEW tables under phoenix 4.12.0 ([#799](https://github.com/opencb/opencga/issues/799))
* \[FIX] Avoid PhoenixIOException timeout when dropping columns from phoenix ([#795](https://github.com/opencb/opencga/issues/795))

## 1.3.11 (August 2019)

#### **Catalog**

* \[FEATURE] Support "application users" ([#1268](https://github.com/opencb/opencga/issues/1268)) (**migration required**) → [https://github.com/opencb/opencga/blob/develop/opencga-app/app/migration/v1.4.2/catalog/migration.js#L24](https://github.com/opencb/opencga/blob/develop/opencga-app/app/migration/v1.4.2/catalog/migration.js#L24)
* \[FEATURE] Add CRAM support ([#1301](https://github.com/opencb/opencga/issues/1301))
* \[IMPROVEMENT] Change long fileId to File file in RelatedFile data model ([#1294](https://github.com/opencb/opencga/issues/1294)) (**migration required**) → [https://github.com/opencb/opencga/blob/v1.3.11/opencga-app/app/migration/v1.3.0/catalog/11\_related_files.js](https://github.com/opencb/opencga/blob/v1.3.11/opencga-app/app/migration/v1.3.0/catalog/11\_related_files.js)
* \[IMPROVEMENT] RelatedFiles can be passed during file link ([#1295](https://github.com/opencb/opencga/issues/1295))
* \[FIX] Fix --sync-all option from opencga-admin.sh users sync command line ([#1297](https://github.com/opencb/opencga/issues/1297))

## 1.3.10 (February 2019)

#### **Catalog**

* \[FIX] Remove base64 conversion of the secret key.
* \[FIX] Update pom dependencies to avoid conflicts.

## 1.3.9 (January 2019)

#### **Catalog**

* \[FEATURE] Support Azure AD authentication.
* \[CHANGE] Add _id_ to Group data model (**migration required**) → [https://github.com/opencb/opencga/blob/v.1.3.9/opencga-app/app/migration/v1.3.0/catalog/10\_add_group-id.js](https://github.com/opencb/opencga/blob/v.1.3.9/opencga-app/app/migration/v1.3.0/catalog/10\_add_group-id.js)

## 1.3.8 (August 2018)

#### **Catalog**

* \[FIX] Fix permission issue affecting users and groups with the "\_" symbol ([#881](https://github.com/opencb/opencga/issues#881))

## 1.3.7 (July 2018)

#### **Catalog**

* \[ENHANCEMENT] Add new _tags_ field to the File data model ([#855](https://github.com/opencb/opencga/issues#855))
* \[CHANGE] Configuration change. Add hooks to configuration file ([#856](https://github.com/opencb/opencga/issues#856))

## 1.3.6 (May 2018)

#### **Catalog**

* \[FIX] Improve performance of sample queries filtering by individual ([#843](https://github.com/opencb/opencga/issues#843))

## 1.3.5 (May 2018)

#### **Catalog**

* \[FIX] Fix issue when assigning permissions given the id(s) of different entities  ([#836](https://github.com/opencb/opencga/issues#836))

## 1.3.4 (April 2018)

#### **Catalog**

* \[ENHANCEMENT] Performance improvement when assigning permissions  ([#829](https://github.com/opencb/opencga/issues#829))

## 1.3.3 (March 2018)

#### **Catalog**

* \[ENHANCEMENT] Remove old deprecated fields from Family data model ([#810](https://github.com/opencb/opencga/issues#810))
* \[ENHANCEMENT] Allow looking for Individuals and Families by a new sample field ([#811](https://github.com/opencb/opencga/issues#811))
* \[FIX] Fix command line to support non-expiring tokens
* \[FIX] Support SKIP_COUNT parameter
* \[CHANGE] Remove family completeness check

## 1.3.2 (February 2018)

#### **Catalog**

* \[FIX] Add missing individual-sample indexes ([#790](https://github.com/opencb/opencga/issues#790))
* \[FIX] Fix smart name resolution ([#791](https://github.com/opencb/opencga/issues#791))

## 1.3.1 (February 2018)

#### **Catalog**

* \[FIX] Propagation of permissions sample-individual not working ([#780](https://github.com/opencb/opencga/issues#780))
* \[ENHANCEMENT] Ask for admin password automatically when using admin command line ([#785](https://github.com/opencb/opencga/issues#785))
* \[FIX] Filtering by file size not working ([#786](https://github.com/opencb/opencga/issues#786))

## 1.3.0 (January 2018)

#### General

* \[FEATURE] Admin migration command line ([#690](https://github.com/opencb/opencga/issues#690))
* \[FEATURE] Implement AutoComplete for CLI ([#714](https://github.com/opencb/opencga/issues#714))

#### **Catalog**

* \[REMOVE] Remove ACL from data models. ([#666](https://github.com/opencb/opencga/issues/666))
* \[ENHANCEMENT] Clean old code and refactoring. ([#667](https://github.com/opencb/opencga/issues/667), [#668](https://github.com/opencb/opencga/issues/668), [#669](https://github.com/opencb/opencga/issues/669), [#670](https://github.com/opencb/opencga/issues/670))
* \[ENHANCEMENT] Remove some hidden and deprecated methods from webservices and command line ([#672](https://github.com/opencb/opencga/issues/672))
* \[CHANGE] Changes to family data model ([#677](https://github.com/opencb/opencga/issues/677))
* \[FEATURE] Add new analysis tool webservices ([#679](https://github.com/opencb/opencga/issues/679))
* **\[FEATURE]** Add version support for _Sample_, _Individual_ and _Family_ ([#684](https://github.com/opencb/opencga/issues/684))
* \[ENHANCEMENT] Clean and remove unnecessary dependencies for the client module ([#687](https://github.com/opencb/opencga/issues/687))
* \[ENHANCEMENT] Change some fields from _Clinical Analysis_ (BETA) data model ([#688](https://github.com/opencb/opencga/issues/688), [#702](https://github.com/opencb/opencga/issues/702))
* \[ENHANCEMENT] Remove unnecessary _Relatives_ data model ([#693](https://github.com/opencb/opencga/issues/693))
* \[ENHANCEMENT] Improve some individual webservices to better support the _Individual-Sample_ relation ([#701](https://github.com/opencb/opencga/issues/701))
* \[CHANGE] Change _individual_ parameter in the sample/create webservice ([#703](https://github.com/opencb/opencga/issues/703))
* \[CHANGE] Internal modification regarding the way the _Sample-Individual_ relation was stored ([#706](https://github.com/opencb/opencga/issues/706))
* \[ENHANCEMENT] Add new _admins_ group in studies ([#711](https://github.com/opencb/opencga/issues/711))
* \[ENHANCEMENT] Add new _stats_ field to _Sample_ data model ([#717](https://github.com/opencb/opencga/issues/717))
* \[CHANGE] Rename _ontologyTerms_ field in _Sample, Individual_ and _diseases_ field in _Family_ for _phenotypes_ ([#718](https://github.com/opencb/opencga/issues/718))
* **\[FEATURE]** Add new option to export and import data from/to catalog ([#720](https://github.com/opencb/opencga/issues/720))
* \[ENHANCEMENT] Improve _groupby_ webservices ([#721](https://github.com/opencb/opencga/issues/721))
* \[ENHANCEMENT] Support a list of ids in all GET webservices ([#727](https://github.com/opencb/opencga/issues/727))
* \[CHANGE] Internal modification: Change ACL delimiter used ([#740](https://github.com/opencb/opencga/issues/740))

#### Variant Storage

* \[FEATURE] Make use of the new VariantMetadata model from Biodata ([#673](https://github.com/opencb/opencga/issues#673))
* \[FEATURE] Major support of Symbolic variants in Variants Storage ([#695](https://github.com/opencb/opencga/issues#695))
* \[FEATURE] Create profiles to select hadoop flavour ([s#707](https://github.com/opencb/opencga/issues#707))
* \[FEATURE] Store info fields on storage-hadoop improvement storage ([#704](https://github.com/opencb/opencga/issues#704))
* \[FEATURE] Allow loading multiple variant files from the same sample with non overlapping variants ([#696](https://github.com/opencb/opencga/issues#696))
* \[FEATURE] New optional pipeline step "fill-gaps" ([#713](https://github.com/opencb/opencga/issues#713))
* \[CHANGE] Rename some Variant REST query parameters ([#751](https://github.com/opencb/opencga/issues#751))
* \[CHANGE] GO and EXPRESSION filter must be combined as an AND with other region ([#694](https://github.com/opencb/opencga/issues#694))
* \[CHANGE] Update CellBase to v4.5.3 improvement ([#770](https://github.com/opencb/opencga/issues#770))
* \[ENHANCEMENT] Speed up GENOTYPE filter with FILES filter, when possible ([#675](https://github.com/opencb/opencga/issues#675))
* \[ENHANCEMENT] Add field "source" to VariantQueryResult ([#758](https://github.com/opencb/opencga/issues#758))
* \[ENHANCEMENT] Indicate if "numTotalResults" is an approximated count in VariantQueryResult ([#749](https://github.com/opencb/opencga/issues#749))
* \[FIX] Inconsistent configuration param to select variant annotator ([#747](https://github.com/opencb/opencga/issues#747))
* \[FIX] Duplicate Key Warn/Error in Stage Collection ([#766](https://github.com/opencb/opencga/issues#766))
* \[FIX] IllegalArgumentException when CellBaseRestVariantAnnotator skips a variant ([#746](https://github.com/opencb/opencga/issues#746))
* \[FIX] Concurrent table modification error when loading in Hadoop with merge=basic ([#709)](https://github.com/opencb/opencga/issues#709)

## 1.2.0 (September 2017)

#### **Catalog**

* \[FIX] Fix job search by input and output files. ([#533](https://github.com/opencb/opencga/issues/533))
* \[ENHANCEMENT] Hide deprecated webservices. ([#599](https://github.com/opencb/opencga/issues/599))
* \[ENHANCEMENT] Set _ontologyTerms_ array during Sample/Individual /create and /update webservices. ([#613](https://github.com/opencb/opencga/issues/613))
* \[FEATURE] Add the concept of release in OpenCGA. ([#616](https://github.com/opencb/opencga/issues/616))
* \[FIX] Fix behaviour where individual ids are not recognized when creating new family. ([#617](https://github.com/opencb/opencga/issues/617))
* \[FEATURE] Implement JWT based session management ([#618](https://github.com/opencb/opencga/issues/618))
* \[ENHANCEMENT] Change List\<Long> for List\<Object> in corresponding data models ([#621](https://github.com/opencb/opencga/issues/621))
* \[ENHANCEMENT] Make ACL permissions part of the query ([#628](https://github.com/opencb/opencga/issues/628))
* \[FIX] Fix count parameter not working in search webservices and command line ([#629](https://github.com/opencb/opencga/issues/629))
* \[CHANGE] Change annotationsets REST webservices. ([#631](https://github.com/opencb/opencga/issues/631))
* \[FEATURE] Support multigroups ([#633](https://github.com/opencb/opencga/issues/633))
* \[ENHANCEMENT] Return HTTP 403 error code when user tries to access not granted data ([#636](https://github.com/opencb/opencga/issues/636))
* \[FEATURE] Add a new members group for every study ([#642](https://github.com/opencb/opencga/issues/642))
* \[FEATURE] Automatically sync OpenCGA groups with external LDAP groups during login ([#647](https://github.com/opencb/opencga/issues/647))
* \[FEATURE] Add new projects/search webservice ([#651](https://github.com/opencb/opencga/issues/651))
* \[FEATURE] Add new _confidential_ parameter to VariableSet data model. It allows defining confidential variable sets and corresponding annotation sets, so a new special permission will be needed to access them. ([#653](https://github.com/opencb/opencga/issues/653))
* \[FEATURE] Propagate permissions from samples to individuals. ([#657](https://github.com/opencb/opencga/issues/657))
* \[ENHANCEMENT] Return HTTP 401 error code when user is not successfully logged in or the token is invalid. ([#658](https://github.com/opencb/opencga/issues/658))\
  \


#### Variant Storage

* \[FEATURE] Improve Solr integration with VariantStorage when querying ([#638](https://github.com/opencb/opencga/issues/638))
* \[ENHANCEMENT] Improve Solr variant iterator by using Solr cursors ([#640](https://github.com/opencb/opencga/issues/))
* \[ENHANCEMENT] Variant Solr Search manager improvements ([#639](https://github.com/opencb/opencga/issues/))
* \[ENHANCEMENT] Configure VariantMerger and VariantNormalizer with VCFHeader ([#630](https://github.com/opencb/opencga/issues/))
* \[ENHANCEMENT] Store DisplayConsequenceType from VariantAnnotation at MongoDB ([#659](https://github.com/opencb/opencga/issues/659))
* \[ENHANCEMENT] Return VariantTraitAssociation as TraitAssociation (EvidenceEntry) ([#692](https://github.com/opencb/opencga/issues/692))
* \[FEATURE] Remove files from variants storage ([#192](https://github.com/opencb/opencga/issues/192))
* \[FEATURE] Command line and rest endpoints for remove operations on variant storage ([#623](https://github.com/opencb/opencga/issues/623))
* \[FEATURE] Simple merge mode for loading variants in opencga-storage-hadoop ([#609](https://github.com/opencb/opencga/issues/609))
* \[FEATURE] VariantQueryParam INCLUDE_FORMATS. Select format fields to return ([#608](https://github.com/opencb/opencga/issues/608))
* \[FEATURE] Store other genotype fields on storage-hadoop ([#602](https://github.com/opencb/opencga/issues/602))
* \[FEATURE] Export variant statistics ([#309](https://github.com/opencb/opencga/issues/309))
* \[FIX] Sample filter not working when the sample is in multiple files ([#641](https://github.com/opencb/opencga/issues/641))
* \[FIX] Possible error loading overlaping multiallelic variants ([#626](https://github.com/opencb/opencga/issues/626))
* \[FIX] Do not delete files (or related entries) from catalog if still loaded in variants storage ([#625](https://github.com/opencb/opencga/issues/625))
* \[FIX] Avoid OutOfMemoryError updating storage metadata from catalog ([#645](https://github.com/opencb/opencga/issues/645))
* \[FIX] Error indexing vcf files containing "variants" in the file name([#691](https://github.com/opencb/opencga/issues/691))

#### Relevant changes 

* CLI install changed and need two additional parameters 1: secretKey, algorithm
* Configuration file has changed and need to be adopted on all opencga installation 
* No logout
* Changes on [#616](https://github.com/opencb/opencga/issues/616), [#618](https://github.com/opencb/opencga/issues/618), [#621](https://github.com/opencb/opencga/issues/621), [#628](https://github.com/opencb/opencga/issues/628) and [#633](https://github.com/opencb/opencga/issues/633) require several migration scripts to be run over the Catalog database: [https://gist.github.com/pfurio/2ca0cb2da46eac9e309101066f8758f5](https://gist.github.com/pfurio/2ca0cb2da46eac9e309101066f8758f5)
* Changes on [#192](https://github.com/opencb/opencga/issues/192) and [#626](https://github.com/opencb/opencga/issues/626) require to execute a migration script on all Variants databases in MongoDB: [https://gist.github.com/j-coll/3dec01abc70644943d33de78105c633e](https://gist.github.com/j-coll/3dec01abc70644943d33de78105c633e)
* Changes in VariantAnnotation model. See [variantAnnotation.avdl](https://github.com/opencb/biodata/blob/v1.2.0/biodata-models/src/main/avro/variantAnnotation.avdl#L172)\

  * Field "exonNumber" replaced with "exonOverlap".
  * Added field "traitAssociation" that will replace "variantTraitAssociation" in next releases.

## 1.1.0 (June 2017)

#### Catalog

* \[ENHANCEMENT] Support integers and floats type for variables. ([#545](https://github.com/opencb/opencga/issues/545))
* \[BUG] Fix link race condition. ([#551](https://github.com/opencb/opencga/issues/551))
* \[FEATURE] Add new parameter _propagate_ to Individual web service when setting permissions to propagate permissions to the related samples. ([#558](https://github.com/opencb/opencga/issues/558))
* \[FEATURE] Add support to give permissions using queries in sample, individual and file web services. ([#560](https://github.com/opencb/opencga/issues/560))
* \[CHANGE] Change Acl REST web services. ([#561](https://github.com/opencb/opencga/issues/561))
* \[FIX] Not return fields that are of no interest (using include/exclude). ([#569](https://github.com/opencb/opencga/issues/569))
* \[FEATURE] Add new admin command line to synchronise and add _users_ from LDAP groups. ([#573](https://github.com/opencb/opencga/issues/573))
* \[FEATURE] Added new Family data model. ([#582](https://github.com/opencb/opencga/issues/582))
* \[FEATURE] Add list\<Sample> to individual/create web service. ([#583](https://github.com/opencb/opencga/issues/583))
* \[FEATURE] Add new /meta/status, /meta/ping and /meta/about web services. ([#572](https://github.com/opencb/opencga/issues/572))
* \[ENHANCEMENT] Support creating an Individual when calling to the Sample create web service. ([#586](https://github.com/opencb/opencga/issues/586))
* \[DEPRECATE] Deprecate _species_ field of Individual data model. ([#588](https://github.com/opencb/opencga/issues/588))
* \[ENHANCEMENT] Deprecate usage of _variableSetId_. Add field _variableSet_ to corresponding web services that accepts an id or a name. ([#589](https://github.com/opencb/opencga/issues/589))
* \[ENHANCEMENT] Support passing an array of _annotationSets_ when creating an Annotable entry (Sample, Cohort, Individual and Family). ([#590](https://github.com/opencb/opencga/issues/590))
* \[ENHANCEMENT] Add new _type_ field to Sample model. ([#591](https://github.com/opencb/opencga/issues/591))
* \[FEATURE] Add _father_ and _mother_ information in _attributes_ field of Individual. ([#592](https://github.com/opencb/opencga/issues/592))
* \[ENHANCEMENT] Add support to change _public/private_ User registration  ([#594](https://github.com/opencb/opencga/issues/594))
* \[DEPRECATE] Deprecate all _xx/create_ and _xx/update _GET webservices. ([#598](https://github.com/opencb/opencga/issues/598))

#### Variant Storage

* \[FEATURE] Make optional to merge non same overlapping variants in MongoDB ([#574](https://github.com/opencb/opencga/issues/574))
* \[FEATURE] Implement a benchmark framework for OpenCGA Storage ([#248](https://github.com/opencb/opencga/issues/248))
* \[ENHANCEMENT] Filter VcfRecord before converting into Variant object when possible ([#577](https://github.com/opencb/opencga/issues/577))
* \[BUGFIX] Not loading new overlapping variants in HBase ([#581](https://github.com/opencb/opencga/issues/581))
* \[ENHANCEMENT] Increment variant size threshold for CellBase annotation ([#596](https://github.com/opencb/opencga/issues/596))
* \[BUGFIX] Fix ArrayIndexOutOfBounds when loading variants data ([#597](https://github.com/opencb/opencga/issues/597))
* \[FEATURE] Add VCF export to gRPC command line ([#606](https://github.com/opencb/opencga/issues/606))\


#### Relevant changes - migration

* Add new permissions to admin user in the general configuration file. To add: VIEW_STUDY, UPDATE_STUDY and SHARE_STUDY
* Added sampleIds parameter in each individual entry. 
* Catalog changes require this migration script: [opencga-1.1.0-migration.js](https://gist.github.com/pfurio/ace5d31a4f42750801ac4070bd830e8b)
* Changes of [#574](https://github.com/opencb/opencga/issues/574) require to execute a migration script on all Variants databases in MongoDB: [opencga\_574\_add_studies_field_to_stage.js](https://gist.github.com/j-coll/8e9ace0b24c9f65fa99be64ebae5a9bb)

## 1.0.2

#### General

* \[BUGFIX] Fix VCF output format ([#584](https://github.com/opencb/opencga/issues/584))

#### Catalog

* \[ENHANCEMENT] New _dateOfBirth_ field added to Individual ([#580](https://github.com/opencb/opencga/issues/580)) and _somatic_ field to Sample ([#576](https://github.com/opencb/opencga/issues/576))
* \[ENHANCEMENT] Performance improvement when annotating new variants ([#575](https://github.com/opencb/opencga/issues/575))

## 1.0.0 (February 2017)

#### Catalog

* \[FIX] Authenticated users can now see public data ([#501](https://github.com/opencb/opencga/issues/501))
* Permissions assigned to _individuals_ are directly propagated to _samples_ ([#509](https://github.com/opencb/opencga/issues/509))
* \[CHANGED] _CREATE_ and _UPDATE_ permissions have been merged into _WRITE_ ([#506](https://github.com/opencb/opencga/issues/506))
* \[CHANGED] Permissions given to folders are now propagated in the database ([#505](https://github.com/opencb/opencga/issues/505)), this increase significantly the performance of the ACL resolution 
* \[CHANGED] Changes in session data model ([#479](https://github.com/opencb/opencga/issues/479))

#### Storage

* Complete the implementation of _exclude_ and _include_ of fields for Variant queries ([#515](https://github.com/opencb/opencga/issues/515))

#### Clients

* \[Python] new Python client implemented following the same architecture than Java and Javascript clients ([https://github.com/opencb/opencga/pull/516](https://github.com/opencb/opencga/pull/516)). This improves the quality significantly and add many new features, thanks [Antonio Rueda](http://docs.opencb.org/display/\~aruemar)!
* Java and Javascript client libs use always POST when available

#### Server

* \[REST] Add new _files/create using _POST to create new folders and files with some content ([#514](https://github.com/opencb/opencga/issues/514))
* \[REST] Rename parameters from _acl/update_ ([2617993](https://github.com/opencb/opencga/commit/2617993fef6aefe06802da5d6dc590f7d099687a))
* \[REST] Hide from Swagger all _create_ and _update_ ACL web services using GET ([cbea817](https://github.com/opencb/opencga/commit/cbea8178ec295444b84473b7453cce74def441b2)), these should always use POST, this affects to _studies, samples, files,_ ... (these will be removed in version 1.1) 
* \[REST] Complete the implementation of _exclude_ and _include_ of fields for Variant queries
* \[REST] Remove from _users_ the GET methods to _create_ and _update_ filters ([5125a22](https://github.com/opencb/opencga/commit/5125a228d558efca54ad557a37f27843a7600248))
* \[REST] Add a warning to Swagger to **all** _create_ and _update_ methods using GET ([ee6d66](https://github.com/opencb/opencga/commit/ee6d66373a6419afea0fb5eafc9c40bdf39af398)). It is encourage that all _create_ and _update_ actions use always POST
* \[GRPC] Complete the implementation of _exclude_ and _include_ of fields for Variant queries

## 1.0.0-rc3 (January 2017)

#### General

* Major changes in maven properties and configuration files. ([#480](https://github.com/opencb/opencga/issues/480))
* catalog-configuration.yml and configuration.yml have been merged. ([#476](https://github.com/opencb/opencga/issues/476))

#### Catalog

* Added support to fetch shared projects and studies.
* Added organism information to the project data model. ([#455](https://github.com/opencb/opencga/issues/455))
* Renamed diskUsage and diskQuota for size and quota respectively in all data models were present.
* Closed sessions are now removed from the array of sessions (but they can still be found in the audit collection). ([#475](https://github.com/opencb/opencga/issues/475))

#### Storage

* Improve error handling for storage operations ([#447](https://github.com/opencb/opencga/issues/447), [#463](https://github.com/opencb/opencga/issues/463), [#465](https://github.com/opencb/opencga/issues/465))
* Add param --resume to opencga-analysis.sh and opencga-storage.sh command line ([#465](https://github.com/opencb/opencga/issues/465))
* Import and export variants datasets ([#459](https://github.com/opencb/opencga/issues/459), [#460](https://github.com/opencb/opencga/issues/460))
* Enable gRPC as top level feature ([#492](https://github.com/opencb/opencga/issues/492))
* New top layer StorageManager connecting catalog with storage ([#486](https://github.com/opencb/opencga/issues/486))
* Shade proto and guava dependencies for Hadoop ([#440](https://github.com/opencb/opencga/issues/440))

#### Server

* Create and update webservices have been all implemented via POST.

#### Known issues

*   OpenCGA storage hadoop is not available in this version. To compile use this line:

    `mvn clean install -DskipTests -pl '!:opencga-storage-hadoop-core'`

## 1.0.0-rc2 (November 2016)

## 1.0.0-rc1 (_September 2016_)

This release constitutes the first release candidate (RC1). This is the biggest release ever with more than **1,400 commits**, special mention to [**j-coll**](https://github.com/j-coll) and [**pfurio**](https://github.com/pfurio) for their contribution in Catalog, Storage and Server components.

#### General

* New command line interfaces (CLI) for users (_opencga.sh_), admins (_opencga-admin.sh_) and analysis (_opencga-analysis.sh_)

#### Catalog

* New catalog configuration file using YAML.
* New authorisation method. A big list of permission have been defined.
* New smart id resolver. Numerical ids are no longer mandatory and ids are now resolved internally given the alias.
* New and improved java and python command lines.
* New javascript, java and R client implementations.
* Support for annotations in cohorts.

#### Storage

* Many performance improvements and fixes in MongoBD storage engine, the most notable include a new load strategy to improve file merging scalability
* New storage engine based on **Apache HBase**, this is completely functional but is tagged as _beta_ until more tests are done.
* New custom variant annotation implemented

#### Server

* An experimental gRPC server implemented

## v0.7.0

Third Beta

## v0.6.0

Second Beta

## v0.5.0

First Beta

* \[IMPROVEMENT] Add direct loader for the first file in a chromosome to load directly to the variants collection ([#354](https://github.com/opencb/opencga/issues/354))
