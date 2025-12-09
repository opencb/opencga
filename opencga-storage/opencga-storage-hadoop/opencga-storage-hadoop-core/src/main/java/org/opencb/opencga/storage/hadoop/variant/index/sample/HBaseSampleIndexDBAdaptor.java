package org.opencb.opencga.storage.hadoop.variant.index.sample;

import com.google.common.collect.Iterators;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.utils.iterators.CloseableIterator;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.index.sample.SampleIndexDBAdaptor;
import org.opencb.opencga.storage.core.variant.index.sample.SampleIndexEntry;
import org.opencb.opencga.storage.core.variant.index.sample.SampleIndexEntryFilter;
import org.opencb.opencga.storage.core.variant.index.sample.query.LocusQuery;
import org.opencb.opencga.storage.core.variant.index.sample.query.SampleIndexQuery;
import org.opencb.opencga.storage.core.variant.index.sample.query.SingleSampleIndexQuery;
import org.opencb.opencga.storage.core.variant.index.sample.schema.SampleIndexSchema;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageOptions;
import org.opencb.opencga.storage.hadoop.variant.utils.HBaseVariantTableNameGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.index.core.IndexUtils.EMPTY_MASK;

/**
 * Created on 14/05/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HBaseSampleIndexDBAdaptor extends SampleIndexDBAdaptor {

    private final HBaseManager hBaseManager;
    private final HBaseVariantTableNameGenerator tableNameGenerator;
    private static byte[] family = GenomeHelper.COLUMN_FAMILY_BYTES;
    private static Logger logger = LoggerFactory.getLogger(HBaseSampleIndexDBAdaptor.class);

    public HBaseSampleIndexDBAdaptor(HBaseManager hBaseManager, HBaseVariantTableNameGenerator tableNameGenerator,
                                     VariantStorageMetadataManager metadataManager) {
        super(metadataManager);
        this.hBaseManager = hBaseManager;
        this.tableNameGenerator = tableNameGenerator;
    }

    @Override
    public HBaseSampleIndexBuilder newSampleIndexBuilder(VariantStorageEngine engine, String study) throws StorageEngineException {
        return new HBaseSampleIndexBuilder(this, study, ((HadoopVariantStorageEngine) engine).getMRExecutor());
    }

    /**
     * Partially processed iterator. Internal usage only.
     *
     * @param query SingleSampleIndexQuery
     * @param schema Sample index schema
     * @return SingleSampleIndexVariantDBIterator
     */
    protected VariantDBIterator internalIterator(SingleSampleIndexQuery query, SampleIndexSchema schema) {
        String tableName = getSampleIndexTableName(query);

        try {
            return hBaseManager.act(tableName, table -> {
                return new SingleSampleIndexVariantDBIterator(table, query, schema, this);
            });
        } catch (IOException e) {
            throw VariantQueryException.internalException(e);
        }
    }

    @Override
    protected RawSingleSampleIndexVariantDBIterator rawInternalIterator(SingleSampleIndexQuery query, SampleIndexSchema schema) {
        String tableName = getSampleIndexTableName(query);

        try {
            return hBaseManager.act(tableName, table -> {
                return new RawSingleSampleIndexVariantDBIterator(table, query, schema, this);
            });
        } catch (IOException e) {
            throw VariantQueryException.internalException(e);
        }
    }

    public String getSampleIndexTableName(SampleIndexQuery query) {
        return tableNameGenerator.getSampleIndexTableName(toStudyId(query.getStudy()), query.getSchema().getVersion());
    }

    public String getSampleIndexTableNameLatest(int studyId) {
        int version = schemaFactory.getSampleIndexConfigurationLatest(studyId, true).getVersion();
        return tableNameGenerator.getSampleIndexTableName(studyId, version);
    }

    public String getSampleIndexTableName(int studyId, int version) {
        return tableNameGenerator.getSampleIndexTableName(studyId, version);
    }

    public HBaseVariantTableNameGenerator getTableNameGenerator() {
        return tableNameGenerator;
    }

    protected Map<String, List<Variant>> queryByGt(int study, int sample, String chromosome, int position)
            throws IOException {
        SampleIndexSchema schema = schemaFactory.getSchema(study, sample, false);
        Result result = queryByGtInternal(study, sample, chromosome, position, schema.getVersion());
        return newConverter(schema).convertToMap(result);
    }

    protected SampleIndexEntryPutBuilder queryByGtBuilder(int study, int sample, String chromosome, int position, SampleIndexSchema schema)
            throws IOException {
        Result result = queryByGtInternal(study, sample, chromosome, position, schema.getVersion());
        return new SampleIndexEntryPutBuilder(sample, chromosome, position, schema,
                newConverter(schema).convertToMapSampleVariantIndex(result));
    }

    private Result queryByGtInternal(int study, int sample, String chromosome, int position, int version) throws IOException {
        String tableName = getSampleIndexTableName(study, version);
        return hBaseManager.act(tableName, table -> {
            Get get = new Get(SampleIndexSchema.toRowKey(sample, chromosome, position));
            get.addFamily(family);
            return table.get(get);
        });
    }

    public Iterator<Map<String, List<Variant>>> iteratorByGt(int study, int sample) throws IOException {
        return iteratorByGt(study, sample, schemaFactory.getSchema(study, sample, false));
    }

    public Iterator<Map<String, List<Variant>>> iteratorByGt(int study, int sample, SampleIndexSchema schema) throws IOException {
        String tableName = getSampleIndexTableName(study, schema.getVersion());
        HBaseToSampleIndexConverter converter = newConverter(schema);
        return hBaseManager.act(tableName, table -> {
            Scan scan = new Scan();
            scan.setRowPrefixFilter(SampleIndexSchema.toRowKey(sample));
            try {
                ResultScanner scanner = table.getScanner(scan);
                Iterator<Result> resultIterator = scanner.iterator();
                return Iterators.transform(resultIterator, converter::convertToMap);
            } catch (IOException e) {
                throw VariantQueryException.internalException(e);
            }
        });
    }

    @Override
    public CloseableIterator<SampleIndexEntry> rawIterator(int study, int sample, Region region, SampleIndexSchema schema)
            throws IOException {
        String tableName = getSampleIndexTableName(study, schema.getVersion());
        return hBaseManager.act(tableName, table -> {
            Scan scan = new Scan();
            if (region != null) {
                scan.setStartRow(SampleIndexSchema.toRowKey(sample, region.getChromosome(), region.getStart()));
                scan.setStopRow(SampleIndexSchema.toRowKey(sample, region.getChromosome(),
                        region.getEnd() + (region.getEnd() == Integer.MAX_VALUE ? 0 : SampleIndexSchema.BATCH_SIZE)));
            } else {
                scan.setRowPrefixFilter(SampleIndexSchema.toRowKey(sample));
            }

            HBaseToSampleIndexConverter converter = newConverter(schema);
            ResultScanner scanner = table.getScanner(scan);
            Iterator<Result> resultIterator = scanner.iterator();
            return CloseableIterator.wrap(Iterators.transform(resultIterator, converter::convert), scanner);
        });
    }

    @Override
    protected long count(SingleSampleIndexQuery query) {
        Collection<LocusQuery> locusQueries;
        if (CollectionUtils.isEmpty(query.getLocusQueries())) {
            // If no locus are defined, get a list of one null element to initialize the stream.
            locusQueries = Collections.singletonList(null);
        } else {
            locusQueries = query.getLocusQueries();
        }

        int studyId = toStudyId(query.getStudy());
        String tableName = getSampleIndexTableName(studyId, query.getSchema().getVersion());

        HBaseToSampleIndexConverter converter = newConverter(query.getSchema());
        try {
            return hBaseManager.act(tableName, table -> {
                long count = 0;
                for (LocusQuery locusQuery : locusQueries) {
                    // Split region in countable regions
                    List<LocusQuery> subLocusQueries = splitLocusQuery(locusQuery);

                    for (LocusQuery subLocusQuery : subLocusQueries) {
                        boolean noLocusFilter = subLocusQuery == null
                                || (subLocusQuery.getVariants().isEmpty()
                                && subLocusQuery.getRegions().size() == 1
                                && matchesWithBatch(subLocusQuery.getRegions().get(0)));
                        // Don't need to parse the variant to filter
                        boolean simpleCount = !query.isMultiFileSample()
                                && CollectionUtils.isEmpty(query.getVariantTypes())
                                && noLocusFilter;
                        try {
                            if (query.emptyOrRegionFilter() && simpleCount) {
                                // Directly sum counters
                                Scan scan = parseCount(query, subLocusQuery);
                                ResultScanner scanner = table.getScanner(scan);
                                Result result = scanner.next();
                                while (result != null) {
                                    count += converter.convertToCount(result);
                                    result = scanner.next();
                                }
                            } else {
                                SampleIndexEntryFilter filter = buildSampleIndexEntryFilter(query, subLocusQuery);
                                Scan scan;
                                if (simpleCount) {
                                    // Fast filter and count. Don't need to parse the variant to filter
                                    scan = parseCountAndFilter(query, subLocusQuery);
                                } else {
                                    // Need to parse the variant to finish filtering. Create a normal scan query.
                                    scan = parse(query, subLocusQuery);
                                }
                                ResultScanner scanner = table.getScanner(scan);
                                Result result = scanner.next();
                                while (result != null) {
                                    SampleIndexEntry sampleIndexEntry = converter.convert(result);
                                    count += filter.filterAndCount(sampleIndexEntry);
                                    result = scanner.next();
                                }
                            }
                        } catch (IOException e) {
                            throw VariantQueryException.internalException(e);
                        }
                    }
                }
                return count;
            });
        } catch (IOException e) {
            throw VariantQueryException.internalException(e);
        }
    }

    protected HBaseToSampleIndexConverter newConverter(SampleIndexSchema schema) {
        return new HBaseToSampleIndexConverter(schema);
    }


    public Scan parse(SingleSampleIndexQuery query, LocusQuery regions) {
        return parse(query, regions, false, false);
    }

    public Scan parseIncludeAll(SingleSampleIndexQuery query, LocusQuery region) {
        return parse(query, region, false, false, true);
    }

    public Scan parseCount(SingleSampleIndexQuery query, LocusQuery regions) {
        return parse(query, regions, true, true);
    }

    public Scan parseCountAndFilter(SingleSampleIndexQuery query, LocusQuery regions) {
        return parse(query, regions, false, true);
    }

    private Scan parse(SingleSampleIndexQuery query, LocusQuery regions, boolean onlyCount, boolean skipGtColumn) {
        return parse(query, regions, onlyCount, skipGtColumn, false);
    }

    private Scan parse(SingleSampleIndexQuery query, LocusQuery locusQuery, boolean onlyCount, boolean skipGtColumn, boolean includeAll) {

        Scan scan = new Scan();
        int studyId = toStudyId(query.getStudy());
        int sampleId = toSampleId(studyId, query.getSample());
        if (locusQuery != null) {
            // Regions from the same group are sorted and do not overlap.
            Region region = locusQuery.getChunkRegion();
            scan.setStartRow(SampleIndexSchema.toRowKey(sampleId, region.getChromosome(), region.getStart()));
            scan.setStopRow(SampleIndexSchema.toRowKey(sampleId, region.getChromosome(), region.getEnd()));
        } else {
            scan.setStartRow(SampleIndexSchema.toRowKey(sampleId));
            scan.setStopRow(SampleIndexSchema.toRowKey(sampleId + 1));
        }
        // If genotypes are not defined, return ALL columns
        for (String gt : query.getGenotypes()) {
            scan.addColumn(family, SampleIndexSchema.toGenotypeDiscrepanciesCountColumn());
            scan.addColumn(family, SampleIndexSchema.toGenotypeCountColumn(gt));
            if (!onlyCount) {
                if (query.getMendelianError()) {
                    scan.addColumn(family, SampleIndexSchema.toMendelianErrorColumn());
                } else {
                    if (!skipGtColumn) {
                        scan.addColumn(family, SampleIndexSchema.toGenotypeColumn(gt));
                    }
                }
                if (includeAll || query.getAnnotationIndexMask() != EMPTY_MASK) {
                    scan.addColumn(family, SampleIndexSchema.toAnnotationIndexColumn(gt));
                    scan.addColumn(family, SampleIndexSchema.toAnnotationIndexCountColumn(gt));
                }
                if (includeAll || !query.getAnnotationIndexQuery().getCtBtTfFilter().isNoOp()) {
                    scan.addColumn(family, SampleIndexSchema.toAnnotationCtBtTfIndexColumn(gt));
                    // All independent indexes are required to parse CtBtTf
                    scan.addColumn(family, SampleIndexSchema.toAnnotationBiotypeIndexColumn(gt));
                    scan.addColumn(family, SampleIndexSchema.toAnnotationConsequenceTypeIndexColumn(gt));
                    scan.addColumn(family, SampleIndexSchema.toAnnotationTranscriptFlagIndexColumn(gt));
                }
                if (includeAll || !query.getAnnotationIndexQuery().getBiotypeFilter().isNoOp()) {
                    scan.addColumn(family, SampleIndexSchema.toAnnotationBiotypeIndexColumn(gt));
                }
                if (includeAll || !query.getAnnotationIndexQuery().getConsequenceTypeFilter().isNoOp()) {
                    scan.addColumn(family, SampleIndexSchema.toAnnotationConsequenceTypeIndexColumn(gt));
                }
                if (includeAll || !query.getAnnotationIndexQuery().getTranscriptFlagFilter().isNoOp()) {
                    scan.addColumn(family, SampleIndexSchema.toAnnotationTranscriptFlagIndexColumn(gt));
                }
                if (includeAll || !query.getAnnotationIndexQuery().getPopulationFrequencyFilter().isNoOp()) {
                    scan.addColumn(family, SampleIndexSchema.toAnnotationPopFreqIndexColumn(gt));
                }
                if (includeAll || !query.getAnnotationIndexQuery().getClinicalFilter().isNoOp()) {
                    scan.addColumn(family, SampleIndexSchema.toAnnotationClinicalIndexColumn(gt));
                }
                if (includeAll || !query.emptyFileIndex()) {
                    scan.addColumn(family, SampleIndexSchema.toFileIndexColumn(gt));
                }
                if (includeAll) {
                    scan.addColumn(family, SampleIndexSchema.toFileDataColumn(gt));
                }
                if (includeAll || query.isIncludeParentColumns()
                        || query.hasFatherFilter() || query.hasMotherFilter() || query.getMendelianErrorType() != null) {
                    scan.addColumn(family, SampleIndexSchema.toParentsGTColumn(gt));
                }
            }
        }
        if (query.getMendelianError()) {
            scan.addColumn(family, SampleIndexSchema.toMendelianErrorColumn());
        }
        scan.setCaching(hBaseManager.getConf().getInt("hbase.client.scanner.caching", 100));

        logger.info("---------");
        logger.info("Study = \"" + query.getStudy() + "\" (id=" + studyId + ")");
        logger.info("Sample = \"" + query.getSample() + "\" (id=" + sampleId + ") , schema version = " + query.getSchema().getVersion());
        logger.info("Table = " + getSampleIndexTableName(query));
        printScan(scan);
        printQuery(locusQuery);
        printQuery(query);

//        try {
//            System.out.println("scan = " + scan.toJSON() + " " + rowKeyToString(scan.getStartRow()) + " -> + "
// + rowKeyToString(scan.getStopRow()));
//        } catch (IOException e) {
//            throw VariantQueryException.internalException(e);
//        }

        return scan;
    }

    public static void printScan(Scan scan) {
        logger.info("StartRow = " + Bytes.toStringBinary(scan.getStartRow()) + " == "
                + SampleIndexSchema.rowKeyToString(scan.getStartRow()));
        logger.info("StopRow = " + Bytes.toStringBinary(scan.getStopRow()) + " == "
                + SampleIndexSchema.rowKeyToString(scan.getStopRow()));
        logger.info("columns = " + scan.getFamilyMap().getOrDefault(family, Collections.emptyNavigableSet())
                .stream().map(Bytes::toString).collect(Collectors.joining(",")));
//        logger.info("MaxResultSize = " + scan.getMaxResultSize());
//        logger.info("Filters = " + scan.getFilter());
//        logger.info("Batch = " + scan.getBatch());
        logger.info("Caching = " + scan.getCaching());
    }

    public boolean createTableIfNeeded(int studyId, int version, ObjectMap options) {
        String sampleIndexTable = getSampleIndexTableName(studyId, version);
        try {
            if (hBaseManager.tableExists(sampleIndexTable)) {
                return false;
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        int files = options.getInt(
                HadoopVariantStorageOptions.EXPECTED_FILES_NUMBER.key(),
                HadoopVariantStorageOptions.EXPECTED_FILES_NUMBER.defaultValue());
        int samples = options.getInt(
                HadoopVariantStorageOptions.EXPECTED_SAMPLES_NUMBER.key(),
                files);
        // Check actual indexed samples size.
        // This value might be larger than "expected samples" if the table is being rebuilt.
        int indexedSamplesSize = metadataManager.getIndexedSamples(studyId).size();
        samples = Math.max(samples, indexedSamplesSize);

        int preSplitSize = options.getInt(
                HadoopVariantStorageOptions.SAMPLE_INDEX_TABLE_PRESPLIT_SIZE.key(),
                HadoopVariantStorageOptions.SAMPLE_INDEX_TABLE_PRESPLIT_SIZE.defaultValue());
        int sampleIndexExtraSplits = options.getInt(
                HadoopVariantStorageOptions.SAMPLE_INDEX_TABLE_PRESPLIT_EXTRA_SPLITS.key(),
                HadoopVariantStorageOptions.SAMPLE_INDEX_TABLE_PRESPLIT_EXTRA_SPLITS.defaultValue());

        int splits = (samples / preSplitSize) + sampleIndexExtraSplits;
        ArrayList<byte[]> preSplits = new ArrayList<>(splits);
        for (int i = 0; i < splits; i++) {
            preSplits.add(SampleIndexSchema.toRowKey(i * preSplitSize));
        }

        Compression.Algorithm compression = Compression.getCompressionAlgorithmByName(
                options.getString(
                        HadoopVariantStorageOptions.SAMPLE_INDEX_TABLE_COMPRESSION.key(),
                        HadoopVariantStorageOptions.SAMPLE_INDEX_TABLE_COMPRESSION.defaultValue()));

        try {
            return hBaseManager.createTableIfNeeded(sampleIndexTable, GenomeHelper.COLUMN_FAMILY_BYTES, preSplits, compression);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void expandTableIfNeeded(int studyId, int version, List<Integer> sampleIds, ObjectMap options) {
        String sampleIndexTable = getSampleIndexTableName(studyId, version);
        int preSplitSize = options.getInt(
                HadoopVariantStorageOptions.SAMPLE_INDEX_TABLE_PRESPLIT_SIZE.key(),
                HadoopVariantStorageOptions.SAMPLE_INDEX_TABLE_PRESPLIT_SIZE.defaultValue());
        int sampleIndexExtraBatches = options.getInt(
                HadoopVariantStorageOptions.SAMPLE_INDEX_TABLE_PRESPLIT_EXTRA_SPLITS.key(),
                HadoopVariantStorageOptions.SAMPLE_INDEX_TABLE_PRESPLIT_EXTRA_SPLITS.defaultValue());
        Set<Integer> batches = new HashSet<>();
        for (Integer sampleId : sampleIds) {
            batches.add((sampleId) / preSplitSize);
        }

        try {
            int newRegions = hBaseManager.expandTableIfNeeded(sampleIndexTable, batches,
                    batch -> Collections.singletonList(SampleIndexSchema.toRowKey(batch * preSplitSize)),
                    sampleIndexExtraBatches, batch -> SampleIndexSchema.toRowKey(batch * preSplitSize));
            if (newRegions != 0) {
                // Log number of new regions
                logger.info("Sample index table '" + sampleIndexTable + "' expanded with " + newRegions + " new regions");
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }


}
