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
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.utils.iterators.CloseableIterator;
import org.opencb.opencga.storage.core.utils.iterators.IntersectMultiKeyIterator;
import org.opencb.opencga.storage.core.utils.iterators.UnionMultiKeyIterator;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantIterable;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.IntersectMultiVariantKeyIterator;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.UnionMultiVariantKeyIterator;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils.QueryOperation;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageOptions;
import org.opencb.opencga.storage.hadoop.variant.index.IndexUtils;
import org.opencb.opencga.storage.hadoop.variant.index.core.filters.IndexFieldFilter;
import org.opencb.opencga.storage.hadoop.variant.index.query.*;
import org.opencb.opencga.storage.hadoop.variant.utils.HBaseVariantTableNameGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantSqlQueryParser.DEFAULT_LOADED_GENOTYPES;
import static org.opencb.opencga.storage.hadoop.variant.index.IndexUtils.EMPTY_MASK;

/**
 * Created on 14/05/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class SampleIndexDBAdaptor implements VariantIterable {

    private final HBaseManager hBaseManager;
    private final HBaseVariantTableNameGenerator tableNameGenerator;
    private final VariantStorageMetadataManager metadataManager;
    private final SampleIndexSchemaFactory schemaFactory;
    private static byte[] family = GenomeHelper.COLUMN_FAMILY_BYTES;
    private static Logger logger = LoggerFactory.getLogger(SampleIndexDBAdaptor.class);

    public SampleIndexDBAdaptor(HBaseManager hBaseManager, HBaseVariantTableNameGenerator tableNameGenerator,
                                VariantStorageMetadataManager metadataManager) {
        this.hBaseManager = hBaseManager;
        this.tableNameGenerator = tableNameGenerator;
        this.metadataManager = metadataManager;
        this.schemaFactory = new SampleIndexSchemaFactory(metadataManager);
    }

    @Override
    public VariantDBIterator iterator(Query query, QueryOptions options) {
        return iterator(parseSampleIndexQuery(query));
    }

    public VariantDBIterator iterator(SampleIndexQuery query) {
        return iterator(query, QueryOptions.empty());
    }

    public VariantDBIterator iterator(SampleIndexQuery query, QueryOptions options) {
        Map<String, List<String>> samples = query.getSamplesMap();

        if (samples.isEmpty()) {
            throw new VariantQueryException("At least one sample expected to query SampleIndex!");
        }
        SampleIndexSchema schema = query.getSchema();
        QueryOperation operation = query.getQueryOperation();

        if (samples.size() == 1) {
            String sample = samples.entrySet().iterator().next().getKey();
            List<String> gts = query.getSamplesMap().get(sample);

            if (gts.isEmpty()) {
                // If empty, should find none. Return empty iterator
                return VariantDBIterator.emptyIterator();
            } else {
                logger.info("Single sample indexes iterator : " + sample);
                SingleSampleIndexVariantDBIterator iterator = internalIterator(query.forSample(sample, gts), schema);
                return iterator.localLimitSkip(options);
            }
        }

        List<VariantDBIterator> iterators = new ArrayList<>(samples.size());
        List<VariantDBIterator> negatedIterators = new ArrayList<>(samples.size());

        for (Map.Entry<String, List<String>> entry : samples.entrySet()) {
            String sample = entry.getKey();
            List<String> gts = entry.getValue();

            if (query.isNegated(sample)) {
                if (!gts.isEmpty()) {
                    negatedIterators.add(internalIterator(query.forSample(sample, gts), schema));
                }
                // Skip if GTs to query is empty!
                // Otherwise, it will return ALL genotypes instead of none
            } else {
                if (gts.isEmpty()) {
                    // If empty, should find none. Add empty iterator for this sample
                    iterators.add(VariantDBIterator.emptyIterator());
                } else {
                    iterators.add(internalIterator(query.forSample(sample, gts), schema));
                }
            }
        }
        VariantDBIterator iterator;
        if (operation.equals(QueryOperation.OR)) {
            logger.info("Union of " + iterators.size() + " sample indexes");
            iterator = new UnionMultiVariantKeyIterator(iterators);
        } else {
            logger.info("Intersection of " + iterators.size() + " sample indexes plus " + negatedIterators.size() + " negated indexes");
            iterator = new IntersectMultiVariantKeyIterator(iterators, negatedIterators);
        }

        return iterator.localLimitSkip(options);
    }

    /**
     * Partially processed iterator. Internal usage only.
     *
     * @param query SingleSampleIndexQuery
     * @param schema Sample index schema
     * @return SingleSampleIndexVariantDBIterator
     */
    private SingleSampleIndexVariantDBIterator internalIterator(SingleSampleIndexQuery query, SampleIndexSchema schema) {
        String tableName = getSampleIndexTableName(query);

        try {
            return hBaseManager.act(tableName, table -> {
                return new SingleSampleIndexVariantDBIterator(table, query, schema, this);
            });
        } catch (IOException e) {
            throw VariantQueryException.internalException(e);
        }
    }

    private RawSingleSampleIndexVariantDBIterator rawInternalIterator(SingleSampleIndexQuery query, SampleIndexSchema schema) {
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

    public VariantStorageMetadataManager getMetadataManager() {
        return metadataManager;
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
        SampleIndexSchema schema = schemaFactory.getSchema(study, sample, false);
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

    public CloseableIterator<SampleIndexEntry> rawIterator(int study, int sample) throws IOException {
        return rawIterator(study, sample, null);
    }

    public CloseableIterator<SampleIndexEntry> rawIterator(int study, int sample, Region region) throws IOException {
        SampleIndexSchema schema = schemaFactory.getSchema(study, sample, false);
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

    /**
     * Partially processed iterator. Internal usage only.
     *
     * @param study study
     * @param sample sample
     * @return SingleSampleIndexVariantDBIterator
     */
    public RawSingleSampleIndexVariantDBIterator rawIterator(String study, String sample) {
        Query query = new Query(VariantQueryParam.STUDY.key(), study).append(VariantQueryParam.SAMPLE.key(), sample);
        SingleSampleIndexQuery sampleIndexQuery = parseSampleIndexQuery(query).forSample(sample);
        return rawInternalIterator(sampleIndexQuery, sampleIndexQuery.getSchema());
    }

    public CloseableIterator<SampleVariantIndexEntry> rawIterator(Query query) throws IOException {
        return rawIterator(parseSampleIndexQuery(query));
    }

    public CloseableIterator<SampleVariantIndexEntry> rawIterator(SampleIndexQuery query) throws IOException {
        return rawIterator(query, new QueryOptions());
    }

    public CloseableIterator<SampleVariantIndexEntry> rawIterator(SampleIndexQuery query, QueryOptions options) throws IOException {
        Map<String, List<String>> samples = query.getSamplesMap();

        if (samples.isEmpty()) {
            throw new VariantQueryException("At least one sample expected to query SampleIndex!");
        }
        SampleIndexSchema schema = query.getSchema();
        QueryOperation operation = query.getQueryOperation();

        if (samples.size() == 1) {
            String sample = samples.entrySet().iterator().next().getKey();
            List<String> gts = query.getSamplesMap().get(sample);

            if (gts.isEmpty()) {
                // If empty, should find none. Return empty iterator
                return CloseableIterator.emptyIterator();
            } else {
                logger.info("Single sample indexes iterator");
                RawSingleSampleIndexVariantDBIterator iterator = rawInternalIterator(query.forSample(sample, gts), schema);
                return iterator.localLimitSkip(options);
            }
        }

        List<RawSingleSampleIndexVariantDBIterator> iterators = new ArrayList<>(samples.size());
        List<RawSingleSampleIndexVariantDBIterator> negatedIterators = new ArrayList<>(samples.size());

        for (Map.Entry<String, List<String>> entry : samples.entrySet()) {
            String sample = entry.getKey();
            List<String> gts = entry.getValue();

            if (query.isNegated(sample)) {
                if (!gts.isEmpty()) {
                    negatedIterators.add(rawInternalIterator(query.forSample(sample, gts), schema));
                }
                // Skip if GTs to query is empty!
                // Otherwise, it will return ALL genotypes instead of none
            } else {
                if (gts.isEmpty()) {
                    // If empty, should find none. Add empty iterator for this sample
                    iterators.add(RawSingleSampleIndexVariantDBIterator.emptyIterator());
                } else {
                    iterators.add(rawInternalIterator(query.forSample(sample, gts), schema));
                }
            }
        }

        final CloseableIterator<SampleVariantIndexEntry> iterator;
        if (operation.equals(QueryOperation.OR)) {
            logger.info("Union of " + iterators.size() + " sample indexes");
            iterator = new UnionMultiKeyIterator<>(
                    Comparator.comparing(SampleVariantIndexEntry::getVariant, VariantDBIterator.VARIANT_COMPARATOR), iterators);
        } else {
            logger.info("Intersection of " + iterators.size() + " sample indexes plus " + negatedIterators.size() + " negated indexes");
            iterator = new IntersectMultiKeyIterator<>(
                    Comparator.comparing(SampleVariantIndexEntry::getVariant, VariantDBIterator.VARIANT_COMPARATOR),
                    iterators, negatedIterators);
        }

        return iterator.localLimitSkip(options);
    }

    public boolean isFastCount(SampleIndexQuery query) {
        return query.getSamplesMap().size() == 1 && query.emptyAnnotationIndex() && query.emptyFileIndex();
    }

    public long count(List<Region> regions, String study, String sample, List<String> gts) {
        return count(newParser().parse(regions, study, sample, gts));
    }

    public long count(SampleIndexQuery query) {
        if (query.getSamplesMap().size() == 1 && query.getMendelianErrorSet().isEmpty()) {
            String sample = query.getSamplesMap().keySet().iterator().next();
            return count(query.forSample(sample));
        } else {
            return Iterators.size(iterator(query));
        }
    }

    private long count(SingleSampleIndexQuery query) {
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
                    List<LocusQuery> subLocusQueries;
                    if (locusQuery != null && locusQuery.getVariants().isEmpty() && locusQuery.getRegions().size() == 1) {
                        subLocusQueries = splitRegion(locusQuery.getRegions().get(0))
                                .stream().map(LocusQuery::buildLocusQuery).collect(Collectors.toList());
                    } else {
                        // Do not split
                        subLocusQueries = Collections.singletonList(locusQuery);
                    }
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

    public SampleIndexSchemaFactory getSchemaFactory() {
        return schemaFactory;
    }

    public SampleIndexSchema getSchemaLatest(Object study) {
        return schemaFactory.getSchemaLatest(toStudyId(study));
    }

    protected int toStudyId(Object study) {
        int studyId;
        if (study == null || study instanceof String && ((String) study).isEmpty()) {
            Map<String, Integer> studies = metadataManager.getStudies(null);
            if (studies.size() == 1) {
                studyId = studies.values().iterator().next();
            } else {
                throw VariantQueryException.missingStudy(studies.keySet());
            }
        } else {
            studyId = metadataManager.getStudyId(study);
        }
        return studyId;
    }

    protected List<String> getAllLoadedGenotypes(int study) {
        List<String> allGts = metadataManager.getStudyMetadata(study)
                .getAttributes()
                .getAsStringList(VariantStorageOptions.LOADED_GENOTYPES.key());
        if (allGts == null || allGts.isEmpty()) {
            allGts = DEFAULT_LOADED_GENOTYPES;
        }
        return allGts;
    }

    /**
     * Split region into regions that match with batches at SampleIndexTable.
     *
     * @param region Region to split
     * @return List of regions.
     */
    protected static List<Region> splitRegion(Region region) {
        List<Region> regions;
        if (region.getEnd() - region.getStart() < SampleIndexSchema.BATCH_SIZE) {
            // Less than one batch. Do not split region
            regions = Collections.singletonList(region);
        } else if (region.getStart() / SampleIndexSchema.BATCH_SIZE + 1 == region.getEnd() / SampleIndexSchema.BATCH_SIZE
                && !startsAtBatch(region)
                && !endsAtBatch(region)) {
            // Consecutive partial batches. Do not split region
            regions = Collections.singletonList(region);
        } else {
            // Copy region before modifying
            region = new Region(region.getChromosome(), region.getStart(), region.getEnd());
            regions = new ArrayList<>(3);
            if (!startsAtBatch(region)) {
                int splitPoint = region.getStart() - region.getStart() % SampleIndexSchema.BATCH_SIZE + SampleIndexSchema.BATCH_SIZE;
                regions.add(new Region(region.getChromosome(), region.getStart(), splitPoint - 1));
                region.setStart(splitPoint);
            }
            regions.add(region);
            if (!endsAtBatch(region)) {
                int splitPoint = region.getEnd() - region.getEnd() % SampleIndexSchema.BATCH_SIZE;
                regions.add(new Region(region.getChromosome(), splitPoint, region.getEnd()));
                region.setEnd(splitPoint - 1);
            }
        }
        return regions;
    }

    protected static boolean matchesWithBatch(Region region) {
        return region == null || startsAtBatch(region) && endsAtBatch(region);
    }

    protected static boolean startsAtBatch(Region region) {
        return region.getStart() % SampleIndexSchema.BATCH_SIZE == 0;
    }

    protected static boolean endsAtBatch(Region region) {
        return region.getEnd() == Integer.MAX_VALUE || (region.getEnd() + 1) % SampleIndexSchema.BATCH_SIZE == 0;
    }

    public SampleIndexEntryFilter buildSampleIndexEntryFilter(SingleSampleIndexQuery query, LocusQuery locusQuery) {
        if (locusQuery == null
                || (locusQuery.getVariants().isEmpty() && locusQuery.getRegions().size() == 1
                && matchesWithBatch(locusQuery.getRegions().get(0)))) {
            return new SampleIndexEntryFilter(query, null);
        } else {
            return new SampleIndexEntryFilter(query, locusQuery);
        }
    }

    public SampleIndexQuery parseSampleIndexQuery(Query query) {
        return newParser().parse(query);
    }

    protected SampleIndexQueryParser newParser() {
        return new SampleIndexQueryParser(metadataManager, schemaFactory);
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
        logger.info("Sample = \"" + query.getSample() + "\" , schema version = " + query.getSchema().getVersion());
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

    public static void printQuery(SampleAnnotationIndexQuery annotationIndexQuery) {
        logger.info("AnnotationIndex = " + IndexUtils.maskToString(
                annotationIndexQuery.getAnnotationIndexMask(), annotationIndexQuery.getAnnotationIndex()));
        if (!annotationIndexQuery.getBiotypeFilter().isNoOp()) {
            logger.info("Biotype filter  = " + annotationIndexQuery.getBiotypeFilter().toString());
        }
        if (!annotationIndexQuery.getConsequenceTypeFilter().isNoOp()) {
            logger.info("CT filter       = " + annotationIndexQuery.getConsequenceTypeFilter().toString());
        }
        if (!annotationIndexQuery.getTranscriptFlagFilter().isNoOp()) {
            logger.info("Tf filter       = " + annotationIndexQuery.getTranscriptFlagFilter().toString());
        }
        if (!annotationIndexQuery.getCtBtTfFilter().isNoOp()) {
            logger.info("CtBtTf filter     = " + annotationIndexQuery.getCtBtTfFilter().toString());
        }
        if (!annotationIndexQuery.getClinicalFilter().isNoOp()) {
            logger.info("Clinical filter = " + annotationIndexQuery.getClinicalFilter());
        }
        if (!annotationIndexQuery.getPopulationFrequencyFilter().isNoOp()) {
            logger.info("PopFreq filter  = " + annotationIndexQuery.getPopulationFrequencyFilter());
        }
    }

    public static void printQuery(SampleIndexQuery query, Query variantsQuery) {
        printQuery(query);

        List<String> nonCoveredParams = VariantQueryUtils.validParams(variantsQuery, true)
                .stream().map(VariantQueryParam::key).collect(Collectors.toList());
        logger.info("Non covered params : " + nonCoveredParams);
    }

    public static void printQuery(SampleIndexQuery query) {
        printQuery(query.getAnnotationIndexQuery());
        logger.info("Study  : " + query.getStudy());
        if (CollectionUtils.isNotEmpty(query.getLocusQueries())) {
            List<Region> regions = query.getAllRegions();
            if (!regions.isEmpty()) {
                if (regions.size() > 10) {
                    logger.info("Regions  : #" + regions.size()
                            + " [ " + regions.get(0) + " , " + regions.get(1) + " .... , " + regions.get(regions.size() - 1) + " ] ");
                } else {
                    logger.info("Regions  : #" + regions.size() + " " + regions);
                }
            }
            List<Variant> variants = query.getAllVariants();
            if (!variants.isEmpty()) {
                if (variants.size() > 10) {
                    logger.info("Variants  : #" + variants.size()
                            + " [ " + variants.get(0) + " , " + variants.get(1) + " .... , " + variants.get(variants.size() - 1) + "] ");
                } else {
                    logger.info("Variants  : #" + variants.size() + " " + variants);
                }
            }
        }

        Iterator<String> iterator = query.getSamplesMap().keySet().iterator();
        while (iterator.hasNext()) {
            String sample = iterator.next();
            logger.info("  Sample : " + sample);
            printSingleSampleIndexQuery(query.forSample(sample), true);
            if (iterator.hasNext()) {
                logger.info("SampleIndex " + query.getQueryOperation().name());
            }
        }
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

    public static void printQuery(LocusQuery locusQuery) {
        if (locusQuery != null) {
            logger.info("ChunkRegion: [ " + locusQuery.getChunkRegion() + " )");
            if (!locusQuery.getRegions().isEmpty()) {
                logger.info("  - Regions: " + locusQuery.getRegions());
            }
            if (!locusQuery.getVariants().isEmpty()) {
                logger.info("  - Variants: " + locusQuery.getVariants());
            }
        }
    }

    public static void printQuery(SingleSampleIndexQuery query) {
        printQuery(query.getAnnotationIndexQuery());
        printSingleSampleIndexQuery(query, false);
    }

    private static void printSingleSampleIndexQuery(SingleSampleIndexQuery query, boolean tab) {
        Iterator<SampleFileIndexQuery> iterator = query.getSampleFileIndexQuery().iterator();
        while (iterator.hasNext()) {
            SampleFileIndexQuery sampleFileIndexQuery = iterator.next();
            for (IndexFieldFilter filter : sampleFileIndexQuery.getFilters()) {
                logger.info((tab ? "      " : "") + "Filter       = " + filter);
            }
            if (iterator.hasNext()) {
                logger.info((tab ? "    " : "") + "FileIndex " + query.getSampleFileIndexQuery().getOperation());
            }
        }
        if (query.hasFatherFilter()) {
            logger.info((tab ? "    " : "") + "FatherFilter       = " + IndexUtils.parentFilterToString(query.getFatherFilter()));
        }
        if (query.hasMotherFilter()) {
            logger.info((tab ? "    " : "") + "MotherFilter       = " + IndexUtils.parentFilterToString(query.getMotherFilter()));
        }
    }

    private int toSampleId(int studyId, String sample) {
        return metadataManager.getSampleId(studyId, sample);
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

        int splits = samples / preSplitSize;
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

    public void updateSampleIndexSchemaStatus(int studyId, int version) throws StorageEngineException {
        StudyMetadata studyMetadata = metadataManager.getStudyMetadata(studyId);
        if (studyMetadata.getSampleIndexConfiguration(version).getStatus()
                != StudyMetadata.SampleIndexConfigurationVersioned.Status.STAGING) {
            // Update only if status is STAGING
            return;
        }
        boolean allSamplesWithThisVersion = isAllSamplesWithThisVersion(studyId, version);
        if (allSamplesWithThisVersion) {
            metadataManager.updateStudyMetadata(studyId, sm -> {
                sm.getSampleIndexConfiguration(version)
                        .setStatus(StudyMetadata.SampleIndexConfigurationVersioned.Status.ACTIVE);
            });
        } else {
            logger.info("Not all samples had the sample index version {} on GENOTYPES and ANNOTATION", version);
        }
    }

    public void deprecateSampleIndexSchemas(int studyId) throws StorageEngineException {
        StudyMetadata studyMetadata = metadataManager.getStudyMetadata(studyId);
        Set<Integer> deprecatables = new LinkedHashSet<>();
        for (StudyMetadata.SampleIndexConfigurationVersioned sampleIndexConfiguration : studyMetadata.getSampleIndexConfigurations()) {
            if (sampleIndexConfiguration.getStatus() == StudyMetadata.SampleIndexConfigurationVersioned.Status.ACTIVE
                    || sampleIndexConfiguration.getStatus() == StudyMetadata.SampleIndexConfigurationVersioned.Status.STAGING) {
                deprecatables.add(sampleIndexConfiguration.getVersion());
            }
        }
        StudyMetadata.SampleIndexConfigurationVersioned latest = studyMetadata.getSampleIndexConfigurationLatest(false);
        if (latest == null) {
            logger.info("No active sample index available for study '{}'", studyMetadata.getName());
            return;
        }
        int latestVersion = latest.getVersion();
        deprecatables.remove(latestVersion);
        if (deprecatables.isEmpty()) {
            logger.info("No sample indexes to deprecate for study '{}'", studyMetadata.getName());
            return;
        }

        int totalSamples = 0;
        int samplesNotInLatest = 0;
        for (SampleMetadata sampleMetadata : metadataManager.sampleMetadataIterable(studyId)) {
            // Only check indexed samples
            if (sampleMetadata.getIndexStatus() == TaskMetadata.Status.READY) {
                totalSamples++;
                if (!sampleWithThisVersion(sampleMetadata, latestVersion)) {
                    samplesNotInLatest++;
                }
            }
        }
        if (samplesNotInLatest > 0) {
            logger.warn("Unable to deprecate sample indexes. {} samples out of {} not in the latest sample index for study '{}'",
                    samplesNotInLatest, totalSamples, studyMetadata.getName());
            return;
        }
        logger.info("Deprecate sample indexes {} for study '{}'", deprecatables, studyMetadata.getName());
        metadataManager.updateStudyMetadata(studyId, sm -> {
            for (Integer version : deprecatables) {
                sm.getSampleIndexConfiguration(version).setStatus(StudyMetadata.SampleIndexConfigurationVersioned.Status.DEPRECATED);
            }
        });
    }

    private boolean isAllSamplesWithThisVersion(int studyId, int version) {
        boolean allSamplesWithThisVersion = true;
        for (SampleMetadata sampleMetadata : metadataManager.sampleMetadataIterable(studyId)) {
            // Only check indexed samples
            if (sampleMetadata.getIndexStatus() == TaskMetadata.Status.READY) {
                allSamplesWithThisVersion = sampleWithThisVersion(sampleMetadata, version);
                if (!allSamplesWithThisVersion) {
                    break;
                }
            }
        }
        return allSamplesWithThisVersion;
    }

    private boolean sampleWithThisVersion(SampleMetadata sampleMetadata, int version) {
        // Check SampleIndex + SampleIndexAnnotation + FamilyIndex (if needed)
        if (sampleMetadata.getSampleIndexStatus(version) != TaskMetadata.Status.READY) {
            return false;
        }
        if (sampleMetadata.getSampleIndexAnnotationStatus(version) != TaskMetadata.Status.READY) {
            return false;
        }
        if (sampleMetadata.isFamilyIndexDefined()) {
            if (sampleMetadata.getFamilyIndexStatus(version) != TaskMetadata.Status.READY) {
                return false;
            }
        }
        return true;
    }


}
