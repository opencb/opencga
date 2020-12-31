package org.opencb.opencga.storage.hadoop.variant.index.sample;

import com.google.common.collect.Iterators;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CollectionUtils;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.utils.iterators.IntersectMultiKeyIterator;
import org.opencb.opencga.storage.core.utils.iterators.UnionMultiKeyIterator;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantIterable;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.utils.iterators.CloseableIterator;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils.QueryOperation;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.IntersectMultiVariantKeyIterator;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.UnionMultiVariantKeyIterator;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.index.IndexUtils;
import org.opencb.opencga.storage.hadoop.variant.index.query.SampleAnnotationIndexQuery.PopulationFrequencyQuery;
import org.opencb.opencga.storage.hadoop.variant.index.query.SampleIndexQuery;
import org.opencb.opencga.storage.hadoop.variant.index.query.SingleSampleIndexQuery;
import org.opencb.opencga.storage.hadoop.variant.utils.HBaseVariantTableNameGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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

    private static final String SAMPLE_INDEX_STATUS = "sampleIndexGenotypes";
    private static final String SAMPLE_INDEX_ANNOTATION_STATUS = "sampleIndexAnnotation";

    @Deprecated // Deprecated to avoid confusion with actual "SAMPLE_INDEX_STATUS"
    private static final String SAMPLE_INDEX_ANNOTATION_STATUS_OLD = "sampleIndex";

    private final HBaseManager hBaseManager;
    private final HBaseVariantTableNameGenerator tableNameGenerator;
    private final VariantStorageMetadataManager metadataManager;
    private final byte[] family;
    private static Logger logger = LoggerFactory.getLogger(SampleIndexDBAdaptor.class);
    private SampleIndexQueryParser parser;
    private final SampleIndexConfiguration configuration;
    private final HBaseToSampleIndexConverter converter;

    public SampleIndexDBAdaptor(HBaseManager hBaseManager, HBaseVariantTableNameGenerator tableNameGenerator,
                                VariantStorageMetadataManager metadataManager) {
        this.hBaseManager = hBaseManager;
        this.tableNameGenerator = tableNameGenerator;
        this.metadataManager = metadataManager;
        family = GenomeHelper.COLUMN_FAMILY_BYTES;
        // TODO: Read configuration from metadata manager
        configuration = SampleIndexConfiguration.defaultConfiguration();
        parser = new SampleIndexQueryParser(metadataManager, configuration);
        converter = new HBaseToSampleIndexConverter(configuration);
    }

    public static TaskMetadata.Status getSampleIndexAnnotationStatus(SampleMetadata sampleMetadata) {
        TaskMetadata.Status status = sampleMetadata.getStatus(SAMPLE_INDEX_ANNOTATION_STATUS, null);
        if (status == null) {
            // The status name was renamed. In case of missing value (null), check for the deprecated value.
            status = sampleMetadata.getStatus(SAMPLE_INDEX_ANNOTATION_STATUS_OLD);
        }
        return status;
    }

    public static SampleMetadata setSampleIndexAnnotationStatus(SampleMetadata sampleMetadata, TaskMetadata.Status status) {
        // Remove deprecated value.
        sampleMetadata.getStatus().remove(SAMPLE_INDEX_ANNOTATION_STATUS_OLD);
        sampleMetadata.setStatus(SAMPLE_INDEX_ANNOTATION_STATUS, status);
        return sampleMetadata;
    }

    public static TaskMetadata.Status getSampleIndexStatus(SampleMetadata sampleMetadata) {
        TaskMetadata.Status status = sampleMetadata.getStatus(SAMPLE_INDEX_STATUS, null);
        if (status == null) {
            // This is a new status. In case of missing value (null), assume it's READY
            status = TaskMetadata.Status.READY;
        }
        return status;
    }

    public static SampleMetadata setSampleIndexStatus(SampleMetadata sampleMetadata, TaskMetadata.Status status) {
        return sampleMetadata.setStatus(SAMPLE_INDEX_STATUS, status);
    }

    @Override
    public VariantDBIterator iterator(Query query, QueryOptions options) {
        return iterator(parser.parse(query));
    }

    public VariantDBIterator iterator(SampleIndexQuery query) {
        return iterator(query, QueryOptions.empty());
    }

    public VariantDBIterator iterator(SampleIndexQuery query, QueryOptions options) {
        Map<String, List<String>> samples = query.getSamplesMap();

        if (samples.isEmpty()) {
            throw new VariantQueryException("At least one sample expected to query SampleIndex!");
        }
        QueryOperation operation = query.getQueryOperation();

        if (samples.size() == 1) {
            String sample = samples.entrySet().iterator().next().getKey();
            List<String> gts = query.getSamplesMap().get(sample);

            if (gts.isEmpty()) {
                // If empty, should find none. Return empty iterator
                return VariantDBIterator.emptyIterator();
            } else {
                logger.info("Single sample indexes iterator");
                SingleSampleIndexVariantDBIterator iterator = internalIterator(query.forSample(sample, gts));
                return applyLimitSkip(iterator, options);
            }
        }

        List<VariantDBIterator> iterators = new ArrayList<>(samples.size());
        List<VariantDBIterator> negatedIterators = new ArrayList<>(samples.size());

        for (Map.Entry<String, List<String>> entry : samples.entrySet()) {
            String sample = entry.getKey();
            List<String> gts = entry.getValue();

            if (query.isNegated(sample)) {
                if (!gts.isEmpty()) {
                    negatedIterators.add(internalIterator(query.forSample(sample, gts)));
                }
                // Skip if GTs to query is empty!
                // Otherwise, it will return ALL genotypes instead of none
            } else {
                if (gts.isEmpty()) {
                    // If empty, should find none. Add empty iterator for this sample
                    iterators.add(VariantDBIterator.emptyIterator());
                } else {
                    iterators.add(internalIterator(query.forSample(sample, gts)));
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

        return applyLimitSkip(iterator, options);
    }

    protected VariantDBIterator applyLimitSkip(VariantDBIterator iterator, QueryOptions options) {
        int limit = options.getInt(QueryOptions.LIMIT, -1);
        int skip = options.getInt(QueryOptions.SKIP, -1);
        // Client site limit-skip
        if (skip > 0) {
            Iterators.advance(iterator, skip);
        }
        if (limit >= 0) {
            Iterator<Variant> it = Iterators.limit(iterator, limit);
            return VariantDBIterator.wrapper(it).addCloseable(iterator);
        } else {
            return iterator;
        }
    }

    /**
     * Partially processed iterator. Internal usage only.
     *
     * @param query SingleSampleIndexQuery
     * @return SingleSampleIndexVariantDBIterator
     */
    private SingleSampleIndexVariantDBIterator internalIterator(SingleSampleIndexQuery query) {
        String tableName = tableNameGenerator.getSampleIndexTableName(toStudyId(query.getStudy()));

        try {
            return hBaseManager.act(tableName, table -> {
                return new SingleSampleIndexVariantDBIterator(table, query, this);
            });
        } catch (IOException e) {
            throw VariantQueryException.internalException(e);
        }
    }

    private RawSingleSampleIndexVariantDBIterator rawInternalIterator(SingleSampleIndexQuery query) {
        String tableName = tableNameGenerator.getSampleIndexTableName(toStudyId(query.getStudy()));

        try {
            return hBaseManager.act(tableName, table -> {
                return new RawSingleSampleIndexVariantDBIterator(table, query, this);
            });
        } catch (IOException e) {
            throw VariantQueryException.internalException(e);
        }
    }

    protected Map<String, List<Variant>> queryByGt(int study, int sample, String chromosome, int position)
            throws IOException {
        Result result = queryByGtInternal(study, sample, chromosome, position);
        return converter.convertToMap(result);
    }

    protected SampleIndexEntryPutBuilder queryByGtBuilder(int study, int sample, String chromosome, int position)
            throws IOException {
        Result result = queryByGtInternal(study, sample, chromosome, position);
        return new SampleIndexEntryPutBuilder(sample, chromosome, position, converter.convertToMapSampleVariantIndex(result));
    }

    private Result queryByGtInternal(int study, int sample, String chromosome, int position) throws IOException {
        String tableName = tableNameGenerator.getSampleIndexTableName(study);
        return hBaseManager.act(tableName, table -> {
            Get get = new Get(SampleIndexSchema.toRowKey(sample, chromosome, position));
            get.addFamily(family);
            return table.get(get);
        });
    }

    public Iterator<Map<String, List<Variant>>> iteratorByGt(int study, int sample) throws IOException {
        String tableName = tableNameGenerator.getSampleIndexTableName(study);

        return hBaseManager.act(tableName, table -> {

            Scan scan = new Scan();
            scan.setRowPrefixFilter(SampleIndexSchema.toRowKey(sample));
            HBaseToSampleIndexConverter converter = new HBaseToSampleIndexConverter(configuration);
            try {
                ResultScanner scanner = table.getScanner(scan);
                Iterator<Result> resultIterator = scanner.iterator();
                return Iterators.transform(resultIterator, converter::convertToMap);
            } catch (IOException e) {
                throw VariantQueryException.internalException(e);
            }
        });
    }

    public Iterator<SampleIndexEntry> rawIterator(int study, int sample) throws IOException {
        String tableName = tableNameGenerator.getSampleIndexTableName(study);

        return hBaseManager.act(tableName, table -> {
            Scan scan = new Scan();
            scan.setRowPrefixFilter(SampleIndexSchema.toRowKey(sample));
            HBaseToSampleIndexConverter converter = new HBaseToSampleIndexConverter(configuration);
            ResultScanner scanner = table.getScanner(scan);
            Iterator<Result> resultIterator = scanner.iterator();
            return Iterators.transform(resultIterator, converter::convert);
        });
    }

    public CloseableIterator<SampleVariantIndexEntry> rawIterator(Query query) throws IOException {
        return rawIterator(parser.parse(query));
    }

    public CloseableIterator<SampleVariantIndexEntry> rawIterator(SampleIndexQuery query) throws IOException {
        Map<String, List<String>> samples = query.getSamplesMap();

        if (samples.isEmpty()) {
            throw new VariantQueryException("At least one sample expected to query SampleIndex!");
        }
        QueryOperation operation = query.getQueryOperation();

        if (samples.size() == 1) {
            String sample = samples.entrySet().iterator().next().getKey();
            List<String> gts = query.getSamplesMap().get(sample);

            if (gts.isEmpty()) {
                // If empty, should find none. Return empty iterator
                return CloseableIterator.emptyIterator();
            } else {
                logger.info("Single sample indexes iterator");
                return rawInternalIterator(query.forSample(sample, gts));
            }
        }

        List<RawSingleSampleIndexVariantDBIterator> iterators = new ArrayList<>(samples.size());
        List<RawSingleSampleIndexVariantDBIterator> negatedIterators = new ArrayList<>(samples.size());

        for (Map.Entry<String, List<String>> entry : samples.entrySet()) {
            String sample = entry.getKey();
            List<String> gts = entry.getValue();

            if (query.isNegated(sample)) {
                if (!gts.isEmpty()) {
                    negatedIterators.add(rawInternalIterator(query.forSample(sample, gts)));
                }
                // Skip if GTs to query is empty!
                // Otherwise, it will return ALL genotypes instead of none
            } else {
                if (gts.isEmpty()) {
                    // If empty, should find none. Add empty iterator for this sample
                    iterators.add(RawSingleSampleIndexVariantDBIterator.emptyIterator());
                } else {
                    iterators.add(rawInternalIterator(query.forSample(sample, gts)));
                }
            }
        }

        CloseableIterator<SampleVariantIndexEntry> iterator;
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

        return iterator;
    }

    public boolean isFastCount(SampleIndexQuery query) {
        return query.getSamplesMap().size() == 1 && query.emptyAnnotationIndex() && query.emptyFileIndex();
    }

    public long count(List<Region> regions, String study, String sample, List<String> gts) {
        return count(parser.parse(regions, study, sample, gts));
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
        List<Region> regionsList;
        if (CollectionUtils.isEmpty(query.getRegions())) {
            // If no regions are defined, get a list of one null element to initialize the stream.
            regionsList = Collections.singletonList(null);
        } else {
            regionsList = VariantQueryUtils.mergeRegions(query.getRegions());
        }

        String tableName = tableNameGenerator.getSampleIndexTableName(toStudyId(query.getStudy()));

        try {
            return hBaseManager.act(tableName, table -> {
                long count = 0;
                for (Region region : regionsList) {
                    // Split region in countable regions
                    List<Region> subRegions = region == null ? Collections.singletonList((Region) null) : splitRegion(region);
                    for (Region subRegion : subRegions) {
                        HBaseToSampleIndexConverter converter = new HBaseToSampleIndexConverter(configuration);
                        boolean noRegionFilter = matchesWithBatch(subRegion);
                        // Don't need to parse the variant to filter
                        boolean simpleCount = !query.isMultiFileSample()
                                && CollectionUtils.isEmpty(query.getVariantTypes())
                                && noRegionFilter;
                        try {
                            if (query.emptyOrRegionFilter() && simpleCount) {
                                // Directly sum counters
                                Scan scan = parseCount(query, subRegion);
                                ResultScanner scanner = table.getScanner(scan);
                                Result result = scanner.next();
                                while (result != null) {
                                    count += converter.convertToCount(result);
                                    result = scanner.next();
                                }
                            } else {
                                SampleIndexEntryFilter filter = buildSampleIndexEntryFilter(query, subRegion);
                                Scan scan;
                                if (simpleCount) {
                                    // Fast filter and count. Don't need to parse the variant to filter
                                    scan = parseCountAndFilter(query, subRegion);
                                } else {
                                    // Need to parse the variant to finish filtering. Create a normal scan query.
                                    scan = parse(query, subRegion);
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

    public SampleIndexQueryParser getSampleIndexQueryParser() {
        return parser;
    }

    public SampleIndexConfiguration getConfiguration() {
        return configuration;
    }

    protected int toStudyId(String study) {
        int studyId;
        if (StringUtils.isEmpty(study)) {
            Map<String, Integer> studies = metadataManager.getStudies(null);
            if (studies.size() == 1) {
                studyId = studies.values().iterator().next();
            } else {
                throw VariantQueryException.studyNotFound(study, studies.keySet());
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

    public SampleIndexEntryFilter buildSampleIndexEntryFilter(SingleSampleIndexQuery query, Region region) {
        if (matchesWithBatch(region)) {
            return new SampleIndexEntryFilter(query, null);
        } else {
            return new SampleIndexEntryFilter(query, region);
        }
    }

    public Scan parse(SingleSampleIndexQuery query, Region region) {
        return parse(query, region, false, false);
    }

    public Scan parseIncludeAll(SingleSampleIndexQuery query, Region region) {
        return parse(query, region, false, false, true);
    }

    public Scan parseCount(SingleSampleIndexQuery query, Region region) {
        return parse(query, region, true, true);
    }

    public Scan parseCountAndFilter(SingleSampleIndexQuery query, Region region) {
        return parse(query, region, false, true);
    }

    private Scan parse(SingleSampleIndexQuery query, Region region, boolean onlyCount, boolean skipGtColumn) {
        return parse(query, region, onlyCount, skipGtColumn, false);
    }

    private Scan parse(SingleSampleIndexQuery query, Region region, boolean onlyCount, boolean skipGtColumn, boolean includeAll) {

        Scan scan = new Scan();
        int studyId = toStudyId(query.getStudy());
        int sampleId = toSampleId(studyId, query.getSample());
        if (region != null) {
            scan.setStartRow(SampleIndexSchema.toRowKey(sampleId, region.getChromosome(), region.getStart()));
            scan.setStopRow(SampleIndexSchema.toRowKey(sampleId, region.getChromosome(),
                    region.getEnd() + (region.getEnd() == Integer.MAX_VALUE ? 0 : SampleIndexSchema.BATCH_SIZE)));
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
                if (includeAll || query.getAnnotationIndexQuery().getBiotypeMask() != EMPTY_MASK) {
                    scan.addColumn(family, SampleIndexSchema.toAnnotationBiotypeIndexColumn(gt));
                }
                if (includeAll || query.getAnnotationIndexQuery().getConsequenceTypeMask() != EMPTY_MASK) {
                    scan.addColumn(family, SampleIndexSchema.toAnnotationConsequenceTypeIndexColumn(gt));
                }
                if (/*includeAll ||*/ query.getAnnotationIndexQuery().getBiotypeMask() != EMPTY_MASK
                        && query.getAnnotationIndexQuery().getConsequenceTypeMask() != EMPTY_MASK) {
                    scan.addColumn(family, SampleIndexSchema.toAnnotationCtBtIndexColumn(gt));
                }
                if (!/*includeAll ||*/ query.getAnnotationIndexQuery().getPopulationFrequencyQueries().isEmpty()) {
                    scan.addColumn(family, SampleIndexSchema.toAnnotationPopFreqIndexColumn(gt));
                }
                if (includeAll || query.getAnnotationIndexQuery().getClinicalMask() != EMPTY_MASK) {
                    scan.addColumn(family, SampleIndexSchema.toAnnotationClinicalIndexColumn(gt));
                }
                if (includeAll || query.getFileIndexMask() != EMPTY_MASK) {
                    scan.addColumn(family, SampleIndexSchema.toFileIndexColumn(gt));
                }
                if (/*includeAll ||*/ query.hasFatherFilter() || query.hasMotherFilter()) {
                    scan.addColumn(family, SampleIndexSchema.toParentsGTColumn(gt));
                }
            }
        }
        if (query.getMendelianError()) {
            scan.addColumn(family, SampleIndexSchema.toMendelianErrorColumn());
        }
        scan.setCaching(hBaseManager.getConf().getInt("hbase.client.scanner.caching", 100));

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
        logger.info("AnnotationIndex = " + IndexUtils.maskToString(query.getAnnotationIndexMask(), query.getAnnotationIndex()));
        if (query.getAnnotationIndexQuery().getBiotypeMask() != EMPTY_MASK) {
            logger.info("BiotypeIndex    = " + IndexUtils.byteToString(query.getAnnotationIndexQuery().getBiotypeMask()));
        }
        if (query.getAnnotationIndexQuery().getConsequenceTypeMask() != EMPTY_MASK) {
            logger.info("CTIndex         = " + IndexUtils.shortToString(query.getAnnotationIndexQuery().getConsequenceTypeMask()));
        }
        if (query.getAnnotationIndexQuery().getClinicalMask() != EMPTY_MASK) {
            logger.info("ClinicalIndex   = " + IndexUtils.byteToString(query.getAnnotationIndexQuery().getClinicalMask()));
        }
        for (PopulationFrequencyQuery pf : query.getAnnotationIndexQuery().getPopulationFrequencyQueries()) {
            logger.info("PopFreq         = " + pf);
        }
        if (query.getSampleFileIndexQuery().hasFileIndexMask1()) {
            boolean[] validFileIndex = query.getSampleFileIndexQuery().getValidFileIndex1();
            for (int i = 0; i < validFileIndex.length; i++) {
                if (validFileIndex[i]) {
                    logger.info("FileIndex1       = " + IndexUtils.maskToString(query.getFileIndexMask1(), (byte) i));
                }
            }
        }
        if (query.getSampleFileIndexQuery().hasFileIndexMask2()) {
            boolean[] validFileIndex2 = query.getSampleFileIndexQuery().getValidFileIndex2();
            for (int i = 0; i < validFileIndex2.length; i++) {
                if (validFileIndex2[i]) {
                    logger.info("FileIndex2       = " + IndexUtils.maskToString(query.getFileIndexMask2(), (byte) i));
                }
            }
        }
        if (query.hasFatherFilter()) {
            logger.info("FatherFilter       = " + IndexUtils.parentFilterToString(query.getFatherFilter()));
        }
        if (query.hasMotherFilter()) {
            logger.info("MotherFilter       = " + IndexUtils.parentFilterToString(query.getMotherFilter()));
        }

//        try {
//            System.out.println("scan = " + scan.toJSON() + " " + rowKeyToString(scan.getStartRow()) + " -> + "
// + rowKeyToString(scan.getStopRow()));
//        } catch (IOException e) {
//            throw VariantQueryException.internalException(e);
//        }

        return scan;
    }

    private int toSampleId(int studyId, String sample) {
        return metadataManager.getSampleId(studyId, sample);
    }


}
