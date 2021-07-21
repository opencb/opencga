package org.opencb.opencga.storage.hadoop.variant.index.sample;

import com.google.common.collect.Iterators;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.config.storage.SampleIndexConfiguration;
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
import org.opencb.opencga.storage.core.variant.query.VariantQueryParser;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils.QueryOperation;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.index.IndexUtils;
import org.opencb.opencga.storage.hadoop.variant.index.core.filters.IndexFieldFilter;
import org.opencb.opencga.storage.hadoop.variant.index.query.SampleAnnotationIndexQuery;
import org.opencb.opencga.storage.hadoop.variant.index.query.SampleFileIndexQuery;
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
    private static final String SAMPLE_INDEX_VERSION = "sampleIndexGenotypesVersion";
    private static final String SAMPLE_INDEX_ANNOTATION_STATUS = "sampleIndexAnnotation";
    private static final String SAMPLE_INDEX_ANNOTATION_VERSION = "sampleIndexAnnotationVersion";

    @Deprecated // Deprecated to avoid confusion with actual "SAMPLE_INDEX_STATUS"
    private static final String SAMPLE_INDEX_ANNOTATION_STATUS_OLD = "sampleIndex";

    private final HBaseManager hBaseManager;
    private final HBaseVariantTableNameGenerator tableNameGenerator;
    private final VariantStorageMetadataManager metadataManager;
    private final byte[] family;
    private static Logger logger = LoggerFactory.getLogger(SampleIndexDBAdaptor.class);
//    private SampleIndexQueryParser parser;
//    private final SampleIndexSchema schema;
    private final Map<Integer, SampleIndexSchema> schemas;
//    private final HBaseToSampleIndexConverter converter;

    public SampleIndexDBAdaptor(HBaseManager hBaseManager, HBaseVariantTableNameGenerator tableNameGenerator,
                                VariantStorageMetadataManager metadataManager) {
        this.hBaseManager = hBaseManager;
        this.tableNameGenerator = tableNameGenerator;
        this.metadataManager = metadataManager;
        family = GenomeHelper.COLUMN_FAMILY_BYTES;
        schemas = new HashMap<>();
    }

    public static TaskMetadata.Status getSampleIndexAnnotationStatus(SampleMetadata sampleMetadata, int latestSampleIndexVersion) {
        TaskMetadata.Status status = sampleMetadata.getStatus(SAMPLE_INDEX_ANNOTATION_STATUS, null);
        if (status == null) {
            // The status name was renamed. In case of missing value (null), check for the deprecated value.
            status = sampleMetadata.getStatus(SAMPLE_INDEX_ANNOTATION_STATUS_OLD);
        }
        if (status == TaskMetadata.Status.READY) {
            int actualSampleIndexVersion = sampleMetadata.getAttributes().getInt(SAMPLE_INDEX_ANNOTATION_VERSION, 1);
            if (actualSampleIndexVersion != latestSampleIndexVersion) {
                logger.debug("Sample index annotation version outdated. Actual : " + actualSampleIndexVersion
                        + " , expected : " + latestSampleIndexVersion);
                status = TaskMetadata.Status.NONE;
            }
        }
        return status;
    }

    public static SampleMetadata setSampleIndexAnnotationStatus(SampleMetadata sampleMetadata, TaskMetadata.Status status, int version) {
        // Remove deprecated value.
        sampleMetadata.getStatus().remove(SAMPLE_INDEX_ANNOTATION_STATUS_OLD);
        sampleMetadata.setStatus(SAMPLE_INDEX_ANNOTATION_STATUS, status);
        sampleMetadata.getAttributes().put(SAMPLE_INDEX_ANNOTATION_VERSION, version);
        return sampleMetadata;
    }

    public static TaskMetadata.Status getSampleIndexStatus(SampleMetadata sampleMetadata, int latestSampleIndexVersion) {
        TaskMetadata.Status status = sampleMetadata.getStatus(SAMPLE_INDEX_STATUS, null);
        if (status == null) {
            // This is a new status. In case of missing value (null), assume it's READY
            status = TaskMetadata.Status.READY;
        }
        if (status == TaskMetadata.Status.READY) {
            int actualSampleIndexVersion = sampleMetadata.getAttributes().getInt(SAMPLE_INDEX_VERSION, 1);
            if (actualSampleIndexVersion != latestSampleIndexVersion) {
                logger.debug("Sample index version outdated. Actual : " + actualSampleIndexVersion
                        + " , expected : " + latestSampleIndexVersion);
                status = TaskMetadata.Status.NONE;
            }
        }
        return status;
    }

    public static SampleMetadata setSampleIndexStatus(SampleMetadata sampleMetadata, TaskMetadata.Status status, int version) {
        sampleMetadata.getAttributes().put(SAMPLE_INDEX_VERSION, version);
        return sampleMetadata.setStatus(SAMPLE_INDEX_STATUS, status);
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
        QueryOperation operation = query.getQueryOperation();

        if (samples.size() == 1) {
            String sample = samples.entrySet().iterator().next().getKey();
            List<String> gts = query.getSamplesMap().get(sample);

            if (gts.isEmpty()) {
                // If empty, should find none. Return empty iterator
                return VariantDBIterator.emptyIterator();
            } else {
                logger.info("Single sample indexes iterator : " + sample);
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
        String tableName = getSampleIndexTableName(toStudyId(query.getStudy()));

        try {
            return hBaseManager.act(tableName, table -> {
                return new SingleSampleIndexVariantDBIterator(table, query, this);
            });
        } catch (IOException e) {
            throw VariantQueryException.internalException(e);
        }
    }

    private RawSingleSampleIndexVariantDBIterator rawInternalIterator(SingleSampleIndexQuery query) {
        String tableName = getSampleIndexTableName(toStudyId(query.getStudy()));

        try {
            return hBaseManager.act(tableName, table -> {
                return new RawSingleSampleIndexVariantDBIterator(table, query, this);
            });
        } catch (IOException e) {
            throw VariantQueryException.internalException(e);
        }
    }

    public String getSampleIndexTableName(int studyId) {
        int version = getSampleIndexConfiguration(studyId).getVersion();
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
        Result result = queryByGtInternal(study, sample, chromosome, position);
        return newConverter(study).convertToMap(result);
    }

    protected SampleIndexEntryPutBuilder queryByGtBuilder(int study, int sample, String chromosome, int position)
            throws IOException {
        Result result = queryByGtInternal(study, sample, chromosome, position);
        return new SampleIndexEntryPutBuilder(sample, chromosome, position, getSchema(study),
                newConverter(study).convertToMapSampleVariantIndex(result));
    }

    private Result queryByGtInternal(int study, int sample, String chromosome, int position) throws IOException {
        String tableName = getSampleIndexTableName(study);
        return hBaseManager.act(tableName, table -> {
            Get get = new Get(SampleIndexSchema.toRowKey(sample, chromosome, position));
            get.addFamily(family);
            return table.get(get);
        });
    }

    public Iterator<Map<String, List<Variant>>> iteratorByGt(int study, int sample) throws IOException {
        String tableName = getSampleIndexTableName(study);

        return hBaseManager.act(tableName, table -> {

            Scan scan = new Scan();
            scan.setRowPrefixFilter(SampleIndexSchema.toRowKey(sample));
            HBaseToSampleIndexConverter converter = newConverter(study);
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
        String tableName = getSampleIndexTableName(study);

        return hBaseManager.act(tableName, table -> {
            Scan scan = new Scan();
            if (region != null) {
                scan.setStartRow(SampleIndexSchema.toRowKey(sample, region.getChromosome(), region.getStart()));
                scan.setStopRow(SampleIndexSchema.toRowKey(sample, region.getChromosome(),
                        region.getEnd() + (region.getEnd() == Integer.MAX_VALUE ? 0 : SampleIndexSchema.BATCH_SIZE)));
            } else {
                scan.setRowPrefixFilter(SampleIndexSchema.toRowKey(sample));
            }
            HBaseToSampleIndexConverter converter = newConverter(study);
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
        return rawInternalIterator(parseSampleIndexQuery(query).forSample(sample));
    }

    public CloseableIterator<SampleVariantIndexEntry> rawIterator(Query query) throws IOException {
        return rawIterator(parseSampleIndexQuery(query));
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
        return count(newParser(toStudyId(study)).parse(regions, study, sample, gts));
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
        Collection<List<Region>> regionGroups;
        if (CollectionUtils.isEmpty(query.getRegionGroups())) {
            // If no regions are defined, get a list of one null element to initialize the stream.
            regionGroups = Collections.singletonList(Collections.emptyList());
        } else {
            regionGroups = query.getRegionGroups();
        }

        String tableName = getSampleIndexTableName(toStudyId(query.getStudy()));

        HBaseToSampleIndexConverter converter = newConverter(toStudyId(query.getStudy()));
        try {
            return hBaseManager.act(tableName, table -> {
                long count = 0;
                for (List<Region> regions : regionGroups) {
                    // Split region in countable regions
                    List<List<Region>> subRegionsGroups;
                    if (regions.size() == 1) {
                        subRegionsGroups = Collections.singletonList(splitRegion(regions.get(0)));
                    } else {
                        // Do not split
                        subRegionsGroups = Collections.singletonList(regions);
                    }
                    for (List<Region> subRegions : subRegionsGroups) {
                        boolean noRegionFilter = subRegions.size() == 1 && matchesWithBatch(subRegions.get(0));
                        // Don't need to parse the variant to filter
                        boolean simpleCount = !query.isMultiFileSample()
                                && CollectionUtils.isEmpty(query.getVariantTypes())
                                && noRegionFilter;
                        try {
                            if (query.emptyOrRegionFilter() && simpleCount) {
                                // Directly sum counters
                                Scan scan = parseCount(query, subRegions);
                                ResultScanner scanner = table.getScanner(scan);
                                Result result = scanner.next();
                                while (result != null) {
                                    count += converter.convertToCount(result);
                                    result = scanner.next();
                                }
                            } else {
                                SampleIndexEntryFilter filter = buildSampleIndexEntryFilter(query, subRegions);
                                Scan scan;
                                if (simpleCount) {
                                    // Fast filter and count. Don't need to parse the variant to filter
                                    scan = parseCountAndFilter(query, subRegions);
                                } else {
                                    // Need to parse the variant to finish filtering. Create a normal scan query.
                                    scan = parse(query, subRegions);
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

    protected HBaseToSampleIndexConverter newConverter(int studyId) {
        return new HBaseToSampleIndexConverter(getSchema(studyId));
    }

    public SampleIndexSchema getSchema(String study) {
        int studyId = toStudyId(study);
        return getSchema(studyId);
    }

    public SampleIndexSchema getSchema(int studyId) {
        SampleIndexSchema sampleIndexSchema = schemas.get(studyId);
        if (sampleIndexSchema == null) {
            SampleIndexConfiguration configuration = getSampleIndexConfiguration(studyId)
                    .getConfiguration();
            sampleIndexSchema = new SampleIndexSchema(configuration);
            schemas.put(studyId, sampleIndexSchema);
        }
        return sampleIndexSchema;
    }

    public StudyMetadata.SampleIndexConfigurationVersioned getSampleIndexConfiguration(int studyId) {
        return metadataManager.getStudyMetadata(studyId).getSampleIndexConfigurationLatest();
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

    public SampleIndexEntryFilter buildSampleIndexEntryFilter(SingleSampleIndexQuery query, List<Region> regions) {
        if (regions == null || regions.size() == 1 && matchesWithBatch(regions.get(0))) {
            return new SampleIndexEntryFilter(query, null);
        } else {
            return new SampleIndexEntryFilter(query, regions);
        }
    }

    public SampleIndexQuery parseSampleIndexQuery(Query query) {
        StudyMetadata defaultStudy = VariantQueryParser.getDefaultStudy(query, metadataManager);
        int studyId;
        if (defaultStudy == null) {
            studyId = toStudyId(null);
        } else {
            studyId = defaultStudy.getId();
        }
        return newParser(studyId).parse(query);
    }

    protected SampleIndexQueryParser newParser(int studyId) {
        return new SampleIndexQueryParser(metadataManager, getSchema(studyId));
    }

    public Scan parse(SingleSampleIndexQuery query, List<Region> regions) {
        return parse(query, regions, false, false);
    }

    public Scan parseIncludeAll(SingleSampleIndexQuery query, List<Region> region) {
        return parse(query, region, false, false, true);
    }

    public Scan parseCount(SingleSampleIndexQuery query, List<Region> regions) {
        return parse(query, regions, true, true);
    }

    public Scan parseCountAndFilter(SingleSampleIndexQuery query, List<Region> regions) {
        return parse(query, regions, false, true);
    }

    private Scan parse(SingleSampleIndexQuery query, List<Region> regions, boolean onlyCount, boolean skipGtColumn) {
        return parse(query, regions, onlyCount, skipGtColumn, false);
    }

    private Scan parse(SingleSampleIndexQuery query, List<Region> regions, boolean onlyCount, boolean skipGtColumn, boolean includeAll) {

        Scan scan = new Scan();
        int studyId = toStudyId(query.getStudy());
        int sampleId = toSampleId(studyId, query.getSample());
        if (!CollectionUtils.isEmpty(regions)) {
            // Regions from the same group are sorted and do not overlap.
            Region first = regions.get(0);
            Region last = regions.get(regions.size() - 1);
            scan.setStartRow(SampleIndexSchema.toRowKey(sampleId, first.getChromosome(), first.getStart()));
            scan.setStopRow(SampleIndexSchema.toRowKey(sampleId, last.getChromosome(),
                    last.getEnd() + (last.getEnd() == Integer.MAX_VALUE ? 0 : SampleIndexSchema.BATCH_SIZE)));
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
                if (includeAll || !query.getAnnotationIndexQuery().getBiotypeFilter().isNoOp()) {
                    scan.addColumn(family, SampleIndexSchema.toAnnotationBiotypeIndexColumn(gt));
                }
                if (includeAll || !query.getAnnotationIndexQuery().getConsequenceTypeFilter().isNoOp()) {
                    scan.addColumn(family, SampleIndexSchema.toAnnotationConsequenceTypeIndexColumn(gt));
                }
                if (includeAll || !query.getAnnotationIndexQuery().getTranscriptFlagFilter().isNoOp()) {
                    scan.addColumn(family, SampleIndexSchema.toAnnotationTranscriptFlagIndexColumn(gt));
                }
                if (includeAll || !query.getAnnotationIndexQuery().getCtBtFilter().isNoOp()) {
                    scan.addColumn(family, SampleIndexSchema.toAnnotationCtBtIndexColumn(gt));
                }
                if (includeAll || !query.getAnnotationIndexQuery().getCtTfFilter().isNoOp()) {
                    scan.addColumn(family, SampleIndexSchema.toAnnotationCtTfIndexColumn(gt));
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
                if (includeAll || query.hasFatherFilter() || query.hasMotherFilter()) {
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
        if (!CollectionUtils.isEmpty(regions)) {
            logger.info("Regions: " + regions);
        }
        logger.info("columns = " + scan.getFamilyMap().getOrDefault(family, Collections.emptyNavigableSet())
                .stream().map(Bytes::toString).collect(Collectors.joining(",")));
//        logger.info("MaxResultSize = " + scan.getMaxResultSize());
//        logger.info("Filters = " + scan.getFilter());
//        logger.info("Batch = " + scan.getBatch());
        logger.info("Caching = " + scan.getCaching());
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
        if (!annotationIndexQuery.getCtBtFilter().isNoOp()) {
            logger.info("CtBt filter     = " + annotationIndexQuery.getCtBtFilter().toString());
        }
        if (!annotationIndexQuery.getCtTfFilter().isNoOp()) {
            logger.info("CtTf filter     = " + annotationIndexQuery.getCtTfFilter().toString());
        }
        if (!annotationIndexQuery.getClinicalFilter().isNoOp()) {
            logger.info("Clinical filter = " + annotationIndexQuery.getClinicalFilter());
        }
        if (!annotationIndexQuery.getPopulationFrequencyFilter().isNoOp()) {
            logger.info("PopFreq filter  = " + annotationIndexQuery.getPopulationFrequencyFilter());
        }
    }

    public static void printQuery(SingleSampleIndexQuery query) {
        printQuery(query.getAnnotationIndexQuery());
        for (SampleFileIndexQuery sampleFileIndexQuery : query.getSampleFileIndexQuery()) {
            for (IndexFieldFilter filter : sampleFileIndexQuery.getFilters()) {
                logger.info("Filter       = " + filter);
            }
            if (query.getSampleFileIndexQuery().getOperation() != null) {
                logger.info("FileIndex " + query.getSampleFileIndexQuery().getOperation());
            }
        }
        if (query.hasFatherFilter()) {
            logger.info("FatherFilter       = " + IndexUtils.parentFilterToString(query.getFatherFilter()));
        }
        if (query.hasMotherFilter()) {
            logger.info("MotherFilter       = " + IndexUtils.parentFilterToString(query.getMotherFilter()));
        }
    }

    private int toSampleId(int studyId, String sample) {
        return metadataManager.getSampleId(studyId, sample);
    }

}
