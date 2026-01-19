package org.opencb.opencga.storage.core.variant.index.sample;

import com.google.common.collect.Iterators;
import org.apache.commons.collections4.CollectionUtils;
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
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantIterable;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQuery;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.IntersectMultiVariantKeyIterator;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.UnionMultiVariantKeyIterator;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.index.core.IndexUtils;
import org.opencb.opencga.storage.core.variant.index.core.filters.IndexFieldFilter;
import org.opencb.opencga.storage.core.variant.index.sample.annotation.SampleAnnotationIndexer;
import org.opencb.opencga.storage.core.variant.index.sample.family.SampleFamilyIndexer;
import org.opencb.opencga.storage.core.variant.index.sample.genotype.*;
import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexEntry;
import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexVariant;
import org.opencb.opencga.storage.core.variant.index.sample.query.*;
import org.opencb.opencga.storage.core.variant.index.sample.schema.SampleIndexSchema;
import org.opencb.opencga.storage.core.variant.index.sample.schema.SampleIndexSchemaFactory;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.query.VariantQueryUtils.DEFAULT_LOADED_GENOTYPES;

public abstract class SampleIndexDBAdaptor implements VariantIterable {

    private static Logger logger = LoggerFactory.getLogger(SampleIndexDBAdaptor.class);
    protected final VariantStorageMetadataManager metadataManager;
    protected final SampleIndexSchemaFactory schemaFactory;

    public SampleIndexDBAdaptor(VariantStorageMetadataManager metadataManager) {
        this.metadataManager = metadataManager;
        this.schemaFactory = new SampleIndexSchemaFactory(metadataManager);
    }

    public SampleIndexQuery parseSampleIndexQuery(Query query) {
        return newParser().parse(query);
    }

    protected SampleIndexQueryParser newParser() {
        return new SampleIndexQueryParser(metadataManager, schemaFactory);
    }

    public abstract SampleGenotypeIndexer newSampleGenotypeIndexer(VariantStorageEngine engine)
            throws StorageEngineException;


    public SampleIndexVariantWriter newSampleIndexVariantWriter(int studyId, int fileId, List<Integer> sampleIds,
                                                                SampleIndexSchema schema, ObjectMap options,
                                                                VariantStorageEngine.SplitData splitData) throws StorageEngineException {
        SampleIndexEntryConverter converter = new SampleIndexEntryConverter(this, studyId, fileId, sampleIds, splitData, options, schema);
        SampleIndexEntryWriter writer = newSampleIndexEntryWriter(studyId, fileId, schema, options);
        return new SampleIndexVariantWriter(converter, writer);
    }

    public abstract SampleIndexEntryWriter newSampleIndexEntryWriter(int studyId, int fileId, SampleIndexSchema schema, ObjectMap options)
            throws StorageEngineException;

    public abstract SampleAnnotationIndexer newSampleAnnotationIndexer(VariantStorageEngine engine)
            throws StorageEngineException;

    public abstract SampleFamilyIndexer newSampleFamilyIndexer(VariantStorageEngine engine) throws StorageEngineException;

    public abstract SampleIndexEntryBuilder queryByGtBuilder(int study, int sample, String chromosome, int position,
                                                             SampleIndexSchema schema) throws IOException;

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
        VariantQueryUtils.QueryOperation operation = query.getQueryOperation();

        if (samples.size() == 1) {
            String sample = samples.entrySet().iterator().next().getKey();
            List<String> gts = query.getSamplesMap().get(sample);

            if (gts.isEmpty()) {
                // If empty, should find none. Return empty iterator
                return VariantDBIterator.emptyIterator();
            } else {
                logger.info("Single sample indexes iterator : " + sample);
                VariantDBIterator iterator = internalIterator(query.forSample(sample, gts), schema);
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
        if (operation.equals(VariantQueryUtils.QueryOperation.OR)) {
            logger.info("Union of " + iterators.size() + " sample indexes");
            iterator = new UnionMultiVariantKeyIterator(iterators);
        } else {
            logger.info("Intersection of " + iterators.size() + " sample indexes plus " + negatedIterators.size() + " negated indexes");
            iterator = new IntersectMultiVariantKeyIterator(iterators, negatedIterators);
        }

        return iterator.localLimitSkip(options);
    }

    protected abstract VariantDBIterator internalIterator(SingleSampleIndexQuery query, SampleIndexSchema schema);

    protected abstract CloseableIterator<SampleIndexVariant> rawInternalIterator(SingleSampleIndexQuery query, SampleIndexSchema schema);

    public CloseableIterator<SampleIndexEntry> indexEntryIterator(int study, int sample) throws IOException {
        return indexEntryIterator(study, sample, null);
    }

    public CloseableIterator<SampleIndexEntry> indexEntryIterator(int study, int sample, Region region) throws IOException {
        return indexEntryIterator(study, sample, region, schemaFactory.getSchema(study, sample, false));
    }

    public abstract CloseableIterator<SampleIndexEntry> indexEntryIterator(int study, int sample, Region region, SampleIndexSchema schema)
            throws IOException;

    /**
     * Partially processed iterator. Internal usage only.
     *
     * @param study  study
     * @param sample sample
     * @return CloseableIterator<SampleIndexVariant>
     */
    public CloseableIterator<SampleIndexVariant> indexVariantIterator(String study, String sample) {
        Query query = new VariantQuery().study(study).sample(sample);
        SingleSampleIndexQuery sampleIndexQuery = parseSampleIndexQuery(query).forSample(sample);
        return rawInternalIterator(sampleIndexQuery, sampleIndexQuery.getSchema());
    }

    public CloseableIterator<SampleIndexVariant> indexVariantIterator(Query query) throws IOException {
        return indexVariantIterator(parseSampleIndexQuery(query));
    }

    public CloseableIterator<SampleIndexVariant> indexVariantIterator(SampleIndexQuery query) throws IOException {
        return indexVariantIterator(query, new QueryOptions());
    }

    public CloseableIterator<SampleIndexVariant> indexVariantIterator(SampleIndexQuery query, QueryOptions options) throws IOException {
        Map<String, List<String>> samples = query.getSamplesMap();

        if (samples.isEmpty()) {
            throw new VariantQueryException("At least one sample expected to query SampleIndex!");
        }
        SampleIndexSchema schema = query.getSchema();
        VariantQueryUtils.QueryOperation operation = query.getQueryOperation();

        if (samples.size() == 1) {
            String sample = samples.entrySet().iterator().next().getKey();
            List<String> gts = query.getSamplesMap().get(sample);

            if (gts.isEmpty()) {
                // If empty, should find none. Return empty iterator
                return CloseableIterator.emptyIterator();
            } else {
                logger.info("Single sample indexes iterator");
                CloseableIterator<SampleIndexVariant> iterator = rawInternalIterator(query.forSample(sample, gts), schema);
                return iterator.localLimitSkip(options);
            }
        }

        List<CloseableIterator<SampleIndexVariant>> iterators = new ArrayList<>(samples.size());
        List<CloseableIterator<SampleIndexVariant>> negatedIterators = new ArrayList<>(samples.size());

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
                    iterators.add(CloseableIterator.emptyIterator());
                } else {
                    iterators.add(rawInternalIterator(query.forSample(sample, gts), schema));
                }
            }
        }

        final CloseableIterator<SampleIndexVariant> iterator;
        if (operation.equals(VariantQueryUtils.QueryOperation.OR)) {
            logger.info("Union of " + iterators.size() + " sample indexes");
            iterator = new UnionMultiKeyIterator<>(
                    Comparator.comparing(SampleIndexVariant::getVariant, VariantDBIterator.VARIANT_COMPARATOR), iterators);
        } else {
            logger.info("Intersection of " + iterators.size() + " sample indexes plus " + negatedIterators.size() + " negated indexes");
            iterator = new IntersectMultiKeyIterator<>(
                    Comparator.comparing(SampleIndexVariant::getVariant, VariantDBIterator.VARIANT_COMPARATOR),
                    iterators, negatedIterators);
        }

        return iterator.localLimitSkip(options);
    }

    public long count(SampleIndexQuery query) {
        if (query.getSamplesMap().size() == 1 && query.getMendelianErrorSet().isEmpty()) {
            String sample = query.getSamplesMap().keySet().iterator().next();
            return count(query.forSample(sample));
        } else {
            return Iterators.size(iterator(query));
        }
    }

    public boolean isFastCount(SampleIndexQuery query) {
        return query.getSamplesMap().size() == 1 && query.emptyAnnotationIndex() && query.emptyFileIndex();
    }

    public long count(List<Region> regions, String study, String sample, List<String> gts) {
        return count(newParser().parse(regions, study, sample, gts));
    }

    protected abstract long count(SingleSampleIndexQuery query);

    public SampleIndexSchemaFactory getSchemaFactory() {
        return schemaFactory;
    }

    public VariantStorageMetadataManager getMetadataManager() {
        return metadataManager;
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

    protected int toSampleId(int studyId, String sample) {
        return metadataManager.getSampleIdOrFail(studyId, sample);
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
     * @param locusQuery Locus query to split
     * @return List of regions.
     */
    protected static List<LocusQuery> splitLocusQuery(LocusQuery locusQuery) {
        if (locusQuery == null || !locusQuery.getVariants().isEmpty() || locusQuery.getRegions().size() != 1) {
            // Do not split
            return Collections.singletonList(locusQuery);
        }
        Region region = locusQuery.getRegions().get(0);
        List<LocusQuery> locusQueries;
        if (region.getEnd() - region.getStart() < SampleIndexSchema.BATCH_SIZE) {
            // Less than one batch. Do not split region
            locusQueries = Collections.singletonList(locusQuery);
        } else if (region.getStart() / SampleIndexSchema.BATCH_SIZE + 1 == region.getEnd() / SampleIndexSchema.BATCH_SIZE
                && !startsAtBatch(region)
                && !endsAtBatch(region)) {
            // Consecutive partial batches. Do not split region
            locusQueries = Collections.singletonList(locusQuery);
        } else {
            locusQueries = new ArrayList<>(3);
//             Copy regions before modifying
            region = new Region(region.getChromosome(), region.getStart(), region.getEnd());
            Region chunkRegion = new Region(locusQuery.getChunkRegion().getChromosome(),
                    locusQuery.getChunkRegion().getStart(),
                    locusQuery.getChunkRegion().getEnd());
            if (!startsAtBatch(region)) {
                int splitPoint = SampleIndexSchema.getChunkStartNext(region.getStart());
                Region startRegion = new Region(region.getChromosome(), region.getStart(), splitPoint - 1);
                locusQueries.add(new LocusQuery(
                        // Keep the exceeded start only for the first split.
                        new Region(chunkRegion.getChromosome(), chunkRegion.getStart(), splitPoint),
                        Collections.singletonList(startRegion),
                        Collections.emptyList()));
                region.setStart(splitPoint);
                chunkRegion.setStart(splitPoint);
            }
            if (!endsAtBatch(region)) {
                int splitPoint = SampleIndexSchema.getChunkStart(region.getEnd());
                Region endRegion = new Region(region.getChromosome(), splitPoint, region.getEnd());
                locusQueries.add(new LocusQuery(
                        SampleIndexSchema.getChunkRegion(endRegion, 0),
                        Collections.singletonList(endRegion),
                        Collections.emptyList()
                ));
                region.setEnd(splitPoint - 1);
                chunkRegion.setEnd(splitPoint);
            }
            locusQueries.add(new LocusQuery(
                    chunkRegion,
                    Collections.singletonList(region),
                    Collections.emptyList()));
            locusQueries.sort(LocusQuery::compareTo);
        }
        return locusQueries;
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
            logger.info("Not all samples from study {} have the sample index version {} on GENOTYPES and ANNOTATION",
                    studyMetadata.getName(), version);
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

    public static void printQuery(LocusQuery locusQuery) {
        if (locusQuery != null) {
            Region chunk = locusQuery.getChunkRegion();
            if (chunk.getStart() == 0 && chunk.getEnd() == Integer.MAX_VALUE) {
                logger.info("ChunkRegion: [ " + chunk.getChromosome() + " )");
            } else {
                logger.info("ChunkRegion: [ " + chunk.getChromosome() + ":" + chunk.getStart() + "-" + chunk.getEnd() + " )");
            }
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

}
