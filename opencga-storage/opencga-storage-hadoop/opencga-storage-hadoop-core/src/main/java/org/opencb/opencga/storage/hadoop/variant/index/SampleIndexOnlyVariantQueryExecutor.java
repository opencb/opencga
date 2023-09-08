package org.opencb.opencga.storage.hadoop.variant.index;

import com.google.common.collect.Iterators;
import htsjdk.variant.vcf.VCFConstants;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.avro.SampleEntry;
import org.opencb.biodata.tools.commons.Converter;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.run.Task;
import org.opencb.opencga.core.common.BatchUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.storage.IndexFieldConfiguration;
import org.opencb.opencga.core.response.VariantQueryResult;
import org.opencb.opencga.storage.core.io.bit.BitBuffer;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.utils.iterators.CloseableIterator;
import org.opencb.opencga.storage.core.variant.adaptors.*;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.query.ParsedVariantQuery;
import org.opencb.opencga.storage.core.variant.query.VariantQueryParser;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.query.executors.VariantQueryExecutor;
import org.opencb.opencga.storage.core.variant.query.projection.VariantQueryProjection;
import org.opencb.opencga.storage.core.variant.query.projection.VariantQueryProjectionParser;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.converters.HBaseToVariantConverter;
import org.opencb.opencga.storage.hadoop.variant.index.core.IndexField;
import org.opencb.opencga.storage.hadoop.variant.index.family.GenotypeCodec;
import org.opencb.opencga.storage.hadoop.variant.index.query.SampleIndexQuery;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexQueryParser;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexSchema;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleVariantIndexEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.query.VariantQueryUtils.NONE;
import static org.opencb.opencga.storage.core.variant.query.VariantQueryUtils.addSamplesMetadataIfRequested;
import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageOptions.SAMPLE_INDEX_QUERY_SAMPLE_INDEX_ONLY_PD_BATCH;
import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageOptions.SAMPLE_INDEX_QUERY_SAMPLE_INDEX_ONLY_PD_BUFFER;
import static org.opencb.opencga.storage.hadoop.variant.index.SampleIndexVariantQueryExecutor.SAMPLE_INDEX_TABLE_SOURCE;

/**
 * Created on 01/04/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class SampleIndexOnlyVariantQueryExecutor extends VariantQueryExecutor {

    private final SampleIndexDBAdaptor sampleIndexDBAdaptor;
    private final VariantHadoopDBAdaptor dbAdaptor;
    private final VariantQueryParser variantQueryParser;
    private final VariantQueryProjectionParser variantQueryProjectionParser;
    private static Logger logger = LoggerFactory.getLogger(SampleIndexOnlyVariantQueryExecutor.class);

    private static final ExecutorService THREAD_POOL = Executors.newCachedThreadPool(new BasicThreadFactory.Builder()
            .namingPattern("sample-index-async-count-%s")
            .build());
    private static final ExecutorService THREAD_POOL_FETCH_CALL = Executors.newCachedThreadPool(new BasicThreadFactory.Builder()
            .namingPattern("sample-index-fetch-call-%s")
            .build());
    private int partialDataBufferSize;
    private int partialDataBatchSize;

    public SampleIndexOnlyVariantQueryExecutor(VariantHadoopDBAdaptor dbAdaptor, SampleIndexDBAdaptor sampleIndexDBAdaptor,
                                               String storageEngineId, ObjectMap options) {
        super(dbAdaptor.getMetadataManager(), storageEngineId, options);
        this.sampleIndexDBAdaptor = sampleIndexDBAdaptor;
        this.dbAdaptor = dbAdaptor;
        variantQueryParser = new VariantQueryParser(null, getMetadataManager());
        variantQueryProjectionParser = new VariantQueryProjectionParser(getMetadataManager());
        partialDataBufferSize = options.getInt(SAMPLE_INDEX_QUERY_SAMPLE_INDEX_ONLY_PD_BUFFER.key(),
                SAMPLE_INDEX_QUERY_SAMPLE_INDEX_ONLY_PD_BUFFER.defaultValue());
        partialDataBatchSize = options.getInt(SAMPLE_INDEX_QUERY_SAMPLE_INDEX_ONLY_PD_BATCH.key(),
                SAMPLE_INDEX_QUERY_SAMPLE_INDEX_ONLY_PD_BATCH.defaultValue());
    }

    @Override
    public boolean canUseThisExecutor(Query query, QueryOptions options) {
        if (SampleIndexQueryParser.validSampleIndexQuery(query)) {

            if (isFullyCoveredQuery(query, options)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public DataResult<Long> count(Query query) {
        StopWatch stopWatch = StopWatch.createStarted();
        SampleIndexQuery sampleIndexQuery = sampleIndexDBAdaptor.parseSampleIndexQuery(query);
        long count = sampleIndexDBAdaptor.count(sampleIndexQuery);
        return new DataResult<>(((int) stopWatch.getTime(TimeUnit.MILLISECONDS)), Collections.emptyList(), 1,
                Collections.singletonList(count), count);
    }

    /**
     * Fetch results exclusively from SampleSecondaryIndex.
     *
     * @param inputQuery Query
     * @param options    Options
     * @param iterator   Shall the resulting object be an iterator instead of a DataResult
     * @return           DataResult or Iterator with the variants that matches the query
     */
    @Override
    protected Object getOrIterator(Query inputQuery, QueryOptions options, boolean iterator) {
        Query query = new Query(inputQuery);
        query.put(SampleIndexQueryParser.INCLUDE_PARENTS_COLUMN, true);
        SampleIndexQuery sampleIndexQuery = sampleIndexDBAdaptor.parseSampleIndexQuery(query);

        logger.info("HBase SampleIndex, skip variants table");

        boolean count;
        Future<Long> asyncCountFuture;
        if (shouldGetCount(options, iterator)) {
            count = true;
            asyncCountFuture = THREAD_POOL.submit(() -> {
                StopWatch stopWatch = StopWatch.createStarted();
                long numMatches = sampleIndexDBAdaptor.count(sampleIndexQuery);
                logger.info("Async count took " + TimeUtils.durationToString(stopWatch));
                return numMatches;
            });
        } else {
            count = false;
            asyncCountFuture = null;
        }

        VariantDBIterator variantIterator = getVariantDBIterator(sampleIndexQuery, inputQuery, options);

        if (iterator) {
            return variantIterator;
        } else {
            VariantQueryResult<Variant> result =
                    addSamplesMetadataIfRequested(variantIterator.toDataResult(), inputQuery, options, getMetadataManager());
            if (count) {
                result.setApproximateCount(false);
                try {
                    result.setNumMatches(asyncCountFuture.get());
                } catch (InterruptedException | ExecutionException e) {
                    throw VariantQueryException.internalException(e);
                }
            }
            result.setSource(SAMPLE_INDEX_TABLE_SOURCE);
            return result;
        }
    }

    private VariantDBIterator getVariantDBIterator(SampleIndexQuery sampleIndexQuery, Query inputQuery, QueryOptions options) {
        ParsedVariantQuery parsedQuery = variantQueryParser.parseQuery(inputQuery, options, true);
        VariantDBIterator variantIterator;
        if (parsedQuery.getProjection().getStudyIds().isEmpty()) {
            logger.info("Using sample index iterator Iterator<Variant>");
            variantIterator = sampleIndexDBAdaptor.iterator(sampleIndexQuery, options);
            variantIterator = variantIterator.map(v -> v.setId(v.toString()));
        } else {
            logger.info("Using sample index raw iterator Iterator<SampleVariantIndexEntry>");
            CloseableIterator<SampleVariantIndexEntry> rawIterator;
            try {
                rawIterator = sampleIndexDBAdaptor.rawIterator(sampleIndexQuery, options);
            } catch (IOException e) {
                throw VariantQueryException.internalException(e).setQuery(inputQuery);
            }
            boolean includeAll = inputQuery.getBoolean("includeAllFromSampleIndex", false);
            SampleVariantIndexEntryToVariantConverter converter = new SampleVariantIndexEntryToVariantConverter(
                    parsedQuery, sampleIndexQuery, dbAdaptor.getMetadataManager(), includeAll);
            variantIterator = VariantDBIterator.wrapper(Iterators.transform(rawIterator, converter::convert));
            AddMissingDataTask task = new AddMissingDataTask(
                    parsedQuery, sampleIndexQuery, dbAdaptor.getMetadataManager());
            variantIterator = variantIterator.mapBuffered(task::apply, partialDataBufferSize);
            variantIterator.addCloseable(rawIterator);
        }
        return variantIterator;
    }

    protected boolean shouldGetCount(QueryOptions options, boolean iterator) {
        return !iterator && (options.getBoolean(QueryOptions.COUNT, false));
    }

    private boolean isFullyCoveredQuery(Query inputQuery, QueryOptions options) {
        Query query = new Query(inputQuery);

//        ParsedVariantQuery parsedVariantQuery = variantQueryProjectionParser.parseQuery(query, options, true);
        SampleIndexQuery sampleIndexQuery = sampleIndexDBAdaptor.parseSampleIndexQuery(query);

        return isQueryCovered(query) && isIncludeCovered(sampleIndexQuery, inputQuery, options);
    }

    private boolean isQueryCovered(Query query) {
        if (VariantQueryUtils.isValidParam(query, VariantQueryUtils.SAMPLE_MENDELIAN_ERROR)
                || VariantQueryUtils.isValidParam(query, VariantQueryUtils.SAMPLE_DE_NOVO)
                || VariantQueryUtils.isValidParam(query, VariantQueryUtils.SAMPLE_DE_NOVO_STRICT)) {
            // Can't use with special filters.
            return false;
        }

        // Check if the query is fully covered
        Set<VariantQueryParam> params = VariantQueryUtils.validParams(query, true);
        params.remove(VariantQueryParam.STUDY);

        return params.isEmpty();
    }

    private boolean isIncludeCovered(SampleIndexQuery sampleIndexQuery, Query inputQuery, QueryOptions options) {
        // Check if the query include fully covered
        Set<VariantField> includeFields = VariantField.getIncludeFields(options);
        if (includeFields.contains(VariantField.ANNOTATION)
                || includeFields.contains(VariantField.STUDIES_STATS)
                || includeFields.contains(VariantField.STUDIES_FILES)
                || includeFields.contains(VariantField.STUDIES_ISSUES)
                || includeFields.contains(VariantField.STUDIES_SCORES)
        ) {
            return false;
        }

        VariantQueryProjection projection = variantQueryProjectionParser.parseVariantQueryProjection(inputQuery, options);
        if (projection.getStudyIds().size() > 1) {
            // Either one or none studies can be returned.
            return false;
        }
        if (projection.getStudyIds().size() == 1) {
            VariantQueryProjection.StudyVariantQueryProjection study = projection.getStudy(projection.getStudyIds().get(0));

            if (sampleIndexQuery.getSamplesMap().size() != 1) {
                // Can only filter by one sample
                return false;
            }
            String sampleName = sampleIndexQuery.getSamplesMap().keySet().iterator().next();
            Integer sampleId = metadataManager.getSampleId(study.getId(), sampleName);

            if (!study.getSamples().contains(sampleId)) {
                // Sample query does not include the sample to be returned
                return false;
            }

            List<String> sampleDataKeys = VariantQueryUtils.getIncludeSampleData(inputQuery);
            if (sampleDataKeys == null) {
                // Undefined, get default sampleDataKeys
                sampleDataKeys = HBaseToVariantConverter.getFixedFormat(study.getStudyMetadata());
            }

            if (sampleDataKeys.size() != 1) {
                // One and only one sampledatakey
                return false;
            }
            if (!sampleDataKeys.get(0).equals(VCFConstants.GENOTYPE_KEY)) {
                // It must return only GT
                return false;
            }

            if (study.getSamples().size() > 1) {
                // Ensure that all includedSamples are members of the same family
                LinkedList<Integer> samplesAux = new LinkedList<>(study.getSamples());
                SampleMetadata sampleMetadata = metadataManager.getSampleMetadata(study.getId(), sampleId);
                samplesAux.remove(sampleId);
                samplesAux.remove(sampleMetadata.getMother());
                samplesAux.remove(sampleMetadata.getFather());
                if (samplesAux.size() != 0) {
                    // Sample query include some samples that are not the parents
                    return false;
                }
                if (sampleMetadata.getFamilyIndexStatus(sampleIndexQuery.getSchema().getVersion()) != TaskMetadata.Status.READY) {
                    // Unable to return parents if the family index is not ready
                    return false;
                }
            }

            return true;
        }
        // Either one or none studies can be returned.
        return projection.getStudyIds().size() == 0;
    }


    private static class SampleVariantIndexEntryToVariantConverter implements Converter<SampleVariantIndexEntry, Variant> {

        enum FamilyRole {
            MOTHER,
            FATHER,
            SAMPLE;
        }

        private final boolean includeStudy;
        private String studyName;
        private List<FamilyRole> familyRoleOrder;
        private String sampleName;
        private String motherName;
        private String fatherName;
        private LinkedHashMap<String, Integer> samplesPosition;
        private List<String> sampleFiles;
        private IndexField<String> filterField;
        private IndexField<String> qualField;
        private final SampleIndexSchema schema;
        private final boolean includeAll;


        SampleVariantIndexEntryToVariantConverter(ParsedVariantQuery parseQuery, SampleIndexQuery sampleIndexQuery,
                                                  VariantStorageMetadataManager metadataManager, boolean includeAll) {
            schema = sampleIndexQuery.getSchema();
            this.includeAll = includeAll;

            VariantQueryProjection projection = parseQuery.getProjection();
            includeStudy = !projection.getStudyIds().isEmpty();
            if (includeStudy) {
                int studyId = projection.getStudyIds().get(0); // only one study
                VariantQueryProjection.StudyVariantQueryProjection projectionStudy = projection.getStudy(studyId);
                studyName = projectionStudy.getStudyMetadata().getName();

                if (sampleIndexQuery.getSamplesMap().size() != 1) {
                    // This should never happen
                    throw new IllegalStateException("Unexpected number of samples. Expected one, found "
                            + sampleIndexQuery.getSamplesMap().keySet());
                }
                sampleName = sampleIndexQuery.getSamplesMap().keySet().iterator().next();
                Integer sampleId = metadataManager.getSampleId(studyId, sampleName);

                familyRoleOrder = new ArrayList<>();
                samplesPosition = new LinkedHashMap<>();
                SampleMetadata sampleMetadata = null; // lazy init
                List<Integer> includeSamples = projectionStudy.getSamples();
                for (Integer includeSampleId : includeSamples) {
                    if (includeSampleId.equals(sampleId)) {
                        familyRoleOrder.add(FamilyRole.SAMPLE);
                        samplesPosition.put(sampleName, samplesPosition.size());
                    } else {
                        if (sampleMetadata == null) {
                            sampleMetadata = metadataManager.getSampleMetadata(studyId, sampleId);
                        }
                        if (includeSampleId.equals(sampleMetadata.getMother())) {
                            familyRoleOrder.add(FamilyRole.MOTHER);
                            motherName = metadataManager.getSampleName(studyId, includeSampleId);
                            samplesPosition.put(motherName, samplesPosition.size());
                        } else if (includeSampleId.equals(sampleMetadata.getFather())) {
                            familyRoleOrder.add(FamilyRole.FATHER);
                            fatherName = metadataManager.getSampleName(studyId, includeSampleId);
                            samplesPosition.put(fatherName, samplesPosition.size());
                        } else {
                            String unknownSampleName = metadataManager.getSampleName(studyId, includeSampleId);
                            throw new IllegalStateException("Unexpected include sample '" + unknownSampleName + "'"
                                    + " not related with sample '" + sampleName + "'");
                        }
                    }
                }

                if (!parseQuery.getInputQuery().getBoolean(VariantQueryParam.INCLUDE_SAMPLE_ID.key())) {
                    this.sampleName = null;
                    this.motherName = null;
                    this.fatherName = null;
                }

                if (includeAll) {
                    if (sampleMetadata == null) {
                        sampleMetadata = metadataManager.getSampleMetadata(studyId, sampleId);
                    }
                    if (sampleMetadata.isMultiFileSample()) {
                        List<Integer> sampleFileIds = sampleMetadata.getFiles();
                        sampleFiles = new ArrayList<>(sampleFileIds.size());
                        for (Integer fileId : sampleFileIds) {
                            sampleFiles.add(metadataManager.getFileName(studyId, fileId));
                        }
                    } else {
                        List<Integer> fileIds = metadataManager.getFileIdsFromSampleId(studyId, sampleId, true);
                        if (fileIds.isEmpty()) {
                            logger.warn("Sample without indexed files!");
                            sampleFiles = Collections.singletonList("sample_without_indexed_files.vcf");
                        } else {
                            String fileName = metadataManager.getFileName(studyId, fileIds.get(0));
                            sampleFiles = Collections.singletonList(fileName);
                        }
                    }
                    filterField = schema.getFileIndex()
                            .getCustomField(IndexFieldConfiguration.Source.FILE, StudyEntry.FILTER);
                    qualField = schema.getFileIndex()
                            .getCustomField(IndexFieldConfiguration.Source.FILE, StudyEntry.QUAL);
                }
            }
        }

        @Override
        public Variant convert(SampleVariantIndexEntry entry) {
            Variant v = entry.getVariant();
            v.setId(v.toString());
            if (includeStudy) {
                StudyEntry studyEntry = new StudyEntry();
                studyEntry.setStudyId(studyName);
                studyEntry.setSampleDataKeys(Collections.singletonList("GT"));
                studyEntry.setSamples(new ArrayList<>(familyRoleOrder.size()));
                SampleEntry sampleEntry = null;
                for (FamilyRole role : familyRoleOrder) {
                    switch (role) {
                        case MOTHER:
                            studyEntry.getSamples().add(new SampleEntry(motherName, null,
                                    Arrays.asList(GenotypeCodec.decodeMother(entry.getParentsCode()))));
                            break;
                        case FATHER:
                            studyEntry.getSamples().add(new SampleEntry(fatherName, null,
                                    Arrays.asList(GenotypeCodec.decodeFather(entry.getParentsCode()))));
                            break;
                        case SAMPLE:
                            sampleEntry = new SampleEntry(sampleName, null,
                                    Arrays.asList(entry.getGenotype()));
                            studyEntry.getSamples().add(sampleEntry);
                            break;
                        default:
                            throw new IllegalStateException("Unexpected value: " + role);
                    }
                }
                if (includeAll) {
                    HashMap<String, String> fileAttributes = new HashMap<>();
                    for (BitBuffer fileIndexBitBuffer : entry.getFilesIndex()) {
                        String filter = filterField.readAndDecode(fileIndexBitBuffer);
                        if (filter == null) {
                            filter = "NA";
                        }
                        fileAttributes.put(StudyEntry.FILTER, filter);
                        String qual = qualField.readAndDecode(fileIndexBitBuffer);
                        if (qual == null) {
                            qual = "NA";
                        }
                        fileAttributes.put(StudyEntry.QUAL, qual);

                        Integer idx = schema.getFileIndex().getFilePositionIndex().readAndDecode(fileIndexBitBuffer);
                        String fileName = sampleFiles.get(idx);
                        studyEntry.setFiles(new ArrayList<>());
                        studyEntry.getFiles().add(new FileEntry(fileName, null, fileAttributes));
                        if (sampleEntry != null) {
                            sampleEntry.setFileIndex(0);
                        }
                    }
                }
                studyEntry.setSortedSamplesPosition(samplesPosition);
                v.setStudies(Collections.singletonList(studyEntry));
            }
            return v;
        }
    }

    private class AddMissingDataTask implements Task<Variant, Variant> {
        private final ParsedVariantQuery parsedQuery;
        private final String studyName;
        private final String sampleName;
        private final List<String> filesFromSample;
        private final List<String> includeSamples;
        private final List<String> allFiles; // from all includedSamples
        private final int gtIdx;

        AddMissingDataTask(ParsedVariantQuery parsedQuery, SampleIndexQuery sampleIndexQuery,
                           VariantStorageMetadataManager metadataManager) {
            this.parsedQuery = parsedQuery;
            VariantQueryProjection projection = this.parsedQuery.getProjection();

            int studyId = projection.getStudyIds().get(0); // only one study
            VariantQueryProjection.StudyVariantQueryProjection projectionStudy = projection.getStudy(studyId);
            studyName = projectionStudy.getStudyMetadata().getName();

            if (sampleIndexQuery.getSamplesMap().size() != 1) {
                // This should never happen
                throw new IllegalStateException("Unexpected number of samples. Expected one, found "
                        + sampleIndexQuery.getSamplesMap().keySet());
            }
            includeSamples = new ArrayList<>(projectionStudy.getSamples().size());
            for (Integer sample : projectionStudy.getSamples()) {
                includeSamples.add(metadataManager.getSampleName(studyId, sample));
            }
            Set<Integer> allFileIds = metadataManager.getFileIdsFromSampleIds(studyId, projectionStudy.getSamples(), true);
            allFiles = new ArrayList<>(allFileIds.size());
            for (Integer fileId : allFileIds) {
                allFiles.add(metadataManager.getFileName(studyId, fileId));
            }

            sampleName = sampleIndexQuery.getSamplesMap().keySet().iterator().next();
            Integer sampleId = metadataManager.getSampleId(studyId, sampleName);
            List<Integer> fileIds = metadataManager.getFileIdsFromSampleId(studyId, sampleId, true);
            filesFromSample = new ArrayList<>(fileIds.size());
            for (Integer fileId : fileIds) {
                filesFromSample.add(metadataManager.getFileName(studyId, fileId));
            }
            List<String> includeSampleData = VariantQueryUtils.getIncludeSampleData(parsedQuery.getInputQuery());
            gtIdx = includeSampleData.indexOf("GT");
        }

        @Override
        public List<Variant> apply(List<Variant> variants) {
            // Multi allelic variants, to be read entirely
            List<Variant> multiAllelic = new ArrayList<>();
            // INDELs (non multiallelic) variants, to fetch the original call
            List<Variant> indels = new ArrayList<>();
            for (Variant variant : variants) {
                boolean secAlt = false;
                for (SampleEntry sample : variant.getStudies().get(0).getSamples()) {
                    if (GenotypeClass.SEC.test(sample.getData().get(0))) {
                        secAlt = true;
                        break;
                    }
                }
                if (secAlt) {
                    multiAllelic.add(variant);
                } else {
                    if (variant.getLengthReference() == 0 || variant.getLengthAlternate() == 0) {
                        indels.add(variant);
                    }
                }
            }
            // Process in multiple treads
            List<Future<?>> futures = new ArrayList<>(10);
            if (!multiAllelic.isEmpty()) {
                List<List<Variant>> batches = BatchUtils.splitBatches(multiAllelic, partialDataBatchSize);
                for (List<Variant> batch : batches) {
                    futures.add(THREAD_POOL_FETCH_CALL.submit(() -> addSecondaryAlternates(batch)));
                }
            }
            if (!indels.isEmpty()) {
                List<List<Variant>> batches = BatchUtils.splitBatches(indels, partialDataBatchSize);
                for (List<Variant> batch : batches) {
                    futures.add(THREAD_POOL_FETCH_CALL.submit(() -> addOriginalCall(batch, studyName)));
                }
            }

            StopWatch stopWatch = StopWatch.createStarted();
            for (Future<?> future : futures) {
                try {
                    // Should end in few seconds
                    future.get(90, TimeUnit.SECONDS);
                } catch (InterruptedException | ExecutionException | TimeoutException e) {
                    throw new VariantQueryException("Error fetching extra data", e);
                }
            }
            logger.info("Fetch {} partial variants in {} in {} threads",
                    multiAllelic.size() + indels.size(),
                    TimeUtils.durationToString(stopWatch),
                    futures.size());
            return variants;
        }

        /**
         * Fetch the Secondary alternates, sample GTs and original call of these variants.
         * @param toReadFull variants to complete
         */
        private void addSecondaryAlternates(List<Variant> toReadFull) {
//            StopWatch stopWatch = StopWatch.createStarted();
            Set<VariantField> includeFields = new HashSet<>(VariantField.getIncludeFields(parsedQuery.getInputOptions()));
            includeFields.add(VariantField.STUDIES_SECONDARY_ALTERNATES);
            includeFields.add(VariantField.STUDIES_FILES);

            QueryOptions options = new QueryOptions(parsedQuery.getInputOptions());
            options.remove(QueryOptions.EXCLUDE);
            options.remove(VariantField.SUMMARY);
            options.put(QueryOptions.INCLUDE, includeFields);
            options.put(VariantHadoopDBAdaptor.QUIET, true);
            options.put(VariantHadoopDBAdaptor.NATIVE, true);

            Map<String, Variant> variantsExtra = dbAdaptor.get(new VariantQuery()
                                    .id(toReadFull)
                                    .study(studyName)
                                    .includeSample(includeSamples)
                                    .includeSampleData("GT") // read only GT
                                    .includeFile(allFiles),
                            options)
                    .getResults().stream().collect(Collectors.toMap(Variant::toString, v -> v));

            for (Variant variant : toReadFull) {
                Variant variantExtra = variantsExtra.get(variant.toString());
                if (variantExtra == null) {
                    // TODO: Should we fail here?
//                    throw new VariantQueryException("Variant " + variant + " not found!");
                    logger.warn("Variant " + variant + " not found!");
                    continue;
                }
                StudyEntry studyExtra = variantExtra.getStudies().get(0);
                StudyEntry study = variant.getStudies().get(0);

                study.setSecondaryAlternates(studyExtra.getSecondaryAlternates());

                mergeFileEntries(study, studyExtra.getFiles(), (fe, newFe) -> {
                    fe.setCall(newFe.getCall());
                });
                // merge sampleEntries
                for (int i = 0; i < includeSamples.size(); i++) {
                    SampleEntry sample = study.getSample(i);
                    SampleEntry sampleExtra = studyExtra.getSample(i);

                    sample.getData().set(gtIdx, sampleExtra.getData().get(0));
                }
            }
//            logger.info(" # Fetch {} SEC_ALTS in {}", toReadFull.size(), TimeUtils.durationToString(stopWatch));
        }

        private void addOriginalCall(List<Variant> variants, String study) {
//            StopWatch stopWatch = StopWatch.createStarted();
            Map<String, List<FileEntry>> filesMap = new HashMap<>(variants.size());
            for (Variant variant : dbAdaptor.iterable(
                    new Query()
                            .append(VariantQueryParam.ID.key(), variants)
                            .append(VariantQueryParam.INCLUDE_FILE.key(), filesFromSample)
                            .append(VariantQueryParam.INCLUDE_SAMPLE.key(), NONE)
                            .append(VariantQueryParam.INCLUDE_STUDY.key(), study),
                    new QueryOptions()
                            .append(VariantHadoopDBAdaptor.NATIVE, true)
                            .append(VariantHadoopDBAdaptor.QUIET, true)
                            .append(QueryOptions.INCLUDE, Arrays.asList(VariantField.STUDIES_FILES)))) {

                List<FileEntry> fileEntries = variant.getStudies().get(0).getFiles();
                // Remove data, as we only want the original call
                fileEntries.forEach(fileEntry -> fileEntry.setData(Collections.emptyMap()));
                filesMap.put(variant.toString(), fileEntries);
            }

            for (Variant variant : variants) {
                List<FileEntry> fileEntries = filesMap.get(variant.toString());
                if (fileEntries == null) {
                    // TODO: Should we fail here?
//                    throw new VariantQueryException("Variant " + variant + " not found!");
                    logger.warn("Variant " + variant + " not found!");
                    continue;
                }
                StudyEntry studyEntry = variant.getStudies().get(0);
                mergeFileEntries(studyEntry, fileEntries, (fe, newFe) -> {
                    fe.setCall(newFe.getCall());
                });
            }
//            logger.info(" # Fetch {} INDEL original call in {}", filesMap.size(), TimeUtils.durationToString(stopWatch));
        }

        private void mergeFileEntries(StudyEntry studyEntry, List<FileEntry> newFileEntries,
                                      BiConsumer<FileEntry, FileEntry> merge) {
            if (studyEntry.getFiles() == null) {
                studyEntry.setFiles(new ArrayList<>(newFileEntries.size()));
            }
            for (FileEntry newFileEntry : newFileEntries) {
                FileEntry fileEntry = studyEntry.getFile(newFileEntry.getFileId());
                if (fileEntry == null) {
                    fileEntry = new FileEntry(newFileEntry.getFileId(), null, new HashMap<>());
                    studyEntry.getFiles().add(fileEntry);
                    if (filesFromSample.contains(fileEntry.getFileId())) {
                        SampleEntry sampleEntry = studyEntry.getSample(sampleName);
                        if (sampleEntry.getFileIndex() == null) {
                            sampleEntry.setFileIndex(studyEntry.getFiles().size() - 1);
                        }
                    }
                }
                merge.accept(fileEntry, newFileEntry);
            }
        }
    }
}
