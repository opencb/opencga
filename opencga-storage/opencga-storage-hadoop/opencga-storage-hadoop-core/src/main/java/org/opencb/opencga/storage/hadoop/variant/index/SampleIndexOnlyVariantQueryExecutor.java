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
import org.opencb.opencga.core.response.VariantQueryResult;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.utils.iterators.CloseableIterator;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.query.ParsedVariantQuery;
import org.opencb.opencga.storage.core.variant.query.VariantQueryParser;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.query.executors.VariantQueryExecutor;
import org.opencb.opencga.storage.core.variant.query.projection.VariantQueryProjection;
import org.opencb.opencga.storage.core.variant.query.projection.VariantQueryProjectionParser;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.converters.HBaseToVariantConverter;
import org.opencb.opencga.storage.hadoop.variant.index.family.GenotypeCodec;
import org.opencb.opencga.storage.hadoop.variant.index.query.SampleIndexQuery;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexQueryParser;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleVariantIndexEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.query.VariantQueryUtils.NONE;
import static org.opencb.opencga.storage.core.variant.query.VariantQueryUtils.addSamplesMetadataIfRequested;
import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageOptions.SAMPLE_INDEX_QUERY_SAMPLE_INDEX_ONLY_MOC_BATCH;
import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageOptions.SAMPLE_INDEX_QUERY_SAMPLE_INDEX_ONLY_MOC_BUFFER;
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
    private Logger logger = LoggerFactory.getLogger(SampleIndexOnlyVariantQueryExecutor.class);

    private static final ExecutorService THREAD_POOL = Executors.newCachedThreadPool(new BasicThreadFactory.Builder()
            .namingPattern("sample-index-async-count-%s")
            .build());
    private static final ExecutorService THREAD_POOL_ORIG_CALL = Executors.newCachedThreadPool(new BasicThreadFactory.Builder()
            .namingPattern("sample-index-fetch-call-%s")
            .build());
    private int missingOriginalCallBufferSize;
    private int missingOriginalCallBatchSize;

    public SampleIndexOnlyVariantQueryExecutor(VariantHadoopDBAdaptor dbAdaptor, SampleIndexDBAdaptor sampleIndexDBAdaptor,
                                               String storageEngineId, ObjectMap options) {
        super(dbAdaptor.getMetadataManager(), storageEngineId, options);
        this.sampleIndexDBAdaptor = sampleIndexDBAdaptor;
        this.dbAdaptor = dbAdaptor;
        variantQueryParser = new VariantQueryParser(null, getMetadataManager());
        variantQueryProjectionParser = new VariantQueryProjectionParser(getMetadataManager());
        missingOriginalCallBufferSize = options.getInt(SAMPLE_INDEX_QUERY_SAMPLE_INDEX_ONLY_MOC_BUFFER.key(),
                SAMPLE_INDEX_QUERY_SAMPLE_INDEX_ONLY_MOC_BUFFER.defaultValue());
        missingOriginalCallBatchSize = options.getInt(SAMPLE_INDEX_QUERY_SAMPLE_INDEX_ONLY_MOC_BATCH.key(),
                SAMPLE_INDEX_QUERY_SAMPLE_INDEX_ONLY_MOC_BATCH.defaultValue());
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
        ParsedVariantQuery parseQuery = variantQueryParser.parseQuery(inputQuery, options, true);
        VariantDBIterator variantIterator;
        if (parseQuery.getProjection().getStudyIds().isEmpty()) {
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
            SampleVariantIndexEntryToVariantConverter converter =
                    new SampleVariantIndexEntryToVariantConverter(parseQuery, sampleIndexQuery, dbAdaptor.getMetadataManager());
            variantIterator = VariantDBIterator.wrapper(Iterators.transform(rawIterator, converter::convert));
            AddMissingOriginalCallTask task = new AddMissingOriginalCallTask(
                    parseQuery, sampleIndexQuery, dbAdaptor.getMetadataManager());
            variantIterator = variantIterator.mapBuffered(task::apply, missingOriginalCallBufferSize);
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
                || includeFields.contains(VariantField.STUDIES_SECONDARY_ALTERNATES)
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
            SAMPLE
        }

        private final boolean includeStudy;
        private String studyName;
        private List<FamilyRole> familyRoleOrder;
        private String sampleName;
        private String motherName;
        private String fatherName;
        private LinkedHashMap<String, Integer> samplesPosition;

        SampleVariantIndexEntryToVariantConverter(ParsedVariantQuery parseQuery, SampleIndexQuery sampleIndexQuery,
                                                  VariantStorageMetadataManager metadataManager) {
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
                for (FamilyRole role : familyRoleOrder) {
                    switch (role) {
                        case MOTHER:
                            studyEntry.getSamples().add(new SampleEntry(motherName, null,
                                    Collections.singletonList(GenotypeCodec.decodeMother(entry.getParentsCode()))));
                            break;
                        case FATHER:
                            studyEntry.getSamples().add(new SampleEntry(fatherName, null,
                                    Collections.singletonList(GenotypeCodec.decodeFather(entry.getParentsCode()))));
                            break;
                        case SAMPLE:
                            studyEntry.getSamples().add(new SampleEntry(sampleName, null,
                                    Collections.singletonList(entry.getGenotype())));
                            break;
                        default:
                            throw new IllegalStateException("Unexpected value: " + role);
                    }
                }
                studyEntry.setSortedSamplesPosition(samplesPosition);
                v.setStudies(Collections.singletonList(studyEntry));
            }
            return v;
        }
    }

    private class AddMissingOriginalCallTask implements Task<Variant, Variant> {
        private String studyName;
        private List<String> files;

        AddMissingOriginalCallTask(ParsedVariantQuery parseQuery, SampleIndexQuery sampleIndexQuery,
                                                  VariantStorageMetadataManager metadataManager) {
            VariantQueryProjection projection = parseQuery.getProjection();

            int studyId = projection.getStudyIds().get(0); // only one study
            VariantQueryProjection.StudyVariantQueryProjection projectionStudy = projection.getStudy(studyId);
            studyName = projectionStudy.getStudyMetadata().getName();

            if (sampleIndexQuery.getSamplesMap().size() != 1) {
                // This should never happen
                throw new IllegalStateException("Unexpected number of samples. Expected one, found "
                        + sampleIndexQuery.getSamplesMap().keySet());
            }
            String sampleName = sampleIndexQuery.getSamplesMap().keySet().iterator().next();
            Integer sampleId = metadataManager.getSampleId(studyId, sampleName);
            List<Integer> fileIds = metadataManager.getFileIdsFromSampleId(studyId, sampleId, true);
            files = new ArrayList<>(fileIds.size());
            for (Integer fileId : fileIds) {
                files.add(metadataManager.getFileName(studyId, fileId));
            }
        }

        @Override
        public List<Variant> apply(List<Variant> variants) {
            List<Variant> indels = variants.stream()
                    .filter(v -> v.getLengthReference() == 0 || v.getLengthAlternate() == 0)
                    .collect(Collectors.toList());
            if (!indels.isEmpty()) {
                StopWatch stopWatch = StopWatch.createStarted();
                List<List<Variant>> batches = BatchUtils.splitBatches(indels, missingOriginalCallBatchSize);
                List<Future<Map<String, List<FileEntry>>>> futures = new ArrayList<>(batches.size());
                for (List<Variant> batch : batches) {
                    futures.add(THREAD_POOL_ORIG_CALL.submit(() -> getOriginalCall(batch, studyName, files)));
                }

                Map<String, List<FileEntry>> map = new HashMap<>(variants.size());
                for (Future<Map<String, List<FileEntry>>> future : futures) {
                    try {
                        map.putAll(future.get(60, TimeUnit.SECONDS));
                    } catch (InterruptedException | ExecutionException | TimeoutException e) {
                        throw new VariantQueryException("Error fetching original call for INDELs");
                    }
                }
                logger.info("Fetch {} INDEL original call in {} in {} threads", map.size(), TimeUtils.durationToString(stopWatch),
                        futures.size());

                for (Variant v : indels) {
                    List<FileEntry> fileEntries = map.get(v.toString());
                    v.getStudies().get(0).setFiles(fileEntries);
                }
            }
            return variants;
        }

        private Map<String, List<FileEntry>> getOriginalCall(List<Variant> variants, String study, List<String> files) {
//            StopWatch stopWatch = StopWatch.createStarted();
            Map<String, List<FileEntry>> filesMap = new HashMap<>(variants.size());
            for (Variant variant : dbAdaptor.iterable(
                    new Query()
                            .append(VariantQueryParam.ID.key(), variants)
                            .append(VariantQueryParam.INCLUDE_FILE.key(), files)
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
//            logger.info(" # Fetch {} INDEL original call in {}", filesMap.size(), TimeUtils.durationToString(stopWatch));
            return filesMap;
        }


    }

}
