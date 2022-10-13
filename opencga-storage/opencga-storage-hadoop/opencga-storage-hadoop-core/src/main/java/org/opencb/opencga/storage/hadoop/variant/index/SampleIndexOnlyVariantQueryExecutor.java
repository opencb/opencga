package org.opencb.opencga.storage.hadoop.variant.index;

import com.google.common.collect.Iterators;
import htsjdk.variant.vcf.VCFConstants;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.SampleEntry;
import org.opencb.biodata.tools.commons.Converter;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.response.VariantQueryResult;
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
import org.opencb.opencga.storage.hadoop.variant.index.query.SampleIndexQuery;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexQueryParser;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleVariantIndexEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

import static org.opencb.opencga.storage.core.variant.query.VariantQueryUtils.addSamplesMetadataIfRequested;
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

    public SampleIndexOnlyVariantQueryExecutor(VariantHadoopDBAdaptor dbAdaptor, SampleIndexDBAdaptor sampleIndexDBAdaptor,
                                               String storageEngineId, ObjectMap options) {
        super(dbAdaptor.getMetadataManager(), storageEngineId, options);
        this.sampleIndexDBAdaptor = sampleIndexDBAdaptor;
        this.dbAdaptor = dbAdaptor;
        variantQueryParser = new VariantQueryParser(null, getMetadataManager());
        variantQueryProjectionParser = new VariantQueryProjectionParser(getMetadataManager());
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
                rawIterator = sampleIndexDBAdaptor.rawIterator(sampleIndexQuery);
            } catch (IOException e) {
                throw VariantQueryException.internalException(e).setQuery(inputQuery);
            }
            SampleVariantIndexEntryToVariantConverter converter =
                    new SampleVariantIndexEntryToVariantConverter(parseQuery, sampleIndexQuery);
            variantIterator = VariantDBIterator.wrapper(Iterators.transform(rawIterator, converter::convert));
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
            if (study.getSamples().size() != 1) {
                // Only one sample can be returned
                return false;
            }
            Integer sampleId = study.getSamples().get(0);
            String sampleName = metadataManager.getSampleName(study.getId(), sampleId);
            if (!sampleIndexQuery.getSamplesMap().containsKey(sampleName)) {
                // Sample query does not include the sample to be returned
                return false;
            }
            if (sampleIndexQuery.getSamplesMap().size() != 1) {
                // Can only filter by one sample
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
            return true;
        }
        // Either one or none studies can be returned.
        return projection.getStudyIds().size() == 0;
    }


    private class SampleVariantIndexEntryToVariantConverter implements Converter<SampleVariantIndexEntry, Variant> {

        private final boolean includeStudy;
        private String studyName;
        private String sampleName;
        private LinkedHashMap<String, Integer> samplesPosition;

        SampleVariantIndexEntryToVariantConverter(ParsedVariantQuery parseQuery, SampleIndexQuery sampleIndexQuery) {
            VariantQueryProjection projection = parseQuery.getProjection();
            includeStudy = !projection.getStudyIds().isEmpty();
            if (includeStudy) {
                int studyId = projection.getStudyIds().get(0); // only one study
                VariantQueryProjection.StudyVariantQueryProjection projectionStudy = projection.getStudy(studyId);
                studyName = projectionStudy.getStudyMetadata().getName();
                List<Integer> samples = projectionStudy.getSamples();
                if (samples.size() != 1) {
                    // This should never happen
                    throw new IllegalStateException("Unexpected number of samples. Expected one, found " + samples);
                }
                String sampleName = dbAdaptor.getMetadataManager().getSampleName(studyId, samples.get(0));
                samplesPosition = new LinkedHashMap<>();
                samplesPosition.put(sampleName, 0);
                if (parseQuery.getInputQuery().getBoolean(VariantQueryParam.INCLUDE_SAMPLE_ID.key())) {
                    this.sampleName = sampleName;
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
                studyEntry.setSamples(Collections.singletonList(
                        new SampleEntry(sampleName, null, Collections.singletonList(entry.getGenotype()))));
                studyEntry.setSortedSamplesPosition(samplesPosition);
                v.setStudies(Collections.singletonList(studyEntry));
            }
            return v;
        }
    }
}
