package org.opencb.opencga.storage.core.variant.stats;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.biodata.models.variant.Genotype;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.models.variant.metadata.DepthCount;
import org.opencb.biodata.models.variant.metadata.IndelLength;
import org.opencb.biodata.models.variant.metadata.SampleVariantStats;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.FacetField;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.SAMPLE;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.STUDY;

public class SampleVariantStatsAggregationQuery {

    private static final ExecutorService THREAD_POOL = Executors.newCachedThreadPool(new BasicThreadFactory.Builder()
            .namingPattern("sample-variant-stats-pool-%s")
            .build());
    private final VariantStorageEngine engine;

    public SampleVariantStatsAggregationQuery(VariantStorageEngine engine) {
        this.engine = engine;
    }


    public DataResult<SampleVariantStats> sampleStatsQuery(String studyStr, String sample, Query inputQuery) throws StorageEngineException {

        if (StringUtils.isEmpty(sample)) {
            throw new VariantQueryException("Missing sample");
        }
        int studyId = engine.getMetadataManager().getStudyId(studyStr);
        int sampleId = engine.getMetadataManager().getSampleIdOrFail(studyId, sample);

        Query query;
        if (inputQuery == null) {
            query = new Query();
        } else {
            query = new Query(inputQuery);
        }

        query.put(STUDY.key(), studyStr);
        query.remove(SAMPLE.key());
        Future<DataResult<FacetField>> submit = THREAD_POOL.submit(() -> {
            DataResult<FacetField> result = engine.facet(
                    new Query(query)
                            .append(SAMPLE.key(), sample),
                    new QueryOptions(QueryOptions.FACET,
                            "chromosome;genotype;type;type[INDEL]>>length;titv;biotype;consequenceType;clinicalSignificance;depth;filter"));
            return result;
        });
        Future<DataResult<FacetField>> submitME = THREAD_POOL.submit(() -> {
            SampleMetadata sampleMetadata = engine.getMetadataManager().getSampleMetadata(studyId, sampleId);
            if (sampleMetadata.getMendelianErrorStatus().equals(TaskMetadata.Status.READY)) {
                DataResult<FacetField> result = engine.facet(
                        new Query(query)
                                .append(VariantQueryUtils.SAMPLE_MENDELIAN_ERROR.key(), sample),
                        new QueryOptions(QueryOptions.FACET,
                                "chromosome>>mendelianError"));
                return result;
            } else {
                return null;
            }
        });

        SampleVariantStats stats = new SampleVariantStats(
                sample,
                0,
                new HashMap<>(),
                new HashMap<>(),
                new HashMap<>(),
                new IndelLength(0, 0, 0, 0, 0),
                new HashMap<>(),
                0f,
                0f,
                0f,
                0f,
                new HashMap<>(),
                new DepthCount(0, 0, 0, 0, 0, 0),
                new HashMap<>(),
                new HashMap<>(),
                new HashMap<>()
        );

        StopWatch stopWatch = StopWatch.createStarted();
        try {
            processSimpleResult(submit.get(), stats);
            processMendelianErrorResult(submitME.get(), stats);
        } catch (InterruptedException | ExecutionException e) {
            throw VariantQueryException.internalException(e);
        }

        DataResult<SampleVariantStats> statsResult = new DataResult<>();
        statsResult.setResults(Collections.singletonList(stats));
        statsResult.setNumMatches(1);
//        statsResult.setEvents(result.getEvents());
        statsResult.setTime(((int) stopWatch.getTime(TimeUnit.MILLISECONDS)));
        return statsResult;
    }

    private void processSimpleResult(DataResult<FacetField> result, SampleVariantStats stats) {
        for (FacetField facetField : result.getResults()) {
            switch (facetField.getName()) {
                case "chromosome":
                    stats.setVariantCount(((int) facetField.getCount()));
                    for (FacetField.Bucket bucket : facetField.getBuckets()) {
                        stats.getChromosomeCount().put(bucket.getValue(), ((int) bucket.getCount()));
                    }
                    break;
                case "type":
                    if (facetField.getBuckets().size() == 1
                            && isIndel(facetField.getBuckets().get(0).getValue())
                            && CollectionUtils.isNotEmpty(facetField.getBuckets().get(0).getFacetFields())) {
                        for (FacetField.Bucket bucket : facetField.getBuckets().get(0).getFacetFields().get(0).getBuckets()) {
                            String[] split = StringUtils.replaceChars(bucket.getValue(), "[]() ", "").split(",");
                            String start = split[0];
                            String endStr = split[1];
                            int count = (int) bucket.getCount();
//                        [start, end)
                            IndelLength indelLength = stats.getIndelLengthCount();
                            if (endStr.equals("inf")) {
                                indelLength.setGte20(indelLength.getGte20() + count);
                            } else {
                                int end = Integer.parseInt(endStr);
                                if (end != 1) {
                                    if (end <= 5) {
                                        indelLength.setLt5(indelLength.getLt5() + count);
                                    } else if (end <= 10) {
                                        indelLength.setLt10(indelLength.getLt10() + count);
                                    } else if (end <= 20) {
                                        indelLength.setLt20(indelLength.getLt20() + count);
                                    } else {
                                        indelLength.setGte20(indelLength.getGte20() + count);
                                    }
                                }
                            }
                        }
                    } else {
                        for (FacetField.Bucket bucket : facetField.getBuckets()) {
                            String variantType = bucket.getValue();
                            if (variantType.equals("SNP")) {
                                variantType = "SNV";
                            }
                            if (variantType.equals("MNP")) {
                                variantType = "MNV";
                            }
                            stats.getTypeCount().merge(variantType, ((int) bucket.getCount()), Integer::sum);
                        }
                    }
                    break;
                case "genotype":
                    for (FacetField.Bucket bucket : facetField.getBuckets()) {
                        Map<String, Integer> genotypeCount = stats.getGenotypeCount();
                        String gtStr = bucket.getValue();
                        if (bucket.getValue().contains("|")) {
                            Genotype gt = new Genotype(gtStr);

                            gt = new Genotype(gt);
                            gt.setPhased(false);
                            gt.normalizeAllelesIdx();
                            gtStr = gt.toString();
                        }
                        genotypeCount.merge(gtStr, ((int) bucket.getCount()), Integer::sum);
                    }
                    break;
                case "filter":
                    for (FacetField.Bucket bucket : facetField.getBuckets()) {
                        stats.getFilterCount().put(bucket.getValue(), ((int) bucket.getCount()));
                    }
                    break;
                case "depth":
                    for (FacetField.Bucket bucket : facetField.getBuckets()) {
                        String[] split = StringUtils.replaceChars(bucket.getValue(), "[]() ", "").split(",");
                        String start = split[0];
                        String endStr = split[1];
                        int count = (int) bucket.getCount();
                        //[start, end)
                        DepthCount depthCount = stats.getDepthCount();
                        if (endStr.equals("inf")) {
                            depthCount.setGte20(depthCount.getGte20() + count);
                        } else {
                            int end = Integer.parseInt(endStr);
                            if (end <= 5) {
                                depthCount.setLt5(depthCount.getLt5() + count);
                            } else if (end <= 10) {
                                depthCount.setLt10(depthCount.getLt10() + count);
                            } else if (end <= 20) {
                                depthCount.setLt20(depthCount.getLt20() + count);
                            } else {
                                depthCount.setGte20(depthCount.getGte20() + count);
                            }
                        }
                    }
                    break;
                case "biotype":
                    for (FacetField.Bucket bucket : facetField.getBuckets()) {
                        stats.getBiotypeCount().put(bucket.getValue(), ((int) bucket.getCount()));
                    }
                    break;
                case "titv":
                    stats.setTiTvRatio(facetField.getAggregationValues().get(0).floatValue());
                    break;
                case "consequenceType":
                    for (FacetField.Bucket bucket : facetField.getBuckets()) {
                        stats.getConsequenceTypeCount().put(bucket.getValue(), ((int) bucket.getCount()));
                    }
                    break;
                case "clinicalSignificance":
                    for (FacetField.Bucket bucket : facetField.getBuckets()) {
                        stats.getClinicalSignificanceCount().put(bucket.getValue(), ((int) bucket.getCount()));
                    }
                    break;
                default:
                    throw new IllegalStateException("Unexpected facet field '" + facetField.getName() + "'");

            }
        }


        int numVariants = stats.getVariantCount();
        int numHet = 0;
        for (Map.Entry<String, Integer> entry : stats.getGenotypeCount().entrySet()) {
            if (GenotypeClass.HET.test(entry.getKey())) {
                numHet += entry.getValue();
            }
        }
        stats.setHeterozygosityRate(((float) numHet) / numVariants);
    }

    private boolean isIndel(String value) {
        return value.equals(VariantType.INDEL.toString())
                || value.equals(VariantType.INSERTION.toString())
                || value.equals(VariantType.DELETION.toString());
    }

    private void processMendelianErrorResult(DataResult<FacetField> facetFieldDataResult, SampleVariantStats stats) {
        if (facetFieldDataResult == null) {
            // Nothing to do!
            return;
        }
        for (FacetField facetField : facetFieldDataResult.getResults()) {
            switch (facetField.getName()) {
                case "chromosome":
                    for (FacetField.Bucket bucket : facetField.getBuckets()) {
                        Map<String, Integer> chrMap = stats.getMendelianErrorCount()
                                .computeIfAbsent(bucket.getValue(), k -> new HashMap<>());
                        for (FacetField meField : bucket.getFacetFields()) {
                            if (meField.getName().equals("mendelianError")) {
                                for (FacetField.Bucket meFieldBucket : meField.getBuckets()) {
                                    chrMap.put(meFieldBucket.getValue(), (int) meFieldBucket.getCount());
                                }
                            }
                        }
                    }
                    break;
                default:
                    throw new IllegalStateException("Unexpected facet field '" + facetField.getName() + "'");
            }
        }
    }
}
