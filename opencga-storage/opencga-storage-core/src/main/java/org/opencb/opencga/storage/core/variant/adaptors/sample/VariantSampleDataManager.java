package org.opencb.opencga.storage.core.variant.adaptors.sample;

import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.avro.SampleEntry;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.*;
import org.opencb.opencga.storage.core.variant.query.VariantQueryParser;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by jacobo on 27/03/19.
 */
public class VariantSampleDataManager {

    public static final String SAMPLE_BATCH_SIZE = "sampleBatchSize";
    public static final int SAMPLE_BATCH_SIZE_DEFAULT = 10000;
    public static final String MERGE = "merge";

    private final VariantDBAdaptor dbAdaptor;
    private final VariantStorageMetadataManager metadataManager;
    private final Map<String, String> normalizeGt = new HashMap<>();
    private final Logger logger = LoggerFactory.getLogger(VariantSampleDataManager.class);

    public VariantSampleDataManager(VariantDBAdaptor dbAdaptor) {
        this.dbAdaptor = dbAdaptor;
        this.metadataManager = dbAdaptor.getMetadataManager();

    }

    public final DataResult<Variant> getSampleData(String variant, String study, QueryOptions options) {
        options = options == null ? new QueryOptions() : options;
        int sampleLimit = options.getInt(SAMPLE_BATCH_SIZE, SAMPLE_BATCH_SIZE_DEFAULT);
        return getSampleData(variant, study, options, sampleLimit);
    }

    public final DataResult<Variant> getSampleData(String variant, String study, QueryOptions options, int sampleLimit) {
        options = options == null ? new QueryOptions() : options;


        StudyMetadata studyMetadata = metadataManager.getStudyMetadata(study);
        boolean studyWithGts = !studyMetadata.getAttributes().getBoolean(VariantStorageOptions.EXCLUDE_GENOTYPES.key(),
                VariantStorageOptions.EXCLUDE_GENOTYPES.defaultValue());

        Set<String> genotypes = new HashSet<>(options.getAsStringList(VariantQueryParam.GENOTYPE.key()));
        if (genotypes.isEmpty()) {
            if (studyWithGts) {
                genotypes.add(GenotypeClass.MAIN_ALT.toString());
            } else {
                genotypes.add(GenotypeClass.NA_GT_VALUE);
            }
        }
        List<String> loadedGenotypes = studyMetadata.getAttributes().getAsStringList(VariantStorageOptions.LOADED_GENOTYPES.key());
        if (loadedGenotypes.size() == 1) {
            if (loadedGenotypes.contains(GenotypeClass.NA_GT_VALUE) || loadedGenotypes.contains("-1")) {
                genotypes = Collections.singleton(GenotypeClass.NA_GT_VALUE);
            }
        } else if (loadedGenotypes.contains(GenotypeClass.NA_GT_VALUE)) {
            genotypes.add(GenotypeClass.NA_GT_VALUE);
        } else {
            genotypes = new HashSet<>(GenotypeClass.filter(genotypes, loadedGenotypes));
        }

        List<String> includeSamples = options.getAsStringList(VariantQueryParam.INCLUDE_SAMPLE.key());

        return getSampleData(variant, study, options, includeSamples, studyWithGts, genotypes, sampleLimit);
    }

    protected DataResult<Variant> getSampleData(
            String variantStr, String study, QueryOptions options, List<String> includeSamples, boolean studyWithGts, Set<String> genotypes,
            int sampleLimit) {
        options = options == null ? new QueryOptions() : options;
        int studyId = metadataManager.getStudyId(study);
        int skip = Math.max(0, options.getInt(QueryOptions.SKIP, 0));
        int limit = Math.max(0, options.getInt(QueryOptions.LIMIT, 10));
        int dbTime = 0;
        int gtCount = 0;
        List<SampleEntry> sampleEntries = new ArrayList<>(limit);
        Map<String, Integer> filesIdx = new HashMap<>();
        List<FileEntry> files = new ArrayList<>(limit);
        Map<String, VariantStats> stats = Collections.emptyMap();
        int fileIdxFormatIdx = -1;
        List<String> format = null;
        VariantAnnotation annotation = null;

        int sampleSkip = 0;
        int readSamples = 0;
        int queries = 0;
        while (true) {
            queries++;
            Query query = new Query(VariantQueryParam.ID.key(), variantStr)
                    .append(VariantQueryParam.STUDY.key(), study)
                    .append(VariantQueryParam.SAMPLE_LIMIT.key(), sampleLimit)
                    .append(VariantQueryParam.SAMPLE_SKIP.key(), sampleSkip)
                    .append(VariantQueryParam.INCLUDE_FORMAT.key(), VariantQueryUtils.ALL + ","
                            + VariantQueryParser.SAMPLE_ID + ","
                            + VariantQueryParser.FILE_IDX);
            if (includeSamples != null && !includeSamples.isEmpty()) {
                query.append(VariantQueryParam.INCLUDE_SAMPLE.key(), includeSamples); // if empty, will return all
            }
            sampleSkip += sampleLimit;
            QueryOptions variantQueryOptions;
            if (queries == 1) {
                variantQueryOptions = new QueryOptions();
            } else {
                variantQueryOptions = new QueryOptions(QueryOptions.EXCLUDE,
                        Arrays.asList(VariantField.ANNOTATION, VariantField.STUDIES_STATS));
            }

            DataResult<Variant> result = dbAdaptor.get(query, variantQueryOptions);
            if (result.getNumResults() == 0) {
                throw VariantQueryException.variantNotFound(variantStr);
            }
            dbTime += result.getTime();
            Variant partialVariant = result.first();

            StudyEntry partialStudy = partialVariant.getStudies().get(0);

            if (queries == 1) {
                annotation = partialVariant.getAnnotation();
                stats = partialStudy.getStats();
                format = partialStudy.getFormat();
                fileIdxFormatIdx = format.indexOf(VariantQueryParser.FILE_IDX);
            }
            boolean hasGt = format.get(0).equals("GT");
            List<String> samples = partialStudy.getOrderedSamplesName();
            readSamples += samples.size();
            for (int i = 0; i < samples.size(); i++) {
                String sample = samples.get(i);
                List<String> sampleData = partialStudy.getSamples().get(i).getData();

                String gt = hasGt ? sampleData.get(0) : GenotypeClass.NA_GT_VALUE;
                if (gt.equals(".")) {
                    gt = GenotypeClass.NA_GT_VALUE;
                }

                if (genotypes.contains(gt)) {
                    // Skip other genotypes
                    gtCount++;
                    if (gtCount > skip) {
                        if (sampleEntries.size() < limit) {
                            Integer sampleId = metadataManager.getSampleId(studyId, sample);
                            FileEntry fileEntry = null;
                            for (Integer fileId : metadataManager.getFileIdsFromSampleIds(studyId, Collections.singleton(sampleId))) {
                                String fileName = metadataManager.getFileName(studyId, fileId);
                                fileEntry = partialStudy.getFile(fileName);
                                break;
                            }
                            if (fileEntry == null) {
                                if (gt.equals(GenotypeClass.NA_GT_VALUE)) {
                                    continue;
                                }
                                List<String> fileNames = new LinkedList<>();
                                for (Integer fileId : metadataManager.getFileIdsFromSampleIds(studyId, Collections.singleton(sampleId))) {
                                    fileNames.add(metadataManager.getFileName(studyId, fileId));
                                }
                                throw new VariantQueryException("No file found for sample '" + sample + "', expected any of " + fileNames);
                            }
                            Integer fileIdx = filesIdx.get(fileEntry.getFileId());
                            if (fileIdx == null) {
                                fileIdx = files.size();
                                filesIdx.put(fileEntry.getFileId(), fileIdx);
                                files.add(fileEntry);
                            }
                            sampleData.set(fileIdxFormatIdx, String.valueOf(fileIdx));
                            sampleEntries.add(new SampleEntry(sample, fileIdx, sampleData));
                        }
                    }
                }
            }

            if (samples.size() < sampleLimit) {
//                logger.debug("Exit end samples");
                break;
            }

            if (gtCount >= skip + limit) {
//                logger.debug("Exit limit");
                break;
            }
        }

        Variant variant = new Variant(variantStr);
        variant.setAnnotation(annotation);
        StudyEntry studyEntry = new StudyEntry(study);
        variant.addStudyEntry(studyEntry);
        studyEntry.setSamples(sampleEntries);
        studyEntry.setFiles(files);
        studyEntry.setStats(stats);
        studyEntry.setFormat(format);
//        String msg = "Queries : " + queries + " , readSamples : " + readSamples;
        return new DataResult<>(dbTime, Collections.emptyList(), 1, Collections.singletonList(variant), 1);
    }

    protected final String normalizeGt(String gt) {
        if (gt.contains("|")) {
            return normalizeGt.computeIfAbsent(gt, k -> {
                Genotype genotype = new Genotype(k.replace('|', '/'));
                genotype.normalizeAllelesIdx();
                return genotype.toString();
            });
        }
        return gt;
    }

}
