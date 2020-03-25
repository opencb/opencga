package org.opencb.opencga.storage.core.variant.query.projection;

import org.apache.commons.collections.CollectionUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.CohortMetadata;
import org.opencb.opencga.storage.core.metadata.models.FileMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;

import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;

public class VariantQueryProjectionParser {

    private final VariantStorageMetadataManager metadataManager;

    public VariantQueryProjectionParser(VariantStorageMetadataManager metadataManager) {
        this.metadataManager = metadataManager;
    }

    @Deprecated
    public static VariantQueryProjection parseVariantQueryFields(
            Query query, QueryOptions options, VariantStorageMetadataManager metadataManager) {
        return new VariantQueryProjectionParser(metadataManager).parseVariantQueryProjection(query, options);
    }

    public VariantQueryProjection parseVariantQueryProjection(Query query, QueryOptions options) {
        Set<VariantField> includeFields = VariantField.getIncludeFields(options);
        List<Integer> includeStudies = getIncludeStudies(query, options, metadataManager, includeFields);

        Map<Integer, VariantQueryProjection.StudyVariantQueryProjection> studies = new HashMap<>(includeStudies.size());
        for (Integer studyId : includeStudies) {
            studies.put(studyId, new VariantQueryProjection.StudyVariantQueryProjection());
        }
        for (Integer studyId : includeStudies) {
            StudyMetadata sm = metadataManager.getStudyMetadata(studyId);
            if (sm == null) {
                throw VariantQueryException.studyNotFound(studyId, metadataManager.getStudyNames());
            }
            studies.get(studyId).setStudyMetadata(sm);
        }

        Map<Integer, List<Integer>> sampleIdsMap = getIncludeSamples(query, options, includeStudies, metadataManager);
        for (VariantQueryProjection.StudyVariantQueryProjection study : studies.values()) {
            study.setSamples(sampleIdsMap.get(study.getId()));
        }
        int numTotalSamples = sampleIdsMap.values().stream().mapToInt(List::size).sum();
        skipAndLimitSamples(query, sampleIdsMap);
        int numSamples = sampleIdsMap.values().stream().mapToInt(List::size).sum();

        Map<Integer, List<Integer>> fileIdsMap = getIncludeFiles(query, includeStudies, includeFields,
                metadataManager, sampleIdsMap);
        for (VariantQueryProjection.StudyVariantQueryProjection study : studies.values()) {
            study.setFiles(fileIdsMap.get(study.getId()));
        }

        for (VariantQueryProjection.StudyVariantQueryProjection study : studies.values()) {
            int studyId = study.getId();
            List<Integer> filesInStudy = study.getFiles();
            study.setMultiFileSamples(new HashMap<>());
            Map<Integer, List<Integer>> multiMap = study.getMultiFileSamples();

            for (Integer sampleId : study.getSamples()) {
                Set<Integer> filesFromSample = new HashSet<>(metadataManager.getFileIdsFromSampleId(studyId, sampleId));
                multiMap.put(sampleId, new ArrayList<>(filesFromSample.size()));
                if (filesFromSample.size() > 1) {
                    if (VariantStorageEngine.LoadSplitData.MULTI.equals(metadataManager.getLoadSplitData(studyId, sampleId))) {
                        boolean hasAnyFile = false;
                        for (Integer fileFromSample : filesFromSample) {
                            if (filesInStudy.contains(fileFromSample)) {
                                hasAnyFile = true;
                                multiMap.get(sampleId).add(fileFromSample);
                            }
                        }
                        if (!hasAnyFile) {
                            for (Integer fileFromSample : filesFromSample) {
                                multiMap.get(sampleId).add(fileFromSample);
                            }
                        }
                    }
                }
            }
        }

        if (studies.values().stream().allMatch(s -> s.getFiles().isEmpty())) {
            includeFields.remove(VariantField.STUDIES_FILES);
            includeFields.removeAll(VariantField.STUDIES_FILES.getChildren());
        }

        if (studies.values().stream().allMatch(s -> s.getSamples().isEmpty())) {
            includeFields.remove(VariantField.STUDIES_SAMPLES);
            includeFields.removeAll(VariantField.STUDIES_SAMPLES.getChildren());
        }

        if (includeFields.contains(VariantField.STUDIES_STATS)) {
            for (VariantQueryProjection.StudyVariantQueryProjection study : studies.values()) {
                int studyId = study.getId();
                List<Integer> cohorts = new LinkedList<>();
                for (CohortMetadata cohort : metadataManager.getCalculatedCohorts(studyId)) {
                    cohorts.add(cohort.getId());
                }
//                metadataManager.cohortIterator(studyId).forEachRemaining(cohort -> {
//                    if (cohort.isReady()/* || cohort.isInvalid()*/) {
//                        cohorts.add(cohort.getId());
//                    }
//                });
                study.setCohorts(cohorts);
            }
        }

        return new VariantQueryProjection(includeFields, studies, numTotalSamples != numSamples, numSamples, numTotalSamples);
    }

    public static <T> void skipAndLimitSamples(Query query, Map<T, List<T>> sampleIds) {
        if (VariantQueryUtils.isValidParam(query, VariantQueryParam.SAMPLE_SKIP)) {
            int skip = query.getInt(VariantQueryParam.SAMPLE_SKIP.key());
            if (skip > 0) {
                for (List<T> value : sampleIds.values()) {
                    if (value.size() < skip) {
                        // Skip all samples from study
                        skip -= value.size();
                        value.clear();
                    } else {
//                        value = value.subList(skip, value.size());
                        value.subList(0, skip).clear();
                        break;
                    }
                }
            }
        }
        if (VariantQueryUtils.isValidParam(query, VariantQueryParam.SAMPLE_LIMIT)) {
            int limit = query.getInt(VariantQueryParam.SAMPLE_LIMIT.key());
            if (limit > 0) {
//                numSamples = limit;
                for (List<T> value : sampleIds.values()) {
                    if (limit >= value.size()) {
                        // include all samples from study
                        limit -= value.size();
                    } else if (limit == 0) {
                        value.clear();
                    } else {
//                        value = value.subList(0, limit);
                        value.subList(limit, value.size()).clear();
                        limit = 0;
                    }
                }
            }
        }
    }

    public static List<Integer> getIncludeStudies(Query query, QueryOptions options, VariantStorageMetadataManager metadataManager) {
        return getIncludeStudies(query, options, metadataManager, VariantField.getIncludeFields(options));
    }

    private static List<Integer> getIncludeStudies(Query query, QueryOptions options, VariantStorageMetadataManager metadataManager,
                                                   Set<VariantField> fields) {
        List<String> studiesList = getIncludeStudiesList(query, fields);

        List<Integer> studyIds;
        if (studiesList == null) {
            studyIds = metadataManager.getStudyIds();
            if (studyIds.size() > 1) {
                Map<Integer, List<Integer>> map = null;
                if (isIncludeSamplesDefined(query, fields)) {
                    map = getIncludeSamples(query, options, studyIds, metadataManager);
                } else if (isIncludeFilesDefined(query, fields)) {
                    map = getIncludeFiles(query, studyIds, fields,
                            metadataManager, null);
                }
                if (map != null) {
                    List<Integer> studyIdsFromSubFields = new ArrayList<>();
                    for (Map.Entry<Integer, List<Integer>> entry : map.entrySet()) {
                        if (!entry.getValue().isEmpty()) {
                            studyIdsFromSubFields.add(entry.getKey());
                        }
                    }
                    if (!studyIdsFromSubFields.isEmpty()) {
                        studyIds = studyIdsFromSubFields;
                    }
                }
            }
        } else {
            studyIds = metadataManager.getStudyIds(studiesList);
        }
        return studyIds;
    }

    public static List<String> getIncludeStudiesList(Query query, Set<VariantField> fields) {
        List<String> studies;
        if (!fields.contains(VariantField.STUDIES)) {
            studies = Collections.emptyList();
        } else if (VariantQueryUtils.isValidParam(query, INCLUDE_STUDY)) {
            String includeStudy = query.getString(VariantQueryParam.INCLUDE_STUDY.key());
            if (VariantQueryUtils.NONE.equals(includeStudy)) {
                studies = Collections.emptyList();
            } else if (VariantQueryUtils.ALL.equals(includeStudy)) {
                studies = null;
            } else {
                studies = query.getAsStringList(VariantQueryParam.INCLUDE_STUDY.key());
            }
        } else if (VariantQueryUtils.isValidParam(query, STUDY)) {
            String value = query.getString(STUDY.key());
            studies = new ArrayList<>(VariantQueryUtils.splitValue(value, VariantQueryUtils.checkOperator(value)));
            studies.removeIf(VariantQueryUtils::isNegated);
            // if empty, all the studies
            if (studies.isEmpty()) {
                studies = null;
            }
        } else {
            studies = null;
        }
        return studies;
    }

    public static boolean isIncludeFilesDefined(Query query, Set<VariantField> fields) {
        if (getIncludeFilesList(query, fields) != null) {
            return true;
        }
        return VariantQueryUtils.isValidParam(query, SAMPLE, true)
                || VariantQueryUtils.isValidParam(query, VariantQueryUtils.SAMPLE_MENDELIAN_ERROR, false)
                || VariantQueryUtils.isValidParam(query, VariantQueryUtils.SAMPLE_DE_NOVO, false)
                || VariantQueryUtils.isValidParam(query, INCLUDE_SAMPLE, false)
                || VariantQueryUtils.isValidParam(query, GENOTYPE, false);
    }

    /**
     * Get list of returned files for each study.
     * <p>
     * Use {@link VariantQueryParam#INCLUDE_FILE} if defined.
     * If missing, get non negated values from {@link VariantQueryParam#FILE}
     * If missing, get files from samples at {@link VariantQueryParam#SAMPLE}
     * <p>
     * Null for undefined returned files. If null, return ALL files.
     * Return NONE if empty list
     *
     * @param includeSamples
     * @param query                     Query with the QueryParams
     * @param studyIds                  Returned studies
     * @param fields                    Returned fields
     * @return List of fileIds to return.
     */
    private static Map<Integer, List<Integer>> getIncludeFiles(Query query, Collection<Integer> studyIds, Set<VariantField> fields,
                                                               VariantStorageMetadataManager metadataManager,
                                                               Map<Integer, List<Integer>> includeSamples) {

        List<String> includeSamplesList = includeSamples == null ? getIncludeSamplesList(query) : null;
        List<String> includeFilesList = getIncludeFilesList(query, fields);
        boolean returnAllFiles = VariantQueryUtils.ALL.equals(query.getString(INCLUDE_FILE.key()));

        Map<Integer, List<Integer>> files = new HashMap<>(studyIds.size());
        for (Integer studyId : studyIds) {
            StudyMetadata sm = metadataManager.getStudyMetadata(studyId);
            if (sm == null) {
                continue;
            }

            List<Integer> fileIds;
            if (includeFilesList != null) {
                fileIds = new ArrayList<>();
                for (String file : includeFilesList) {
                    Integer fileId = metadataManager.getFileId(studyId, file);
                    if (fileId != null) {
                        fileIds.add(fileId);
                    }
                }
            } else if (returnAllFiles) {
                fileIds = new ArrayList<>(metadataManager.getIndexedFiles(studyId));
            } else if (includeSamples != null) {
                List<Integer> sampleIds = includeSamples.get(studyId);
                Set<Integer> fileSet = metadataManager.getFileIdsFromSampleIds(studyId, sampleIds);
                fileIds = new ArrayList<>(fileSet);
            } else if (includeSamplesList != null && !includeSamplesList.isEmpty()) {
                List<Integer> sampleIds = new ArrayList<>();
                for (String sample : includeSamplesList) {
                    Integer sampleId = metadataManager.getSampleId(studyId, sample);
                    if (sampleId == null) {
//                        throw VariantQueryException.sampleNotFound(sample, sm.getName());
                        break;
                    }
                    sampleIds.add(sampleId);
                }
                Set<Integer> fileSet = metadataManager.getFileIdsFromSampleIds(studyId, sampleIds);
                fileIds = new ArrayList<>(fileSet);
            } else {
                // Return all files
                fileIds = new ArrayList<>(metadataManager.getIndexedFiles(studyId));
            }
            files.put(studyId, fileIds);
        }

        return files;
    }

    /**
     * Get list of returned files.
     * <p>
     * Use {@link VariantQueryParam#INCLUDE_FILE} if defined.
     * If missing, get non negated values from {@link VariantQueryParam#FILE}
     * <p>
     * Null for undefined returned files. If null, return ALL files.
     * Return NONE if empty list
     *
     * Does not validate if file names are valid at any study.
     *
     * @param query                     Query with the QueryParams
     * @param fields                    Returned fields
     * @return List of fileIds to return.
     */
    public static List<String> getIncludeFilesList(Query query, Set<VariantField> fields) {
        List<String> includeFiles;
        if (!fields.contains(VariantField.STUDIES_FILES)) {
            includeFiles = Collections.emptyList();
        } else {
            includeFiles = getIncludeFilesList(query);
        }
        return includeFiles;
    }

    public static List<String> getIncludeFilesList(Query query) {
        List<String> includeFiles = null;
        if (query.containsKey(INCLUDE_FILE.key())) {
            String files = query.getString(INCLUDE_FILE.key());
            if (files.equals(VariantQueryUtils.ALL)) {
                includeFiles = null;
            } else if (files.equals(VariantQueryUtils.NONE)) {
                includeFiles = Collections.emptyList();
            } else {
                includeFiles = query.getAsStringList(INCLUDE_FILE.key());
            }
            return includeFiles;
        }
        if (VariantQueryUtils.isValidParam(query, FILE)) {
            String files = query.getString(FILE.key());
            includeFiles = VariantQueryUtils.splitValue(files, VariantQueryUtils.checkOperator(files))
                    .stream()
                    .filter(value -> !VariantQueryUtils.isNegated(value))
                    .collect(Collectors.toList());
        }
        if (VariantQueryUtils.isValidParam(query, FILE_DATA)) {
            Map<String, String> infoMap = VariantQueryUtils.parseInfo(query).getValue();
            if (includeFiles == null) {
                includeFiles = new ArrayList<>(infoMap.size());
            }
            includeFiles.addAll(infoMap.keySet());
        }
        if (CollectionUtils.isEmpty(includeFiles)) {
            includeFiles = null;
        }
        return includeFiles;
    }

    public static boolean isIncludeSamplesDefined(Query query, Set<VariantField> fields) {
        if (getIncludeSamplesList(query, fields) != null) {
            return true;
        }
        return VariantQueryUtils.isValidParam(query, FILE, true) || VariantQueryUtils.isValidParam(query, INCLUDE_FILE, true);
    }

    public static Map<Integer, List<Integer>> getIncludeSamples(Query query, QueryOptions options,
                                                                VariantStorageMetadataManager variantStorageMetadataManager) {
        List<Integer> includeStudies = getIncludeStudies(query, options, variantStorageMetadataManager);
        return getIncludeSamples(query, options, includeStudies, variantStorageMetadataManager);
    }

    public static Map<Integer, List<Integer>> getIncludeSamples(
            Query query, QueryOptions options, Collection<Integer> studyIds,
            VariantStorageMetadataManager metadataManager) {

        List<String> includeFilesList = getIncludeFilesList(query);
        List<String> includeSamplesList = getIncludeSamplesList(query, options);
        boolean includeAllSamples = query.getString(VariantQueryParam.INCLUDE_SAMPLE.key()).equals(VariantQueryUtils.ALL);
        boolean includeNoneSamples = query.getString(VariantQueryParam.INCLUDE_SAMPLE.key()).equals(VariantQueryUtils.NONE);
        if (!includeNoneSamples) {
            if (includeSamplesList == null && CollectionUtils.isEmpty(includeFilesList)) {
                includeAllSamples = true;
            }
        }

        Map<Integer, List<Integer>> samples = new LinkedHashMap<>(studyIds.size());
        for (Integer studyId : studyIds) {
            StudyMetadata sm = metadataManager.getStudyMetadata(studyId);
            if (sm == null) {
                continue;
            }

            List<Integer> sampleIds;
            if (includeNoneSamples) {
                sampleIds = Collections.emptyList();
            } else if (includeAllSamples) {
                sampleIds = metadataManager.getIndexedSamples(sm.getId());
            } else if (includeSamplesList == null && CollectionUtils.isNotEmpty(includeFilesList)) {
                // Include from files
                Set<Integer> sampleSet = new LinkedHashSet<>();
                for (String file : includeFilesList) {
                    Integer fileId = metadataManager.getFileId(sm.getId(), file, true);
                    if (fileId == null) {
                        continue;
                    }
                    FileMetadata fileMetadata = metadataManager.getFileMetadata(studyId, fileId);
                    if (CollectionUtils.isNotEmpty(fileMetadata.getSamples())) {
                        sampleSet.addAll(fileMetadata.getSamples());
                    }
                }
                sampleIds = new ArrayList<>(sampleSet);
            } else {
                Object includeSampleRaw = query.get(INCLUDE_SAMPLE.key());
                if (includeSampleRaw instanceof Collection
                        && !((Collection) includeSampleRaw).isEmpty()
                        && ((Collection) includeSampleRaw).iterator().next() instanceof Integer) {
                    sampleIds = new ArrayList<>((Collection<Integer>) includeSampleRaw);
                } else {
                    sampleIds = new ArrayList<>(includeSamplesList.size());
                    for (String sample : includeSamplesList) {
                        Integer sampleId = metadataManager.getSampleId(studyId, sample);
                        if (sampleId != null) {
                            sampleIds.add(sampleId);
                        }
                    }
                    /*
                    LinkedHashMap<String, Integer> includeSamplesPosition
                            = metadataManager.getSamplesPosition(sm, includeSamplesSet);

                    sampleIds = Arrays.asList(new Integer[includeSamplesPosition.size()]);
                    for (Map.Entry<String, Integer> entry : includeSamplesPosition.entrySet()) {
                        String sample = entry.getKey();
                        Integer position = entry.getValue();
                        Integer sampleId = metadataManager.getSampleId(studyId, sample);
                        sampleIds.set(position, sampleId);
                    }
                     */
                }
                sampleIds.removeIf(id -> !metadataManager.isSampleIndexed(studyId, id));
            }
            samples.put(studyId, sampleIds);
        }

        return samples;
    }

    public static List<String> getIncludeSamplesList(Query query, QueryOptions options) {
        return getIncludeSamplesList(query, VariantField.getIncludeFields(options));
    }

    public static List<String> getIncludeSamplesList(Query query, Set<VariantField> fields) {
        List<String> samples;
        if (!fields.contains(VariantField.STUDIES_SAMPLES)) {
            samples = Collections.emptyList();
        } else {
            //Remove the studyName, if any
            samples = getIncludeSamplesList(query);
        }
        return samples;
    }

    /**
     * Get list of returned samples.
     * <p>
     * Null for undefined returned samples. If null, return ALL samples.
     * Return NONE if empty list
     *
     * @param query Query with the QueryParams
     * @return List of samples to return.
     */
    public static List<String> getIncludeSamplesList(Query query) {
        List<String> samples;
        if (VariantQueryUtils.isValidParam(query, INCLUDE_SAMPLE)) {
            String samplesString = query.getString(VariantQueryParam.INCLUDE_SAMPLE.key());
            if (samplesString.equals(VariantQueryUtils.ALL)) {
                samples = null; // Undefined. All by default
            } else if (samplesString.equals(VariantQueryUtils.NONE)) {
                samples = Collections.emptyList();
            } else {
                samples = query.getAsStringList(VariantQueryParam.INCLUDE_SAMPLE.key());
            }
        } else {
            samples = null;
            if (VariantQueryUtils.isValidParam(query, SAMPLE)) {
                String value = query.getString(SAMPLE.key());
                samples = VariantQueryUtils.splitValue(value, VariantQueryUtils.checkOperator(value))
                        .stream()
                        .filter((v) -> !VariantQueryUtils.isNegated(v)) // Discard negated
                        .collect(Collectors.toList());
            }
            if (VariantQueryUtils.isValidParam(query, GENOTYPE)) {
                HashMap<Object, List<String>> map = new LinkedHashMap<>();
                VariantQueryUtils.parseGenotypeFilter(query.getString(GENOTYPE.key()), map);
                if (samples == null) {
                    samples = new ArrayList<>(map.size());
                }
                map.keySet().stream().map(Object::toString).forEach(samples::add);
            }
            if (VariantQueryUtils.isValidParam(query, SAMPLE_DATA)) {
                Map<String, String> formatMap = VariantQueryUtils.parseFormat(query).getValue();
                if (samples == null) {
                    samples = new ArrayList<>(formatMap.size());
                }
                samples.addAll(formatMap.keySet());
            }
            if (VariantQueryUtils.isValidParam(query, VariantQueryUtils.SAMPLE_MENDELIAN_ERROR)) {
                String value = query.getString(VariantQueryUtils.SAMPLE_MENDELIAN_ERROR.key());
                if (samples == null) {
                    samples = new ArrayList<>();
                }
                samples.addAll(VariantQueryUtils.splitValue(value, VariantQueryUtils.checkOperator(value)));
            }
            if (VariantQueryUtils.isValidParam(query, VariantQueryUtils.SAMPLE_DE_NOVO)) {
                String value = query.getString(VariantQueryUtils.SAMPLE_DE_NOVO.key());
                if (samples == null) {
                    samples = new ArrayList<>();
                }
                samples.addAll(VariantQueryUtils.splitValue(value, VariantQueryUtils.checkOperator(value)));
            }
            if (CollectionUtils.isEmpty(samples)) {
                samples = null;
            }
        }
        if (samples != null) {
            samples = samples.stream()
                    .map(s -> s.contains(":") ? s.split(":")[1] : s)
                    .distinct() // Remove possible duplicates
                    .collect(Collectors.toList());
        }
        return samples;
    }
}
