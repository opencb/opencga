/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.storage.core.metadata;

import com.google.common.base.Throwables;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.adaptors.ProjectMetadataAdaptor;
import org.opencb.opencga.storage.core.metadata.adaptors.StudyMetadataDBAdaptor;
import org.opencb.opencga.storage.core.metadata.adaptors.VariantFileMetadataDBAdaptor;
import org.opencb.opencga.storage.core.metadata.adaptors.VariantStorageMetadataDBAdaptorFactory;
import org.opencb.opencga.storage.core.metadata.models.*;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.isNegated;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.removeNegation;
import static org.opencb.opencga.storage.core.variant.annotation.annotators.AbstractCellBaseVariantAnnotator.toCellBaseSpeciesName;

/**
 * @author Jacobo Coll <jacobo167@gmail.com>
 */
public class VariantStorageMetadataManager implements AutoCloseable {
    public static final String CACHED = "cached";
    public static final String READ_ONLY = "ro";
    public static final QueryOptions RO_CACHED_OPTIONS = new QueryOptions(READ_ONLY, true)
            .append(CACHED, true);
    protected static Logger logger = LoggerFactory.getLogger(VariantStorageMetadataManager.class);

    private final ProjectMetadataAdaptor projectDBAdaptor;
    private final StudyMetadataDBAdaptor studyMetadataDBAdaptor;
    private VariantFileMetadataDBAdaptor fileDBAdaptor;

    private final Map<String, StudyConfiguration> stringStudyConfigurationMap = new HashMap<>();
    private final Map<Integer, StudyConfiguration> intStudyConfigurationMap = new HashMap<>();

    public VariantStorageMetadataManager(ProjectMetadataAdaptor projectDBAdaptor, StudyMetadataDBAdaptor studyMetadataDBAdaptor,
                                         VariantFileMetadataDBAdaptor fileDBAdaptor) {
        this.projectDBAdaptor = projectDBAdaptor;
        this.studyMetadataDBAdaptor = studyMetadataDBAdaptor;
        this.fileDBAdaptor = fileDBAdaptor;
    }

    public VariantStorageMetadataManager(VariantStorageMetadataDBAdaptorFactory dbAdaptorFactory) {
        this.projectDBAdaptor = dbAdaptorFactory.buildProjectMetadataDBAdaptor();
        this.studyMetadataDBAdaptor = dbAdaptorFactory.buildStudyConfigurationDBAdaptor();
        this.fileDBAdaptor = dbAdaptorFactory.buildVariantFileMetadataDBAdaptor();
    }

    public long lockStudy(int studyId) throws StorageEngineException {
        try {
            return lockStudy(studyId, 10000, 20000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new StorageEngineException("Unable to lock the Study " + studyId, e);
        } catch (TimeoutException e) {
            throw new StorageEngineException("Unable to lock the Study " + studyId, e);
        }
    }

    public long lockStudy(int studyId, long lockDuration, long timeout) throws InterruptedException, TimeoutException {
        return studyMetadataDBAdaptor.lockStudy(studyId, lockDuration, timeout, null);
    }

    public long lockStudy(int studyId, long lockDuration, long timeout, String lockName) throws InterruptedException, TimeoutException {
        return studyMetadataDBAdaptor.lockStudy(studyId, lockDuration, timeout, lockName);
    }

    public void unLockStudy(int studyId, long lockId) {
        studyMetadataDBAdaptor.unLockStudy(studyId, lockId, null);
    }

    public void unLockStudy(int studyId, long lockId, String lockName) {
        studyMetadataDBAdaptor.unLockStudy(studyId, lockId, lockName);
    }

    public StudyConfiguration createStudy(String studyName) throws StorageEngineException {
        lockAndUpdateProject(projectMetadata -> {
            if (!getStudies(null).containsKey(studyName)) {
                StudyConfiguration studyConfiguration = new StudyConfiguration(newStudyId(), studyName);
                updateStudyConfiguration(studyConfiguration, null);
            }
            return projectMetadata;
        });
        return getStudyConfiguration(studyName, null).first();
    }

    public interface UpdateFunction<T, E extends Exception> {
        T update(T t) throws E;
    }

    @Deprecated
    public <E extends Exception> StudyConfiguration lockAndUpdateOld(String studyName, UpdateFunction<StudyConfiguration, E> updater)
            throws StorageEngineException, E {
        Integer studyId = getStudyId(studyName, null);
        return lockAndUpdateOld(studyId, updater);
    }

    @Deprecated
    public <E extends Exception> StudyConfiguration lockAndUpdateOld(int studyId, UpdateFunction<StudyConfiguration, E> updater)
            throws StorageEngineException, E {
        checkStudyId(studyId);
        long lock = lockStudy(studyId);
        try {
            StudyConfiguration sc = getStudyConfiguration(studyId, new QueryOptions(CACHED, false)).first();

            sc = updater.update(sc);

            updateStudyConfiguration(sc, QueryOptions.empty());
            return sc;
        } finally {
            unLockStudy(studyId, lock);
        }
    }


    public <E extends Exception> StudyMetadata lockAndUpdate(String studyName, UpdateFunction<StudyMetadata, E> updater)
            throws StorageEngineException, E {
        Integer studyId = getStudyId(studyName, null);
        return lockAndUpdate(studyId, updater);
    }


    public <E extends Exception> StudyMetadata lockAndUpdate(int studyId, UpdateFunction<StudyMetadata, E> updater)
            throws StorageEngineException, E {
        checkStudyId(studyId);
        long lock = lockStudy(studyId);
        try {
            StudyMetadata sm = getStudyMetadata(studyId);

            sm = updater.update(sm);

            updateStudyMetadata(sm);
            return sm;
        } finally {
            unLockStudy(studyId, lock);
        }
    }

    public StudyMetadata getStudyMetadata(String name) {
        Integer studyId = getStudyId(name, null);
        return studyMetadataDBAdaptor.getStudyMetadata(studyId, null);
    }

    public StudyMetadata getStudyMetadata(int id) {
        return studyMetadataDBAdaptor.getStudyMetadata(id, null);
    }

    public void updateStudyMetadata(StudyMetadata sm) {
        studyMetadataDBAdaptor.updateStudyMetadata(sm);
    }

    @Deprecated
    public final QueryResult<StudyConfiguration> getStudyConfiguration(String studyName, QueryOptions options) {
        if (StringUtils.isNumeric(studyName)) {
            return getStudyConfiguration(Integer.valueOf(studyName), options);
        }
        QueryResult<StudyConfiguration> result;
        final boolean cached = options != null && options.getBoolean(CACHED, false);
        final boolean readOnly = options != null && options.getBoolean(READ_ONLY, false);
        if (stringStudyConfigurationMap.containsKey(studyName)) {
            if (cached) {
                StudyConfiguration studyConfiguration = stringStudyConfigurationMap.get(studyName);
                if (!readOnly) {
                    studyConfiguration = studyConfiguration.newInstance();
                }
                return new QueryResult<>(studyConfiguration.getStudyName(), 0, 1, 1, "", "", Collections.singletonList(studyConfiguration));
            }
            Long timeStamp = stringStudyConfigurationMap.get(studyName).getTimeStamp();
            result = studyMetadataDBAdaptor.getStudyConfiguration(studyName, timeStamp, options);
            if (result.getNumTotalResults() == 0) { //No changes. Return old value
                StudyConfiguration studyConfiguration = stringStudyConfigurationMap.get(studyName);
                if (!readOnly) {
                    studyConfiguration = studyConfiguration.newInstance();
                }
                return new QueryResult<>(studyName, 0, 1, 1, "", "", Collections.singletonList(studyConfiguration));
            }
        } else {
            result = studyMetadataDBAdaptor.getStudyConfiguration(studyName, null, options);
        }

        StudyConfiguration studyConfiguration = result.first();
        if (studyConfiguration != null) {
            intStudyConfigurationMap.put(studyConfiguration.getStudyId(), studyConfiguration);
            stringStudyConfigurationMap.put(studyConfiguration.getStudyName(), studyConfiguration);
            if (studyName != null && !studyName.equals(studyConfiguration.getStudyName())) {
                stringStudyConfigurationMap.put(studyName, studyConfiguration);
            }
            if (!readOnly) {
                result.setResult(Collections.singletonList(studyConfiguration.newInstance()));
            }
        }
        return result;

    }

    @Deprecated
    public final QueryResult<StudyConfiguration> getStudyConfiguration(int studyId, QueryOptions options) {
        QueryResult<StudyConfiguration> result;
        final boolean cached = options != null && options.getBoolean(CACHED, false);
        final boolean readOnly = options != null && options.getBoolean(READ_ONLY, false);
        if (intStudyConfigurationMap.containsKey(studyId)) {
            if (cached) {
                StudyConfiguration studyConfiguration = intStudyConfigurationMap.get(studyId);
                if (!readOnly) {
                    studyConfiguration = studyConfiguration.newInstance();
                }
                return new QueryResult<>(studyConfiguration.getStudyName(), 0, 1, 1, "", "", Collections.singletonList(studyConfiguration));
            }
            result = studyMetadataDBAdaptor.getStudyConfiguration(studyId, intStudyConfigurationMap.get(studyId).getTimeStamp(), options);
            if (result.getNumTotalResults() == 0) { //No changes. Return old value
                StudyConfiguration studyConfiguration = intStudyConfigurationMap.get(studyId);
                if (!readOnly) {
                    studyConfiguration = studyConfiguration.newInstance();
                }
                return new QueryResult<>(studyConfiguration.getStudyName(), 0, 1, 1, "", "", Collections.singletonList(studyConfiguration));
            }
        } else {
            result = studyMetadataDBAdaptor.getStudyConfiguration(studyId, null, options);
        }

        StudyConfiguration studyConfiguration = result.first();
        if (studyConfiguration != null) {
            intStudyConfigurationMap.put(studyConfiguration.getStudyId(), studyConfiguration);
            stringStudyConfigurationMap.put(studyConfiguration.getStudyName(), studyConfiguration);
            if (!readOnly) {
                result.setResult(Collections.singletonList(studyConfiguration.newInstance()));
            }
        }
        return result;

    }

    public Thread buildShutdownHook(String jobOperationName, int studyId, Integer... files) {
        return buildShutdownHook(jobOperationName, studyId, Arrays.asList(files));
    }

    public Thread buildShutdownHook(String jobOperationName, int studyId, List<Integer> files) {
        return new Thread(() -> {
            try {
                logger.error("Shutdown hook while '" + jobOperationName + "' !");
                atomicSetStatus(studyId, BatchFileTask.Status.ERROR, jobOperationName, files);
            } catch (Exception e) {
                logger.error("Error terminating!", e);
                throw Throwables.propagate(e);
            }
        });
    }

    public List<String> getStudyNames(QueryOptions options) {
        return studyMetadataDBAdaptor.getStudyNames(options);
    }

    public String getStudyName(int studyId) {
        return getStudies(null).inverse().get(studyId);
    }

    public List<Integer> getStudyIds(QueryOptions options) {
        return studyMetadataDBAdaptor.getStudyIds(options);
    }

    public BiMap<String, Integer> getStudies(QueryOptions options) {
        return HashBiMap.create(studyMetadataDBAdaptor.getStudies(options));
    }

    @Deprecated
    public final QueryResult updateStudyConfiguration(StudyConfiguration studyConfiguration, QueryOptions options) {
        long timeStamp = System.currentTimeMillis();
        logger.debug("Timestamp : {} -> {}", studyConfiguration.getTimeStamp(), timeStamp);
        studyConfiguration.setTimeStamp(timeStamp);
        Map<Integer, String> headers = studyConfiguration.getHeaders();

        if (logger.isDebugEnabled()) {
            studyConfiguration.setHeaders(null);
            logger.debug("Updating studyConfiguration : {}", studyConfiguration.toJson());
            studyConfiguration.setHeaders(headers);
        }

        // Store a copy of the StudyConfiguration.
        StudyConfiguration copy = studyConfiguration.newInstance();
        stringStudyConfigurationMap.put(copy.getStudyName(), copy);
        intStudyConfigurationMap.put(copy.getStudyId(), copy);
        return studyMetadataDBAdaptor.updateStudyConfiguration(copy, options);
    }

    /**
     * Get studyIds from a list of studies.
     * Replaces studyNames for studyIds.
     * Excludes those studies that starts with '!'
     *
     * @param studiesNames  List of study names or study ids
     * @param options       Options
     * @return              List of study Ids
     */
    public List<Integer> getStudyIds(List studiesNames, QueryOptions options) {
        return getStudyIds(studiesNames, getStudies(options));
    }

    /**
     * Get studyIds from a list of studies.
     * Replaces studyNames for studyIds.
     * Excludes those studies that starts with '!'
     *
     * @param studiesNames  List of study names or study ids
     * @param studies       Map of available studies. See {@link VariantStorageMetadataManager#getStudies}
     * @return              List of study Ids
     */
    public List<Integer> getStudyIds(List studiesNames, Map<String, Integer> studies) {
        List<Integer> studiesIds;
        if (studiesNames == null) {
            return Collections.emptyList();
        }
        studiesIds = new ArrayList<>(studiesNames.size());
        for (Object studyObj : studiesNames) {
            Integer studyId = getStudyId(studyObj, true, studies);
            if (studyId != null) {
                studiesIds.add(studyId);
            }
        }
        return studiesIds;
    }

    public Integer getStudyId(Object studyObj, QueryOptions options) {
        return getStudyId(studyObj, options, true);
    }

    public Integer getStudyId(Object studyObj, QueryOptions options, boolean skipNegated) {
        if (studyObj instanceof Integer) {
            return ((Integer) studyObj);
        } else if (studyObj instanceof String && StringUtils.isNumeric((String) studyObj)) {
            return Integer.parseInt((String) studyObj);
        } else {
            return getStudyId(studyObj, skipNegated, getStudies(options));
        }
    }

    public Integer getStudyId(Object studyObj, boolean skipNegated, Map<String, Integer> studies) {
        Integer studyId;
        if (studyObj instanceof Integer) {
            studyId = ((Integer) studyObj);
        } else {
            String studyName = studyObj.toString();
            if (isNegated(studyName)) { //Skip negated studies
                if (skipNegated) {
                    return null;
                } else {
                    studyName = removeNegation(studyName);
                }
            }
            if (StringUtils.isNumeric(studyName)) {
                studyId = Integer.parseInt(studyName);
            } else {
                Integer value = studies.get(studyName);
                if (value == null) {
                    throw VariantQueryException.studyNotFound(studyName, studies.keySet());
                }
                studyId = value;
            }
        }
        if (!studies.containsValue(studyId)) {
            throw VariantQueryException.studyNotFound(studyId, studies.keySet());
        }
        return studyId;
    }

    /**
     * Given a study reference (name or id) and a default study, returns the associated StudyConfiguration.
     *
     * @param study     Study reference (name or id)
     * @param defaultStudyConfiguration Default studyConfiguration
     * @param options   Query options
     * @return          Assiciated StudyConfiguration
     * @throws    VariantQueryException is the study does not exists
     */
    @Deprecated
    public StudyConfiguration getStudyConfiguration(String study, StudyConfiguration defaultStudyConfiguration, QueryOptions options)
            throws VariantQueryException {
        StudyConfiguration studyConfiguration;
        if (StringUtils.isEmpty(study)) {
            studyConfiguration = defaultStudyConfiguration;
            if (studyConfiguration == null) {
                throw VariantQueryException.studyNotFound(study, getStudyNames(options));
            }
        } else if (StringUtils.isNumeric(study)) {
            int studyInt = Integer.parseInt(study);
            if (defaultStudyConfiguration != null && studyInt == defaultStudyConfiguration.getStudyId()) {
                studyConfiguration = defaultStudyConfiguration;
            } else {
                studyConfiguration = getStudyConfiguration(studyInt, options).first();
            }
            if (studyConfiguration == null) {
                throw VariantQueryException.studyNotFound(studyInt, getStudyNames(options));
            }
        } else {
            if (defaultStudyConfiguration != null && defaultStudyConfiguration.getStudyName().equals(study)) {
                studyConfiguration = defaultStudyConfiguration;
            } else {
                studyConfiguration = getStudyConfiguration(study, options).first();
            }
            if (studyConfiguration == null) {
                throw VariantQueryException.studyNotFound(study, getStudyNames(options));
            }
        }
        return studyConfiguration;
    }

    public <E extends Exception> ProjectMetadata lockAndUpdateProject(UpdateFunction<ProjectMetadata, E> function)
            throws StorageEngineException, E {
        Objects.requireNonNull(function);
        long lock;
        try {
            lock = projectDBAdaptor.lockProject(1000, 10000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new StorageEngineException("Unable to lock the Project", e);
        } catch (TimeoutException e) {
            throw new StorageEngineException("Unable to lock the Project", e);
        }
        try {
            ProjectMetadata projectMetadata = getProjectMetadata();
            int countersHash = (projectMetadata == null ? Collections.emptyMap() : projectMetadata.getCounters()).hashCode();

            projectMetadata = function.update(projectMetadata);
            int newCountersHash = (projectMetadata == null ? Collections.emptyMap() : projectMetadata.getCounters()).hashCode();

            // If the function modifies the internal counters, update them
            boolean updateCounters = countersHash != newCountersHash;

            projectDBAdaptor.updateProjectMetadata(projectMetadata, updateCounters);
            return projectMetadata;
        } finally {
            projectDBAdaptor.unLockProject(lock);
        }
    }

    public ProjectMetadata getProjectMetadata() {
        return projectDBAdaptor.getProjectMetadata().first();
    }

    public ProjectMetadata getProjectMetadata(ObjectMap options) throws StorageEngineException {
        ProjectMetadata projectMetadata = getProjectMetadata();
        if (options != null && (projectMetadata == null
                || StringUtils.isEmpty(projectMetadata.getSpecies()) && options.containsKey(VariantAnnotationManager.SPECIES)
                || StringUtils.isEmpty(projectMetadata.getAssembly()) && options.containsKey(VariantAnnotationManager.ASSEMBLY))) {

            projectMetadata = lockAndUpdateProject(pm -> {
                if (pm == null) {
                    pm = new ProjectMetadata();
                }
                if (pm.getRelease() <= 0) {
                    pm.setRelease(options.getInt(VariantStorageEngine.Options.RELEASE.key(),
                            VariantStorageEngine.Options.RELEASE.defaultValue()));
                }
                if (StringUtils.isEmpty(pm.getSpecies())) {
                    pm.setSpecies(toCellBaseSpeciesName(options.getString(VariantAnnotationManager.SPECIES)));
                }
                if (StringUtils.isEmpty(pm.getAssembly())) {
                    pm.setAssembly(options.getString(VariantAnnotationManager.ASSEMBLY));
                }

                return pm;
            });
        }
        return projectMetadata;
    }


    public QueryResult<Long> countVariantFileMetadata(Query query) {
        return fileDBAdaptor.count(query);
    }

    public QueryResult<VariantFileMetadata> getVariantFileMetadata(int studyId, int fileId, QueryOptions options)
            throws StorageEngineException {
        return fileDBAdaptor.get(studyId, fileId, options);
    }

    public Iterator<VariantFileMetadata> variantFileMetadataIterator(Query query, QueryOptions options)
            throws StorageEngineException {
        try {
            return fileDBAdaptor.iterator(query, options);
        } catch (IOException e) {
            throw new StorageEngineException("Error reading VariantFileMetadata", e);
        }
    }

    public void updateVariantFileMetadata(int studyId, VariantFileMetadata metadata) throws StorageEngineException {
        fileDBAdaptor.updateVariantFileMetadata(studyId, metadata);
    }

    public void updateVariantFileMetadata(String study, VariantFileMetadata metadata) throws StorageEngineException {
        Integer studyId = getStudyId(study, null);
        fileDBAdaptor.updateVariantFileMetadata(studyId, metadata);
    }

    public void deleteVariantFileMetadata(int studyId, int fileId) throws StorageEngineException {
        try {
            fileDBAdaptor.delete(studyId, fileId);
        } catch (IOException e) {
            throw new StorageEngineException("Error deleting VariantFileMetadata for file " + fileId, e);
        }
    }

    public FileMetadata getFileMetadata(int studyId, int fileId) {
        return studyMetadataDBAdaptor.getFileMetadata(studyId, fileId, null);
    }

    public void updateFileMetadata(int studyId, FileMetadata file) {
        studyMetadataDBAdaptor.updateFileMetadata(studyId, file, null);
    }

    public <E extends Exception> void updateFileMetadata(int studyId, int fileId, UpdateFunction<FileMetadata, E> update) throws E {
        FileMetadata fileMetadata = getFileMetadata(studyId, fileId);
        fileMetadata = update.update(fileMetadata);
        updateFileMetadata(studyId, fileMetadata);
    }

    public Integer getFileId(int studyId, String fileName) {
        return studyMetadataDBAdaptor.getFileId(studyId, fileName);
    }

    public String getFileName(int studyId, int fileId) {
        return getFileMetadata(studyId, fileId).getName();
    }

    public Integer getFileId(int studyId, Object fileObj) {
        return getFileId(studyId, fileObj, false);
    }

    public Integer getFileId(int studyId, Object fileObj, boolean onlyIndexed) {
        Integer fileId = parseResourceId(studyId, fileObj,
                o -> getFileId(studyId, o),
                o -> fileIdExists(studyId, o, onlyIndexed));

        if (fileId != null && onlyIndexed) {
            if (!getFileMetadata(studyId, fileId).isIndexed()) {
                fileId = null;
            }
        }

        return fileId;
    }

    /**
     * Get list of fileIds from a study.
     *
     * @param studyId Study id
     * @param files   List of files
     * @return List of file ids within this study
     * @throws VariantQueryException if the list of files contains files from other studies
     */
    public List<Integer> getFileIds(int studyId, List<?> files) throws VariantQueryException {
        Objects.requireNonNull(files);
        List<Integer> fileIds = new ArrayList<>(files.size());
        for (Object fileObj : files) {
            Integer fileId = getFileId(studyId, fileObj);
            if (fileId == null) {
                String studyName = getStudyMetadata(studyId).getName();
                throw VariantQueryException.fileNotFound(fileObj, studyName);
            }
            fileIds.add(fileId);
        }
        return fileIds;
    }

    public Pair<Integer, Integer> getFileIdPair(Object fileObj, boolean skipNegated, StudyMetadata defaultStudy) {
        return getResourcePair(fileObj, skipNegated, defaultStudy, this::fileIdExists, this::getFileId, "file");
    }

    private boolean fileIdExists(int studyId, int fileId) {
        return fileIdExists(studyId, fileId, false);
    }

    private boolean fileIdExists(int studyId, int fileId, boolean onlyIndexed) {
        FileMetadata fileMetadata = getFileMetadata(studyId, fileId);
        if (fileMetadata == null) {
            return false;
        } else {
            if (onlyIndexed) {
                return fileMetadata.isIndexed();
            } else {
                return true;
            }
        }
    }

    public LinkedHashSet<Integer> getIndexedFiles(int studyId) {
        return studyMetadataDBAdaptor.getIndexedFiles(studyId);
    }

    public SampleMetadata getSampleMetadata(int studyId, int sampleId) {
        return studyMetadataDBAdaptor.getSampleMetadata(studyId, sampleId, null);
    }

    public void updateSampleMetadata(int studyId, SampleMetadata sample) {
        studyMetadataDBAdaptor.updateSampleMetadata(studyId, sample, null);
    }

    public Iterator<SampleMetadata> sampleMetadataIterator(int studyId) {
        return studyMetadataDBAdaptor.sampleMetadataIterator(studyId);
    }

    public Integer getSampleId(int studyId, String sampleName) {
        return studyMetadataDBAdaptor.getSampleId(studyId, sampleName);
    }

    public Integer getSampleId(int studyId, Object sampleObj) {
        return getSampleId(studyId, sampleObj, false);
    }

    public Integer getSampleId(int studyId, Object sampleObj, boolean indexed) {
        Integer sampleId = parseResourceId(studyId, sampleObj,
                o -> getSampleId(studyId, o),
                o -> getSampleMetadata(studyId, o) != null);
        if (sampleId != null && indexed) {
            if (getIndexedSamplesMap(studyId).containsValue(sampleId)) {
                return sampleId;
            } else {
                return null;
            }
        } else {
            return sampleId;
        }
    }

    public String getSampleName(int studyId, int sampleId) {
        return getSampleMetadata(studyId, sampleId).getName();
    }

    public BiMap<String, Integer> getIndexedSamplesMap(int studyId) {
        return studyMetadataDBAdaptor.getIndexedSamplesMap(studyId);
    }

    public List<Integer> getIndexedSamples(int studyId) {
        return studyMetadataDBAdaptor.getIndexedSamples(studyId);
    }

//    public BiMap<String, Integer> getIndexedSamples(int studyId, int... fileIds) {
//        return studyMetadataDBAdaptor.getIndexedSamples(studyId);
//    }

    /**
     * Get a list of the samples to be returned, given a study and a list of samples to be returned.
     * The result can be used as SamplesPosition in {@link org.opencb.biodata.models.variant.StudyEntry#setSamplesPosition}
     *
     * @param sm                    Study metadata
     * @param includeSamples        List of samples to be included in the result
     * @return The samples IDs
     */
    public LinkedHashMap<String, Integer> getSamplesPosition(StudyMetadata sm, LinkedHashSet<?> includeSamples) {
        LinkedHashMap<String, Integer> samplesPosition;
        // If null, return ALL samples
        if (includeSamples == null) {
            List<Integer> orderedSamplesPosition = getIndexedSamples(sm.getId());
            samplesPosition = new LinkedHashMap<>(orderedSamplesPosition.size());
            for (Integer sampleId : orderedSamplesPosition) {
                samplesPosition.put(getSampleMetadata(sm.getId(), sampleId).getName(), samplesPosition.size());
            }
        } else {
            samplesPosition = new LinkedHashMap<>(includeSamples.size());
            int index = 0;
            List<Integer> indexedSamplesId = getIndexedSamples(sm.getId());
            for (Object includeSampleObj : includeSamples) {
                Integer sampleId = getSampleId(sm.getId(), includeSampleObj, false);
                String includeSample = getSampleMetadata(sm.getId(), sampleId).getName();

                if (!samplesPosition.containsKey(includeSample)) {
                    if (indexedSamplesId.contains(sampleId)) {
                        samplesPosition.put(includeSample, index++);
                    }
                }
            }
        }
        return samplesPosition;
    }

    public CohortMetadata getCohortMetadata(int studyId, int cohortId) {
        return studyMetadataDBAdaptor.getCohortMetadata(studyId, cohortId, null);
    }

    public void updateCohortMetadata(int studyId, CohortMetadata cohort) {
        studyMetadataDBAdaptor.updateCohortMetadata(studyId, cohort, null);
    }

    public Integer getCohortId(int studyId, String cohortName) {
        return studyMetadataDBAdaptor.getCohortId(studyId, cohortName);
    }

    public Integer getCohortId(int studyId, Object cohortObj) {
        return parseResourceId(studyId, cohortObj,
                o -> getCohortId(studyId, o),
                o -> getCohortMetadata(studyId, o) != null);
    }

    public List<Integer> getCohortIds(int studyId, List<?> cohorts) {
        Objects.requireNonNull(cohorts);
        List<Integer> cohortIds = new ArrayList<>(cohorts.size());
        for (Object cohortObj : cohorts) {
            Integer cohortId = getCohortId(studyId, cohortObj);
            if (cohortId == null) {
                List<String> availableCohorts = new LinkedList<>();
                cohortIterator(studyId).forEachRemaining(c -> availableCohorts.add(c.getName()));
                throw VariantQueryException.cohortNotFound(cohortObj.toString(), studyId, availableCohorts);
            }
            cohortIds.add(cohortId);
        }
        return cohortIds;
    }

    public String getCohortName(int studyId, int cohortId) {
        return getCohortMetadata(studyId, cohortId).getName();
    }

    public Iterator<CohortMetadata> cohortIterator(int studyId) {
        return studyMetadataDBAdaptor.cohortIterator(studyId);
    }

    public BatchFileTask getTask(int studyId, int taskId) {
        return studyMetadataDBAdaptor.getTask(studyId, taskId, null);
    }

    // Use taskId to filter task!
    @Deprecated
    public BatchFileTask getTask(int studyId, String taskName, List<Integer> fileIds) {
        BatchFileTask task = null;
        Iterator<BatchFileTask> it = taskIterator(studyId, true);
        while (it.hasNext()) {
            BatchFileTask t = it.next();
            if (t != null && t.getOperationName().equals(taskName) && t.getFileIds().equals(fileIds)) {
                task = t;
                break;
            }
        }
        if (task == null) {
            throw new IllegalStateException("Batch task " + taskName + " for files " + fileIds + " not found!");
        }
        return task;
    }

    public Iterator<BatchFileTask> taskIterator(int studyId) {
        return taskIterator(studyId, false);
    }

    public Iterator<BatchFileTask> taskIterator(int studyId, boolean reversed) {
        return studyMetadataDBAdaptor.taskIterator(studyId, reversed);
    }

    public void updateTask(int studyId, BatchFileTask task) {
        studyMetadataDBAdaptor.updateTask(studyId, task, null);
    }

    private Pair<Integer, Integer> getResourcePair(Object obj, boolean skipNegated, StudyMetadata defaultStudy,
                                                   BiPredicate<Integer, Integer> validId,
                                                   BiFunction<Integer, String, Integer> toId, String resourceName) {
        Objects.requireNonNull(obj, resourceName);
        final Integer studyId;
        final Integer id;

        if (obj instanceof Number) {
            id = ((Number) obj).intValue();
            if (defaultStudy != null) {
                if (getFileMetadata(defaultStudy.getId(), Math.abs(id)) != null) {
                    studyId = defaultStudy.getId();
                } else {
                    studyId = null;
                }
            } else {
                studyId = null;
            }
        } else {
            String resourceStr = obj.toString();
            if (isNegated(resourceStr)) { //Skip negated studies
                if (skipNegated) {
                    return null;
                } else {
                    resourceStr = removeNegation(resourceStr);
                }
            }
            String[] studyResource = VariantQueryUtils.splitStudyResource(resourceStr);
            if (studyResource.length == 2) {
                String study = studyResource[0];
                resourceStr = studyResource[1];
                StudyMetadata sm;
                if (defaultStudy != null
                        && (study.equals(defaultStudy.getName())
                        || NumberUtils.isParsable(study) && Integer.valueOf(study).equals(defaultStudy.getId()))) {
                    sm = defaultStudy;
                } else {
                    sm = getStudyMetadata(study);
                    if (sm == null) {
                        throw VariantQueryException.studyNotFound(study);
                    }
                }
                studyId = sm.getId();
                id = toId.apply(studyId, resourceStr);
            } else if (defaultStudy != null) {
                if (NumberUtils.isParsable(resourceStr)) {
                    id = Integer.parseInt(resourceStr);
                    if (validId.test(defaultStudy.getId(), Math.abs(id))) {
                        studyId = defaultStudy.getId();
                    } else {
                        studyId = null;
                    }
                } else {
                    id = toId.apply(defaultStudy.getId(), resourceStr);
                    if (id != null) {
                        studyId = defaultStudy.getId();
                    } else {
                        studyId = null;
                    }
                }
            } else if (NumberUtils.isParsable(resourceStr)) {
                studyId = null;
                id = Integer.parseInt(resourceStr);
            } else {
                studyId = null;
                id = null;
            }
        }

        if (studyId == null) {
            return getResourcePair(obj, toId, resourceName);
        }

        return Pair.of(studyId, id);
    }

    private Pair<Integer, Integer> getResourcePair(Object obj, BiFunction<Integer, String, Integer> toId, String resourceName) {
        Map<String, Integer> studies = getStudies(null);
        Collection<Integer> studyIds = studies.values();
        Integer resourceIdFromStudy;
        for (Integer studyId : studyIds) {
            StudyMetadata sm = getStudyMetadata(studyId);
            resourceIdFromStudy = toId.apply(sm.getId(), obj.toString());
            if (resourceIdFromStudy != null) {
                return Pair.of(sm.getId(), resourceIdFromStudy);
            }
        }
        throw VariantQueryException.missingStudyFor(resourceName, obj.toString(), studies.keySet());
    }

    private Integer parseResourceId(int studyId, Object obj, Function<String, Integer> toId, Predicate<Integer> validId) {
        final Integer id;
        if (obj instanceof Number) {
            int aux = ((Number) obj).intValue();
            if (validId.test(aux)) {
                id = aux;
            } else {
                id = null;
            }
        } else {
            String str = obj.toString();
            if (isNegated(str)) {
                str = removeNegation(str);
            }
            if (StringUtils.isNumeric(str)) {
                id = Integer.parseInt(str);
            } else {
                String[] split = VariantQueryUtils.splitStudyResource(str);
                if (split.length == 2) {
                    String study = split[0];
                    str = split[1];
                    String studyName = getStudyName(studyId);
                    if (study.equals(studyName)
                            || StringUtils.isNumeric(study) && Integer.valueOf(study).equals(studyId)) {
                        if (StringUtils.isNumeric(str)) {
                            int aux = Integer.valueOf(str);
                            if (validId.test(aux)) {
                                id = aux;
                            } else {
                                id = null;
                            }
                        } else {
                            id = toId.apply(str);
                        }
                    } else {
                        id = null;
                    }
                } else if (StringUtils.isNumeric(str)) {
                    int aux = Integer.valueOf(str);
                    if (validId.test(aux)) {
                        id = aux;
                    } else {
                        id = null;
                    }
                } else {
                    id = toId.apply(str);
                }
            }
        }
        return id;
    }

    // TODO: Return sampleId and studyId as a Pair
    public int getSampleId(Object sampleObj, StudyConfiguration defaultStudyConfiguration) {
        int sampleId;
        if (sampleObj instanceof Number) {
            sampleId = ((Number) sampleObj).intValue();
        } else {
            String sampleStr = sampleObj.toString();
            if (isNegated(sampleStr)) {
                sampleStr = removeNegation(sampleStr);
            }
            if (StringUtils.isNumeric(sampleStr)) {
                sampleId = Integer.parseInt(sampleStr);
            } else {
                String[] split = VariantQueryUtils.splitStudyResource(sampleStr);
                if (split.length == 2) {  //Expect to be as <study>:<sample>
                    String study = split[0];
                    sampleStr = split[1];
                    StudyConfiguration sc;
                    if (defaultStudyConfiguration != null && study.equals(defaultStudyConfiguration.getStudyName())) {
                        sc = defaultStudyConfiguration;
                    } else {
                        QueryResult<StudyConfiguration> queryResult = getStudyConfiguration(study, new QueryOptions());
                        if (queryResult.getResult().isEmpty()) {
                            throw VariantQueryException.studyNotFound(study);
                        }
                        sc = queryResult.first();
                    }
                    if (!sc.getSampleIds().containsKey(sampleStr)) {
                        throw VariantQueryException.sampleNotFound(sampleStr, study);
                    }
                    sampleId = sc.getSampleIds().get(sampleStr);
                } else if (defaultStudyConfiguration != null) {
                    if (!defaultStudyConfiguration.getSampleIds().containsKey(sampleStr)) {
                        throw VariantQueryException.sampleNotFound(sampleStr, defaultStudyConfiguration.getStudyName());
                    }
                    sampleId = defaultStudyConfiguration.getSampleIds().get(sampleStr);
                } else {
                    //Unable to identify that sample!
                    List<String> studyNames = getStudyNames(null);
                    throw VariantQueryException.missingStudyForSample(sampleStr, studyNames);
                }
            }
        }
        return sampleId;
    }

    // TODO: Return cohortId and studyId as a Pair
    /**
     * Finds the cohortId from a cohort reference.
     *
     * @param cohort    Cohort reference (name or id)
     * @param studyConfiguration  Default study configuration
     * @return  Cohort id
     * @throws VariantQueryException if the cohort does not exist
     */
    public int getCohortId(String cohort, StudyConfiguration studyConfiguration) throws VariantQueryException {
        int cohortId;
        if (StringUtils.isNumeric(cohort)) {
            cohortId = Integer.parseInt(cohort);
            if (!studyConfiguration.getCohortIds().containsValue(cohortId)) {
                throw VariantQueryException.cohortNotFound(cohortId, studyConfiguration.getStudyId(),
                        studyConfiguration.getCohortIds().keySet());
            }
        } else {
            Integer cohortIdNullable = studyConfiguration.getCohortIds().get(cohort);
            if (cohortIdNullable == null) {
                throw VariantQueryException.cohortNotFound(cohort, studyConfiguration.getStudyId(),
                        studyConfiguration.getCohortIds().keySet());
            }
            cohortId = cohortIdNullable;
        }
        return cohortId;
    }

    public Set<Integer> getFileIdsFromSampleIds(int studyId, Collection<Integer> sampleIds) {
        Set<Integer> fileIds = new HashSet<>();
        for (Integer sampleId : sampleIds) {
            fileIds.addAll(getSampleMetadata(studyId, sampleId).getFiles());
        }
        return fileIds;
    }

    /*
     * Before load file, register the new sample names.
     * If SAMPLE_IDS is missing, will auto-generate sampleIds
     */
    public void registerFileSamples(int studyId, int fileId, VariantFileMetadata variantFileMetadata)
            throws StorageEngineException {

        FileMetadata fileMetadata = getFileMetadata(studyId, fileId);


        //Assign new sampleIds
        LinkedHashSet<Integer> samples = new LinkedHashSet<>(variantFileMetadata.getSampleIds().size());
        for (String sample : variantFileMetadata.getSampleIds()) {
            samples.add(registerSample(studyId, fileId, sample));
        }

        fileMetadata.setSamples(samples);

        updateFileMetadata(studyId, fileMetadata);

//        if (studyConfiguration.getSamplesInFiles().containsKey(fileId)) {
//            LinkedHashSet<Integer> sampleIds = studyConfiguration.getSamplesInFiles().get(fileId);
//            List<String> missingSamples = new LinkedList<>();
//            for (String sample : variantFileMetadata.getSampleIds()) {
//                if (!sampleIds.contains(studyConfiguration.getSampleIds().get(sample))) {
//                    missingSamples.add(sample);
//                }
//            }
//            if (!missingSamples.isEmpty()) {
//                throw new StorageEngineException("Samples " + missingSamples.toString() + " were not in file " + fileId);
//            }
//            if (sampleIds.size() != variantFileMetadata.getSampleIds().size()) {
//                throw new StorageEngineException("Incorrect number of samples in file " + fileId);
//            }
//        } else {
//            LinkedHashSet<Integer> sampleIdsInFile = new LinkedHashSet<>(variantFileMetadata.getSampleIds().size());
//            for (String sample : variantFileMetadata.getSampleIds()) {
//                sampleIdsInFile.add(studyConfiguration.getSampleIds().get(sample));
//            }
//            studyConfiguration.getSamplesInFiles().put(fileId, sampleIdsInFile);
//        }
    }

    protected Integer registerSample(int studyId, Integer fileId, String sample) throws StorageEngineException {
        Integer sampleId = getSampleId(studyId, sample);
        SampleMetadata sampleMetadata;
        boolean update = false;
        if (sampleId == null) {
            //If the sample was not in the original studyId, a new SampleId is assigned.
            sampleId = newSampleId(studyId);

            sampleMetadata = new SampleMetadata(studyId, sampleId, sample);
            update = true;
        } else {
            sampleMetadata = getSampleMetadata(studyId, sampleId);
        }
        if (fileId != null) {
            sampleMetadata.getFiles().add(fileId);
            update = true;
        }
        if (update) {
            updateSampleMetadata(studyId, sampleMetadata);
        }
        return sampleId;
    }

    public int registerSearchIndexSamples(StudyConfiguration studyConfiguration, List<String> samples, boolean resume)
            throws StorageEngineException {
        if (samples == null || samples.isEmpty()) {
            throw new StorageEngineException("Missing samples to index");
        }

        List<Integer> sampleIds = new ArrayList<>(samples.size());

        List<String> alreadyIndexedSamples = new ArrayList<>();
        Set<Integer> searchIndexSampleSets = new HashSet<>();

        for (String sample : samples) {
            Integer sampleId = getSampleId(studyConfiguration.getId(), sample);
            if (sampleId == null) {
                throw VariantQueryException.sampleNotFound(sample, studyConfiguration.getStudyName());
            }
            sampleIds.add(sampleId);
            Integer searchIndex = studyConfiguration.getSearchIndexedSampleSets().get(sampleId);
            if (searchIndex != null) {
                searchIndexSampleSets.add(searchIndex);
                alreadyIndexedSamples.add(sample);
            }
        }

        final int id;
        if (!alreadyIndexedSamples.isEmpty()) {
            // All samples are already indexed, and in the same collection
            if (alreadyIndexedSamples.size() == samples.size() && searchIndexSampleSets.size() == 1) {
                id = searchIndexSampleSets.iterator().next();
                BatchFileTask.Status status = studyConfiguration.getSearchIndexedSampleSetsStatus().get(id);
                switch (status) {
                    case DONE:
                    case READY:
                        throw new StorageEngineException("Samples already in search index.");
                    case RUNNING:
                        // Resume if resume=true
                        if (!resume) {
                            throw new StorageEngineException("Samples already being indexed. Resume operation to continue.");
                        }
                    case ERROR:
                        // Resume
                        logger.info("Resume load of secondary index in status " + status);
                        break;
                    default:
                        throw new IllegalStateException("Unknown status " + status);
                }

            } else {
                throw new StorageEngineException("Samples " + alreadyIndexedSamples + " already in search index");
            }
        } else {
            id = newSearchIndexSamplesId(studyConfiguration);
            for (Integer sampleId : sampleIds) {
                studyConfiguration.getSearchIndexedSampleSets().put(sampleId, id);
            }
            studyConfiguration.getSearchIndexedSampleSetsStatus().put(id, BatchFileTask.Status.RUNNING);
        }

        return id;
    }

    /**
     * Check if the StudyConfiguration is correct.
     *
     * @param studyConfiguration StudyConfiguration to check
     * @throws StorageEngineException If object is null
     */
    public static void checkStudyConfiguration(StudyConfiguration studyConfiguration) throws StorageEngineException {
        if (studyConfiguration == null) {
            throw new StorageEngineException("StudyConfiguration is null");
        }
        checkStudyId(studyConfiguration.getStudyId());
    }

    public int registerFile(int studyId, VariantFileMetadata variantFileMetadata) throws StorageEngineException {
        int fileId = registerFile(studyId, variantFileMetadata.getPath());
        registerFileSamples(studyId, fileId, variantFileMetadata);
        return fileId;
    }

    /**
     * Check if the file(name,id) can be added to the StudyConfiguration.
     *
     * Will fail if:
     * fileName was already in the studyConfiguration.fileIds with a different fileId
     * fileId was already in the studyConfiguration.fileIds with a different fileName
     * fileId was already in the studyConfiguration.indexedFiles
     *
     * @param studyId   studyId
     * @param filePath  File path
     * @return fileId related to that file.
     * @throws StorageEngineException if the file is not valid for being loaded
     */
    public int registerFile(int studyId, String filePath) throws StorageEngineException {

        String fileName = Paths.get(filePath).getFileName().toString();
        Integer fileId = getFileId(studyId, fileName);


        FileMetadata fileMetadata;
        if (fileId != null) {
            fileMetadata = getFileMetadata(studyId, fileId);

            if (fileMetadata.isIndexed()) {
                throw StorageEngineException.alreadyLoaded(fileId, fileName);
            }

            // The file is not loaded. Check if it's being loaded.
            if (!fileMetadata.getPath().equals(filePath)) {
                // Only register if the file is being loaded. Otherwise, replace the filePath
                Iterator<BatchFileTask> iterator = taskIterator(studyId, true);
                while (iterator.hasNext()) {
                    BatchFileTask task = iterator.next();
                    if (task.getFileIds().contains(fileId)) {
                        if (task.getType().equals(BatchFileTask.Type.REMOVE)) {
                            // If the file was removed. Can be replaced.
                            break;
                        } else {
                            throw StorageEngineException.unableToExecute("Already registered with a different path", fileId, fileName);
                        }
                    }
                }
                // Replace filePath
                fileMetadata.setPath(filePath);
                updateFileMetadata(studyId, fileMetadata);
            }
        } else {
            fileId = newFileId(studyId);
            fileMetadata = new FileMetadata()
                    .setId(fileId)
                    .setName(fileName)
                    .setPath(filePath);
            updateFileMetadata(studyId, fileMetadata);
        }

        return fileId;
    }

    public void registerCohorts(String study, Map<String, ? extends Collection<String>> cohorts)
            throws StorageEngineException {
        registerCohorts(getStudyId(study, null), cohorts);
    }

    public Map<String, Integer> registerCohorts(int studyId, Map<String, ? extends Collection<String>> cohorts)
            throws StorageEngineException {
        Map<String, Integer> cohortIds = new HashMap<>();
        for (Map.Entry<String, ? extends Collection<String>> entry : cohorts.entrySet()) {
            String cohortName = entry.getKey();
            Collection<String> samples = entry.getValue();

            List<Integer> sampleIds = new ArrayList<>(samples.size());
            for (String sample : samples) {
                Integer sampleId = getSampleId(studyId, sample);
                if (sampleId == null) {
                    sampleId = registerSample(studyId, null, sample);
                }
                sampleIds.add(sampleId);
            }

            Integer cohortId = getCohortId(studyId, cohortName);
            List<Integer> oldSamples;
            CohortMetadata cohort;
            if (cohortId == null) {
                cohortId = newCohortId(studyId);
                cohort = new CohortMetadata(studyId, cohortId, cohortName, sampleIds);
                oldSamples = null;
            } else {
                cohort = getCohortMetadata(studyId, cohortId);
                oldSamples = cohort.getSamples();
                cohort.setSamples(sampleIds);
            }
            cohortIds.put(cohortName, cohortId);

            if (oldSamples != null && !oldSamples.equals(sampleIds)) {
                // Cohort has been modified!
                if (cohort.isReady()) {
                    cohort.setStatus(BatchFileTask.Status.ERROR);
                }
            }

            updateCohortMetadata(studyId, cohort);
        }
        return cohortIds;
    }

    protected int newFileId(int studyId) throws StorageEngineException {
//        return studyConfiguration.getFileIds().values().stream().max(Integer::compareTo).orElse(0) + 1;
        return projectDBAdaptor.generateId(studyId, "file");
    }

    protected int newSampleId(int studyId) throws StorageEngineException {
//        return studyConfiguration.getSampleIds().values().stream().max(Integer::compareTo).orElse(0) + 1;
        return projectDBAdaptor.generateId(studyId, "sample");
    }

    protected int newCohortId(int studyId) throws StorageEngineException {
//        return studyConfiguration.getCohortIds().values().stream().max(Integer::compareTo).orElse(0) + 1;
        return projectDBAdaptor.generateId(studyId, "cohort");
    }

    protected int newTaskId(int studyId) throws StorageEngineException {
        return projectDBAdaptor.generateId(studyId, "task");
    }

    protected int newStudyId() throws StorageEngineException {
        return projectDBAdaptor.generateId((Integer) null, "study");
    }

    protected int newSearchIndexSamplesId(StudyConfiguration studyConfiguration) throws StorageEngineException {
        return projectDBAdaptor.generateId(studyConfiguration, "searchIndexSamples");
    }


    public static void checkStudyId(int studyId) throws StorageEngineException {
        if (studyId < 0) {
            throw new StorageEngineException("Invalid studyId : " + studyId);
        }
    }

    public BatchFileTask.Status setStatus(int studyId, int taskId, BatchFileTask.Status status) {
        return setStatus(studyId, getTask(studyId, taskId), status);
    }

    @Deprecated
    public BatchFileTask.Status setStatus(int studyId, String taskName, List<Integer> fileIds, BatchFileTask.Status status) {
        BatchFileTask task = getTask(studyId, taskName, fileIds);
        return setStatus(studyId, task, status);
    }

    private BatchFileTask.Status setStatus(int studyId, BatchFileTask task, BatchFileTask.Status status) {
        BatchFileTask.Status previousStatus = task.currentStatus();
        task.addStatus(Calendar.getInstance().getTime(), status);
        updateTask(studyId, task);

        return previousStatus;
    }

    public BatchFileTask.Status atomicSetStatus(int studyId, BatchFileTask.Status status, String operationName,
                                                List<Integer> files)
            throws StorageEngineException {
        return setStatus(studyId, operationName, files, status);
    }

    @Deprecated
    public static BatchFileTask getOperation(StudyConfiguration studyConfiguration, String operationName, List<Integer> files) {
        List<BatchFileTask> batches = studyConfiguration.getBatches();
        BatchFileTask operation = null;
        for (int i = batches.size() - 1; i >= 0; i--) {
            operation = batches.get(i);
            if (operation.getOperationName().equals(operationName) && operation.getFileIds().equals(files)) {
                break;
            }
            operation = null;
        }
        return operation;
    }

    @Deprecated
    public static BatchFileTask.Status setStatus(StudyConfiguration studyConfiguration, BatchFileTask.Status status,
                                                 String operationName, List<Integer> files) {
        BatchFileTask operation = getOperation(studyConfiguration, operationName, files);
        if (operation == null) {
            throw new IllegalStateException("Batch operation " + operationName + " for files " + files + " not found!");
        }
        BatchFileTask.Status previousStatus = operation.currentStatus();
        operation.addStatus(Calendar.getInstance().getTime(), status);
        return previousStatus;
    }

    /**
     * Adds a new {@link BatchFileTask} to the StudyConfiguration.
     *
     * Only allow one running operation at the same time
     *  If any operation is in ERROR and is not the same operation, throw {@link StorageEngineException#otherOperationInProgressException}
     *  If any operation is DONE, RUNNING, is same operation and resume=true, continue
     *  If all operations are ready, continue
     *
     * @param studyConfiguration StudyConfiguration
     * @param jobOperationName   Job operation name used to create the jobName and as {@link BatchFileTask#operationName}
     * @param fileIds            Files to be processed in this batch.
     * @param resume             Resume operation. Assume that previous operation went wrong.
     * @param type               Operation type as {@link BatchFileTask#type}
     * @return                   The current batchOperation
     * @throws StorageEngineException if the operation can't be executed
     */
    @Deprecated
    public static BatchFileTask addBatchOperation(StudyConfiguration studyConfiguration, String jobOperationName,
                                                  List<Integer> fileIds, boolean resume, BatchFileTask.Type type)
            throws StorageEngineException {
        throw new UnsupportedOperationException("Deprecated");
    }

    /**
     * Adds a new {@link BatchFileTask} to the StudyConfiguration.
     *
     * Allow execute concurrent operations depending on the "allowConcurrent" predicate.
     *  If any operation is in ERROR, is not the same operation, and concurrency is not allowed,
     *      throw {@link StorageEngineException#otherOperationInProgressException}
     *  If any operation is DONE, RUNNING, is same operation and resume=true, continue
     *  If all operations are ready, continue
     *
     * @param studyId            Study id
     * @param jobOperationName   Job operation name used to create the jobName and as {@link BatchFileTask#operationName}
     * @param fileIds            Files to be processed in this batch.
     * @param resume             Resume operation. Assume that previous operation went wrong.
     * @param type               Operation type as {@link BatchFileTask#type}
     * @param allowConcurrent    Predicate to test if the new operation can be executed at the same time as a non ready operation.
     *                           If not, throws {@link StorageEngineException#otherOperationInProgressException}
     * @return                   The current batchOperation
     * @throws StorageEngineException if the operation can't be executed
     */
    public BatchFileTask addBatchOperation(int studyId, String jobOperationName,
                                                  List<Integer> fileIds, boolean resume, BatchFileTask.Type type,
                                                  Predicate<BatchFileTask> allowConcurrent)
            throws StorageEngineException {

        BatchFileTask resumeOperation = null;
        boolean updateOperation = false;
        Iterator<BatchFileTask> iterator = taskIterator(studyId);
        while (iterator.hasNext()) {
            BatchFileTask operation = iterator.next();
            BatchFileTask.Status currentStatus = operation.currentStatus();

            switch (currentStatus) {
                case READY:
                    // Ignore ready operations
                    break;
                case DONE:
                case RUNNING:
                    if (!resume) {
                        if (operation.sameOperation(fileIds, type, jobOperationName)) {
                            throw StorageEngineException.currentOperationInProgressException(operation);
                        } else {
                            if (allowConcurrent.test(operation)) {
                                break;
                            } else {
                                throw StorageEngineException.otherOperationInProgressException(operation, jobOperationName, fileIds);
                            }
                        }
                    }
                    // DO NOT BREAK!. Resuming last loading, go to error case.
                case ERROR:
                    if (!operation.sameOperation(fileIds, type, jobOperationName)) {
                        if (allowConcurrent.test(operation)) {
                            break;
                        } else {
                            throw StorageEngineException.otherOperationInProgressException(operation, jobOperationName, fileIds, resume);
                        }
                    } else {
                        logger.info("Resuming last batch operation \"" + operation.getOperationName() + "\" due to error.");
                        resumeOperation = operation;
                    }
                    break;
                default:
                    throw new IllegalArgumentException("Unknown Status " + currentStatus);
            }
        }

        BatchFileTask operation;
        if (resumeOperation == null) {
            operation = new BatchFileTask(newTaskId(studyId), jobOperationName, fileIds, System.currentTimeMillis(), type);
            updateOperation = true;
        } else {
            operation = resumeOperation;
        }

        if (!Objects.equals(operation.currentStatus(), BatchFileTask.Status.DONE)) {
            operation.addStatus(Calendar.getInstance().getTime(), BatchFileTask.Status.RUNNING);
            updateOperation = true;
        }
        if (updateOperation) {
            updateTask(studyId, operation);
        }
        return operation;
    }

    @Override
    public void close() throws IOException {
        studyMetadataDBAdaptor.close();
    }
}
