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
import com.google.common.collect.Iterators;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.adaptors.*;
import org.opencb.opencga.storage.core.metadata.models.*;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.annotation.annotators.AbstractCellBaseVariantAnnotator.toCellBaseSpeciesName;
import static org.opencb.opencga.storage.core.variant.query.VariantQueryUtils.isNegated;
import static org.opencb.opencga.storage.core.variant.query.VariantQueryUtils.removeNegation;

/**
 * @author Jacobo Coll <jacobo167@gmail.com>
 */
public class VariantStorageMetadataManager implements AutoCloseable {
    public static final String SECONDARY_INDEX_PREFIX = "__SECONDARY_INDEX_COHORT_";

    protected static Logger logger = LoggerFactory.getLogger(VariantStorageMetadataManager.class);

    private final ProjectMetadataAdaptor projectDBAdaptor;
    private final StudyMetadataDBAdaptor studyDBAdaptor;
    private final FileMetadataDBAdaptor fileDBAdaptor;
    private final SampleMetadataDBAdaptor sampleDBAdaptor;
    private final CohortMetadataDBAdaptor cohortDBAdaptor;
    private final TaskMetadataDBAdaptor taskDBAdaptor;

    private final MetadataCache<String, Integer> sampleIdCache;
    private final MetadataCache<Integer, String> sampleNameCache;
    private final MetadataCache<Integer, Boolean> sampleIdIndexedCache;
    private final MetadataCache<Integer, LinkedHashSet<Integer>> sampleIdsFromFileIdCache;
    // Store ordinal from VariantStorageEngine.SplitData. -1 for null values.
    private final MetadataCache<Integer, Integer> splitDataCache;

    private final MetadataCache<String, Integer> fileIdCache;
    private final MetadataCache<Integer, String> fileNameCache;
    private final MetadataCache<Integer, Boolean> fileIdIndexedCache;
    private final MetadataCache<Integer, List<Integer>> fileIdsFromSampleIdCache;

    private final MetadataCache<String, Integer> cohortIdCache;
    private final MetadataCache<Integer, String> cohortNameCache;

    private final int lockDuration;
    private final int lockTimeout;

    public VariantStorageMetadataManager(VariantStorageMetadataDBAdaptorFactory dbAdaptorFactory) {
        this.projectDBAdaptor = dbAdaptorFactory.buildProjectMetadataDBAdaptor();
        this.studyDBAdaptor = dbAdaptorFactory.buildStudyMetadataDBAdaptor();
        this.fileDBAdaptor = dbAdaptorFactory.buildFileMetadataDBAdaptor();
        this.sampleDBAdaptor = dbAdaptorFactory.buildSampleMetadataDBAdaptor();
        this.cohortDBAdaptor = dbAdaptorFactory.buildCohortMetadataDBAdaptor();
        this.taskDBAdaptor = dbAdaptorFactory.buildTaskDBAdaptor();
        lockDuration = dbAdaptorFactory.getConfiguration()
                .getInt(VariantStorageOptions.METADATA_LOCK_DURATION.key(), VariantStorageOptions.METADATA_LOCK_DURATION.defaultValue());
        lockTimeout = dbAdaptorFactory.getConfiguration()
                .getInt(VariantStorageOptions.METADATA_LOCK_TIMEOUT.key(), VariantStorageOptions.METADATA_LOCK_TIMEOUT.defaultValue());
        sampleIdCache = new MetadataCache<>(sampleDBAdaptor::getSampleId);
        sampleNameCache = new MetadataCache<>((studyId, sampleId) -> {
            SampleMetadata sampleMetadata = sampleDBAdaptor.getSampleMetadata(studyId, sampleId, null);
            if (sampleMetadata == null) {
                throw VariantQueryException.sampleNotFound(sampleId, getStudyName(studyId));
            }
            return sampleMetadata.getName();
        });
        sampleIdIndexedCache = new MetadataCache<>((studyId, sampleId) -> {
            SampleMetadata sampleMetadata = sampleDBAdaptor.getSampleMetadata(studyId, sampleId, null);
            if (sampleMetadata == null) {
                throw VariantQueryException.sampleNotFound(sampleId, getStudyName(studyId));
            }
            return sampleMetadata.isIndexed();
        });
        sampleIdsFromFileIdCache = new MetadataCache<>((studyId, fileId) -> {
            FileMetadata fileMetadata = fileDBAdaptor.getFileMetadata(studyId, fileId, null);
            if (fileMetadata == null) {
                throw VariantQueryException.fileNotFound(fileId, getStudyName(studyId));
            }
            return fileMetadata.getSamples();
        });
        splitDataCache = new MetadataCache<>((studyId, sampleId) -> {
            SampleMetadata sampleMetadata = sampleDBAdaptor.getSampleMetadata(studyId, sampleId, null);
            if (sampleMetadata == null) {
                throw VariantQueryException.sampleNotFound(sampleId, getStudyName(studyId));
            }
            VariantStorageEngine.SplitData splitData = sampleMetadata.getSplitData();
            if (splitData == null) {
                return -1;
            } else {
                return splitData.ordinal();
            }
        });

        fileIdCache = new MetadataCache<>(fileDBAdaptor::getFileId);
        fileNameCache = new MetadataCache<>((studyId, fileId) -> {
            FileMetadata fileMetadata = fileDBAdaptor.getFileMetadata(studyId, fileId, null);
            if (fileMetadata == null) {
                throw VariantQueryException.fileNotFound(fileId, getStudyName(studyId));
            }
            return fileMetadata.getName();
        });
        fileIdIndexedCache = new MetadataCache<>((studyId, fileId) -> {
            FileMetadata fileMetadata = fileDBAdaptor.getFileMetadata(studyId, fileId, null);
            if (fileMetadata == null) {
                throw VariantQueryException.fileNotFound(fileId, getStudyName(studyId));
            }
            return fileMetadata.isIndexed();
        });
        fileIdsFromSampleIdCache = new MetadataCache<>((studyId, sampleId) -> {
            SampleMetadata sampleMetadata = getSampleMetadata(studyId, sampleId);
            if (sampleMetadata == null) {
                throw VariantQueryException.sampleNotFound(sampleId, getStudyName(studyId));
            }
            return sampleMetadata.getFiles();
        });

        cohortIdCache = new MetadataCache<>(cohortDBAdaptor::getCohortId);
        cohortNameCache = new MetadataCache<>((studyId, cohortId) -> {
            CohortMetadata cohortMetadata = cohortDBAdaptor.getCohortMetadata(studyId, cohortId, null);
            if (cohortMetadata == null) {
                throw VariantQueryException.cohortNotFound(cohortId, studyId, getAvailableCohorts(studyId));
            }
            return cohortMetadata.getName();
        });
    }

    public Lock lockStudy(int studyId) throws StorageEngineException {
        return lockStudy(studyId, lockDuration, lockTimeout);
    }

    public Lock lockStudy(int studyId, long lockDuration, long timeout) throws StorageEngineException {
        return studyDBAdaptor.lock(studyId, lockDuration, timeout, null);
    }

    public Lock lockStudy(int studyId, long lockDuration, long timeout, String lockName) throws StorageEngineException {
        return studyDBAdaptor.lock(studyId, lockDuration, timeout, lockName);
    }

    public StudyMetadata createStudy(String studyName) throws StorageEngineException {
        updateProjectMetadata(projectMetadata -> {
            if (!getStudies().containsKey(studyName)) {
                StudyMetadata studyMetadata = new StudyMetadata(newStudyId(), studyName);
                unsecureUpdateStudyMetadata(studyMetadata);
            }
            return projectMetadata;
        });
        return getStudyMetadata(studyName);
    }

    public interface UpdateFunction<T, E extends Exception> {
        T update(T t) throws E;
    }

    public <E extends Exception> StudyMetadata updateStudyMetadata(Object study, UpdateFunction<StudyMetadata, E> updater)
            throws StorageEngineException, E {
        int studyId = getStudyId(study);

        Lock lock = lockStudy(studyId);
        try {
            StudyMetadata sm = getStudyMetadata(studyId);

            sm = updater.update(sm);

            lock.checkLocked();
            unsecureUpdateStudyMetadata(sm);
            return sm;
        } finally {
            lock.unlock();
        }
    }

    public StudyMetadata getStudyMetadata(String name) {
        Integer studyId = getStudyIdOrNull(name);
        if (studyId == null) {
            return null;
        } else {
            return studyDBAdaptor.getStudyMetadata(studyId, null);
        }
    }

    public StudyMetadata getStudyMetadata(int id) {
        return studyDBAdaptor.getStudyMetadata(id, null);
    }

    public void unsecureUpdateStudyMetadata(StudyMetadata sm) {
        studyDBAdaptor.updateStudyMetadata(sm);
    }

    @Deprecated
    public final DataResult<StudyConfiguration> getStudyConfiguration(Object study, QueryOptions options) {
        if (study instanceof Number) {
            return studyDBAdaptor.getStudyConfiguration(((Number) study).intValue(), null, options);
        } else {
            String studyName = study.toString();
            if (StringUtils.isNumeric(studyName)) {
                return studyDBAdaptor.getStudyConfiguration(Integer.valueOf(studyName), null, options);
            } else {
                return studyDBAdaptor.getStudyConfiguration(studyName, null, options);
            }
        }
    }

    public Thread buildShutdownHook(String jobOperationName, int studyId, int taskId) {
        return new Thread(() -> {
            try {
                logger.error("Shutdown hook while '" + jobOperationName + "' !");
                setStatus(studyId, taskId, TaskMetadata.Status.ERROR);
            } catch (Exception e) {
                logger.error("Error terminating!", e);
                throw Throwables.propagate(e);
            }
        });
    }

    public List<String> getStudyNames() {
        return studyDBAdaptor.getStudyNames(null);
    }

    public String getStudyName(int studyId) {
        return getStudies(null).inverse().get(studyId);
    }

    public List<Integer> getStudyIds() {
        return studyDBAdaptor.getStudyIds(null);
    }

    public BiMap<String, Integer> getStudies() {
        return getStudies(null);
    }

    public BiMap<String, Integer> getStudies(QueryOptions options) {
        return HashBiMap.create(studyDBAdaptor.getStudies(options));
    }

    @Deprecated
    public final DataResult updateStudyConfiguration(StudyConfiguration studyConfiguration, QueryOptions options) {
        long timeStamp = System.currentTimeMillis();
        logger.debug("Timestamp : {} -> {}", studyConfiguration.getTimeStamp(), timeStamp);
        studyConfiguration.setTimeStamp(timeStamp);

        return studyDBAdaptor.updateStudyConfiguration(studyConfiguration, options);
    }

    public Integer getStudyIdOrNull(Object studyObj) {
        return getStudyIdOrNull(studyObj, getStudies(null));
    }

    public Integer getStudyIdOrNull(Object studyObj, Map<String, Integer> studies) {
        Integer studyId;
        if (studyObj instanceof Integer) {
            studyId = ((Integer) studyObj);
        } else {
            String studyName = studyObj.toString();
            if (isNegated(studyName)) {
                studyName = removeNegation(studyName);
            }
            if (StringUtils.isNumeric(studyName)) {
                studyId = Integer.parseInt(studyName);
            } else {
                studyId = studies.get(studyName);
            }
        }

        if (!studies.containsValue(studyId)) {
            return null;
        } else {
            return studyId;
        }
    }

    /**
     * Get studyIds from a list of studies.
     * Replaces studyNames for studyIds.
     * Excludes those studies that starts with '!'
     *
     * @param studiesNames  List of study names or study ids
     * @return              List of study Ids
     */
    public List<Integer> getStudyIds(List<?> studiesNames) {
        return getStudyIds(studiesNames, getStudies(null));
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
    public List<Integer> getStudyIds(List<?> studiesNames, Map<String, Integer> studies) {
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

    public int getStudyId(Object studyObj) {
        return getStudyId(studyObj, false, getStudies(null));
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

    public VariantScoreMetadata getVariantScoreMetadata(int studyId, int scoreId) {
        return getStudyMetadata(studyId).getVariantScores().stream().filter(s -> s.getId() == scoreId).findFirst().orElse(null);
    }

    public VariantScoreMetadata getVariantScoreMetadata(int studyId, String scoreMetadataName) {
        return getVariantScoreMetadata(getStudyMetadata(studyId), scoreMetadataName);
    }

    public VariantScoreMetadata getVariantScoreMetadata(StudyMetadata studyMetadata, String scoreName) {
        for (VariantScoreMetadata s : studyMetadata.getVariantScores()) {
            if (s.getName().equalsIgnoreCase(scoreName)) {
                return s;
//                if (s.getCohortId1() == cohort1 && Objects.equals(s.getCohortId2(), cohort2)) {
//                    return s;
//                }
            }
        }
        return null;
    }

    public VariantScoreMetadata getOrCreateVariantScoreMetadata(int studyId, String scoreMetadataName, int cohort1, Integer cohort2)
            throws StorageEngineException {
        StudyMetadata sm = updateStudyMetadata(studyId, studyMetadata -> {
            VariantScoreMetadata scoreMetadata = getVariantScoreMetadata(studyMetadata, scoreMetadataName);
            if (scoreMetadata != null) {
                if (cohort1 == scoreMetadata.getCohortId1() && Objects.equals(scoreMetadata.getCohortId2(), cohort2)) {
                    return studyMetadata;
                } else {
                    String cohort1Name = getCohortName(studyId, scoreMetadata.getCohortId1());
                    String cohort2Name = scoreMetadata.getCohortId2() == null ? null : getCohortName(studyId, scoreMetadata.getCohortId2());

                    throw new StorageEngineException(
                            "Variant score '" + scoreMetadataName + "' already exists in study '" + studyMetadata.getName() + "' "
                                    + "for cohorts '" + cohort1Name + "' and '" + cohort2Name + "'. "
                                    + "Attempting to overwrite the VariantScore with cohorts '" + getCohortName(studyId, cohort1) + "' "
                                    + "and '" + (cohort2 == null ? null : getCohortName(studyId, cohort2)) + "'");
                }
            }

            if (scoreMetadataName.isEmpty()) {
                throw new IllegalArgumentException("Variant score name can not be empty");
            } else if (StringUtils.containsAny(scoreMetadataName, ':', ' ')) {
                throw new IllegalArgumentException("Variant score name can not contain ':' or ' '");
            }
            int scoreId = newVariantScoreId(studyMetadata.getId());
            scoreMetadata = new VariantScoreMetadata(studyMetadata.getId(), scoreId, scoreMetadataName, "", cohort1, cohort2);
            studyMetadata.getVariantScores().add(scoreMetadata);
            return studyMetadata;
        });
        return getVariantScoreMetadata(sm, scoreMetadataName);
    }

    public <E extends Exception> VariantScoreMetadata updateVariantScoreMetadata(int studyId, int scoreId,
                                                                                 UpdateFunction<VariantScoreMetadata, E> updater)
            throws StorageEngineException, E {
        updateStudyMetadata(studyId, studyMetadata -> {
            VariantScoreMetadata scoreMetadata = studyMetadata.getVariantScores()
                    .stream()
                    .filter(s -> s.getId() == scoreId)
                    .findFirst()
                    .orElse(null);

            if (scoreMetadata == null) {
                throw VariantQueryException.scoreNotFound(scoreId, getStudyName(studyId));
            }

            updater.update(scoreMetadata);

            return studyMetadata;
        });

        return getVariantScoreMetadata(studyId, scoreId);
    }

    public void removeVariantScoreMetadata(VariantScoreMetadata scoreMetadata) throws StorageEngineException {
        updateStudyMetadata(scoreMetadata.getStudyId(), studyMetadata -> {
            studyMetadata.getVariantScores().removeIf(s -> s.getId() == scoreMetadata.getId());
            return studyMetadata;
        });
    }

    public <E extends Exception> ProjectMetadata updateProjectMetadata(UpdateFunction<ProjectMetadata, E> function)
            throws StorageEngineException, E {
        Objects.requireNonNull(function);
        Lock lock;
        try {
            lock = projectDBAdaptor.lockProject(lockDuration, lockTimeout);
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

            lock.checkLocked();
            projectDBAdaptor.updateProjectMetadata(projectMetadata, updateCounters);
            return projectMetadata;
        } finally {
            lock.unlock();
        }
    }

    public boolean exists() {
        return projectDBAdaptor.exists();
    }

    public ProjectMetadata getProjectMetadata() {
        return projectDBAdaptor.getProjectMetadata().first();
    }

    public ProjectMetadata getProjectMetadata(ObjectMap options) throws StorageEngineException {
        ProjectMetadata projectMetadata = getProjectMetadata();
        if (options != null && (projectMetadata == null
                || StringUtils.isEmpty(projectMetadata.getSpecies()) && options.containsKey(VariantStorageOptions.SPECIES.key())
                || StringUtils.isEmpty(projectMetadata.getAssembly()) && options.containsKey(VariantStorageOptions.ASSEMBLY.key()))) {

            projectMetadata = updateProjectMetadata(pm -> {
                if (pm == null) {
                    pm = new ProjectMetadata();
                }
                if (pm.getRelease() <= 0) {
                    pm.setRelease(options.getInt(VariantStorageOptions.RELEASE.key(),
                            VariantStorageOptions.RELEASE.defaultValue()));
                }
                if (StringUtils.isEmpty(pm.getSpecies())) {
                    pm.setSpecies(toCellBaseSpeciesName(options.getString(VariantStorageOptions.SPECIES.key())));
                }
                if (StringUtils.isEmpty(pm.getAssembly())) {
                    pm.setAssembly(options.getString(VariantStorageOptions.ASSEMBLY.key()));
                }

                return pm;
            });
        }
        return projectMetadata;
    }

    public DataResult<VariantFileMetadata> getVariantFileMetadata(int studyId, int fileId, QueryOptions options)
            throws StorageEngineException {
        return fileDBAdaptor.getVariantFileMetadata(studyId, fileId, options);
    }

    public Iterator<VariantFileMetadata> variantFileMetadataIterator(int studyId, QueryOptions options)
            throws StorageEngineException {
        Query query = new Query(FileMetadataDBAdaptor.VariantFileMetadataQueryParam.STUDY_ID.key(), studyId);
        return variantFileMetadataIterator(query, options);
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
        int studyId = getStudyId(study);
        fileDBAdaptor.updateVariantFileMetadata(studyId, metadata);
    }

    public void removeVariantFileMetadata(int studyId, int fileId) throws StorageEngineException {
        try {
            fileDBAdaptor.removeVariantFileMetadata(studyId, fileId);
        } catch (IOException e) {
            throw new StorageEngineException("Error deleting VariantFileMetadata for file " + fileId, e);
        }
    }

    public FileMetadata getFileMetadata(int studyId, Object fileObj) {
        Integer fileId = getFileId(studyId, fileObj, false, false);
        if (fileId == null) {
            return null;
        } else {
            return fileDBAdaptor.getFileMetadata(studyId, fileId, null);
        }
    }

    public void unsecureUpdateFileMetadata(int studyId, FileMetadata file) {
        file.setStudyId(studyId);
        fileDBAdaptor.updateFileMetadata(studyId, file, null);
    }

    public <E extends Exception> FileMetadata updateFileMetadata(int studyId, int fileId, UpdateFunction<FileMetadata, E> update)
            throws E, StorageEngineException {
        getFileName(studyId, fileId); // Check file exists
        Lock lock = fileDBAdaptor.lock(studyId, fileId, lockDuration, lockTimeout);
        try {
            FileMetadata fileMetadata = getFileMetadata(studyId, fileId);
            fileMetadata = update.update(fileMetadata);
            lock.checkLocked();
            unsecureUpdateFileMetadata(studyId, fileMetadata);
            return fileMetadata;
        } finally {
            lock.unlock();
        }
    }

    private Integer getFileId(int studyId, String fileName) {
        checkName("File name", fileName);
        // Allow fileIds as fileName
        if (StringUtils.isNumeric(fileName)) {
            return Integer.valueOf(fileName);
        } else {
            return fileIdCache.get(studyId, fileName);
        }
    }

    public String getFileName(int studyId, int fileId) {
        return fileNameCache.get(studyId, fileId);
    }

    public Integer getFileId(int studyId, Object fileObj) {
        return getFileId(studyId, fileObj, false);
    }

    public Integer getFileId(int studyId, Object fileObj, boolean onlyIndexed) {
        return getFileId(studyId, fileObj, onlyIndexed, true);
    }

    private Integer getFileId(int studyId, Object fileObj, boolean onlyIndexed, boolean validate) {
        if (fileObj instanceof URI) {
            fileObj = UriUtils.fileName(((URI) fileObj));
        } else if (fileObj instanceof Path) {
            fileObj = ((Path) fileObj).getFileName().toString();
        }
        Integer fileId = parseResourceId(studyId, fileObj,
                o -> getFileId(studyId, o),
                validate ? o -> fileIdExists(studyId, o, onlyIndexed) : o -> true);

        if (fileId != null && onlyIndexed) {
            if (isFileIndexed(studyId, fileId)) {
                return fileId;
            } else {
                return null;
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

    private boolean fileIdExists(int studyId, int fileId, boolean indexed) {
        if (indexed) {
            return isFileIndexed(studyId, fileId);
        } else {
            return fileIdExists(studyId, fileId);
        }
    }

    private boolean fileIdExists(int studyId, int fileId) {
        return getFileName(studyId, fileId) != null;
    }

    public boolean isFileIndexed(int studyId, Integer fileId) {
        return fileIdIndexedCache.get(studyId, fileId, false);
    }

    public LinkedHashSet<Integer> getIndexedFiles(int studyId) {
        return fileDBAdaptor.getIndexedFiles(studyId);
    }

    public void addIndexedFiles(int studyId, List<Integer> fileIds) throws StorageEngineException {
        // First update the samples
        Set<Integer> samples = new HashSet<>();
        for (Integer fileId : fileIds) {
            FileMetadata fileMetadata = getFileMetadata(studyId, fileId);
            samples.addAll(fileMetadata.getSamples());
        }
        for (Integer sample : samples) {
            if (!isSampleIndexed(studyId, sample)) {
                updateSampleMetadata(studyId, sample, sampleMetadata -> sampleMetadata.setIndexStatus(TaskMetadata.Status.READY));
            }
        }

        // Finally, update the files and update the list of indexed files
        for (Integer fileId : fileIds) {
            String name = updateFileMetadata(studyId, fileId, fileMetadata -> fileMetadata.setIndexStatus(TaskMetadata.Status.READY))
                    .getName();
            logger.info("Register file " + name + " as INDEXED");
        }
        fileDBAdaptor.addIndexedFiles(studyId, fileIds);
        fileIdsFromSampleIdCache.clear();
        fileIdIndexedCache.clear();
    }

    public void removeIndexedFiles(int studyId, Collection<Integer> fileIds) throws StorageEngineException {
        Set<Integer> samples = new HashSet<>();
        for (Integer fileId : fileIds) {
            updateFileMetadata(studyId, fileId, fileMetadata -> {
                samples.addAll(fileMetadata.getSamples());
                return fileMetadata.setIndexStatus(TaskMetadata.Status.NONE);
            });
//            deleteVariantFileMetadata(studyId, fileId);
        }
        for (Integer sample : samples) {
            updateSampleMetadata(studyId, sample, sampleMetadata -> {
                boolean indexed = false;
                for (Integer fileId : sampleMetadata.getFiles()) {
                    if (!fileIds.contains(fileId)) {
                        if (getFileMetadata(studyId, fileId).isIndexed()) {
                            indexed = true;
                        }
                    }
                }
                if (!indexed) {
                    sampleMetadata.setIndexStatus(TaskMetadata.Status.NONE);
                }
                return sampleMetadata;
            });
        }
        fileDBAdaptor.removeIndexedFiles(studyId, fileIds);
    }

    public Iterator<FileMetadata> fileMetadataIterator(int studyId) {
        return fileDBAdaptor.fileIterator(studyId);
    }

    public SampleMetadata getSampleMetadata(int studyId, int sampleId) {
        return sampleDBAdaptor.getSampleMetadata(studyId, sampleId, null);
    }

    public void unsecureUpdateSampleMetadata(int studyId, SampleMetadata sample) {
        sample.setStudyId(studyId);
        sampleDBAdaptor.updateSampleMetadata(studyId, sample, null);
    }

    public <E extends Exception> SampleMetadata updateSampleMetadata(int studyId, int sampleId, UpdateFunction<SampleMetadata, E> update)
            throws E, StorageEngineException {
        getSampleName(studyId, sampleId); // Check sample exists
        Lock lock = sampleDBAdaptor.lock(studyId, sampleId, lockDuration, lockTimeout);
        try {
            SampleMetadata sample = getSampleMetadata(studyId, sampleId);
            sample = update.update(sample);
            lock.checkLocked();
            unsecureUpdateSampleMetadata(studyId, sample);
            return sample;
        } finally {
            lock.unlock();
        }
    }

    public Iterator<SampleMetadata> sampleMetadataIterator(int studyId) {
        return sampleDBAdaptor.sampleMetadataIterator(studyId);
    }

    private Integer getSampleId(int studyId, String sampleName) {
        checkName("Sample name", sampleName);
        return sampleIdCache.get(studyId, sampleName);
    }

    public Pair<Integer, Integer> getSampleIdPair(Object sampleObj, boolean skipNegated, StudyMetadata defaultStudy) {
        return getResourcePair(sampleObj, skipNegated, defaultStudy, this::sampleExists, this::getSampleId, "sample");
    }

    public List<Integer> getSampleIds(int studyId, String samplesStr) {
        List<String> samples = Arrays.asList(samplesStr.split(","));
        return getSampleIds(studyId, samples);
    }

    public List<Integer> getSampleIds(int studyId, List<String> samples) {
        List<Integer> sampleIds = new ArrayList<>(samples.size());
        for (String sample : samples) {
            Integer sampleId = getSampleId(studyId, sample);
            if (sampleId == null) {
                throw VariantQueryException.sampleNotFound(sample, getStudyName(studyId));
            }
        }
        return sampleIds;
    }

    public Integer getSampleId(int studyId, Object sampleObj) {
        return getSampleId(studyId, sampleObj, false);
    }

    public Integer getSampleId(int studyId, Object sampleObj, boolean indexed) {
        Integer sampleId = parseResourceId(studyId, sampleObj,
                o -> getSampleId(studyId, o),
                o -> {
                    try {
                        return getSampleName(studyId, o) != null;
                    } catch (VariantQueryException e) {
                        return false;
                    }
                });
        if (indexed && sampleId != null) {
            if (isSampleIndexed(studyId, sampleId)) {
                return sampleId;
            } else {
                return null;
            }
        } else {
            return sampleId;
        }
    }

    public boolean isSampleIndexed(int studyId, Integer sampleId) {
        return sampleIdIndexedCache.get(studyId, sampleId, false);
    }

    public String getSampleName(int studyId, int sampleId) {
        return sampleNameCache.get(studyId, sampleId);
    }

    public boolean sampleExists(int studyId, int sampleId) {
        return getSampleName(studyId, sampleId) != null;
    }

    public BiMap<String, Integer> getIndexedSamplesMap(int studyId) {
        return sampleDBAdaptor.getIndexedSamplesMap(studyId);
    }

    public List<Integer> getIndexedSamples(int studyId) {
        return sampleDBAdaptor.getIndexedSamples(studyId);
    }

//    public BiMap<String, Integer> getIndexedSamples(int studyId, int... fileIds) {
//        return studyDBAdaptor.getIndexedSamples(studyId);
//    }

    /**
     * Get a list of the samples to be returned, given a study and a list of samples to be returned.
     * The result can be used as SamplesPosition in {@link org.opencb.biodata.models.variant.StudyEntry#setSamplesPosition(Map)}
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
                samplesPosition.put(getSampleName(sm.getId(), sampleId), samplesPosition.size());
            }
        } else {
            samplesPosition = new LinkedHashMap<>(includeSamples.size());
            int index = 0;
            for (Object includeSampleObj : includeSamples) {
                Integer sampleId = getSampleId(sm.getId(), includeSampleObj, true);
                if (sampleId == null) {
                    continue;
//                    throw VariantQueryException.sampleNotFound(includeSampleObj, sm.getName());
                }
                String includeSample = getSampleName(sm.getId(), sampleId);

                if (!samplesPosition.containsKey(includeSample)) {
                    samplesPosition.put(includeSample, index++);
                }
            }
        }
        return samplesPosition;
    }

    public CohortMetadata getCohortMetadata(int studyId, Object cohort) {
        Integer cohortId = getCohortId(studyId, cohort, false);
        if (cohortId == null) {
            return null;
        } else {
            return cohortDBAdaptor.getCohortMetadata(studyId, cohortId, null);
        }
    }

    public void unsecureUpdateCohortMetadata(int studyId, CohortMetadata cohort) {
        cohort.setStudyId(studyId);
        cohortDBAdaptor.updateCohortMetadata(studyId, cohort, null);
    }

    public <E extends Exception> CohortMetadata updateCohortMetadata(int studyId, int cohortId, UpdateFunction<CohortMetadata, E> update)
            throws E, StorageEngineException {
        getCohortName(studyId, cohortId); // Check cohort exists
        Lock lock = cohortDBAdaptor.lock(studyId, cohortId, lockDuration, lockTimeout);
        try {
            CohortMetadata cohortMetadata = getCohortMetadata(studyId, cohortId);
            cohortMetadata = update.update(cohortMetadata);
            lock.checkLocked();
            unsecureUpdateCohortMetadata(studyId, cohortMetadata);
            return cohortMetadata;
        } finally {
            lock.unlock();
        }
    }

    public void removeCohort(int studyId, Object cohort) {
        Integer cohortId = getCohortId(studyId, cohort);
        if (cohortId == null) {
            throw VariantQueryException.cohortNotFound(cohort.toString(), studyId, this);
        }
        cohortDBAdaptor.removeCohort(studyId, cohortId);
    }

    public Integer getCohortId(int studyId, String cohortName) {
        checkName("Cohort name", cohortName);
        return cohortIdCache.get(studyId, cohortName);
    }

    public Integer getCohortId(int studyId, Object cohortObj) {
        return getCohortId(studyId, cohortObj, true);
    }

    private Integer getCohortId(int studyId, Object cohortObj, boolean validate) {
        return parseResourceId(studyId, cohortObj,
                o -> getCohortId(studyId, o),
                validate ? o -> getCohortName(studyId, o) != null : o -> true);
    }

    public List<Integer> getCohortIds(int studyId, Collection<?> cohorts) {
        Objects.requireNonNull(cohorts);
        List<Integer> cohortIds = new ArrayList<>(cohorts.size());
        for (Object cohortObj : cohorts) {
            Integer cohortId = getCohortId(studyId, cohortObj);
            if (cohortId == null) {
                throw VariantQueryException.cohortNotFound(cohortObj.toString(), studyId, getAvailableCohorts(studyId));
            }
            cohortIds.add(cohortId);
        }
        return cohortIds;
    }

    protected List<String> getAvailableCohorts(int studyId) {
        List<String> availableCohorts = new LinkedList<>();
        cohortIterator(studyId).forEachRemaining(c -> availableCohorts.add(c.getName()));
        return availableCohorts;
    }

    public String getCohortName(int studyId, int cohortId) {
        return cohortNameCache.get(studyId, cohortId);
    }

    public Iterator<CohortMetadata> cohortIterator(int studyId) {
        return cohortDBAdaptor.cohortIterator(studyId);
    }

    public Iterator<CohortMetadata> secondaryIndexCohortIterator(int studyId) {
        return Iterators.filter(cohortDBAdaptor.cohortIterator(studyId), cohort -> cohort.getName().startsWith(SECONDARY_INDEX_PREFIX));
    }

    public Iterable<CohortMetadata> getCalculatedCohorts(int studyId) {
        return () -> Iterators.filter(cohortIterator(studyId), CohortMetadata::isStatsReady);
    }

    public Iterable<CohortMetadata> getInvalidCohorts(int studyId) {
        return () -> Iterators.filter(cohortIterator(studyId), CohortMetadata::isInvalid);
    }

    public CohortMetadata setSamplesToCohort(int studyId, String cohortName, Collection<Integer> samples) throws StorageEngineException {
        return updateCohortSamples(studyId, cohortName, samples, false);
    }

    public CohortMetadata addSamplesToCohort(int studyId, String cohortName, Collection<Integer> samples) throws StorageEngineException {
        return updateCohortSamples(studyId, cohortName, samples, true);
    }

    private CohortMetadata updateCohortSamples(int studyId, String cohortName, Collection<Integer> sampleIds,
                                               boolean addSamples)
            throws StorageEngineException {
        boolean secondaryIndexCohort = cohortName.startsWith(SECONDARY_INDEX_PREFIX);

        boolean newCohort;
        Integer cohortId = getCohortId(studyId, cohortName);
        if (cohortId == null) {
            newCohort = true;
            cohortId = newCohortId(studyId);
            unsecureUpdateCohortMetadata(studyId, new CohortMetadata(studyId, cohortId, cohortName,
                    Collections.emptyList(),
                    Collections.emptyList()));
        } else {
            newCohort = false;
        }

        // Discard already added samples
        if (!newCohort && addSamples) {
            // Remove already added samples
            CohortMetadata cohortMetadata = getCohortMetadata(studyId, cohortId);

            Set<Integer> samplesToAdd = new HashSet<>(sampleIds);
            samplesToAdd.removeAll(cohortMetadata.getSamples());
            if (samplesToAdd.isEmpty()) {
                // All samples already in cohort! Nothing to do
                return cohortMetadata;
            } else {
                sampleIds = samplesToAdd;
            }
        }

        // Register cohort in samples
        for (Integer sampleId : sampleIds) {
            Integer finalCohortId = cohortId;
            if (secondaryIndexCohort) {
                updateSampleMetadata(studyId, sampleId, sampleMetadata -> sampleMetadata.addSecondaryIndexCohort(finalCohortId));
            } else {
                updateSampleMetadata(studyId, sampleId, sampleMetadata -> sampleMetadata.addCohort(finalCohortId));
            }
        }

        // Check removed samples from the cohort
        // If replacing samples, and the cohort is not new, this operation may remove some samples from the cohort.
        if (!addSamples && !newCohort) {
            CohortMetadata cohortMetadata = getCohortMetadata(studyId, cohortId);

            for (Integer sampleFromCohort : cohortMetadata.getSamples()) {
                Integer finalCohortId = cohortId;
                if (!sampleIds.contains(sampleFromCohort)) {
                    if (secondaryIndexCohort) {
                        updateSampleMetadata(studyId, sampleFromCohort, sampleMetadata -> {
                            sampleMetadata.getSecondaryIndexCohorts().remove(finalCohortId);
                            return sampleMetadata;
                        });
                    } else {
                        updateSampleMetadata(studyId, sampleFromCohort, sampleMetadata -> {
                            sampleMetadata.getCohorts().remove(finalCohortId);
                            return sampleMetadata;
                        });
                    }
                }
            }
        }
        List<Integer> fileIds = new ArrayList<>(getFileIdsFromSampleIds(studyId, sampleIds));

        // Then, add samples to the cohort
        Collection<Integer> finalSampleIds = sampleIds;
        return updateCohortMetadata(studyId, cohortId,
                cohort -> {
                    List<Integer> sampleIdsList = new ArrayList<>(finalSampleIds);
                    sampleIdsList.sort(Integer::compareTo);

                    List<Integer> oldSamples = cohort.getSamples();
                    List<Integer> oldFiles = cohort.getFiles() == null ? Collections.emptyList() : cohort.getFiles();
                    oldSamples.sort(Integer::compareTo);
                    final List<Integer> newSamples;
                    final List<Integer> newFiles;
                    if (addSamples) {
                        Set<Integer> allSamples = new HashSet<>(oldSamples.size() + finalSampleIds.size());
                        allSamples.addAll(oldSamples);
                        allSamples.addAll(finalSampleIds);
                        newSamples = new ArrayList<>(allSamples);

                        Set<Integer> allFiles = new HashSet<>(oldFiles.size() + fileIds.size());
                        allFiles.addAll(oldFiles);
                        allFiles.addAll(fileIds);
                        newFiles = new ArrayList<>(allFiles);
                    } else {
                        newSamples = sampleIdsList;
                        newFiles = fileIds;
                    }
                    cohort.setSamples(newSamples);
                    cohort.setFiles(newFiles);

                    if (cohort.isStatsReady()) {
                        if (!oldSamples.equals(sampleIdsList) || !oldFiles.equals(fileIds)) {
                            // Cohort has been modified! Invalidate stats
                            cohort.setStatsStatus(TaskMetadata.Status.ERROR);
                        }
                    }
                    return cohort;
                }
        );
    }

    public TaskMetadata getTask(int studyId, int taskId) {
        return taskDBAdaptor.getTask(studyId, taskId, null);
    }

    // Use taskId to filter task!
    @Deprecated
    public TaskMetadata getTask(int studyId, String taskName, List<Integer> fileIds) {
        TaskMetadata task = null;
        Iterator<TaskMetadata> it = taskIterator(studyId, null, true);
        while (it.hasNext()) {
            TaskMetadata t = it.next();
            if (t != null && t.getName().equals(taskName) && t.getFileIds().equals(fileIds)) {
                task = t;
                break;
            }
        }
        if (task == null) {
            throw new IllegalStateException("Batch task " + taskName + " for files " + fileIds + " not found!");
        }
        return task;
    }

    public Iterator<TaskMetadata> taskIterator(int studyId) {
        return taskIterator(studyId, null);
    }

    public Iterator<TaskMetadata> taskIterator(int studyId, List<TaskMetadata.Status> statusFilter) {
        return taskIterator(studyId, statusFilter, false);
    }

    public Iterator<TaskMetadata> taskIterator(int studyId, List<TaskMetadata.Status> statusFilter, boolean reversed) {
        return taskDBAdaptor.taskIterator(studyId, statusFilter, reversed);
    }

    public Iterable<TaskMetadata> getRunningTasks(int studyId) {
        return taskDBAdaptor.getRunningTasks(studyId);
    }

    public void unsecureUpdateTask(int studyId, TaskMetadata task) throws StorageEngineException {
        task.setStudyId(studyId);
        if (task.getId() == 0) {
            task.setId(newTaskId(studyId));
        }
        taskDBAdaptor.updateTask(studyId, task, null);
    }

    public <E extends Exception> TaskMetadata updateTask(int studyId, int taskId, UpdateFunction<TaskMetadata, E> update)
            throws E, StorageEngineException {
        getTask(studyId, taskId); // Check task exists
        Lock lock = taskDBAdaptor.lock(studyId, taskId, lockDuration, lockTimeout);
        try {
            TaskMetadata task = getTask(studyId, taskId);
            task = update.update(task);
            lock.checkLocked();
            unsecureUpdateTask(studyId, task);
            return task;
        } finally {
            lock.unlock();
        }
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
                if (validId.test(defaultStudy.getId(), id)) {
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
                id = toId.apply(defaultStudy.getId(), resourceStr);
                if (id != null) {
                    studyId = defaultStudy.getId();
                } else {
                    studyId = null;
                }
            } else {
                studyId = null;
                id = null;
            }
        }

        if (studyId == null) {
            return getResourcePair(obj, toId, resourceName, skipNegated);
        }

        return Pair.of(studyId, id);
    }

    private Pair<Integer, Integer> getResourcePair(
            Object obj, BiFunction<Integer, String, Integer> toId, String resourceName, boolean skipNegated) {
        Map<String, Integer> studies = getStudies(null);
        Collection<Integer> studyIds = studies.values();
        Integer resourceIdFromStudy;
        String resourceStr = obj.toString();
        if (isNegated(resourceStr)) { //Skip negated studies
            if (skipNegated) {
                return null;
            } else {
                resourceStr = removeNegation(resourceStr);
            }
        }
        for (Integer studyId : studyIds) {
            StudyMetadata sm = getStudyMetadata(studyId);
            resourceIdFromStudy = toId.apply(sm.getId(), resourceStr);
            if (resourceIdFromStudy != null) {
                return Pair.of(sm.getId(), resourceIdFromStudy);
            }
        }
        throw VariantQueryException.missingStudyFor(resourceName, resourceStr, studies.keySet());
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
            if (!(obj instanceof String)) {
                throw new IllegalArgumentException("Unable to parse obj type " + obj.getClass());
            }
            String str = obj.toString();
            if (isNegated(str)) {
                str = removeNegation(str);
            }
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
            } else {
                id = toId.apply(str);
            }
        }
        return id;
    }
    public LinkedHashSet<Integer> getSampleIdsFromFileId(int studyId, int fileId) {
        return sampleIdsFromFileIdCache.get(studyId, fileId);
    }

    public Set<Integer> getFileIdsFromSampleIds(int studyId, Collection<Integer> sampleIds) {
        return getFileIdsFromSampleIds(studyId, sampleIds, false);
    }

    public Set<Integer> getFileIdsFromSampleIds(int studyId, Collection<Integer> sampleIds, boolean requireIndexed) {
        Set<Integer> fileIds = new LinkedHashSet<>();
        for (Integer sampleId : sampleIds) {
            fileIds.addAll(fileIdsFromSampleIdCache.get(studyId, sampleId, Collections.emptyList()));
        }
        if (requireIndexed) {
            fileIds.removeIf(fileId -> !isFileIndexed(studyId, fileId));
        }
        return fileIds;
    }

    public List<Integer> getFileIdsFromSampleId(int studyId, int sampleId) {
        return fileIdsFromSampleIdCache.get(studyId, sampleId, Collections.emptyList());
    }

    public VariantStorageEngine.SplitData getLoadSplitData(int studyId, int sampleId) {
        Integer ordinal = splitDataCache.get(studyId, sampleId);
        if (ordinal < 0) {
            return null;
        } else {
            return VariantStorageEngine.SplitData.values()[ordinal];
        }
    }

    /*
     * Before load file, register the new sample names.
     * If SAMPLE_IDS is missing, will auto-generate sampleIds
     */
    public void registerFileSamples(int studyId, int fileId, VariantFileMetadata variantFileMetadata)
            throws StorageEngineException {
        registerFileSamples(studyId, fileId, variantFileMetadata.getSampleIds());
    }

    /*
     * Before load file, register the new sample names.
     * If SAMPLE_IDS is missing, will auto-generate sampleIds
     */
    public void registerFileSamples(int studyId, int fileId, List<String> sampleIds)
            throws StorageEngineException {

        // Register samples and add file
        LinkedHashSet<Integer> samples = new LinkedHashSet<>(sampleIds.size());
        for (String sample : sampleIds) {
            samples.add(registerSample(studyId, fileId, sample));
        }

        updateFileMetadata(studyId, fileId, fileMetadata -> {
            //Assign new sampleIds
            fileMetadata.setSamples(samples);
            return fileMetadata;
        });

    }

    public List<Integer> registerSamples(int studyId, Collection<String> samples) throws StorageEngineException {
        List<Integer> sampleIds = new ArrayList<>(samples.size());
        for (String sample : samples) {
            Integer sampleId = getSampleId(studyId, sample);
            if (sampleId == null) {
                sampleId = registerSample(studyId, null, sample);
            }
            sampleIds.add(sampleId);
        }
        return sampleIds;
    }

    protected Integer registerSample(int studyId, Integer fileId, String sample) throws StorageEngineException {
        Integer sampleId = getSampleId(studyId, sample);
        SampleMetadata sampleMetadata;

        if (sampleId == null) {
            // Create sample with lock
            try (Lock lock = lockStudy(studyId)) {
                sampleId = getSampleId(studyId, sample);
                if (sampleId == null) {
                    //If the sample was not in the original studyId, a new SampleId is assigned.
                    sampleId = newSampleId(studyId);

                    sampleMetadata = new SampleMetadata(studyId, sampleId, sample);
                    if (fileId != null) {
                        sampleMetadata.getFiles().add(fileId);
                    }
                    unsecureUpdateSampleMetadata(studyId, sampleMetadata);
                    return sampleId;
                }
            }
        }

        sampleMetadata = getSampleMetadata(studyId, sampleId);
        if (fileId != null) {
            if (!sampleMetadata.getFiles().contains(fileId)) {
                updateSampleMetadata(studyId, sampleId, s -> {
                    s.getFiles().add(fileId);
                    return s;
                });
            }
        }
        return sampleId;
    }

    public int registerSecondaryIndexSamples(int studyId, List<String> samples, boolean resume)
            throws StorageEngineException {
        if (samples == null || samples.isEmpty()) {
            throw new StorageEngineException("Missing samples to index");
        }

        List<Integer> sampleIds = new ArrayList<>(samples.size());

        List<String> alreadyIndexedSamples = new ArrayList<>();
        Set<Integer> searchIndexSampleSets = new HashSet<>();
        StudyMetadata studyMetadata = getStudyMetadata(studyId);

        for (String sample : samples) {
            Integer sampleId = getSampleId(studyId, sample);
            if (sampleId == null) {
                throw VariantQueryException.sampleNotFound(sample, studyMetadata.getName());
            }
            sampleIds.add(sampleId);
            SampleMetadata sampleMetadata = getSampleMetadata(studyId, sampleId);
            Set<Integer> sampleCohorts = sampleMetadata.getSecondaryIndexCohorts();
            if (!sampleCohorts.isEmpty()) {
                searchIndexSampleSets.addAll(sampleCohorts);
                alreadyIndexedSamples.add(sample);
            }
        }

        final int id;
        if (!alreadyIndexedSamples.isEmpty()) {
            // All samples are already indexed, and in the same collection
            if (alreadyIndexedSamples.size() == samples.size() && searchIndexSampleSets.size() == 1) {
                id = searchIndexSampleSets.iterator().next();
                CohortMetadata secondaryIndexCohort = getCohortMetadata(studyId, id);
                if (secondaryIndexCohort.getSamples().size() != sampleIds.size()
                        || !secondaryIndexCohort.getSamples().containsAll(sampleIds)) {
                    System.out.println("secondaryIndexCohort = " + secondaryIndexCohort.getSamples());
                    System.out.println("sampleIds = " + sampleIds);
                    throw new StorageEngineException("Must provide all the samples from the secondary index: "
                            + secondaryIndexCohort.getSamples()
                            .stream()
                            .map(sampleId -> getSampleName(studyId, sampleId))
                            .collect(Collectors.joining("\", \"", "\"", "\"")));
                }

                TaskMetadata.Status status = secondaryIndexCohort.getSecondaryIndexStatus();
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
            id = setSamplesToCohort(studyId, SECONDARY_INDEX_PREFIX + newSecondaryIndexSampleSetId(studyId), sampleIds).getId();
            updateCohortMetadata(studyId, id, cohortMetadata -> cohortMetadata.setSecondaryIndexStatus(TaskMetadata.Status.RUNNING));
        }

        return id;
    }

    public int registerFile(int studyId, VariantFileMetadata variantFileMetadata) throws StorageEngineException {
        return registerFile(studyId, variantFileMetadata.getPath(), variantFileMetadata.getSampleIds());
    }

    public int registerFile(int studyId, String path, List<String> sampleNames)
            throws StorageEngineException {
        int fileId = registerFile(studyId, path);
        registerFileSamples(studyId, fileId, sampleNames);
        return fileId;
    }

    /**
     * Check if the file(name,id) can be added to the Study metadata.
     *
     * Will fail if:
     * fileName was already in the study fileIds with a different fileId
     * fileId was already in the study fileIds with a different fileName
     * fileId was already in the study indexedFiles
     *
     * @param studyId   studyId
     * @param filePath  File path
     * @return fileId related to that file.
     * @throws StorageEngineException if the file is not valid for being loaded
     */
    public int registerFile(int studyId, String filePath) throws StorageEngineException {

        String fileName = Paths.get(filePath).getFileName().toString();
        Integer fileId = getFileId(studyId, fileName);

        if (fileId != null) {
            updateFileMetadata(studyId, fileId, fileMetadata -> {
                if (fileMetadata.isIndexed()) {
                    throw StorageEngineException.alreadyLoaded(fileMetadata.getId(), fileName);
                }

                // The file is not loaded. Check if it's being loaded.
                if (!fileMetadata.getPath().equals(filePath)) {
//                    // Only register if the file is being loaded. Otherwise, replace the filePath
//                    Iterator<TaskMetadata> iterator = taskIterator(studyId, null, true);
//                    while (iterator.hasNext()) {
//                        TaskMetadata task = iterator.next();
//                        if (task.getFileIds().contains(fileMetadata.getId())) {
//                            if (task.getType().equals(TaskMetadata.Type.REMOVE)) {
//                                // If the file was removed. Can be replaced.
//                                break;
//                            } else {
//                                throw StorageEngineException.unableToExecute("Already registered with a different path",
//                                        fileMetadata.getId(), fileName);
//                            }
//                        }
//                    }
                    if (fileMetadata.getIndexStatus().equals(TaskMetadata.Status.NONE)) {
                        // Replace filePath
                        fileMetadata.setPath(filePath);
                    } else {
                        throw StorageEngineException.unableToExecute("Already registered with a different path",
                                fileMetadata.getId(), fileName);
                    }
                }
                return fileMetadata;
            });
        } else {
            fileId = newFileId(studyId);
            try (Lock lock = lockStudy(studyId)) {
                FileMetadata fileMetadata = new FileMetadata()
                        .setId(fileId)
                        .setName(fileName)
                        .setPath(filePath);
                unsecureUpdateFileMetadata(studyId, fileMetadata);
            }
        }

        return fileId;
    }

    public Integer registerCohort(String study, String cohortName, Collection<String> samples) throws StorageEngineException {
        return registerCohorts(study, Collections.singletonMap(cohortName, samples)).get(cohortName);
    }

    public CohortMetadata registerTemporaryCohort(String study, String alias, List<String> samples) throws StorageEngineException {
        int studyId = getStudyId(study);
        String temporaryCohortName = "TEMP_" + alias + "_" + TimeUtils.getTimeMillis();

        List<Integer> sampleIds = registerSamples(studyId, samples);
        int cohortId = newCohortId(studyId);
        CohortMetadata cohortMetadata = new CohortMetadata(studyId, cohortId, temporaryCohortName,
                sampleIds,
                Collections.emptyList());
        cohortMetadata.getAttributes().put("alias", alias);
        cohortMetadata.setStatus("TEMPORARY", TaskMetadata.Status.RUNNING);

        unsecureUpdateCohortMetadata(studyId, cohortMetadata);
        return cohortMetadata;
    }

    public Map<String, Integer> registerCohorts(String study, Map<String, ? extends Collection<String>> cohorts)
            throws StorageEngineException {
        int studyId = getStudyId(study);
        Map<String, Integer> cohortIds = new HashMap<>();

        for (Map.Entry<String, ? extends Collection<String>> entry : cohorts.entrySet()) {
            String cohortName = entry.getKey();
            Collection<String> samples = entry.getValue();
            List<Integer> sampleIds = registerSamples(studyId, samples);
            CohortMetadata cohortMetadata = setSamplesToCohort(studyId, cohortName, sampleIds);
            cohortIds.put(cohortName, cohortMetadata.getId());
        }
        return cohortIds;
    }

    protected int newFileId(int studyId) throws StorageEngineException {
        return projectDBAdaptor.generateId(studyId, "file");
    }

    protected int newSampleId(int studyId) throws StorageEngineException {
        return projectDBAdaptor.generateId(studyId, "sample");
    }

    protected int newCohortId(int studyId) throws StorageEngineException {
        return projectDBAdaptor.generateId(studyId, "cohort");
    }

    protected int newVariantScoreId(int studyId) throws StorageEngineException {
        return projectDBAdaptor.generateId(studyId, "score");
    }

    protected int newTaskId(int studyId) throws StorageEngineException {
        return projectDBAdaptor.generateId(studyId, "task");
    }

    protected int newStudyId() throws StorageEngineException {
        return projectDBAdaptor.generateId((Integer) null, "study");
    }

    protected int newSecondaryIndexSampleSetId(int studyId) throws StorageEngineException {
        return projectDBAdaptor.generateId(studyId, "secondaryIndexSampleSet");
    }


    public static void checkStudyId(int studyId) throws StorageEngineException {
        if (studyId < 0) {
            throw new StorageEngineException("Invalid studyId : " + studyId);
        }
    }

    public TaskMetadata.Status setStatus(int studyId, int taskId, TaskMetadata.Status status) throws StorageEngineException {
        AtomicReference<TaskMetadata.Status> previousStatus = new AtomicReference<>();
        updateTask(studyId, taskId, task -> {
            previousStatus.set(task.currentStatus());
            task.addStatus(Calendar.getInstance().getTime(), status);
            return task;
        });
        return previousStatus.get();
    }

    @Deprecated
    public TaskMetadata.Status setStatus(int studyId, String taskName, List<Integer> fileIds, TaskMetadata.Status status)
            throws StorageEngineException {
        TaskMetadata task = getTask(studyId, taskName, fileIds);
        TaskMetadata.Status previousStatus = task.currentStatus();
        task.addStatus(Calendar.getInstance().getTime(), status);
        unsecureUpdateTask(studyId, task);

        return previousStatus;
    }

    @Deprecated
    public TaskMetadata.Status atomicSetStatus(int studyId, TaskMetadata.Status status, String operationName,
                                               List<Integer> files) throws StorageEngineException {
        return setStatus(studyId, operationName, files, status);
    }

    public TaskMetadata addRunningTask(int studyId, String jobOperationName, List<Integer> fileIds) throws StorageEngineException {
        return addRunningTask(studyId, jobOperationName, fileIds, false, TaskMetadata.Type.OTHER);
    }

    public TaskMetadata addRunningTask(int studyId, String jobOperationName, List<Integer> fileIds, boolean resume,
                                       TaskMetadata.Type type)
            throws StorageEngineException {

        return addRunningTask(studyId, jobOperationName, fileIds, resume, type, b -> false);
    }
    /**
     * Adds a new {@link TaskMetadata} to the Study Metadata.
     *
     * Allow execute concurrent operations depending on the "allowConcurrent" predicate.
     *  If any operation is in ERROR, is not the same operation, and concurrency is not allowed,
     *      throw {@link StorageEngineException#otherOperationInProgressException}
     *  If any operation is DONE, RUNNING, is same operation and resume=true, continue
     *  If all operations are ready, continue
     *
     * @param studyId            Study id
     * @param jobOperationName   Job operation name used to create the jobName and as {@link TaskMetadata#getOperationName()}
     * @param fileIds            Files to be processed in this batch.
     * @param resume             Resume operation. Assume that previous operation went wrong.
     * @param type               Operation type as {@link TaskMetadata.Type}
     * @param allowConcurrent    Predicate to test if the new operation can be executed at the same time as a non ready operation.
     *                           If not, throws {@link StorageEngineException#otherOperationInProgressException}
     * @return                   The current batchOperation
     * @throws StorageEngineException if the operation can't be executed
     */
    public TaskMetadata addRunningTask(int studyId, String jobOperationName,
                                       List<Integer> fileIds, boolean resume, TaskMetadata.Type type,
                                       Predicate<TaskMetadata> allowConcurrent)
            throws StorageEngineException {

        TaskMetadata resumeTask = null;
        Iterator<TaskMetadata> iterator = taskIterator(studyId, Arrays.asList(
                TaskMetadata.Status.DONE,
                TaskMetadata.Status.RUNNING,
                TaskMetadata.Status.ERROR));
        while (iterator.hasNext()) {
            TaskMetadata task = iterator.next();
            TaskMetadata.Status currentStatus = task.currentStatus();

            switch (currentStatus) {
                case READY:
                    logger.warn("Unexpected READY task. IGNORE");
                    // Ignore ready operations
                    break;
                case DONE:
                case RUNNING:
                    if (!resume) {
                        if (task.sameOperation(fileIds, type, jobOperationName)) {
                            throw StorageEngineException.currentOperationInProgressException(task);
                        } else {
                            if (allowConcurrent.test(task)) {
                                break;
                            } else {
                                throw StorageEngineException.otherOperationInProgressException(task, jobOperationName, fileIds);
                            }
                        }
                    }
                    // DO NOT BREAK!. Resuming last loading, go to error case.
                case ERROR:
                    if (!task.sameOperation(fileIds, type, jobOperationName)) {
                        if (allowConcurrent.test(task)) {
                            break;
                        } else {
                            throw StorageEngineException.otherOperationInProgressException(task, jobOperationName, fileIds, resume);
                        }
                    } else {
                        logger.info("Resuming last batch operation \"" + task.getName() + "\" due to error.");
                        resumeTask = task;
                    }
                    break;
                default:
                    throw new IllegalArgumentException("Unknown Status " + currentStatus);
            }
        }

        TaskMetadata task;
        if (resumeTask == null) {
            task = new TaskMetadata(newTaskId(studyId), jobOperationName, fileIds, System.currentTimeMillis(), type);
            task.addStatus(Calendar.getInstance().getTime(), TaskMetadata.Status.RUNNING);
            unsecureUpdateTask(studyId, task);
        } else {
            task = resumeTask;

            if (!Objects.equals(task.currentStatus(), TaskMetadata.Status.DONE)) {
                TreeMap<Date, TaskMetadata.Status> status = task.getStatus();
                task = updateTask(studyId, task.getId(), thisTask -> {
                    if (!thisTask.getStatus().equals(status)) {
                        throw new StorageEngineException("Attempt to execute a concurrent modification of task " + thisTask.getName()
                                + " (" + thisTask.getId() + ") ");
                    } else {
                        return thisTask.addStatus(Calendar.getInstance().getTime(), TaskMetadata.Status.RUNNING);
                    }
                });
            }
        }
        return task;
    }

    protected void checkName(final String type, String name) {
        if (StringUtils.isEmpty(name)) {
            throw new IllegalArgumentException(type + " can not be empty!");
        }
    }

    @Override
    public void close() throws IOException {
        studyDBAdaptor.close();
    }
}
