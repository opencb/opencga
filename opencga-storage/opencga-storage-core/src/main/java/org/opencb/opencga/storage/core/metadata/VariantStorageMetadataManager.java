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
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.adaptors.*;
import org.opencb.opencga.storage.core.metadata.models.*;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
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

import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.isNegated;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.removeNegation;
import static org.opencb.opencga.storage.core.variant.annotation.annotators.AbstractCellBaseVariantAnnotator.toCellBaseSpeciesName;

/**
 * @author Jacobo Coll <jacobo167@gmail.com>
 */
public class VariantStorageMetadataManager implements AutoCloseable {
    private static final int DEFAULT_LOCK_DURATION = 1000;
    private static final int DEFAULT_TIMEOUT = 10000;
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

    private final MetadataCache<String, Integer> fileIdCache;
    private final MetadataCache<Integer, String> fileNameCache;
    private final MetadataCache<Integer, Boolean> fileIdIndexedCache;
    private final MetadataCache<Integer, Set<Integer>> fileIdsFromSampleIdCache;

    private final MetadataCache<String, Integer> cohortIdCache;
    private final MetadataCache<Integer, String> cohortNameCache;

    public VariantStorageMetadataManager(VariantStorageMetadataDBAdaptorFactory dbAdaptorFactory) {
        this.projectDBAdaptor = dbAdaptorFactory.buildProjectMetadataDBAdaptor();
        this.studyDBAdaptor = dbAdaptorFactory.buildStudyMetadataDBAdaptor();
        this.fileDBAdaptor = dbAdaptorFactory.buildFileMetadataDBAdaptor();
        this.sampleDBAdaptor = dbAdaptorFactory.buildSampleMetadataDBAdaptor();
        this.cohortDBAdaptor = dbAdaptorFactory.buildCohortMetadataDBAdaptor();
        this.taskDBAdaptor = dbAdaptorFactory.buildTaskDBAdaptor();
        sampleIdCache = new MetadataCache<>(sampleDBAdaptor::getSampleId);
        sampleNameCache = new MetadataCache<>((studyId, sampleId) -> {
            SampleMetadata sampleMetadata = sampleDBAdaptor.getSampleMetadata(studyId, sampleId, null);
            if (sampleMetadata == null) {
                throw VariantQueryException.sampleNotFound(sampleId, studyId);
            }
            return sampleMetadata.getName();
        });
        sampleIdIndexedCache = new MetadataCache<>((studyId, sampleId) -> {
            SampleMetadata sampleMetadata = sampleDBAdaptor.getSampleMetadata(studyId, sampleId, null);
            if (sampleMetadata == null) {
                throw VariantQueryException.sampleNotFound(sampleId, studyId);
            }
            return sampleMetadata.isIndexed();
        });

        fileIdCache = new MetadataCache<>(fileDBAdaptor::getFileId);
        fileNameCache = new MetadataCache<>((studyId, fileId) -> {
            FileMetadata fileMetadata = fileDBAdaptor.getFileMetadata(studyId, fileId, null);
            if (fileMetadata == null) {
                throw VariantQueryException.fileNotFound(fileId, studyId);
            }
            return fileMetadata.getName();
        });
        fileIdIndexedCache = new MetadataCache<>((studyId, fileId) -> {
            FileMetadata fileMetadata = fileDBAdaptor.getFileMetadata(studyId, fileId, null);
            if (fileMetadata == null) {
                throw VariantQueryException.fileNotFound(fileId, studyId);
            }
            return fileMetadata.isIndexed();
        });
        fileIdsFromSampleIdCache = new MetadataCache<>((studyId, sampleId) -> {
            SampleMetadata sampleMetadata = getSampleMetadata(studyId, sampleId);
            if (sampleMetadata == null) {
                throw VariantQueryException.sampleNotFound(sampleId, studyId);
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

    public long lockStudy(int studyId) throws StorageEngineException {
        return lockStudy(studyId, 10000, 60000);
    }

    public long lockStudy(int studyId, long lockDuration, long timeout) throws StorageEngineException {
        return studyDBAdaptor.lockStudy(studyId, lockDuration, timeout, null);
    }

    public long lockStudy(int studyId, long lockDuration, long timeout, String lockName) throws StorageEngineException {
        return studyDBAdaptor.lockStudy(studyId, lockDuration, timeout, lockName);
    }

    public void unLockStudy(int studyId, long lockId) {
        studyDBAdaptor.unLockStudy(studyId, lockId, null);
    }

    public void unLockStudy(int studyId, long lockId, String lockName) {
        studyDBAdaptor.unLockStudy(studyId, lockId, lockName);
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
        long lock = lockStudy(studyId);
        try {
            StudyMetadata sm = getStudyMetadata(studyId);

            sm = updater.update(sm);

            unsecureUpdateStudyMetadata(sm);
            return sm;
        } finally {
            unLockStudy(studyId, lock);
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
    public final QueryResult<StudyConfiguration> getStudyConfiguration(Object study, QueryOptions options) {
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

    @Deprecated
    public List<String> getStudyNames(QueryOptions options) {
        return studyDBAdaptor.getStudyNames(options);
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
    public final QueryResult updateStudyConfiguration(StudyConfiguration studyConfiguration, QueryOptions options) {
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

    public <E extends Exception> ProjectMetadata updateProjectMetadata(UpdateFunction<ProjectMetadata, E> function)
            throws StorageEngineException, E {
        Objects.requireNonNull(function);
        long lock;
        try {
            lock = projectDBAdaptor.lockProject(DEFAULT_LOCK_DURATION, DEFAULT_TIMEOUT);
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

            projectMetadata = updateProjectMetadata(pm -> {
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
        return fileDBAdaptor.getVariantFileMetadata(studyId, fileId, options);
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
        FileMetadata fileMetadata = getFileMetadata(studyId, fileId);
        Locked lock = fileDBAdaptor.lock(studyId, fileMetadata.getId(), DEFAULT_LOCK_DURATION, DEFAULT_TIMEOUT);
        try {
            fileMetadata = update.update(fileMetadata);
            unsecureUpdateFileMetadata(studyId, fileMetadata);
        } finally {
            lock.unlock();
        }
        return fileMetadata;
    }

    private Integer getFileId(int studyId, String fileName) {
        checkName("File name", fileName);
        return fileIdCache.get(studyId, fileName);
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
            updateSampleMetadata(studyId, sample, sampleMetadata -> sampleMetadata.setIndexStatus(TaskMetadata.Status.READY));
        }

        // Finally, update the files and update the list of indexed files
        for (Integer fileId : fileIds) {
            String name = updateFileMetadata(studyId, fileId, fileMetadata -> fileMetadata.setIndexStatus(TaskMetadata.Status.READY))
                    .getName();
            logger.info("Register file " + name + " as INDEXED");
        }
        fileDBAdaptor.addIndexedFiles(studyId, fileIds);
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
        SampleMetadata sample = getSampleMetadata(studyId, sampleId);
        Locked lock = sampleDBAdaptor.lock(studyId, sample.getId(), DEFAULT_LOCK_DURATION, DEFAULT_TIMEOUT);
        try {
            sample = update.update(sample);
            unsecureUpdateSampleMetadata(studyId, sample);
        } finally {
            lock.unlock();
        }
        return sample;
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

    public Integer getSampleId(int studyId, Object sampleObj) {
        return getSampleId(studyId, sampleObj, false);
    }

    public Integer getSampleId(int studyId, Object sampleObj, boolean indexed) {
        Integer sampleId = parseResourceId(studyId, sampleObj,
                o -> getSampleId(studyId, o),
                o -> getSampleName(studyId, o) != null);
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
            Set<Integer> indexedSamplesId = new HashSet<>(getIndexedSamples(sm.getId()));
            for (Object includeSampleObj : includeSamples) {
                Integer sampleId = getSampleId(sm.getId(), includeSampleObj, false);
                if (sampleId == null) {
                    continue;
//                    throw VariantQueryException.sampleNotFound(includeSampleObj, sm.getName());
                }
                String includeSample = getSampleName(sm.getId(), sampleId);

                if (!samplesPosition.containsKey(includeSample)) {
                    if (indexedSamplesId.contains(sampleId)) {
                        samplesPosition.put(includeSample, index++);
                    }
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

    protected void unsecureUpdateCohortMetadata(int studyId, CohortMetadata cohort) {
        cohort.setStudyId(studyId);
        cohortDBAdaptor.updateCohortMetadata(studyId, cohort, null);
    }

    public <E extends Exception> CohortMetadata updateCohortMetadata(int studyId, int cohortId, UpdateFunction<CohortMetadata, E> update)
            throws E, StorageEngineException {
        CohortMetadata cohortMetadata = getCohortMetadata(studyId, cohortId);
        Locked lock = cohortDBAdaptor.lock(studyId, cohortMetadata.getId(), DEFAULT_LOCK_DURATION, DEFAULT_TIMEOUT);
        try {
            cohortMetadata = update.update(cohortMetadata);
            unsecureUpdateCohortMetadata(studyId, cohortMetadata);
        } finally {
            lock.unlock();
        }
        return cohortMetadata;
    }

    public void removeCohort(int studyId, Object cohort) {

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
            unsecureUpdateCohortMetadata(studyId, new CohortMetadata(studyId, cohortId, cohortName, Collections.emptyList()));
        } else {
            newCohort = false;
        }

        // First register cohort in samples
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

        // Then, add samples to the cohort
        return updateCohortMetadata(studyId, cohortId,
                cohort -> {
                    List<Integer> sampleIdsList = new ArrayList<>(sampleIds);
                    sampleIdsList.sort(Integer::compareTo);

                    List<Integer> oldSamples = cohort.getSamples();
                    oldSamples.sort(Integer::compareTo);
                    List<Integer> newSamples;
                    if (addSamples) {
                        Set<Integer> allSamples = new HashSet<>(oldSamples);
                        allSamples.addAll(sampleIds);
                        newSamples = new ArrayList<>(allSamples);
                    } else {
                        newSamples = sampleIdsList;
                    }
                    cohort.setSamples(newSamples);

                    if (!oldSamples.equals(sampleIds)) {
                        // Cohort has been modified! Invalidate if needed.
                        if (cohort.isStatsReady()) {
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
        Iterator<TaskMetadata> it = taskIterator(studyId, true);
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
        return taskIterator(studyId, false);
    }

    public Iterator<TaskMetadata> taskIterator(int studyId, boolean reversed) {
        return taskDBAdaptor.taskIterator(studyId, reversed);
    }

    public Iterable<TaskMetadata> getRunningTasks(int studyId) {
        return taskDBAdaptor.getRunningTasks(studyId);
    }

    private void unsecureUpdateTask(int studyId, TaskMetadata task) {
        task.setStudyId(studyId);
        taskDBAdaptor.updateTask(studyId, task, null);
    }

    public <E extends Exception> TaskMetadata updateTask(int studyId, int taskId, UpdateFunction<TaskMetadata, E> update)
            throws E, StorageEngineException {
        TaskMetadata task = getTask(studyId, taskId);
        Locked lock = taskDBAdaptor.lock(studyId, task.getId(), DEFAULT_LOCK_DURATION, DEFAULT_TIMEOUT);
        try {
            task = update.update(task);
            unsecureUpdateTask(studyId, task);
        } finally {
            lock.unlock();
        }
        return task;
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
                if (NumberUtils.isParsable(resourceStr)) {
                    id = Integer.parseInt(resourceStr);
                    if (validId.test(defaultStudy.getId(), id)) {
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

    public Set<Integer> getFileIdsFromSampleIds(int studyId, Collection<Integer> sampleIds) {
        Set<Integer> fileIds = new LinkedHashSet<>();
        for (Integer sampleId : sampleIds) {
            fileIds.addAll(fileIdsFromSampleIdCache.get(studyId, sampleId, Collections.emptySet()));
        }
        return fileIds;
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


        updateFileMetadata(studyId, fileId, fileMetadata -> {
            //Assign new sampleIds
            LinkedHashSet<Integer> samples = new LinkedHashSet<>(sampleIds.size());
            for (String sample : sampleIds) {
                samples.add(registerSample(studyId, fileId, sample));
            }
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
            unsecureUpdateSampleMetadata(studyId, sampleMetadata);
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
                    // Only register if the file is being loaded. Otherwise, replace the filePath
                    Iterator<TaskMetadata> iterator = taskIterator(studyId, true);
                    while (iterator.hasNext()) {
                        TaskMetadata task = iterator.next();
                        if (task.getFileIds().contains(fileMetadata.getId())) {
                            if (task.getType().equals(TaskMetadata.Type.REMOVE)) {
                                // If the file was removed. Can be replaced.
                                break;
                            } else {
                                throw StorageEngineException.unableToExecute("Already registered with a different path",
                                        fileMetadata.getId(), fileName);
                            }
                        }
                    }
                    // Replace filePath
                    fileMetadata.setPath(filePath);
                }
                return fileMetadata;
            });
        } else {
            fileId = newFileId(studyId);
            FileMetadata fileMetadata = new FileMetadata()
                    .setId(fileId)
                    .setName(fileName)
                    .setPath(filePath);
            unsecureUpdateFileMetadata(studyId, fileMetadata);
        }

        return fileId;
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
    public TaskMetadata.Status setStatus(int studyId, String taskName, List<Integer> fileIds, TaskMetadata.Status status) {
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
     * @param type               Operation type as {@link TaskMetadata#type}
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
        Iterator<TaskMetadata> iterator = taskIterator(studyId);
        while (iterator.hasNext()) {
            TaskMetadata task = iterator.next();
            TaskMetadata.Status currentStatus = task.currentStatus();

            switch (currentStatus) {
                case READY:
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
