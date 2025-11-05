package org.opencb.opencga.storage.core.metadata.local;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterators;
import org.apache.avro.generic.GenericRecord;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.biodata.models.variant.metadata.SampleVariantStats;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.core.models.common.mixins.GenericRecordAvroJsonMixin;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.io.managers.IOConnector;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.adaptors.*;
import org.opencb.opencga.storage.core.metadata.models.*;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.*;

public class LocalVariantStorageMetadataDBAdaptorFactory implements VariantStorageMetadataDBAdaptorFactory {

    public static final String METADATA_JSON = "metadata.json.gz";

    private final ObjectMap configuration;
    private final LocalMetadataStorage store;
    private final DelegatedProjectMetadataAdaptor projectMetadataAdaptor;
    private final DelegatedStudyMetadataDBAdaptor studyMetadataDBAdaptor;
    private final DelegatedSampleMetadataDBAdaptor sampleMetadataDBAdaptor;
    private final DelegatedCohortMetadataDBAdaptor cohortMetadataDBAdaptor;
    private final DelegatedTaskMetadataDBAdaptor taskMetadataDBAdaptor;
    private final DelegatedFileMetadataDBAdaptor fileMetadataDBAdaptor;
    private static Logger logger = LoggerFactory.getLogger(LocalVariantStorageMetadataDBAdaptorFactory.class);

    public LocalVariantStorageMetadataDBAdaptorFactory(VariantStorageMetadataDBAdaptorFactory delegated) {
        projectMetadataAdaptor = new DelegatedProjectMetadataAdaptor();
        studyMetadataDBAdaptor = new DelegatedStudyMetadataDBAdaptor(delegated.buildStudyMetadataDBAdaptor());
        fileMetadataDBAdaptor = new DelegatedFileMetadataDBAdaptor(delegated.buildFileMetadataDBAdaptor());
        sampleMetadataDBAdaptor = new DelegatedSampleMetadataDBAdaptor(delegated.buildSampleMetadataDBAdaptor());
        cohortMetadataDBAdaptor = new DelegatedCohortMetadataDBAdaptor(delegated.buildCohortMetadataDBAdaptor());
        taskMetadataDBAdaptor = new DelegatedTaskMetadataDBAdaptor(delegated.buildTaskDBAdaptor());

        store = new LocalMetadataStorage(delegated.buildProjectMetadataDBAdaptor().getProjectMetadata().first());
        configuration = delegated.getConfiguration();
    }

    public LocalVariantStorageMetadataDBAdaptorFactory(URI[] files, IOConnector connector) throws IOException {
        this(readLocalMetadataStore(files, connector));
    }


    public LocalVariantStorageMetadataDBAdaptorFactory(LocalMetadataStorage store) {
        projectMetadataAdaptor = new DelegatedProjectMetadataAdaptor();
        studyMetadataDBAdaptor = new DelegatedStudyMetadataDBAdaptor();
        fileMetadataDBAdaptor = new DelegatedFileMetadataDBAdaptor();
        sampleMetadataDBAdaptor = new DelegatedSampleMetadataDBAdaptor();
        cohortMetadataDBAdaptor = new DelegatedCohortMetadataDBAdaptor();
        taskMetadataDBAdaptor = new DelegatedTaskMetadataDBAdaptor();

        this.store = store;
        configuration = new ObjectMap();
    }

    public List<URI> writeToFile(URI outdir, IOConnector connector) throws IOException {
        URI file = outdir.resolve(LocalVariantStorageMetadataDBAdaptorFactory.METADATA_JSON);
        logger.info("Writing metadata to " + file);
        ObjectMapper objectMapper = getObjectMapper();
        try (OutputStream outputStream = connector.newOutputStream(file)) {
            objectMapper.writeValue(outputStream, store);
        }
        List<URI> files = new ArrayList<>();
        files.add(file);
        return files;
    }

    private static LocalMetadataStorage readLocalMetadataStore(URI[] files, IOConnector connector) throws IOException {
        ObjectMapper objectMapper = getObjectMapper();
        URI metadataFile = null;
        for (URI file : files) {
            if (UriUtils.fileName(file).equals(METADATA_JSON)) {
                metadataFile = file;
            }
        }
        LocalMetadataStorage store;
        if (metadataFile != null) {
            logger.info("Loading metadata from " + metadataFile);
            try (InputStream is = connector.newInputStream(metadataFile)) {
                store = objectMapper.readValue(is, LocalMetadataStorage.class);
            }
        } else {
            throw new IllegalArgumentException("No metadata.json file found in " + Arrays.toString(files));
        }
        return store;
    }

    private static ObjectMapper getObjectMapper() {
//        ObjectMapper objectMapper = JacksonUtils.getDefaultNonNullObjectMapper();
        ObjectMapper objectMapper = new ObjectMapper();
//        objectMapper.configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.addMixIn(GenericRecord.class, GenericRecordAvroJsonMixin.class);
        objectMapper.addMixIn(SampleVariantStats.class, SampleVariantStatsMixin.class);
        return objectMapper;
    }

    @Override
    public ObjectMap getConfiguration() {
        return configuration;
    }

    @Override
    public ProjectMetadataAdaptor buildProjectMetadataDBAdaptor() {
        return projectMetadataAdaptor;
    }

    @Override
    public StudyMetadataDBAdaptor buildStudyMetadataDBAdaptor() {
        return studyMetadataDBAdaptor;
    }

    @Override
    public FileMetadataDBAdaptor buildFileMetadataDBAdaptor() {
        return fileMetadataDBAdaptor;
    }

    @Override
    public SampleMetadataDBAdaptor buildSampleMetadataDBAdaptor() {
        return sampleMetadataDBAdaptor;
    }

    @Override
    public CohortMetadataDBAdaptor buildCohortMetadataDBAdaptor() {
        return cohortMetadataDBAdaptor;
    }

    @Override
    public TaskMetadataDBAdaptor buildTaskDBAdaptor() {
        return taskMetadataDBAdaptor;
    }

    public class DelegatedProjectMetadataAdaptor implements ProjectMetadataAdaptor {

        public DelegatedProjectMetadataAdaptor() {
        }

        @Override
        public Lock lockProject(long lockDuration, long timeout, String lockName) throws StorageEngineException {
            throw new UnsupportedOperationException("ReadOnly DBAdaptor");
        }

        @Override
        public DataResult<ProjectMetadata> getProjectMetadata() {
            return new DataResult<>(0, Collections.emptyList(), 0, Collections.singletonList(store.getProjectMetadata()), 0);
        }

        @Override
        public DataResult updateProjectMetadata(ProjectMetadata projectMetadata, boolean updateCounters) {
            throw new UnsupportedOperationException("ReadOnly DBAdaptor");
        }

        @Override
        public int generateId(Integer studyId, String idType) throws StorageEngineException {
            throw new UnsupportedOperationException("ReadOnly DBAdaptor");
        }

        @Override
        public boolean exists() {
            return true;
        }
    }

    private LocalMetadataStorage.StudyMetadataStore getStudy(int studyId) {
        LocalMetadataStorage.StudyMetadataStore study = store.getStudies().get(studyId);
        if (study == null) {
            if (studyMetadataDBAdaptor == null) {
                throw VariantQueryException.studyNotFound(studyId);
            } else {
                studyMetadataDBAdaptor.getStudyMetadata(studyId, 0L);
            }
        }
        return store.getStudy(studyId);
    }

    public class DelegatedStudyMetadataDBAdaptor implements StudyMetadataDBAdaptor {

        private final StudyMetadataDBAdaptor delegatedStudyDBA;

        public DelegatedStudyMetadataDBAdaptor() {
            delegatedStudyDBA = null;
        }

        public DelegatedStudyMetadataDBAdaptor(StudyMetadataDBAdaptor delegatedStudyDBA) {
            this.delegatedStudyDBA = delegatedStudyDBA;
        }

        @Override
        public Lock lock(int studyId, long lockDuration, long timeout, String lockName) throws StorageEngineException {
            throw new UnsupportedOperationException("ReadOnly DBAdaptor");
        }

        @Override
        public DataResult updateStudyConfiguration(StudyConfiguration studyConfiguration, QueryOptions options) {
            throw new UnsupportedOperationException("ReadOnly DBAdaptor");
        }

        @Override
        public StudyMetadata getStudyMetadata(int id, Long timeStamp) {
            LocalMetadataStorage.StudyMetadataStore study = store.getStudies().get(id);
            if (study == null) {
                // Not in store. Check delegated
                if (delegatedStudyDBA != null) {
                    StudyMetadata studyMetadata = delegatedStudyDBA.getStudyMetadata(id, timeStamp);
                    study = new LocalMetadataStorage.StudyMetadataStore(studyMetadata);
                    store.addStudy(study);
                }
            }
            if (study == null) {
                return null;
            } else {
                return study.getStudyMetadata();
            }
        }

        @Override
        public void updateStudyMetadata(StudyMetadata sm) {
            throw new UnsupportedOperationException("ReadOnly DBAdaptor");
        }

        @Override
        public Map<String, Integer> getStudies(QueryOptions options) {
            if (store.getStudyNameIdsMap().isEmpty()) {
                // Not in store. Check delegated
                if (delegatedStudyDBA != null) {
                    store.getStudyNameIdsMap().putAll(delegatedStudyDBA.getStudies(options));
                }
            }
            return store.getStudyNameIdsMap();
        }

        @Override
        public void close() throws IOException {
            if (delegatedStudyDBA != null) {
                delegatedStudyDBA.close();
            }
        }
    }

    public class DelegatedSampleMetadataDBAdaptor implements SampleMetadataDBAdaptor {

        private final SampleMetadataDBAdaptor delegatedSampleDBA;

        public DelegatedSampleMetadataDBAdaptor() {
            this.delegatedSampleDBA = null;
        }

        public DelegatedSampleMetadataDBAdaptor(SampleMetadataDBAdaptor delegatedSampleDBA) {
            this.delegatedSampleDBA = delegatedSampleDBA;
        }

        @Override
        public SampleMetadata getSampleMetadata(int studyId, int sampleId, Long timeStamp) {
            LocalMetadataStorage.StudyMetadataStore studyStore = getStudy(studyId);
            SampleMetadata sampleMetadata = studyStore.getSamples().get(sampleId);
            if (sampleMetadata == null) {
                // Not in store. Check delegated
                if (delegatedSampleDBA != null) {
                    sampleMetadata = delegatedSampleDBA.getSampleMetadata(studyId, sampleId, timeStamp);
                    if (sampleMetadata != null) {
                        studyStore.addSample(sampleMetadata);
                    }
                }
            }
            return sampleMetadata;
        }

        @Override
        public void updateSampleMetadata(int studyId, SampleMetadata sample, Long timeStamp) {
            throw new UnsupportedOperationException("ReadOnly DBAdaptor");
        }

        @Override
        public Iterator<SampleMetadata> sampleMetadataIterator(int studyId) {
            LocalMetadataStorage.StudyMetadataStore studyStore = getStudy(studyId);
            if (delegatedSampleDBA == null) {
                // Get from local store
                return studyStore.getSamples().values().iterator();
            } else {
                return Iterators.transform(delegatedSampleDBA.sampleMetadataIterator(studyId), input -> {
                    studyStore.addSample(input);
                    return input;
                });
            }
        }

        @Override
        public Integer getSampleId(int studyId, String sampleName) {
            LocalMetadataStorage.StudyMetadataStore studyStore = getStudy(studyId);
            Integer id = studyStore.getSampleNameIdsMap().get(sampleName);
            if (id == null) {
                // Not in store. Check delegated
                if (delegatedSampleDBA != null) {
                    id = delegatedSampleDBA.getSampleId(studyId, sampleName);
                    if (id != null) {
                        studyStore.getSampleNameIdsMap().put(sampleName, id);
                    }
                }
            }
            return id;
        }

        @Override
        public Lock lock(int studyId, int id, long lockDuration, long timeout) throws StorageEngineException {
            throw new UnsupportedOperationException("ReadOnly DBAdaptor");
        }

    }

    public class DelegatedFileMetadataDBAdaptor implements FileMetadataDBAdaptor {

        private final FileMetadataDBAdaptor delegatedFileDBA;

        public DelegatedFileMetadataDBAdaptor() {
            this.delegatedFileDBA = null;
        }

        public DelegatedFileMetadataDBAdaptor(FileMetadataDBAdaptor delegatedFileDBA) {
            this.delegatedFileDBA = delegatedFileDBA;
        }

        @Override
        public FileMetadata getFileMetadata(int studyId, int fileId, Long timeStamp) {
            LocalMetadataStorage.StudyMetadataStore studyStore = getStudy(studyId);
            FileMetadata fileMetadata = studyStore.getFiles().get(fileId);
            if (fileMetadata == null) {
                // Not in store. Check delegated
                if (delegatedFileDBA != null) {
                    fileMetadata = delegatedFileDBA.getFileMetadata(studyId, fileId, timeStamp);
                    if (fileMetadata != null) {
                        studyStore.addFile(fileMetadata);
                    }
                }
            }
            return fileMetadata;
        }

        @Override
        public Iterator<FileMetadata> fileIterator(int studyId) {
            LocalMetadataStorage.StudyMetadataStore studyStore = getStudy(studyId);
            if (delegatedFileDBA == null) {
                // Get from local store
                return studyStore.getFiles().values().iterator();
            } else {
                return Iterators.transform(delegatedFileDBA.fileIterator(studyId), input -> {
                    studyStore.addFile(input);
                    return input;
                });
            }
        }

        @Override
        public Integer getFileId(int studyId, String fileName) {
            LocalMetadataStorage.StudyMetadataStore studyStore = getStudy(studyId);
            Integer id = studyStore.getFileNameIdsMap().get(fileName);
            if (id == null) {
                // Not in store. Check delegated
                if (delegatedFileDBA != null) {
                    id = delegatedFileDBA.getFileId(studyId, fileName);
                    if (id != null) {
                        studyStore.getFileNameIdsMap().put(fileName, id);
                    }
                }
            }
            return id;
        }

        @Override
        public LinkedHashSet<Integer> getIndexedFiles(int studyId, boolean includePartial) {
            if (delegatedFileDBA == null) {
                LinkedHashSet<Integer> fileIds = new LinkedHashSet<>();
                for (Map.Entry<Integer, FileMetadata> entry : getStudy(studyId).getFiles().entrySet()) {
                    if (entry.getValue().isIndexed()) {
                        fileIds.add(entry.getKey());
                    }
                }
                return fileIds;
            } else {
                return delegatedFileDBA.getIndexedFiles(studyId, includePartial);
            }
        }

        @Override
        public DataResult count(Query query) {
            throw new UnsupportedOperationException("Unimplemented method");
        }

        @Override
        public Iterator<VariantFileMetadata> iterator(Query query, QueryOptions options) throws IOException {
//            return delegatedFileDBA.iterator(query, options);
            throw new UnsupportedOperationException("Unsupported operation");
        }

        @Override
        public void updateFileMetadata(int studyId, FileMetadata file, Long timeStamp) {
            throw new UnsupportedOperationException("ReadOnly DBAdaptor");
        }

        @Override
        public void updateVariantFileMetadata(String studyId, VariantFileMetadata metadata) throws StorageEngineException {
            throw new UnsupportedOperationException("ReadOnly DBAdaptor");
        }

        @Override
        public void removeVariantFileMetadata(int study, int file) throws IOException {
            throw new UnsupportedOperationException("ReadOnly DBAdaptor");
        }

        @Override
        public Lock lock(int studyId, int id, long lockDuration, long timeout) throws StorageEngineException {
            throw new UnsupportedOperationException("ReadOnly DBAdaptor");
        }

        @Override
        public void close() throws IOException {
            if (delegatedFileDBA != null) {
                delegatedFileDBA.close();
            }
        }
    }

    public class DelegatedCohortMetadataDBAdaptor implements CohortMetadataDBAdaptor {

        private final CohortMetadataDBAdaptor delegatedCohortDBA;

        public DelegatedCohortMetadataDBAdaptor() {
            this.delegatedCohortDBA = null;
        }

        public DelegatedCohortMetadataDBAdaptor(CohortMetadataDBAdaptor delegatedCohortDBA) {
            this.delegatedCohortDBA = delegatedCohortDBA;
        }

        @Override
        public CohortMetadata getCohortMetadata(int studyId, int cohortId, Long timeStamp) {
            LocalMetadataStorage.StudyMetadataStore studyStore = getStudy(studyId);
            CohortMetadata cohortMetadata = studyStore.getCohorts().get(cohortId);
            if (cohortMetadata == null) {
                // Not in store. Check delegated
                if (delegatedCohortDBA != null) {
                    cohortMetadata = delegatedCohortDBA.getCohortMetadata(studyId, cohortId, timeStamp);
                    if (cohortMetadata != null) {
                        studyStore.addCohort(cohortMetadata);
                    }
                }
            }
            return cohortMetadata;
        }

        @Override
        public void updateCohortMetadata(int studyId, CohortMetadata cohort, Long timeStamp) {
            throw new UnsupportedOperationException("ReadOnly DBAdaptor");
        }

        @Override
        public Integer getCohortId(int studyId, String cohortName) {
            LocalMetadataStorage.StudyMetadataStore studyStore = getStudy(studyId);
            Integer id = studyStore.getCohortNameIdsMap().get(cohortName);
            if (id == null) {
                // Not in store. Check delegated
                if (delegatedCohortDBA != null) {
                    id = delegatedCohortDBA.getCohortId(studyId, cohortName);
                    if (id != null) {
                        studyStore.getCohortNameIdsMap().put(cohortName, id);
                    }
                }
            }
            return id;
        }

        @Override
        public Iterator<CohortMetadata> cohortIterator(int studyId) {
            LocalMetadataStorage.StudyMetadataStore studyStore = getStudy(studyId);
            if (delegatedCohortDBA == null) {
                // Get from local store
                return studyStore.getCohorts().values().iterator();
            } else {
                return Iterators.transform(delegatedCohortDBA.cohortIterator(studyId), input -> {
                    studyStore.addCohort(input);
                    return input;
                });
            }
        }

        @Override
        public void removeCohort(int studyId, int cohortId) {
            throw new UnsupportedOperationException("ReadOnly DBAdaptor");
        }

        @Override
        public Lock lock(int studyId, int id, long lockDuration, long timeout) throws StorageEngineException {
            throw new UnsupportedOperationException("ReadOnly DBAdaptor");
        }
    }

    public class DelegatedTaskMetadataDBAdaptor implements TaskMetadataDBAdaptor {

        private final TaskMetadataDBAdaptor delegatedTaskDBA;

        public DelegatedTaskMetadataDBAdaptor() {
            this.delegatedTaskDBA = null;
        }

        public DelegatedTaskMetadataDBAdaptor(TaskMetadataDBAdaptor delegatedTaskDBA) {
            this.delegatedTaskDBA = delegatedTaskDBA;
        }

        @Override
        public TaskMetadata getTask(int studyId, int taskId, Long timeStamp) {
            LocalMetadataStorage.StudyMetadataStore studyStore = getStudy(studyId);
            TaskMetadata taskMetadata = studyStore.getTasks().get(taskId);
            if (taskMetadata == null) {
                // Not in store. Check delegated
                if (delegatedTaskDBA != null) {
                    taskMetadata = delegatedTaskDBA.getTask(studyId, taskId, timeStamp);
                    if (taskMetadata != null) {
                        studyStore.getTasks().put(taskId, taskMetadata);
                    }
                }
            }
            return taskMetadata;
        }

        @Override
        public Iterator<TaskMetadata> taskIterator(int studyId, List<TaskMetadata.Status> statusFilter, boolean reversed) {
            Map<Integer, TaskMetadata> tasksMap = getStudy(studyId).getTasks();
            if (delegatedTaskDBA == null) {
                // Get from local store
                return tasksMap.values().iterator();
            } else {
                return Iterators.transform(delegatedTaskDBA.taskIterator(studyId, statusFilter, reversed), input -> {
                    tasksMap.put(input.getId(), input);
                    return input;
                });
            }
        }

        @Override
        public void updateTask(int studyId, TaskMetadata task, Long timeStamp) {
            throw new UnsupportedOperationException("ReadOnly DBAdaptor");
        }

        @Override
        public Lock lock(int studyId, int id, long lockDuration, long timeout) throws StorageEngineException {
            throw new UnsupportedOperationException("ReadOnly DBAdaptor");
        }
    }

}
