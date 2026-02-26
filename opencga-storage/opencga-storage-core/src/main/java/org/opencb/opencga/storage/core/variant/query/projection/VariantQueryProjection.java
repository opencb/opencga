package org.opencb.opencga.storage.core.variant.query.projection;

import com.google.common.collect.Iterables;
import org.opencb.commons.datastore.core.Event;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.query.ResourceId;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created on 14/01/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public final class VariantQueryProjection {
    private final Set<VariantField> fields;

    private final Map<Integer, StudyVariantQueryProjection> studies;

    private final boolean samplePagination;
    private final int numSamples;
    private final int numTotalSamples;

    private List<Event> events = new ArrayList<>();

    public VariantQueryProjection(VariantStorageMetadataManager metadataManager, StudyMetadata studyMetadata,
                                  List<Integer> sampleIds, List<Integer> fileIds) {
        this.fields = VariantField.getIncludeFields(null);
        List<ResourceId> samples = new ArrayList<>(sampleIds.size());
        for (Integer sampleId : sampleIds) {
            samples.add(new ResourceId(ResourceId.Type.SAMPLE, sampleId, metadataManager.getSampleName(studyMetadata.getId(), sampleId)));
        }
        List<ResourceId> files = new ArrayList<>(fileIds.size());
        for (Integer fileId : fileIds) {
            files.add(new ResourceId(ResourceId.Type.FILE, fileId, metadataManager.getFileName(studyMetadata.getId(), fileId)));
        }
        this.studies = Collections.singletonMap(studyMetadata.getId(), new StudyVariantQueryProjection(studyMetadata, samples,
                Collections.emptyMap(), files, Collections.emptyList()));
        this.numSamples = sampleIds.size();
        this.numTotalSamples = numSamples;
        this.samplePagination = false;
    }

    public VariantQueryProjection(Set<VariantField> fields, Map<Integer, StudyVariantQueryProjection> studies,
                                  boolean samplePagination, int numSamples, int numTotalSamples) {
        this.fields = fields;
        this.studies = studies;
        this.samplePagination = samplePagination;
        this.numSamples = numSamples;
        this.numTotalSamples = numTotalSamples;
    }

    public Set<VariantField> getFields() {
        return fields;
    }

    public Map<Integer, StudyVariantQueryProjection> getStudies() {
        return studies;
    }

    public StudyVariantQueryProjection getStudy(Integer studyId) {
        return studies.get(studyId);
    }

    public List<Integer> getStudyIds() {
        return new ArrayList<>(studies.keySet());
    }

    public boolean getSamplePagination() {
        return samplePagination;
    }

    public int getNumSamples() {
        return numSamples;
    }

    public int getNumTotalSamples() {
        return numTotalSamples;
    }

    public Iterable<StudyMetadata> getStudyMetadatas() {
        return Iterables.transform(studies.values(), StudyVariantQueryProjection::getStudyMetadata);
    }

    public Map<String, List<String>> getSampleNames() {
        return studies.values().stream().collect(Collectors.toMap(s -> s.studyMetadata.getName(), s -> s.sampleNames));
    }

    @Deprecated
    public Map<Integer, List<Integer>> getFiles() {
        return studies.values().stream().collect(Collectors.toMap(s -> s.studyMetadata.getId(), s -> s.fileIds));
    }

    public List<Event> getEvents() {
        return events;
    }

    public VariantQueryProjection setEvents(List<Event> events) {
        this.events = events;
        return this;
    }

    public static class StudyVariantQueryProjection {
        private StudyMetadata studyMetadata;
        private List<ResourceId> samples = Collections.emptyList();
        private List<Integer> sampleIds = Collections.emptyList();
        private List<String> sampleNames = Collections.emptyList();
        private Map<Integer, List<Integer>> multiFileSampleFiles = Collections.emptyMap();
        private Set<Integer> multiFileSamples = Collections.emptySet();
        private List<ResourceId> files = Collections.emptyList();
        private List<Integer> fileIds = Collections.emptyList();
        private List<Integer> cohorts = Collections.emptyList();

        public StudyVariantQueryProjection() {
        }

        @Deprecated
        public StudyVariantQueryProjection(StudyMetadata studyMetadata, List<ResourceId> samples,
                                           Map<Integer, List<Integer>> multiFileSampleFiles,
                                           List<ResourceId> files, List<Integer> cohorts) {
            this.studyMetadata = studyMetadata;
            this.multiFileSampleFiles = multiFileSampleFiles;
            this.cohorts = cohorts;
            setFiles(files);
            setSamples(samples);
        }

        public int getId() {
            return studyMetadata.getId();
        }

        public String getName() {
            return studyMetadata.getName();
        }

        public StudyMetadata getStudyMetadata() {
            return studyMetadata;
        }

        public StudyVariantQueryProjection setStudyMetadata(StudyMetadata studyMetadata) {
            this.studyMetadata = studyMetadata;
            return this;
        }

        public List<ResourceId> getSamples() {
            return samples;
        }

        public StudyVariantQueryProjection setSamples(List<ResourceId> samples) {
            if (samples == null) {
                this.samples = null;
                this.sampleIds = null;
                this.sampleNames = null;
            } else {
                this.samples = samples;
                sampleIds = samples.stream().map(ResourceId::getId).collect(Collectors.toList());
                sampleNames = samples.stream().map(ResourceId::getName).collect(Collectors.toList());
            }
            return this;
        }

        public List<Integer> getSampleIds() {
            return sampleIds;
        }

        public List<String> getSampleNames() {
            return sampleNames;
        }

        public Map<Integer, List<Integer>> getMultiFileSampleFiles() {
            return multiFileSampleFiles;
        }

        public StudyVariantQueryProjection setMultiFileSampleFiles(Map<Integer, List<Integer>> multiFileSampleFiles) {
            this.multiFileSampleFiles = multiFileSampleFiles;
            return this;
        }

        public Set<Integer> getMultiFileSamples() {
            return multiFileSamples;
        }

        public StudyVariantQueryProjection setMultiFileSamples(Set<Integer> multiFileSamples) {
            this.multiFileSamples = multiFileSamples;
            return this;
        }

        public List<Integer> getFileIds() {
            return fileIds;
        }

        public List<ResourceId> getFiles() {
            return files;
        }

        public StudyVariantQueryProjection setFiles(List<ResourceId> files) {
            if (files == null) {
                this.files = null;
                this.fileIds = null;
            } else {
                this.files = files;
                this.fileIds = files.stream().map(ResourceId::getId).collect(Collectors.toList());
            }
            return this;
        }

        public List<Integer> getCohorts() {
            return cohorts;
        }

        public StudyVariantQueryProjection setCohorts(List<Integer> cohorts) {
            this.cohorts = cohorts;
            return this;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("StudyVariantQueryProjection{");
            sb.append("studyMetadata=").append(studyMetadata);
            sb.append(", samples=").append(sampleIds);
            sb.append(", multiFileSamples=").append(multiFileSamples);
            sb.append(", multiFileSampleFiles=").append(multiFileSampleFiles);
            sb.append(", files=").append(files);
            sb.append(", cohortIds=").append(cohorts);
            sb.append('}');
            return sb.toString();
        }
    }

}
