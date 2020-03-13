package org.opencb.opencga.storage.core.variant.query.projection;

import com.google.common.collect.Iterables;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;

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


    public VariantQueryProjection(StudyMetadata studyMetadata, List<Integer> samples, List<Integer> files) {
        this.fields = VariantField.getIncludeFields(null);
        this.studies = Collections.singletonMap(studyMetadata.getId(), new StudyVariantQueryProjection(studyMetadata, samples,
                Collections.emptyMap(), files, Collections.emptyList()));
        this.numSamples = samples.size();
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

    @Deprecated
    public Map<Integer, List<Integer>> getSamples() {
        return studies.values().stream().collect(Collectors.toMap(s -> s.studyMetadata.getId(), s -> s.samples));
    }

    @Deprecated
    public Map<Integer, List<Integer>> getFiles() {
        return studies.values().stream().collect(Collectors.toMap(s -> s.studyMetadata.getId(), s -> s.files));
    }

    public static class StudyVariantQueryProjection {
        private StudyMetadata studyMetadata;
        private List<Integer> samples;
        private Map<Integer, List<Integer>> multiFileSamples;
        private List<Integer> files;
        private List<Integer> cohorts;

        public StudyVariantQueryProjection() {
        }

        public StudyVariantQueryProjection(StudyMetadata studyMetadata, List<Integer> samples, Map<Integer, List<Integer>> multiFileSamples,
                                           List<Integer> files, List<Integer> cohorts) {
            this.studyMetadata = studyMetadata;
            this.samples = samples;
            this.multiFileSamples = multiFileSamples;
            this.files = files;
            this.cohorts = cohorts;
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

        public List<Integer> getSamples() {
            return samples;
        }

        public StudyVariantQueryProjection setSamples(List<Integer> samples) {
            this.samples = samples;
            return this;
        }

        public Map<Integer, List<Integer>> getMultiFileSamples() {
            return multiFileSamples;
        }

        public StudyVariantQueryProjection setMultiFileSamples(Map<Integer, List<Integer>> multiFileSamples) {
            this.multiFileSamples = multiFileSamples;
            return this;
        }

        public List<Integer> getFiles() {
            return files;
        }

        public StudyVariantQueryProjection setFiles(List<Integer> files) {
            this.files = files;
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
            sb.append(", samples=").append(samples);
            sb.append(", multiFileSamples=").append(multiFileSamples);
            sb.append(", files=").append(files);
            sb.append(", cohortIds=").append(cohorts);
            sb.append('}');
            return sb.toString();
        }
    }

}
