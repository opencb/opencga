package org.opencb.opencga.storage.core.variant.adaptors;

import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created on 14/01/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public final class VariantQueryFields {
    private final Set<VariantField> fields;
    private final List<Integer> studies;
    private final Map<Integer, StudyMetadata> studyMetadatas;
    private final Map<Integer, List<Integer>> samples;
    private final Map<Integer, List<Integer>> files;
    private final Map<Integer, List<Integer>> cohortIds;

    public VariantQueryFields(StudyMetadata studyMetadata, List<Integer> samples, List<Integer> files) {
        this.fields = VariantField.getIncludeFields(null);
        this.studies = Collections.singletonList(studyMetadata.getId());
        this.studyMetadatas = Collections.singletonMap(studyMetadata.getId(), studyMetadata);
        this.samples = Collections.singletonMap(studyMetadata.getId(), samples);
        this.files = Collections.singletonMap(studyMetadata.getId(), files);
        this.cohortIds = Collections.emptyMap();
    }

    VariantQueryFields(Set<VariantField> fields, List<Integer> studies, Map<Integer, StudyMetadata> studyMetadatas,
                       Map<Integer, List<Integer>> samples, Map<Integer, List<Integer>> files, Map<Integer, List<Integer>> cohortIds) {
        this.fields = fields;
        this.studies = studies;
        this.studyMetadatas = studyMetadatas;
        this.samples = samples;
        this.files = files;
        this.cohortIds = cohortIds;
    }

    public Set<VariantField> getFields() {
        return fields;
    }

    public List<Integer> getStudies() {
        return studies;
    }

    @Deprecated
    public Map<Integer, StudyConfiguration> getStudyConfigurations() {
        throw new UnsupportedOperationException();
    }

    public Map<Integer, StudyMetadata> getStudyMetadatas() {
        return studyMetadatas;
    }

    public Map<Integer, List<Integer>> getSamples() {
        return samples;
    }

    public Map<Integer, List<Integer>> getFiles() {
        return files;
    }

    public Map<Integer, List<Integer>> getCohorts() {
        return cohortIds;
    }
}
