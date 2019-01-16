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
//        private final Map<Integer, List<Integer>> cohortIds;

    public VariantQueryFields(StudyConfiguration studyConfiguration, List<Integer> samples, List<Integer> files) {
        this.fields = VariantField.getIncludeFields(null);
        this.studies = Collections.singletonList(studyConfiguration.getStudyId());
        this.studyMetadatas = Collections.singletonMap(studyConfiguration.getStudyId(), studyConfiguration);
        this.samples = Collections.singletonMap(studyConfiguration.getStudyId(), samples);
        this.files = Collections.singletonMap(studyConfiguration.getStudyId(), files);
    }

    VariantQueryFields(Set<VariantField> fields, List<Integer> studies, Map<Integer, StudyMetadata> studyMetadatas,
                       Map<Integer, List<Integer>> samples, Map<Integer, List<Integer>> files) {
        this.fields = fields;
        this.studies = studies;
        this.studyMetadatas = studyMetadatas;
        this.samples = samples;
        this.files = files;
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
}
