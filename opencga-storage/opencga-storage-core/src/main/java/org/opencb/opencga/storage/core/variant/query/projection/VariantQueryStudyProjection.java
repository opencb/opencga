package org.opencb.opencga.storage.core.variant.query.projection;

import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;

import java.util.List;
import java.util.Map;

public class VariantQueryStudyProjection {

    private final int studyId;
    private final StudyMetadata studyMetadata;
    private final List<Integer> samples;
    private final Map<Integer, List<Integer>> multiFileSamples;
    private final List<Integer> files;
    private final List<Integer> cohortIds;

    VariantQueryStudyProjection(int studyId, StudyMetadata studyMetadata, List<Integer> samples,
                                       Map<Integer, List<Integer>> multiFileSamples, List<Integer> files, List<Integer> cohortIds) {
        this.studyId = studyId;
        this.studyMetadata = studyMetadata;
        this.samples = samples;
        this.multiFileSamples = multiFileSamples;
        this.files = files;
        this.cohortIds = cohortIds;
    }
}
