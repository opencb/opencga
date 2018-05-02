package org.opencb.opencga.storage.core.variant.transform;

import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.run.Task;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;

import java.util.List;

/**
 * Created on 20/02/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class RemapVariantIdsTask implements Task<Variant, Variant> {
    private final StudyConfiguration studyConfiguration;
    private final String fileIdStr;

    public RemapVariantIdsTask(StudyConfiguration studyConfiguration, int fileId) {
        this.studyConfiguration = studyConfiguration;
        this.fileIdStr = String.valueOf(fileId);
    }

    @Override
    public List<Variant> apply(List<Variant> batch) throws Exception {
        batch.forEach(variant -> variant.getStudies()
                .forEach(studyEntry -> {
                    studyEntry.setStudyId(Integer.toString(studyConfiguration.getStudyId()));
                    studyEntry.getFiles().forEach(fileEntry -> fileEntry.setFileId(fileIdStr));
                }));
        return batch;
    }
}
