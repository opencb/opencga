package org.opencb.opencga.storage.core.variant.transform;

import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.commons.run.Task;

import java.util.List;

/**
 * Replaces {@link StudyEntry#getStudyId()} and {@link FileEntry#getFileId()} with the given values.
 *
 * Created on 20/02/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class RemapVariantIdsTask implements Task<Variant, Variant> {
    private final String fileIdStr;
    private String studyIdStr;

    public RemapVariantIdsTask(int studyId, int fileId) {
        this.studyIdStr = String.valueOf(studyId);
        this.fileIdStr = String.valueOf(fileId);
    }

    @Override
    public List<Variant> apply(List<Variant> batch) throws Exception {
        batch.forEach(variant -> variant.getStudies()
                .forEach((StudyEntry studyEntry) -> {
                    studyEntry.setStudyId(studyIdStr);
                    studyEntry.getFiles().forEach((FileEntry fileEntry) -> fileEntry.setFileId(fileIdStr));
                }));
        return batch;
    }
}
