package org.opencb.opencga.storage.mongodb.variant.gaps;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.opencga.core.testclassification.duration.LongTests;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.gaps.FillGapsTest;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageTest;

import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertTrue;

@Category(LongTests.class)
public class MongoDBFillGapsTest extends FillGapsTest implements MongoDBVariantStorageTest {

    @Override
    protected void logVariansStatus(VariantDBAdaptor dbAdaptor) throws Exception {
        // no op
    }

    @Override
    protected void logVariansStatus(StudyMetadata studyMetadata, VariantDBAdaptor dbAdaptor, URI outputUri) throws Exception {
        // no op
    }

    @Override
    @Test
    @Ignore("Requires aggregate() which is not supported in MongoDB")
    public void testFillMissingFilterNonRef() throws Exception {
    }

    @Override
    @Test
    @Ignore("Requires aggregate() which is not supported in MongoDB")
    public void testFillMissingPlatinumFiles() throws Exception {
    }

    @Override
    protected void checkInputValuesAreUnmodified(String aggregatedStudy, String referenceStudy) throws Exception {
        VariantDBAdaptor dbAdaptor = variantStorageEngine.getDBAdaptor();
        int studyId2 = metadataManager.getStudyId(referenceStudy);
        Set<Integer> study2FileIds = new HashSet<>(metadataManager.getIndexedFiles(studyId2));

        for (Variant variant : dbAdaptor) {
            StudyEntry study2Entry = variant.getStudy(referenceStudy);
            if (study2Entry == null) {
                continue;
            }
            // Study 2 should only have original loaded file entries, no gap-filled ones
            for (FileEntry file : study2Entry.getFiles()) {
                int fileId = metadataManager.getFileId(studyId2, file.getFileId());
                assertTrue("Unexpected file " + file.getFileId() + " in " + referenceStudy + " at " + variant,
                        study2FileIds.contains(fileId));
            }
        }
    }
}
