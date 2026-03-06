package org.opencb.opencga.storage.mongodb.variant.gaps;

import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.testclassification.duration.LongTests;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.gaps.FillGapsTest;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageTest;

import java.net.URI;

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
    protected void checkInputValuesAreUnmodified(String aggregatedStudy, String referenceStudy) throws Exception {
        throw new Exception("Unimplemented check");
    }
}
