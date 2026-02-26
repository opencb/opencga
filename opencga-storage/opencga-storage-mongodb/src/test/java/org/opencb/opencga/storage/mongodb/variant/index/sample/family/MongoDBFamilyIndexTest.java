package org.opencb.opencga.storage.mongodb.variant.index.sample.family;

import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.testclassification.duration.MediumTests;
import org.opencb.opencga.storage.core.variant.index.sample.family.FamilyIndexTest;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageEngine;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageTest;
import org.opencb.opencga.storage.mongodb.variant.index.sample.MongodBSampleIndexTest;

import java.net.URI;

/**
 * MongoDB implementation of {@link FamilyIndexTest}.
 */
@Category(MediumTests.class)
public class MongoDBFamilyIndexTest extends FamilyIndexTest implements MongoDBVariantStorageTest {

    @Override
    protected void postLoad(URI outputUri) throws Exception {
        URI outdir = newOutputUri("post-load", getTmpRootDir().toUri());
        MongoDBVariantStorageEngine engine = ((MongoDBVariantStorageEngine) variantStorageEngine);
        for (String studyName : engine.getMetadataManager().getStudies().keySet()) {
            MongodBSampleIndexTest.printSampleIndexContents(studyName, outdir,
                    engine.getDBAdaptor(),
                    engine.getSampleIndexDBAdaptor());
        }
    }
}
