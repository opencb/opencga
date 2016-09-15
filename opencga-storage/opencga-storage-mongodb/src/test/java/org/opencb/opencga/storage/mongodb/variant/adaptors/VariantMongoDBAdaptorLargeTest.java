package org.opencb.opencga.storage.mongodb.variant.adaptors;

import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptorLargeTest;
import org.opencb.opencga.storage.mongodb.variant.MongoVariantStorageManagerTestUtils;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantMongoDBAdaptorLargeTest extends VariantDBAdaptorLargeTest implements MongoVariantStorageManagerTestUtils {

    @Override
    protected int skippedVariants() {
        return 4;
    }
}
