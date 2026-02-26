package org.opencb.opencga.storage.mongodb.variant.annotation;

import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.testclassification.duration.MediumTests;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManagerTest;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageTest;

/**
 * Created on 24/04/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Category(MediumTests.class)
public class MongoDBVariantAnnotationManagerTest extends VariantAnnotationManagerTest implements MongoDBVariantStorageTest {
}
