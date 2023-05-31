package org.opencb.opencga.storage.mongodb.variant.search;

import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.testclassification.duration.ShortTests;
import org.opencb.opencga.storage.core.variant.search.VariantSearchIndexTest;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageTest;

/**
 * Created on 19/04/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Category(ShortTests.class)
public class MongoDBVariantSearchIndexTest extends VariantSearchIndexTest implements MongoDBVariantStorageTest {
}
