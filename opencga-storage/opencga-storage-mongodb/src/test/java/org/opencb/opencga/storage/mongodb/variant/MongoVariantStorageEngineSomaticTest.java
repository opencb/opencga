package org.opencb.opencga.storage.mongodb.variant;

import org.bson.Document;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.datastore.mongodb.MongoDBIterator;
import org.opencb.opencga.core.testclassification.duration.MediumTests;
import org.opencb.opencga.storage.core.variant.VariantStorageEngineSomaticTest;
import org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass;
import org.opencb.opencga.storage.mongodb.variant.adaptors.VariantMongoDBAdaptor;
import org.opencb.opencga.storage.mongodb.variant.converters.DocumentToVariantConverter;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;
import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToStudyEntryConverter.FILE_GENOTYPE_FIELD;

/**
 * Created on 27/10/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Category(MediumTests.class)
public class MongoVariantStorageEngineSomaticTest extends VariantStorageEngineSomaticTest implements MongoDBVariantStorageTest {

    @Test
    @Override
    public void indexWithOtherFieldsExcludeGT() throws Exception {
        super.indexWithOtherFieldsExcludeGT(
                new ObjectMap(MongoDBVariantStorageOptions.EXTRA_GENOTYPE_FIELDS_COMPRESS.key(), false),
                new ObjectMap(MongoDBVariantStorageOptions.EXTRA_GENOTYPE_FIELDS_COMPRESS.key(), true));

        try (VariantMongoDBAdaptor dbAdaptor = getVariantStorageEngine().getDBAdaptor()) {
            MongoDBCollection variantsCollection = dbAdaptor.getVariantsCollection();

            MongoDBIterator<Document> it = variantsCollection.nativeQuery().find(new Document(), new QueryOptions());
            while (it.hasNext()) {
                Document document = it.next();
                List<Document> files = document.get(DocumentToVariantConverter.FILES_FIELD, List.class);
                for (Document file : files) {
                    // Somatic files (no GT field) store "NA" genotype in mgt for all samples
                    Document mgt = file.get(FILE_GENOTYPE_FIELD, Document.class);
                    assertNotNull(mgt);
                    assertEquals(Collections.singleton(GenotypeClass.NA_GT_VALUE), mgt.keySet());
                }
            }
        }
    }

}
