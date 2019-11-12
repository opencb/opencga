package org.opencb.opencga.storage.mongodb.variant;

import org.bson.Document;
import org.junit.Test;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.storage.core.variant.VariantStorageEngineSomaticTest;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.mongodb.variant.adaptors.VariantMongoDBAdaptor;
import org.opencb.opencga.storage.mongodb.variant.converters.DocumentToVariantConverter;

import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToStudyVariantEntryConverter.GENOTYPES_FIELD;

/**
 * Created on 27/10/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class MongoVariantStorageEngineSomaticTest extends VariantStorageEngineSomaticTest implements MongoDBVariantStorageTest {

    @Test
    @Override
    public void indexWithOtherFieldsExcludeGT() throws Exception {
        super.indexWithOtherFieldsExcludeGT(
                new ObjectMap(MongoDBVariantStorageOptions.EXTRA_GENOTYPE_FIELDS_COMPRESS.key(), false),
                new ObjectMap(MongoDBVariantStorageOptions.EXTRA_GENOTYPE_FIELDS_COMPRESS.key(), true));

        try (VariantMongoDBAdaptor dbAdaptor = getVariantStorageEngine().getDBAdaptor()) {
            MongoDBCollection variantsCollection = dbAdaptor.getVariantsCollection();

            for (Document document : variantsCollection.nativeQuery().find(new Document(), new QueryOptions())) {
                assertFalse(((Document) document.get(DocumentToVariantConverter.STUDIES_FIELD, List.class).get(0))
                        .containsKey(GENOTYPES_FIELD));
                System.out.println("dbObject = " + document);
            }
        }
    }

}
