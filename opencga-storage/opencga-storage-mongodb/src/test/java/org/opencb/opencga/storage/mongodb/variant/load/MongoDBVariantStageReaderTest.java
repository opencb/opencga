package org.opencb.opencga.storage.mongodb.variant.load;

import org.bson.Document;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.storage.mongodb.variant.MongoVariantStorageManagerTestUtils;
import org.opencb.opencga.storage.mongodb.variant.converters.VariantStringIdComplexTypeConverter;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.opencb.opencga.storage.core.variant.VariantStorageManagerTestUtils.DB_NAME;

/**
 * Created on 14/06/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class MongoDBVariantStageReaderTest implements MongoVariantStorageManagerTestUtils{


    private MongoDBCollection collection;

    @Before
    public void setUp() throws Exception {
        clearDB(DB_NAME);
        MongoDataStoreManager mongoDataStoreManager = getMongoDataStoreManager(DB_NAME);

        collection = mongoDataStoreManager.get(DB_NAME).getCollection("stage");
        MongoDBVariantStageLoader loader = new MongoDBVariantStageLoader(collection, 1, 1, 100);



        loader.open();
        loader.pre();
        loader.write(getVariant("10:100:A:T"));
        loader.write(getVariant("11:100:A:T"));
        loader.write(getVariant("1:100:A:T"));
        loader.write(getVariant("2:100:A:T"));
        loader.write(getVariant("22:100:A:T"));
        loader.write(getVariant("X:100:A:T"));
        loader.post();
        loader.close();

    }


    public Variant getVariant(String variantString) {
        Variant variant = new Variant(variantString);
        variant.addStudyEntry(new StudyEntry("1", "1"));
        return variant;
    }

    @Test
    public void testReadStage() throws Exception {
        for (String chr : Arrays.asList("10", "11", "1", "2", "22", "X")) {
            MongoDBVariantStageReader reader = new MongoDBVariantStageReader(collection, 1, Collections.singletonList(chr));

            List<Document> read = readAll(reader);

            for (Document document : read) {
                Assert.assertTrue(document.getString("_id").startsWith(VariantStringIdComplexTypeConverter.convertChromosome(chr)));
            }
        }
    }

    @Test
    public void testReadStageAll() throws Exception {
        MongoDBVariantStageReader reader = new MongoDBVariantStageReader(collection, 1, Collections.emptyList());

        List<Document> read = readAll(reader);

        Assert.assertEquals(read.size(), 6);
    }

    public List<Document> readAll(MongoDBVariantStageReader reader) {
        List<Document> read;
        reader.open();
        reader.pre();
        read = reader.read(500);
        reader.post();
        reader.close();
        return read;
    }
}