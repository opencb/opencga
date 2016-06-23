package org.opencb.opencga.storage.mongodb.variant.load;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
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

import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static org.opencb.opencga.storage.core.variant.VariantStorageManagerTestUtils.DB_NAME;

/**
 * Created on 14/06/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class MongoDBVariantStageReaderTest implements MongoVariantStorageManagerTestUtils{


    private MongoDBCollection collection;
    private Multimap<String, Variant> variantMap;

    @Before
    public void setUp() throws Exception {
        clearDB(DB_NAME);
        variantMap = HashMultimap.create();
        MongoDataStoreManager mongoDataStoreManager = getMongoDataStoreManager(DB_NAME);

        collection = mongoDataStoreManager.get(DB_NAME).getCollection("stage");
        MongoDBVariantStageLoader loader = new MongoDBVariantStageLoader(collection, 1, 1, 100, false);



        loader.open();
        loader.pre();
        writeVariant(loader, "10:100:A:T");
        writeVariant(loader, "11:100:A:T");
        writeVariant(loader, "1:100:A:T");
        writeVariant(loader, "2:100:A:T");
        writeVariant(loader, "22:100:A:T");
        writeVariant(loader, "X:100:A:T");
        loader.post();
        loader.close();

    }

    public void writeVariant(MongoDBVariantStageLoader loader, String variantStr) {
        Variant variant = new Variant(variantStr);
        variant.addStudyEntry(new StudyEntry("1", "1"));
        variantMap.put(variant.getChromosome(), variant);
        loader.write(variant);
    }


    @Test
    public void testReadStage() throws Exception {
        for (List<String> chrs : asList(asList("11"),
                                        asList("10", "1"),
                                        asList("1"),
                                        asList("1", "X"),
                                        asList("2"),
                                        asList("22"),
                                        asList("X"))) {
            MongoDBVariantStageReader reader = new MongoDBVariantStageReader(collection, 1, chrs);

            List<Document> read = readAll(reader);

            for (Document document : read) {
                boolean contains = false;
                for (String chr : chrs) {
                    contains |= document.getString("_id").startsWith(VariantStringIdComplexTypeConverter.convertChromosome(chr));
                }
                Assert.assertTrue(contains);
            }
            int vars = 0;
            for (String chr : chrs) {
                vars += variantMap.get(chr).size();
            }
            Assert.assertEquals(vars, read.size());
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