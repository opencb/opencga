/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageTest;
import org.opencb.opencga.storage.mongodb.variant.converters.VariantStringIdConverter;
import org.opencb.opencga.storage.mongodb.variant.load.stage.MongoDBVariantStageConverterTask;
import org.opencb.opencga.storage.mongodb.variant.load.stage.MongoDBVariantStageLoader;
import org.opencb.opencga.storage.mongodb.variant.load.stage.MongoDBVariantStageReader;

import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static org.opencb.opencga.storage.core.variant.VariantStorageBaseTest.DB_NAME;

/**
 * Created on 14/06/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class MongoDBVariantStageReaderTest implements MongoDBVariantStorageTest {


    private MongoDBCollection collection;
    private Multimap<String, Variant> variantMap;

    @Before
    public void setUp() throws Exception {
        clearDB(DB_NAME);
        variantMap = HashMultimap.create();
        MongoDataStoreManager mongoDataStoreManager = getMongoDataStoreManager(DB_NAME);

        collection = mongoDataStoreManager.get(DB_NAME).getCollection("stage");
        MongoDBVariantStageLoader loader = new MongoDBVariantStageLoader(collection, 1, 1, false);
        MongoDBVariantStageConverterTask converterTask = new MongoDBVariantStageConverterTask(null);

        loader.open();
        loader.pre();
        writeVariant(converterTask, loader, "10:100:A:T");
        writeVariant(converterTask, loader, "11:100:A:T");
        writeVariant(converterTask, loader, "1:100:A:T");
        writeVariant(converterTask, loader, "2:100:A:T");
        writeVariant(converterTask, loader, "22:100:A:T");
        writeVariant(converterTask, loader, "X:100:A:T");
        loader.post();
        loader.close();

    }

    public void writeVariant(MongoDBVariantStageConverterTask converterTask, MongoDBVariantStageLoader loader, String variantStr) {
        Variant variant = new Variant(variantStr);
        variant.setNames(Collections.emptyList());
        StudyEntry studyEntry = new StudyEntry("1", "1");
        studyEntry.setSampleDataKeys(Collections.emptyList());
        variant.addStudyEntry(studyEntry);
        variantMap.put(variant.getChromosome(), variant);
        loader.write(converterTask.apply(Collections.singletonList(variant)));
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
                    contains |= document.getString("_id").startsWith(VariantStringIdConverter.convertChromosome(chr));
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