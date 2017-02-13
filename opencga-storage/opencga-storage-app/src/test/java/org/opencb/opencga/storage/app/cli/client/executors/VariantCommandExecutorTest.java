/*
 * Copyright 2015-2016 OpenCB
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

package org.opencb.opencga.storage.app.cli.client.executors;

import htsjdk.variant.variantcontext.VariantContext;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.tools.variant.converters.VariantContextToAvroVariantConverter;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageEngine;

import java.io.FileInputStream;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by imedina on 09/02/17.
 */
public class VariantCommandExecutorTest {

    @Test
    public void export() throws Exception {
        VariantStorageEngine variantStorageEngine;


        StorageConfiguration storageConfiguration = StorageConfiguration.load(new FileInputStream
                ("/home/imedina/appl/opencga/opencga-storage/build/conf/storage-configuration.yml"));

        variantStorageEngine = new MongoDBVariantStorageEngine();
        variantStorageEngine.setConfiguration(storageConfiguration, "mongodb");

        VariantDBAdaptor variantDBAdaptor = variantStorageEngine.getDBAdaptor("export_test");
        List<String> studyNames = variantDBAdaptor.getStudyConfigurationManager().getStudyNames(new QueryOptions());

        try {
            Query query = new Query();
            QueryOptions options = new QueryOptions();

//            VariantContextToAvroVariantConverter variantContextToAvroVariantConverter =
//                    new VariantContextToAvroVariantConverter("default", Collections.singletonList("NA06984"), Collections.emptyList());
//            VariantDBIterator iterator = variantDBAdaptor.iterator(query, options);
//            while (iterator.hasNext()) {
//                Variant variant = iterator.next();
//                VariantContext variantContext = variantContextToAvroVariantConverter.from(variant);
//
////                System.out.println(variant.toJson());
//                System.out.println(variantContext.toStringDecodeGenotypes());
//            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}