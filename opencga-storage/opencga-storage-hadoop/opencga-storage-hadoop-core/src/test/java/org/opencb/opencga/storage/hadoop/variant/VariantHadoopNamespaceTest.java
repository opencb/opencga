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

package org.opencb.opencga.storage.hadoop.variant;

import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created on 19/07/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantHadoopNamespaceTest extends VariantStorageBaseTest implements HadoopVariantStorageTest {

    @Rule
    public ExternalResource externalResource = new HadoopExternalResource();

    @Before
    public void setUp() throws Exception {
    }

    @Override
    public Map<String, ?> getOtherStorageConfigurationOptions() {
        return new ObjectMap(HadoopVariantStorageEngine.VARIANT_TABLE_INDEXES_SKIP, true);
    }

    @Test
    public void testNamespace() throws Exception {
        HadoopVariantStorageEngine variantStorageManager = getVariantStorageEngine();
        variantStorageManager.close();
        metadataManager = null;

        variantStorageManager.getOptions().put(HadoopVariantStorageEngine.HBASE_NAMESPACE, "opencga");
        assertEquals("opencga:opencga_variants_test_variants", variantStorageManager.getVariantTableName());

        VariantHadoopDBAdaptor dbAdaptor = variantStorageManager.getDBAdaptor();
        Admin admin = dbAdaptor.getConnection().getAdmin();
        admin.createNamespace(NamespaceDescriptor.create("opencga").build());


        runDefaultETL(getResourceUri("s1.genome.vcf"), variantStorageManager, newStudyMetadata(),
                new ObjectMap().append(VariantStorageEngine.Options.ANNOTATE.key(), true)
                        .append(VariantStorageEngine.Options.CALCULATE_STATS.key(), true));

        NamespaceDescriptor[] namespaceDescriptors = admin.listNamespaceDescriptors();
        for (NamespaceDescriptor namespaceDescriptor : namespaceDescriptors) {
            System.out.println("namespaceDescriptor = " + namespaceDescriptor);
            for (TableName tableName : admin.listTableNamesByNamespace(namespaceDescriptor.getName())) {
                System.out.println("\ttableName = " + tableName);
            }
            if (namespaceDescriptor.getName().equals("opencga")) {
                Assert.assertEquals(6, admin.listTableNamesByNamespace(namespaceDescriptor.getName()).length);
            }
            if (namespaceDescriptor.getName().equals("default")) {
                Assert.assertEquals(0, admin.listTableNamesByNamespace(namespaceDescriptor.getName()).length);
            }
        }

        assertTrue(variantStorageManager.getDBAdaptor().count(null).first() > 0);
    }

    @Test
    public void testNoNamespace() throws Exception {
        runDefaultETL(smallInputUri, getVariantStorageEngine(), newStudyMetadata(),
                new ObjectMap()
                        .append(HadoopVariantStorageEngine.HBASE_NAMESPACE, "")
                        .append(VariantStorageEngine.Options.ANNOTATE.key(), true)
                        .append(VariantStorageEngine.Options.CALCULATE_STATS.key(), true));

        HadoopVariantStorageEngine variantStorageManager = getVariantStorageEngine();
        Admin admin = variantStorageManager.getDBAdaptor().getConnection().getAdmin();

        for (NamespaceDescriptor namespaceDescriptor : admin.listNamespaceDescriptors()) {
            System.out.println("namespaceDescriptor = " + namespaceDescriptor);
            for (TableName tableName : admin.listTableNamesByNamespace(namespaceDescriptor.getName())) {
                System.out.println("\ttableName = " + tableName);
            }
        }
        assertTrue(variantStorageManager.getDBAdaptor().count(null).first() > 0);
    }

}
