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
import org.junit.experimental.categories.Category;
import org.junit.rules.ExternalResource;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.testclassification.duration.LongTests;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.utils.HBaseVariantTableNameGenerator;

import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.Assert.*;

/**
 * Created on 19/07/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Category(LongTests.class)
public class VariantHadoopNamespaceTest extends VariantStorageBaseTest implements HadoopVariantStorageTest {

    @Rule
    public ExternalResource externalResource = new HadoopExternalResource();

    @Before
    public void setUp() throws Exception {
    }

    @Override
    public Map<String, ?> getOtherStorageConfigurationOptions() {
        return new ObjectMap(HadoopVariantStorageOptions.VARIANT_TABLE_INDEXES_SKIP.key(), true);
    }

    @Test
    public void testNamespace() throws Exception {
        HadoopVariantStorageEngine variantStorageManager = getVariantStorageEngine();
        variantStorageManager.close();
        metadataManager = null;

        variantStorageManager.getOptions().put(HadoopVariantStorageOptions.HBASE_NAMESPACE.key(), "opencga");
        assertEquals("opencga:opencga_variants_test_variants", variantStorageManager.getVariantTableName());

        VariantHadoopDBAdaptor dbAdaptor = variantStorageManager.getDBAdaptor();

        try (Admin admin = dbAdaptor.getHBaseManager().getConnection().getAdmin()) {
            admin.createNamespace(NamespaceDescriptor.create("opencga").build());


            runDefaultETL(getResourceUri("s1.genome.vcf"), variantStorageManager, newStudyMetadata(),
                    new ObjectMap().append(VariantStorageOptions.ANNOTATE.key(), true)
                            .append(VariantStorageOptions.LOAD_ARCHIVE.key(), true)
                            .append(VariantStorageOptions.STATS_CALCULATE.key(), true));

            NamespaceDescriptor[] namespaceDescriptors = admin.listNamespaceDescriptors();
            int expectedNumTables = 5;
            for (NamespaceDescriptor namespaceDescriptor : namespaceDescriptors) {
//                System.out.println("namespaceDescriptor = " + namespaceDescriptor);
//                for (TableName tableName : admin.listTableNamesByNamespace(namespaceDescriptor.getName())) {
//                    System.out.println("\ttableName = " + tableName);
//                }
                if (namespaceDescriptor.getName().equals("opencga")) {
                    Assert.assertEquals(expectedNumTables, admin.listTableNamesByNamespace(namespaceDescriptor.getName()).length);
                }
                if (namespaceDescriptor.getName().equals("default")) {
                    Assert.assertEquals(0, admin.listTableNamesByNamespace(namespaceDescriptor.getName()).length);
                }
            }

            int numTables = 0;
            for (TableName tableName : admin.listTableNames(Pattern.compile("opencga:opencga.*"))) {
                numTables++;
                String tableWithNS = tableName.getNameWithNamespaceInclAsString();
                String tableNoNs = tableName.getNameAsString().split(":")[1];

                assertTrue(tableWithNS, HBaseVariantTableNameGenerator.isValidTable("opencga", DB_NAME, tableWithNS));
                assertTrue(tableNoNs, HBaseVariantTableNameGenerator.isValidTable("", DB_NAME, tableNoNs));

                assertFalse(tableWithNS, HBaseVariantTableNameGenerator.isValidTable("", DB_NAME, tableWithNS));
                assertFalse(tableWithNS, HBaseVariantTableNameGenerator.isValidTable("default", DB_NAME, tableWithNS));
                assertFalse(tableNoNs, HBaseVariantTableNameGenerator.isValidTable("opencga", DB_NAME, tableNoNs));
            }
            assertEquals(expectedNumTables, numTables);
        }
        assertTrue(variantStorageManager.getDBAdaptor().count().first() > 0);
    }

    @Test
    public void testNoNamespace() throws Exception {
        runDefaultETL(smallInputUri, getVariantStorageEngine(), newStudyMetadata(),
                new ObjectMap()
                        .append(HadoopVariantStorageOptions.HBASE_NAMESPACE.key(), "")
                        .append(VariantStorageOptions.ANNOTATE.key(), true)
                        .append(VariantStorageOptions.STATS_CALCULATE.key(), true));

        HadoopVariantStorageEngine variantStorageManager = getVariantStorageEngine();
        Admin admin = variantStorageManager.getDBAdaptor().getConnection().getAdmin();

        for (NamespaceDescriptor namespaceDescriptor : admin.listNamespaceDescriptors()) {
            System.out.println("namespaceDescriptor = " + namespaceDescriptor);
            for (TableName tableName : admin.listTableNamesByNamespace(namespaceDescriptor.getName())) {
                System.out.println("\ttableName = " + tableName);
            }
        }
        assertTrue(variantStorageManager.getDBAdaptor().count().first() > 0);
    }

}
