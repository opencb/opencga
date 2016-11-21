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
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;

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

    @Test
    public void testNamespace() throws Exception {
        HadoopVariantStorageManager variantStorageManager = getVariantStorageManager();
        VariantHadoopDBAdaptor dbAdaptor = variantStorageManager.getDBAdaptor();
        Admin admin = dbAdaptor.getConnection().getAdmin();
//        admin.createNamespace(NamespaceDescriptor.create("opencga").build());


        runDefaultETL(smallInputUri, variantStorageManager, newStudyConfiguration(),
                new ObjectMap()
                        .append(HadoopVariantStorageManager.OPENCGA_STORAGE_HADOOP_HBASE_NAMESPACE, "opencga")
                        .append(VariantStorageManager.Options.ANNOTATE.key(), true)
                        .append(VariantStorageManager.Options.CALCULATE_STATS.key(), true));

        NamespaceDescriptor[] namespaceDescriptors = admin.listNamespaceDescriptors();
        for (NamespaceDescriptor namespaceDescriptor : namespaceDescriptors) {
            System.out.println("namespaceDescriptor = " + namespaceDescriptor);
            if (namespaceDescriptor.getName().equals("opencga")) {
                Assert.assertEquals(2, admin.listTableNamesByNamespace(namespaceDescriptor.getName()).length);
            }
            for (TableName tableName : admin.listTableNamesByNamespace(namespaceDescriptor.getName())) {
                System.out.println("\ttableName = " + tableName);
            }
        }

        assertTrue(variantStorageManager.getDBAdaptor(DB_NAME).count(null).first() > 0);
    }

    @Test
    public void testNoNamespace() throws Exception {
        runDefaultETL(smallInputUri, getVariantStorageManager(), newStudyConfiguration(),
                new ObjectMap()
                        .append(HadoopVariantStorageManager.OPENCGA_STORAGE_HADOOP_HBASE_NAMESPACE, "")
                        .append(VariantStorageManager.Options.ANNOTATE.key(), true)
                        .append(VariantStorageManager.Options.CALCULATE_STATS.key(), true));

        HadoopVariantStorageManager variantStorageManager = getVariantStorageManager();
        Admin admin = variantStorageManager.getDBAdaptor().getConnection().getAdmin();

        for (NamespaceDescriptor namespaceDescriptor : admin.listNamespaceDescriptors()) {
            System.out.println("namespaceDescriptor = " + namespaceDescriptor);
            for (TableName tableName : admin.listTableNamesByNamespace(namespaceDescriptor.getName())) {
                System.out.println("\ttableName = " + tableName);
            }
        }
        assertTrue(variantStorageManager.getDBAdaptor(DB_NAME).count(null).first() > 0);
    }

}
