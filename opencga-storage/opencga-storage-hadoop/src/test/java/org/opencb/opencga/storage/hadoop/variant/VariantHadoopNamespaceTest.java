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
import org.opencb.opencga.storage.core.variant.VariantStorageManagerTestUtils;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;

/**
 * Created on 19/07/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantHadoopNamespaceTest extends VariantStorageManagerTestUtils implements HadoopVariantStorageManagerTestUtils{

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
        admin.createNamespace(NamespaceDescriptor.create("opencga").build());


        runDefaultETL(smallInputUri, variantStorageManager, newStudyConfiguration(),
                new ObjectMap()
                        .append(HadoopVariantStorageManager.OPENCGA_STORAGE_HADOOP_HBASE_NAMESPACE, "opencga")
                        .append(VariantStorageManager.Options.ANNOTATE.key(), false));

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
    }

    @Test
    public void testNoNamespace() throws Exception {
        runDefaultETL(smallInputUri, getVariantStorageManager(), newStudyConfiguration(),
                new ObjectMap()
                        .append(HadoopVariantStorageManager.OPENCGA_STORAGE_HADOOP_HBASE_NAMESPACE, "")
                        .append(VariantStorageManager.Options.ANNOTATE.key(), false));

        HadoopVariantStorageManager variantStorageManager = getVariantStorageManager();
        Admin admin = variantStorageManager.getDBAdaptor().getConnection().getAdmin();

        for (NamespaceDescriptor namespaceDescriptor : admin.listNamespaceDescriptors()) {
            System.out.println("namespaceDescriptor = " + namespaceDescriptor);
            for (TableName tableName : admin.listTableNamesByNamespace(namespaceDescriptor.getName())) {
                System.out.println("tableName = " + tableName);
            }
        }

    }

}
