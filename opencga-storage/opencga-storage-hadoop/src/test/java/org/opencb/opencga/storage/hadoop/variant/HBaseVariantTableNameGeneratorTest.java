package org.opencb.opencga.storage.hadoop.variant;

import org.apache.hadoop.conf.Configuration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.commons.datastore.core.ObjectMap;

import static org.junit.Assert.assertEquals;
import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageManager.*;

/**
 * Created on 02/08/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HBaseVariantTableNameGeneratorTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void archiveNameDefault() throws Exception {
        assertEquals(ARCHIVE_TABLE_PREFIX + "44", HadoopVariantStorageManager.getArchiveTableName(44, new ObjectMap()));
        assertEquals(ARCHIVE_TABLE_PREFIX + "44", HadoopVariantStorageManager.getArchiveTableName(44, new Configuration()));
    }

    @Test
    public void archiveNameWithPrefix() throws Exception {
        String myPrefix = "prefix";

        ObjectMap options = new ObjectMap().append(OPENCGA_STORAGE_HADOOP_HBASE_ARCHIVE_TABLE_PREFIX, myPrefix);
        assertEquals(myPrefix + "_" + "44", HadoopVariantStorageManager.getArchiveTableName(44, options));
        Configuration conf = new Configuration();
        conf.set(OPENCGA_STORAGE_HADOOP_HBASE_ARCHIVE_TABLE_PREFIX, myPrefix);
        assertEquals(myPrefix + "_" + "44", HadoopVariantStorageManager.getArchiveTableName(44, conf));
    }

    @Test
    public void archiveNameWithPrefixUnderscore() throws Exception {
        String myPrefix = "prefix_";

        ObjectMap options = new ObjectMap().append(OPENCGA_STORAGE_HADOOP_HBASE_ARCHIVE_TABLE_PREFIX, myPrefix);
        assertEquals(myPrefix + "44", HadoopVariantStorageManager.getArchiveTableName(44, options));
        Configuration conf = new Configuration();
        conf.set(OPENCGA_STORAGE_HADOOP_HBASE_ARCHIVE_TABLE_PREFIX, myPrefix);
        assertEquals(myPrefix + "44", HadoopVariantStorageManager.getArchiveTableName(44, conf));
    }

    @Test
    public void archiveNameWithEmptyPrefix() throws Exception {
        String myPrefix = "";

        ObjectMap options = new ObjectMap().append(OPENCGA_STORAGE_HADOOP_HBASE_ARCHIVE_TABLE_PREFIX, myPrefix);
        assertEquals(ARCHIVE_TABLE_PREFIX + "44", HadoopVariantStorageManager.getArchiveTableName(44, options));
        Configuration conf = new Configuration();
        conf.set(OPENCGA_STORAGE_HADOOP_HBASE_ARCHIVE_TABLE_PREFIX, myPrefix);
        assertEquals(ARCHIVE_TABLE_PREFIX + "44", HadoopVariantStorageManager.getArchiveTableName(44, conf));
    }

    @Test
    public void archiveNameWithNullPrefix() throws Exception {
        String myPrefix = null;

        ObjectMap options = new ObjectMap().append(OPENCGA_STORAGE_HADOOP_HBASE_ARCHIVE_TABLE_PREFIX, myPrefix);
        assertEquals(ARCHIVE_TABLE_PREFIX + "44", HadoopVariantStorageManager.getArchiveTableName(44, options));
        //Configuration object does not accept null values
//        Configuration conf = new Configuration();
//        conf.set(OPENCGA_STORAGE_HADOOP_HBASE_ARCHIVE_TABLE_PREFIX, myPrefix);
//        assertEquals(ARCHIVE_TABLE_PREFIX + "44", HadoopVariantStorageManager.getArchiveTableName(44, conf));
    }

    @Test
    public void archiveNameWithNamespace() throws Exception {
        String namespace = "ns";

        ObjectMap options = new ObjectMap().append(OPENCGA_STORAGE_HADOOP_HBASE_NAMESPACE, namespace);
        assertEquals(namespace + ":" + ARCHIVE_TABLE_PREFIX + "44", HadoopVariantStorageManager.getArchiveTableName(44, options));
    }

    @Test
    public void variantNameWithNamespace() throws Exception {
        String namespace = "ns";
        String table = "table";

        ObjectMap options = new ObjectMap().append(OPENCGA_STORAGE_HADOOP_HBASE_NAMESPACE, namespace);
        assertEquals(namespace + ":" + table, HadoopVariantStorageManager.getVariantTableName(table, options));
    }

    @Test
    public void variantNameWithNamespaceSame() throws Exception {
        String namespace = "ns";
        String table = "table";

        ObjectMap options = new ObjectMap().append(OPENCGA_STORAGE_HADOOP_HBASE_NAMESPACE, namespace);
        assertEquals(namespace + ":" + table, HadoopVariantStorageManager.getVariantTableName(namespace + ":" + table, options));
    }

    @Test
    public void variantNameWithNamespaceInline() throws Exception {
        String namespace = "ns";
        String table = "table";

        ObjectMap options = new ObjectMap();
        assertEquals(namespace + ":" + table, HadoopVariantStorageManager.getVariantTableName(namespace + ":" + table, options));
    }

    @Test
    public void variantNameWithNamespaceWrong() throws Exception {
        String namespace = "ns";
        String table = "table";

        ObjectMap options = new ObjectMap().append(OPENCGA_STORAGE_HADOOP_HBASE_NAMESPACE, namespace);
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Wrong namespace");
        HadoopVariantStorageManager.getVariantTableName("wrong_ns" + ":" + table, options);
    }

    @Test
    public void variantNameWithEmptyNamespace() throws Exception {
        String namespace = "ns";
        String table = "table";

        ObjectMap options = new ObjectMap();
        assertEquals(namespace + ":" + table, HadoopVariantStorageManager.getVariantTableName(namespace + ":" + table, options));
    }

    @Test
    public void variantNameWithNamespaceMalformed() throws Exception {
        String namespace = "ns@234";
        String table = "table";

        ObjectMap options = new ObjectMap();
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Illegal character");
        HadoopVariantStorageManager.getVariantTableName(namespace + ":" + table, options);
    }
    @Test
    public void variantNameMalformed() throws Exception {
        String table = "table_#";

        ObjectMap options = new ObjectMap();
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Illegal character");
        HadoopVariantStorageManager.getVariantTableName(table, options);
    }

}
