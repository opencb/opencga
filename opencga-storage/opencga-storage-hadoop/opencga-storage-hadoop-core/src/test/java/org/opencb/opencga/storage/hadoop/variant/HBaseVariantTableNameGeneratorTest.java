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

import org.apache.hadoop.conf.Configuration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opencb.commons.datastore.core.ObjectMap;

import static org.junit.Assert.assertEquals;
import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine.*;

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
        assertEquals(DEFAULT_ARCHIVE_TABLE_PREFIX + "44", HadoopVariantStorageEngine.getArchiveTableName(44, new ObjectMap()));
        assertEquals(DEFAULT_ARCHIVE_TABLE_PREFIX + "44", HadoopVariantStorageEngine.getArchiveTableName(44, new Configuration()));
    }

    @Test
    public void archiveNameWithPrefix() throws Exception {
        String myPrefix = "prefix";

        ObjectMap options = new ObjectMap().append(ARCHIVE_TABLE_PREFIX, myPrefix);
        assertEquals(myPrefix + "_" + "44", HadoopVariantStorageEngine.getArchiveTableName(44, options));
        Configuration conf = new Configuration();
        conf.set(ARCHIVE_TABLE_PREFIX, myPrefix);
        assertEquals(myPrefix + "_" + "44", HadoopVariantStorageEngine.getArchiveTableName(44, conf));
    }

    @Test
    public void archiveNameWithPrefixUnderscore() throws Exception {
        String myPrefix = "prefix_";

        ObjectMap options = new ObjectMap().append(ARCHIVE_TABLE_PREFIX, myPrefix);
        assertEquals(myPrefix + "44", HadoopVariantStorageEngine.getArchiveTableName(44, options));
        Configuration conf = new Configuration();
        conf.set(ARCHIVE_TABLE_PREFIX, myPrefix);
        assertEquals(myPrefix + "44", HadoopVariantStorageEngine.getArchiveTableName(44, conf));
    }

    @Test
    public void archiveNameWithEmptyPrefix() throws Exception {
        String myPrefix = "";

        ObjectMap options = new ObjectMap().append(ARCHIVE_TABLE_PREFIX, myPrefix);
        assertEquals(DEFAULT_ARCHIVE_TABLE_PREFIX + "44", HadoopVariantStorageEngine.getArchiveTableName(44, options));
        Configuration conf = new Configuration();
        conf.set(ARCHIVE_TABLE_PREFIX, myPrefix);
        assertEquals(DEFAULT_ARCHIVE_TABLE_PREFIX + "44", HadoopVariantStorageEngine.getArchiveTableName(44, conf));
    }

    @Test
    public void archiveNameWithNullPrefix() throws Exception {
        String myPrefix = null;

        ObjectMap options = new ObjectMap().append(ARCHIVE_TABLE_PREFIX, myPrefix);
        assertEquals(DEFAULT_ARCHIVE_TABLE_PREFIX + "44", HadoopVariantStorageEngine.getArchiveTableName(44, options));
        //Configuration object does not accept null values
//        Configuration conf = new Configuration();
//        conf.set(OPENCGA_STORAGE_HADOOP_HBASE_ARCHIVE_TABLE_PREFIX, myPrefix);
//        assertEquals(ARCHIVE_TABLE_PREFIX + "44", HadoopVariantStorageEngine.getArchiveTableName(44, conf));
    }

    @Test
    public void archiveNameWithNamespace() throws Exception {
        String namespace = "ns";

        ObjectMap options = new ObjectMap().append(HBASE_NAMESPACE, namespace);
        assertEquals(namespace + ":" + DEFAULT_ARCHIVE_TABLE_PREFIX + "44", HadoopVariantStorageEngine.getArchiveTableName(44, options));
    }

    @Test
    public void variantNameWithNamespace() throws Exception {
        String namespace = "ns";
        String table = "table";

        ObjectMap options = new ObjectMap().append(HBASE_NAMESPACE, namespace);
        assertEquals(namespace + ":" + table, HadoopVariantStorageEngine.getVariantTableName(table, options));
    }

    @Test
    public void variantNameWithNamespaceSame() throws Exception {
        String namespace = "ns";
        String table = "table";

        ObjectMap options = new ObjectMap().append(HBASE_NAMESPACE, namespace);
        assertEquals(namespace + ":" + table, HadoopVariantStorageEngine.getVariantTableName(namespace + ":" + table, options));
    }

    @Test
    public void variantNameWithNamespaceInline() throws Exception {
        String namespace = "ns";
        String table = "table";

        ObjectMap options = new ObjectMap();
        assertEquals(namespace + ":" + table, HadoopVariantStorageEngine.getVariantTableName(namespace + ":" + table, options));
    }

    @Test
    public void variantNameWithNamespaceWrong() throws Exception {
        String namespace = "ns";
        String table = "table";

        ObjectMap options = new ObjectMap().append(HBASE_NAMESPACE, namespace);
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Wrong namespace");
        HadoopVariantStorageEngine.getVariantTableName("wrong_ns" + ":" + table, options);
    }

    @Test
    public void variantNameWithEmptyNamespace() throws Exception {
        String namespace = "ns";
        String table = "table";

        ObjectMap options = new ObjectMap();
        assertEquals(namespace + ":" + table, HadoopVariantStorageEngine.getVariantTableName(namespace + ":" + table, options));
    }

    @Test
    public void variantNameWithNamespaceMalformed() throws Exception {
        String namespace = "ns@234";
        String table = "table";

        ObjectMap options = new ObjectMap();
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Illegal character");
        HadoopVariantStorageEngine.getVariantTableName(namespace + ":" + table, options);
    }
    @Test
    public void variantNameMalformed() throws Exception {
        String table = "table_#";

        ObjectMap options = new ObjectMap();
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Illegal character");
        HadoopVariantStorageEngine.getVariantTableName(table, options);
    }

}
