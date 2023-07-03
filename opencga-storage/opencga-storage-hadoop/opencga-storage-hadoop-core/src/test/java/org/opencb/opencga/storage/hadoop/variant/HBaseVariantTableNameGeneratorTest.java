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
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.testclassification.duration.ShortTests;
import org.opencb.opencga.storage.hadoop.variant.utils.HBaseVariantTableNameGenerator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created on 02/08/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Category(ShortTests.class)
public class HBaseVariantTableNameGeneratorTest {

    private static final String DB_NAME = "dbName";

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void archiveNameDefault() throws Exception {
        assertEquals(DB_NAME + "_archive_44", HBaseVariantTableNameGenerator.getArchiveTableName(DB_NAME, 44, new ObjectMap()));
        assertEquals(DB_NAME + "_archive_44", HBaseVariantTableNameGenerator.getArchiveTableName(DB_NAME, 44, new Configuration()));
    }

    @Test
    public void archiveNameWithDBNameUnderscore() throws Exception {
        assertEquals(DB_NAME + "_archive_44", HBaseVariantTableNameGenerator.getArchiveTableName("", DB_NAME + "_", 44));
    }

    @Test
    public void archiveNameWithNamespace() throws Exception {
        String namespace = "ns";

        ObjectMap options = new ObjectMap().append(HadoopVariantStorageOptions.HBASE_NAMESPACE.key(), namespace);
        assertEquals(namespace + ":" + DB_NAME + "_archive_44", HBaseVariantTableNameGenerator.getArchiveTableName(DB_NAME, 44, options));
    }

    @Test
    public void variantNameWithNamespace() throws Exception {
        String namespace = "ns";
        String dbName = "table";

        ObjectMap options = new ObjectMap().append(HadoopVariantStorageOptions.HBASE_NAMESPACE.key(), namespace);
        assertEquals(namespace + ":" + dbName + "_variants", HBaseVariantTableNameGenerator.getVariantTableName(dbName, options));
    }

    @Test
    public void variantNameWithNamespaceSame() throws Exception {
        String namespace = "ns";
        String table = "table";

        ObjectMap options = new ObjectMap().append(HadoopVariantStorageOptions.HBASE_NAMESPACE.key(), namespace);
        assertEquals(namespace + ":" + table+"_variants", HBaseVariantTableNameGenerator.getVariantTableName(namespace + ":" + table, options));
    }

    @Test
    public void variantNameWithNamespaceInline() throws Exception {
        String namespace = "ns";
        String table = "table";

        ObjectMap options = new ObjectMap();
        assertEquals(namespace + ":" + table + "_variants", HBaseVariantTableNameGenerator.getVariantTableName(namespace + ":" + table, options));
    }

    @Test
    public void variantNameWithNamespaceWrong() throws Exception {
        String namespace = "ns";
        String table = "table";

        ObjectMap options = new ObjectMap().append(HadoopVariantStorageOptions.HBASE_NAMESPACE.key(), namespace);
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Wrong namespace");
        HBaseVariantTableNameGenerator.getVariantTableName("wrong_ns" + ":" + table, options);
    }

    @Test
    public void variantNameWithEmptyNamespace() throws Exception {
        String namespace = "ns";
        String table = "table";

        ObjectMap options = new ObjectMap();
        assertEquals(namespace + ":" + table + "_variants", HBaseVariantTableNameGenerator.getVariantTableName(namespace + ":" + table, options));
    }

    @Test
    public void variantNameWithNamespaceMalformed() throws Exception {
        String namespace = "ns@234";
        String table = "table";

        ObjectMap options = new ObjectMap();
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Illegal character");
        HBaseVariantTableNameGenerator.getVariantTableName(namespace + ":" + table, options);
    }
    @Test
    public void variantNameMalformed() throws Exception {
        String table = "table_#";

        ObjectMap options = new ObjectMap();
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Illegal character");
        HBaseVariantTableNameGenerator.getVariantTableName(table, options);
    }

    @Test
    public void getDBNameFromVariantsTable() throws Exception {
        assertEquals("dbName", HBaseVariantTableNameGenerator.getDBNameFromVariantsTableName("dbName_variants"));
    }

    @Test
    public void checkArchiveTable() throws Exception {
        assertEquals("dbName_3_archive_4", HBaseVariantTableNameGenerator.getDBNameFromArchiveTableName("dbName_3_archive_4_archive_3"));
        assertTrue(HBaseVariantTableNameGenerator.isValidArchiveTableName("dbName_3_archive_4", "dbName_3_archive_4_archive_3"));
    }

    @Test
    public void getDBNameFromSampleIndexTable() throws Exception {
        assertEquals("dbname", HBaseVariantTableNameGenerator.getDBNameFromSampleIndexTableName("dbname_variant_sample_index_33_v45"));
        assertEquals("dbname_variant_sample_index", HBaseVariantTableNameGenerator.getDBNameFromSampleIndexTableName("dbname_variant_sample_index_variant_sample_index_33_v45"));
    }

    @Test
    public void getDBNameFromVariantsTableMalformed() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Invalid variants table name");
        HBaseVariantTableNameGenerator.getDBNameFromVariantsTableName("dbName_archive_3");
    }
}
