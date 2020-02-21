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

/**
 *
 */
package org.opencb.opencga.storage.hadoop.variant.mr;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.util.SchemaUtil;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.AbstractVariantsTableDriver;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageOptions;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveDriver;
import org.opencb.opencga.storage.hadoop.variant.utils.HBaseVariantTableNameGenerator;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * @author Matthias Haimel mh719+git@cam.ac.uk
 */
public class VariantTableHelper extends GenomeHelper {

    private final byte[] variantsTable;
    private HBaseVariantTableNameGenerator generator;

    public VariantTableHelper(Configuration conf) {
        this(conf, conf.get(AbstractVariantsTableDriver.CONFIG_VARIANT_TABLE_NAME, StringUtils.EMPTY));
    }

    public VariantTableHelper(Configuration conf, String variantsTable) {
        super(conf);
        if (StringUtils.isEmpty(variantsTable)) {
            for (Map.Entry<String, String> entry : conf) {
                System.out.println(entry.getKey() + " : " + entry.getValue());
            }
            System.out.flush();
            throw new IllegalArgumentException("Property for Variants Table name missing or empty!!!");
        }
        this.variantsTable = Bytes.toBytes(variantsTable);
        String dbName = HBaseVariantTableNameGenerator.getDBNameFromVariantsTableName(variantsTable);
        generator = new HBaseVariantTableNameGenerator(dbName, conf);

//        setStudyId(HBaseVariantTableNameGenerator.getStudyIdFromArchiveTable(archiveTable));
    }

    public static boolean createVariantTableIfNeeded(GenomeHelper genomeHelper, String tableName) throws IOException {
        try (Connection con = ConnectionFactory.createConnection(genomeHelper.getConf())) {
            return createVariantTableIfNeeded(genomeHelper, tableName, con);
        }
    }

    public static boolean createVariantTableIfNeeded(GenomeHelper genomeHelper, String tableName, Connection con)
            throws IOException {
        VariantPhoenixHelper variantPhoenixHelper = new VariantPhoenixHelper(genomeHelper);

        String namespace = SchemaUtil.getSchemaNameFromFullName(tableName);
        if (StringUtils.isNotEmpty(namespace)) {
//            HBaseManager.createNamespaceIfNeeded(con, namespace);
            try (java.sql.Connection jdbcConnection = variantPhoenixHelper.newJdbcConnection()) {
                variantPhoenixHelper.createSchemaIfNeeded(jdbcConnection, namespace);
                LoggerFactory.getLogger(AbstractVariantsTableDriver.class).info("Phoenix connection is autoclosed ... " + jdbcConnection);
            } catch (ClassNotFoundException | SQLException e) {
                throw new IOException(e);
            }
        }

        int nsplits = genomeHelper.getConf().getInt(
                HadoopVariantStorageOptions.VARIANT_TABLE_PRESPLIT_SIZE.key(),
                HadoopVariantStorageOptions.VARIANT_TABLE_PRESPLIT_SIZE.defaultValue());
        List<byte[]> splitList = generateBootPreSplitsHuman(
                nsplits,
                VariantPhoenixKeyFactory::generateVariantRowKey);
        boolean newTable = HBaseManager.createTableIfNeeded(con, tableName, COLUMN_FAMILY_BYTES,
                splitList, Compression.getCompressionAlgorithmByName(
                        genomeHelper.getConf().get(
                                HadoopVariantStorageOptions.VARIANT_TABLE_COMPRESSION.key(),
                                HadoopVariantStorageOptions.VARIANT_TABLE_COMPRESSION.defaultValue())));
        if (newTable) {
            try (java.sql.Connection jdbcConnection = variantPhoenixHelper.newJdbcConnection()) {
                variantPhoenixHelper.createTableIfNeeded(jdbcConnection, tableName);
                LoggerFactory.getLogger(AbstractVariantsTableDriver.class).info("Phoenix connection is autoclosed ... " + jdbcConnection);
            } catch (ClassNotFoundException | SQLException e) {
                throw new IOException(e);
            }
        }
        return newTable;
    }

    public byte[] getVariantsTable() {
        return variantsTable;
    }

    public byte[] getMetaTable() {
        return Bytes.toBytes(generator.getMetaTableName());
    }

    public String getMetaTableAsString() {
        return generator.getMetaTableName();
    }

    public String getVariantsTableAsString() {
        return Bytes.toString(getVariantsTable());
    }

    public static void setVariantsTable(Configuration conf, String variantsTable) {
        conf.set(AbstractVariantsTableDriver.CONFIG_VARIANT_TABLE_NAME, variantsTable);
    }

    public static String getVariantsTable(Configuration conf) {
        return conf.get(AbstractVariantsTableDriver.CONFIG_VARIANT_TABLE_NAME);
    }

    public static void setArchiveTable(Configuration conf, String archiveTable) {
        conf.set(ArchiveDriver.CONFIG_ARCHIVE_TABLE_NAME, archiveTable);
    }

    public HBaseVariantTableNameGenerator getHBaseVariantTableNameGenerator() {
        return generator;
    }
}
