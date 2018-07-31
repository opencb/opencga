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
package org.opencb.opencga.storage.hadoop.variant.index;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.util.SchemaUtil;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.AbstractVariantsTableDriver;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveDriver;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.metadata.HBaseVariantStorageMetadataDBAdaptorFactory;
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

    public boolean createVariantTableIfNeeded(Connection con) throws IOException {
        return createVariantTableIfNeeded(this, getVariantsTableAsString(), con);
    }

    public boolean createVariantTableIfNeeded() throws IOException {
        return createVariantTableIfNeeded(this, getVariantsTableAsString());
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

        int nsplits = genomeHelper.getConf().getInt(HadoopVariantStorageEngine.VARIANT_TABLE_PRESPLIT_SIZE, 100);
        List<byte[]> splitList = generateBootPreSplitsHuman(
                nsplits,
                (chr, pos) -> VariantPhoenixKeyFactory.generateVariantRowKey(chr, pos, "", ""));
        boolean newTable = HBaseManager.createTableIfNeeded(con, tableName, genomeHelper.getColumnFamily(),
                splitList, Compression.getCompressionAlgorithmByName(
                        genomeHelper.getConf().get(
                                HadoopVariantStorageEngine.VARIANT_TABLE_COMPRESSION,
                                Compression.Algorithm.SNAPPY.getName())));
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

    public StudyConfiguration readStudyConfiguration() throws IOException {
        try (StudyConfigurationManager scm = new StudyConfigurationManager(new HBaseVariantStorageMetadataDBAdaptorFactory(this))) {
            QueryResult<StudyConfiguration> query = scm.getStudyConfiguration(getStudyId(), new QueryOptions());
            if (query.getResult().size() != 1) {
                throw new IllegalStateException("Only one study configuration expected for study");
            }
            return query.first();
        }
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
