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
import org.opencb.opencga.storage.hadoop.variant.AbstractAnalysisTableDriver;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.metadata.HBaseVariantStorageMetadataDBAdaptorFactory;
import org.opencb.opencga.storage.hadoop.variant.utils.HBaseVariantTableNameGenerator;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

/**
 * @author Matthias Haimel mh719+git@cam.ac.uk
 */
public class VariantTableHelper extends GenomeHelper {

    public static final String OPENCGA_STORAGE_HADOOP_VCF_TRANSFORM_TABLE_OUTPUT = "opencga.storage.hadoop.vcf.transform.table.output";
    public static final String OPENCGA_STORAGE_HADOOP_VCF_TRANSFORM_TABLE_INPUT = "opencga.storage.hadoop.vcf.transform.table.input";
    private final byte[] analysisTable;
    private final byte[] archiveTable;
    private HBaseVariantTableNameGenerator generator;
    private int studyId;

    public VariantTableHelper(Configuration conf) {
        this(conf, conf.get(OPENCGA_STORAGE_HADOOP_VCF_TRANSFORM_TABLE_INPUT, StringUtils.EMPTY),
                conf.get(OPENCGA_STORAGE_HADOOP_VCF_TRANSFORM_TABLE_OUTPUT, StringUtils.EMPTY));
    }

    public VariantTableHelper(Configuration conf, String archiveTable, String analysisTable) {
        super(conf);
        if (StringUtils.isEmpty(analysisTable)) {
            throw new IllegalArgumentException("Property for Analysis Table name missing or empty!!!");
        }
        if (StringUtils.isEmpty(archiveTable)) {
            throw new IllegalArgumentException("Property for Archive Table name missing or empty!!!");
        }
        this.analysisTable = Bytes.toBytes(analysisTable);
        this.archiveTable = Bytes.toBytes(archiveTable);
        String dbName = HBaseVariantTableNameGenerator.getDBNameFromVariantsTableName(analysisTable);
        generator = new HBaseVariantTableNameGenerator(dbName, conf);

        studyId = HBaseVariantTableNameGenerator.getStudyIdFromArchiveTable(archiveTable);
    }

    public boolean createVariantTableIfNeeded(Connection con) throws IOException {
        return createVariantTableIfNeeded(this, getAnalysisTableAsString(), con);
    }

    public boolean createVariantTableIfNeeded() throws IOException {
        return createVariantTableIfNeeded(this, getAnalysisTableAsString());
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
                LoggerFactory.getLogger(AbstractAnalysisTableDriver.class).info("Phoenix connection is autoclosed ... " + jdbcConnection);
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
                LoggerFactory.getLogger(AbstractAnalysisTableDriver.class).info("Phoenix connection is autoclosed ... " + jdbcConnection);
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

    public byte[] getArchiveTable() {
        return archiveTable;
    }

    public String getArchiveTableAsString() {
        return Bytes.toString(getArchiveTable());
    }

    public byte[] getAnalysisTable() {
        return analysisTable;
    }

    public byte[] getMetaTable() {
        return Bytes.toBytes(generator.getMetaTableName());
    }

    public String getMetaTableAsString() {
        return generator.getMetaTableName();
    }

    public String getAnalysisTableAsString() {
        return Bytes.toString(getAnalysisTable());
    }

    public static void setAnalysisTable(Configuration conf, String analysisTable) {
        conf.set(OPENCGA_STORAGE_HADOOP_VCF_TRANSFORM_TABLE_OUTPUT, analysisTable);
    }

    public static void setArchiveTable(Configuration conf, String archiveTable) {
        conf.set(OPENCGA_STORAGE_HADOOP_VCF_TRANSFORM_TABLE_INPUT, archiveTable);
    }

    public int getStudyId() {
        return studyId;
    }

    public HBaseVariantTableNameGenerator getHBaseVariantTableNameGenerator() {
        return generator;
    }
}
