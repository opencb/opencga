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

/**
 *
 */
package org.opencb.opencga.storage.hadoop.variant.index;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.metadata.HBaseStudyConfigurationDBAdaptor;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Matthias Haimel mh719+git@cam.ac.uk
 */
public class VariantTableHelper extends GenomeHelper {

    public static final String OPENCGA_STORAGE_HADOOP_VCF_TRANSFORM_TABLE_OUTPUT = "opencga.storage.hadoop.vcf.transform.table.output";
    public static final String OPENCGA_STORAGE_HADOOP_VCF_TRANSFORM_TABLE_INPUT = "opencga.storage.hadoop.vcf.transform.table.input";
    private final AtomicReference<byte[]> analysisTable = new AtomicReference<>();
    private final AtomicReference<byte[]> archiveTable = new AtomicReference<>();

    public VariantTableHelper(Configuration conf) {
        this(conf, null);
    }

    public VariantTableHelper(Configuration conf, Connection con) {
        this(conf, conf.get(OPENCGA_STORAGE_HADOOP_VCF_TRANSFORM_TABLE_INPUT, StringUtils.EMPTY),
                conf.get(OPENCGA_STORAGE_HADOOP_VCF_TRANSFORM_TABLE_OUTPUT, StringUtils.EMPTY), con);
    }


    public VariantTableHelper(Configuration conf, String archiveTable, String analysisTable, Connection con) {
        super(conf);
        if (StringUtils.isEmpty(analysisTable)) {
            throw new IllegalArgumentException("Property for Analysis Table name missing or empty!!!");
        }
        if (StringUtils.isEmpty(archiveTable)) {
            throw new IllegalArgumentException("Property for Archive Table name missing or empty!!!");
        }
        setAnalysisTable(analysisTable);
        setArchiveTable(archiveTable);
    }

    public StudyConfiguration readStudyConfiguration() throws IOException {
        try (StudyConfigurationManager scm = new StudyConfigurationManager(
                new HBaseStudyConfigurationDBAdaptor(getAnalysisTableAsString(), getConf(), null, null))) {
            QueryResult<StudyConfiguration> query = scm.getStudyConfiguration(getStudyId(), new QueryOptions());
            if (query.getResult().size() != 1) {
                throw new IllegalStateException("Only one study configuration expected for study");
            }
            return query.first();
        }
    }

    public byte[] getAnalysisTable() {
        return analysisTable.get();
    }

    public byte[] getArchiveTable() {
        return archiveTable.get();
    }

    public String getAnalysisTableAsString() {
        return Bytes.toString(getAnalysisTable());
    }

    private void setAnalysisTable(String tableName) {
        this.analysisTable.set(Bytes.toBytes(tableName));
    }

    private void setArchiveTable(String tableName) {
        this.archiveTable.set(Bytes.toBytes(tableName));
    }

    public static void setAnalysisTable(Configuration conf, String analysisTable) {
        conf.set(OPENCGA_STORAGE_HADOOP_VCF_TRANSFORM_TABLE_OUTPUT, analysisTable);
    }

    public static void setArchiveTable(Configuration conf, String archiveTable) {
        conf.set(OPENCGA_STORAGE_HADOOP_VCF_TRANSFORM_TABLE_INPUT, archiveTable);
    }

}
