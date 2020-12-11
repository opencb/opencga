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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.ConnectionConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.hadoop.utils.AbstractHBaseDriver;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveDriver;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveTableHelper;
import org.opencb.opencga.storage.hadoop.variant.gaps.FillMissingFromArchiveTask;
import org.opencb.opencga.storage.hadoop.variant.metadata.HBaseVariantStorageMetadataDBAdaptorFactory;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantTableHelper;
import org.opencb.opencga.storage.hadoop.variant.utils.HBaseVariantTableNameGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine.*;

/**
 * Created by mh719 on 21/11/2016.
 */
public abstract class AbstractVariantsTableDriver extends AbstractHBaseDriver implements Tool {

    public static final String CONFIG_VARIANT_TABLE_NAME           = "opencga.variant.table.name";
    public static final String TIMESTAMP                           = "opencga.variant.table.timestamp";
    public static final String NONE_TIMESTAMP                      = "none";

    private final Logger logger = LoggerFactory.getLogger(AbstractVariantsTableDriver.class);
    private GenomeHelper helper;
    private VariantStorageMetadataManager metadataManager;
    private List<Integer> fileIds;
    protected HBaseVariantTableNameGenerator generator;
    private HBaseManager hBaseManager;
    private Integer studyId;

    public AbstractVariantsTableDriver() {
        super(HBaseConfiguration.create());
    }

    public AbstractVariantsTableDriver(Configuration conf) {
        super(conf);
    }

    @Override
    protected void parseAndValidateParameters() throws IOException {
        super.parseAndValidateParameters();
        Configuration conf = getConf();
        String archiveTable = getArchiveTable();
        String variantTable = getVariantsTable();

        int maxKeyValueSize = conf.getInt(HadoopVariantStorageOptions.MR_HBASE_KEYVALUE_SIZE_MAX.key(),
                HadoopVariantStorageOptions.MR_HBASE_KEYVALUE_SIZE_MAX.defaultValue());
        logger.info("HBASE: set " + ConnectionConfiguration.MAX_KEYVALUE_SIZE_KEY + " to " + maxKeyValueSize);
        conf.setInt(ConnectionConfiguration.MAX_KEYVALUE_SIZE_KEY, maxKeyValueSize); // always overwrite server default (usually 1MB)

        /* -------------------------------*/
        // Validate parameters CHECK
//        if (StringUtils.isEmpty(archiveTable)) {
//            throw new IllegalArgumentException("No archive hbase table basename specified!!!");
//        }
        if (StringUtils.isEmpty(variantTable)) {
            throw new IllegalArgumentException("No variant hbase table specified!!!");
        }
        HBaseVariantTableNameGenerator.checkValidVariantsTableName(variantTable);
        if (variantTable.equals(archiveTable)) {
            throw new IllegalArgumentException("archive and variant tables must be different");
        }
        VariantTableHelper.setVariantsTable(getConf(), table);
//        if (studyId < 0) {
//            throw new IllegalArgumentException("No Study id specified!!!");
//        }

        String dbName = HBaseVariantTableNameGenerator.getDBNameFromVariantsTableName(getVariantsTable());
        generator = new HBaseVariantTableNameGenerator(dbName, getConf());

        initVariantTableHelper(getStudyId());

        /* -------------------------------*/
        // Validate input CHECK
        try (HBaseManager hBaseManager = new HBaseManager(conf)) {
            checkTablesExist(hBaseManager, variantTable);
        }

        // Increase the ScannerTimeoutPeriod to avoid ScannerTimeoutExceptions
        // See opencb/opencga#352 for more info.
        int scannerTimeout = getConf().getInt(HadoopVariantStorageOptions.MR_HBASE_SCANNER_TIMEOUT.key(),
                getConf().getInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, HConstants.DEFAULT_HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD));
        logger.info("Set Scanner timeout to " + scannerTimeout + " ...");
        conf.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, scannerTimeout);

    }

    @Override
    protected void preExecution() throws IOException, StorageEngineException {
        super.preExecution();
        preExecution(getVariantsTable());
    }

    protected void preExecution(String variantTable) throws IOException, StorageEngineException {
        // do nothing
    }

    @Override
    protected void close() throws IOException, StorageEngineException {
        super.close();
        if (metadataManager != null) {
            metadataManager.close();
            metadataManager = null;
        }
        if (hBaseManager != null) {
            hBaseManager.close();
            hBaseManager = null;
        }
    }

    protected Class<?> getMapperClass() {
        return null;
    }

    @Override
    protected final void setupJob(Job job, String table) throws IOException {
        setupJob(job, getArchiveTable(), getVariantsTable());
    }

    protected abstract Job setupJob(Job job, String archiveTable, String variantTable) throws IOException;

    /**
     * Give the name of the action that the job is doing.
     *
     * Used to create the jobName and as {@link TaskMetadata#getName()}
     *
     * e.g. : "Delete", "Load", "Annotate", ...
     *
     * @return Job action
     */
    protected abstract String getJobOperationName();

    protected final Scan createArchiveTableScan(List<Integer> files) {
        Scan scan = new Scan();
        int caching = getConf().getInt(HadoopVariantStorageOptions.MR_HBASE_SCAN_CACHING.key(), 50);
        logger.info("Scan set Caching to " + caching);
        scan.setCaching(caching);        // 1 is the default in Scan, 200 caused timeout issues.
        scan.setCacheBlocks(false);  // don't set to true for MR jobs

        // specify return columns (file IDs)
        FilterList filter = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        for (Integer id : files) {
            filter.addFilter(new QualifierFilter(CompareFilter.CompareOp.EQUAL,
                    new BinaryComparator(Bytes.toBytes(ArchiveTableHelper.getNonRefColumnName(id)))));
        }
        filter.addFilter(new ColumnPrefixFilter(FillMissingFromArchiveTask.VARIANT_COLUMN_B_PREFIX));
        scan.setFilter(filter);
        return scan;
    }

    protected final Scan createVariantsTableScan() {
        Scan scan = new Scan();
//        int caching = getConf().getInt(AbstractAnalysisTableDriver.HBASE_SCAN_CACHING, 50);
//        getLog().info("Scan set Caching to " + caching);
//        scan.setCaching(caching);        // 1 is the default in Scan, 200 caused timeout issues.
        scan.setCacheBlocks(false);  // don't set to true for MR jobs
        scan.addFamily(GenomeHelper.COLUMN_FAMILY_BYTES); // Ignore PHOENIX columns!!!
        return scan;
    }

    @Override
    protected String getJobName() {
        String variantTable = getVariantsTable();
        List<Integer> files = getFiles();
        StringBuilder sb = new StringBuilder("opencga: ").append(getJobOperationName())
                .append(" from VariantTable '").append(variantTable).append('\'');
        if (!files.isEmpty()) {
            sb.append(" for ").append(files.size()).append(" files: ");
            if (files.size() > 50) {
                sb.append('[');
                sb.append(files.subList(0, 15).stream().map(Object::toString).collect(Collectors.joining(", ")));
                sb.append(" ... ");
                sb.append(files.subList(files.size() - 15, files.size()).stream().map(Object::toString).collect(Collectors.joining(", ")));
                sb.append(']');
            } else {
                sb.append(files);
            }
        }
        return sb.toString();
    }

    protected List<Integer> getFiles() {
        if (fileIds == null) {
            fileIds = getFiles(getConf());
        }
        return fileIds;
    }

    public static List<Integer> getFiles(Configuration conf) {
        String[] fileArr = conf.getStrings(FILE_ID, new String[0]);
        return Arrays.stream(fileArr)
                    .filter(s -> StringUtils.isNotEmpty(s) && !s.equals("."))
                    .map(Integer::parseInt)
                    .collect(Collectors.toList());
    }

    public static void setFiles(Configuration conf, Collection<Integer> fileIds) {
        conf.setStrings(FILE_ID, fileIds.stream().map(Object::toString).toArray(String[]::new));
    }

    protected void setStudyId(int studyId) {
        this.studyId = studyId;
        getConf().setInt(STUDY_ID, studyId);
    }

    protected int getStudyId() {
        if (studyId == null) {
            int studyId = Integer.valueOf(getParam(STUDY_ID, "-1"));
            if (studyId < 0) {
                String study = getParam(VariantStorageOptions.STUDY.key());
                if (StringUtils.isNotEmpty(study)) {
                    try {
                        studyId = getMetadataManager().getStudyId(study);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }
            }
            this.studyId = studyId;
        }
        return studyId;
    }

    protected String getVariantsTable() {
        return table;
    }

    protected String getArchiveTable() {
        return getConf().get(ArchiveDriver.CONFIG_ARCHIVE_TABLE_NAME, StringUtils.EMPTY);
    }

    protected HBaseVariantTableNameGenerator getTableNameGenerator() {
        String dbName = HBaseVariantTableNameGenerator.getDBNameFromVariantsTableName(getVariantsTable());
        return new HBaseVariantTableNameGenerator(dbName, getConf());
    }

    protected StudyMetadata readStudyMetadata() throws IOException {
        VariantStorageMetadataManager metadataManager = getMetadataManager();
        int studyId = getStudyId();
        StudyMetadata studyMetadata = metadataManager.getStudyMetadata(studyId);
        if (studyMetadata == null) {
            throw new IllegalStateException("StudyMetadata " + studyId + " not found!");
        }
        return studyMetadata;
    }

    protected HBaseManager getHBaseManager() {
        if (hBaseManager == null) {
            hBaseManager = new HBaseManager(getConf());
        }
        return hBaseManager;
    }

    protected VariantStorageMetadataManager getMetadataManager() throws IOException {
        if (metadataManager == null) {
            metadataManager = new VariantStorageMetadataManager(new HBaseVariantStorageMetadataDBAdaptorFactory(
                    getHBaseManager(), generator.getMetaTableName(), getConf()));
        }
        return metadataManager;
    }

    private void checkTablesExist(HBaseManager hBaseManager, String... tables) throws IOException {
        for (String fileName : tables) {
            if (StringUtils.isNotEmpty(fileName) && !hBaseManager.tableExists(fileName)) {
                throw new IOException("Table " + fileName + " does not exist!!!");
            }
        }
    }

    private GenomeHelper initVariantTableHelper(Integer studyId) {
        Configuration conf = getConf();
        GenomeHelper.setStudyId(conf, studyId);
        VariantTableHelper.setVariantsTable(conf, getVariantsTable());
        helper = new GenomeHelper(conf, studyId);
        return helper;
    }

    protected GenomeHelper getHelper() {
        return helper;
    }

    @Override
    protected final String getUsage() {
        Map<String, String> otherParams = getParams();
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> entry : otherParams.entrySet()) {
            if (entry.getValue().endsWith("*")) {
                sb.append(entry.getKey()).append(' ').append(entry.getValue(), 0, entry.getValue().length() - 1).append(' ');
            } else {
                sb.append('[').append(entry.getKey()).append(' ').append(entry.getValue()).append("] ");
            }
        }
        return "Usage: " + getClass().getSimpleName() + " [generic options] <variants-table> " + sb + "[<key> <value>]*";
    }

    protected Map<String, String> getParams() {
        return Collections.emptyMap();
    }

    public static void setNoneTimestamp(Job job) {
        job.getConfiguration().set(TIMESTAMP, NONE_TIMESTAMP);
    }

    public int privateMain(String[] args) throws Exception {
        return privateMain(args, getConf());
    }

    public int privateMain(String[] args, Configuration conf) throws Exception {
        // info https://code.google.com/p/temapred/wiki/HbaseWithJava
        if (conf != null) {
            setConf(conf);
        }
        return ToolRunner.run(this, args);
    }


    public static String[] buildArgs(String archiveTable, String variantsTable, int studyId, Collection<?> fileIds, ObjectMap other) {
        other.put(ArchiveDriver.CONFIG_ARCHIVE_TABLE_NAME, archiveTable);
        other.put(AbstractVariantsTableDriver.CONFIG_VARIANT_TABLE_NAME, variantsTable);

        other.put(STUDY_ID, studyId);

        if (fileIds != null && !fileIds.isEmpty()) {
            other.put(FILE_ID, fileIds.stream().map(Object::toString).collect(Collectors.joining(",")));
        }
        return buildArgs(variantsTable, other);
    }

    public static void addOtherParams(ObjectMap other, StringBuilder stringBuilder) {
        for (Map.Entry<String, Object> entry : other.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            if (value != null) {
                if (value instanceof Number || value instanceof Boolean) {
                    stringBuilder.append(' ').append(key).append(' ').append(value);
                } else {
                    String valueStr = other.getString(key);
                    if (valueStr != null && !valueStr.contains(" ") && !valueStr.isEmpty()) {
                        stringBuilder.append(' ').append(key).append(' ').append(valueStr);
                    }
                }
            }
        }
    }

}
