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
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.hadoop.utils.AbstractHBaseDriver;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveDriver;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveTableHelper;
import org.opencb.opencga.storage.hadoop.variant.gaps.FillMissingFromArchiveTask;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableHelper;
import org.opencb.opencga.storage.hadoop.variant.metadata.HBaseVariantStorageMetadataDBAdaptorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine.MAPREDUCE_HBASE_SCANNER_TIMEOUT;

/**
 * Created by mh719 on 21/11/2016.
 */
public abstract class AbstractAnalysisTableDriver extends AbstractHBaseDriver implements Tool {

    public static final String CONFIG_VARIANT_TABLE_NAME           = "opencga.variant.table.name";
    public static final String TIMESTAMP                           = "opencga.variant.table.timestamp";

    private final Logger logger = LoggerFactory.getLogger(AbstractAnalysisTableDriver.class);
    private VariantTableHelper variantTablehelper;
    private StudyConfigurationManager scm;
    private List<Integer> fileIds;

    public AbstractAnalysisTableDriver() {
        super(HBaseConfiguration.create());
    }

    public AbstractAnalysisTableDriver(Configuration conf) {
        super(conf);
    }

    @Override
    protected void parseAndValidateParameters() throws IOException {
        super.parseAndValidateParameters();
        Configuration conf = getConf();
        String archiveTable = getArchiveTable();
        String variantTable = getAnalysisTable();
        Integer studyId = getStudyId();

        int maxKeyValueSize = conf.getInt(HadoopVariantStorageEngine.MAPREDUCE_HBASE_KEYVALUE_SIZE_MAX, 10485760); // 10MB
        logger.info("HBASE: set " + ConnectionConfiguration.MAX_KEYVALUE_SIZE_KEY + " to " + maxKeyValueSize);
        conf.setInt(ConnectionConfiguration.MAX_KEYVALUE_SIZE_KEY, maxKeyValueSize); // always overwrite server default (usually 1MB)

        /* -------------------------------*/
        // Validate parameters CHECK
        if (StringUtils.isEmpty(archiveTable)) {
            throw new IllegalArgumentException("No archive hbase table basename specified!!!");
        }
        if (StringUtils.isEmpty(variantTable)) {
            throw new IllegalArgumentException("No variant hbase table specified!!!");
        }
        if (archiveTable.equals(variantTable)) {
            throw new IllegalArgumentException("archive and variant tables must be different");
        }
        if (studyId < 0) {
            throw new IllegalArgumentException("No Study id specified!!!");
        }

        initVariantTableHelper(studyId, archiveTable, variantTable);

        /* -------------------------------*/
        // Validate input CHECK
        try (HBaseManager hBaseManager = new HBaseManager(conf)) {
            checkTablesExist(hBaseManager, archiveTable, variantTable);
        }

        // Increase the ScannerTimeoutPeriod to avoid ScannerTimeoutExceptions
        // See opencb/opencga#352 for more info.
        int scannerTimeout = getConf().getInt(MAPREDUCE_HBASE_SCANNER_TIMEOUT,
                getConf().getInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, HConstants.DEFAULT_HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD));
        logger.info("Set Scanner timeout to " + scannerTimeout + " ...");
        conf.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, scannerTimeout);

    }

    @Override
    protected void preExecution() throws IOException, StorageEngineException {
        super.preExecution();
        preExecution(getAnalysisTable());
    }

    protected void preExecution(String variantTable) throws IOException, StorageEngineException {
        // do nothing
    }

    @Override
    protected void close() throws IOException, StorageEngineException {
        super.close();
        if (scm != null) {
            scm.close();
            scm = null;
        }
    }

    protected abstract Class<?> getMapperClass();

    @Override
    protected final void setupJob(Job job, String table) throws IOException {
        setupJob(job, getArchiveTable(), getAnalysisTable());
    }

    protected abstract Job setupJob(Job job, String archiveTable, String variantTable) throws IOException;

    /**
     * Give the name of the action that the job is doing.
     *
     * Used to create the jobName and as {@link org.opencb.opencga.storage.core.metadata.BatchFileOperation#operationName}
     *
     * e.g. : "Delete", "Load", "Annotate", ...
     *
     * @return Job action
     */
    protected abstract String getJobOperationName();

    protected final Scan createArchiveTableScan(List<Integer> files) {
        Scan scan = new Scan();
        int caching = getConf().getInt(HadoopVariantStorageEngine.MAPREDUCE_HBASE_SCAN_CACHING, 50);
        logger.info("Scan set Caching to " + caching);
        scan.setCaching(caching);        // 1 is the default in Scan, 200 caused timeout issues.
        scan.setCacheBlocks(false);  // don't set to true for MR jobs
        // https://hbase.apache.org/book.html#perf.hbase.client.seek
        int lookAhead = getConf().getInt("hadoop.load.variant.scan.lookahead", -1);
        if (lookAhead > 0) {
            logger.info("Scan set LOOKAHEAD to " + lookAhead);
            scan.setAttribute(Scan.HINT_LOOKAHEAD, Bytes.toBytes(lookAhead));
        }
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
        scan.addFamily(getHelper().getColumnFamily()); // Ignore PHOENIX columns!!!
        return scan;
    }

    @Override
    protected String getJobName() {
        String variantTable = getAnalysisTable();
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

    protected List<Integer> getFilesToUse() throws IOException {
        StudyConfiguration studyConfiguration = readStudyConfiguration();
        LinkedHashSet<Integer> indexedFiles = studyConfiguration.getIndexedFiles();
        List<Integer> files = getFiles();
        if (files.isEmpty()) { // no files specified - use all indexed files for study
            files = new ArrayList<>(indexedFiles);
        } else { // Validate that they exist
            List<Integer> notIndexed = files
                    .stream()
                    .filter(fid -> !indexedFiles.contains(fid))
                    .collect(Collectors.toList());
            if (!notIndexed.isEmpty()) {
                throw new IllegalStateException("Provided File ID(s) not indexed!!!" + notIndexed);
            }
        }
        if (files.isEmpty()) { // if still empty (no files provided and / or found in study
            throw new IllegalArgumentException("No files specified / available for study " + getHelper().getStudyId());
        }
        return files;
    }

    protected List<Integer> getFiles() {
        if (fileIds == null) {
            fileIds = getFiles(getConf());
        }
        return fileIds;
    }

    public static List<Integer> getFiles(Configuration conf) {
        String[] fileArr = conf.getStrings(HadoopVariantStorageEngine.FILE_ID, new String[0]);
        return Arrays.stream(fileArr)
                    .filter(s -> StringUtils.isNotEmpty(s) && !s.equals("."))
                    .map(Integer::parseInt)
                    .collect(Collectors.toList());
    }

    protected int getStudyId() {
        return getConf().getInt(HadoopVariantStorageEngine.STUDY_ID, -1);
    }

    protected String getAnalysisTable() {
        return getConf().get(CONFIG_VARIANT_TABLE_NAME, StringUtils.EMPTY);
    }

    protected String getArchiveTable() {
        return getConf().get(ArchiveDriver.CONFIG_ARCHIVE_TABLE_NAME, StringUtils.EMPTY);
    }

    protected StudyConfiguration readStudyConfiguration() throws IOException {
        StudyConfigurationManager scm = getStudyConfigurationManager();
        int studyId = getHelper().getStudyId();
        QueryResult<StudyConfiguration> res = scm.getStudyConfiguration(studyId, new QueryOptions());
        if (res.getResult().size() != 1) {
            throw new IllegalStateException("StudyConfiguration " + studyId + " not found! " + res.getResult().size());
        }
        return res.first();
    }

    protected StudyConfigurationManager getStudyConfigurationManager() throws IOException {
        if (scm == null) {
            scm = new StudyConfigurationManager(new HBaseVariantStorageMetadataDBAdaptorFactory(getHelper()));
        }
        return scm;
    }

    private void checkTablesExist(HBaseManager hBaseManager, String... tables) throws IOException {
        for (String fileName : tables) {
            if (!hBaseManager.tableExists(fileName)) {
                throw new IOException("Table " + fileName + " does not exist!!!");
            }
        }
    }

    private VariantTableHelper initVariantTableHelper(Integer studyId, String archiveTable, String analysisTable) {
        Configuration conf = getConf();
        VariantTableHelper.setStudyId(conf, studyId);
        VariantTableHelper.setAnalysisTable(conf, analysisTable);
        VariantTableHelper.setArchiveTable(conf, archiveTable);
        variantTablehelper = new VariantTableHelper(conf, archiveTable, analysisTable);
        return variantTablehelper;
    }

    protected VariantTableHelper getHelper() {
        return variantTablehelper;
    }

    @Override
    protected final int getFixedSizeArgs() {
        return 4;
    }

    @Override
    protected final String getUsage() {
        return "Usage: " + getClass().getSimpleName()
                + " [generic options] <archive-table> <variants-table> <studyId> <fileIds> [<key> <value>]*";
    }

    @Override
    protected final void parseFixedParams(String[] args) {
        getConf().set(ArchiveDriver.CONFIG_ARCHIVE_TABLE_NAME, args[0]);
        getConf().set(CONFIG_VARIANT_TABLE_NAME, args[1]);
        getConf().set(HadoopVariantStorageEngine.STUDY_ID, args[2]);
        if (args[3].equals(".") || args[3].isEmpty()) {
            getConf().unset(HadoopVariantStorageEngine.FILE_ID);
            getConf().unset(VariantQueryParam.FILE.key());
        } else {
            getConf().setStrings(HadoopVariantStorageEngine.FILE_ID, args[3].split(","));
            getConf().setStrings(VariantQueryParam.FILE.key(), args[3].split(","));
        }
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


    public static String buildCommandLineArgs(String archiveTable, String variantsTable, int studyId, Collection<?> fileIds,
                                              ObjectMap other) {
        StringBuilder stringBuilder = new StringBuilder()
//                .append(server).append(' ')
                .append(archiveTable).append(' ')
                .append(variantsTable).append(' ')
                .append(studyId).append(' ');

        if (fileIds == null || fileIds.isEmpty()) {
            stringBuilder.append('.');
        } else {
            stringBuilder.append(fileIds.stream().map(Object::toString).collect(Collectors.joining(",")));
        }
        addOtherParams(other, stringBuilder);
        return stringBuilder.toString();
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
