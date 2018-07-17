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

package org.opencb.opencga.storage.hadoop.variant.archive;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.AbstractVariantsTableDriver;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.archive.mr.VariantToVcfSliceMapper;
import org.opencb.opencga.storage.hadoop.variant.archive.mr.VcfSliceCombiner;
import org.opencb.opencga.storage.hadoop.variant.archive.mr.VcfSliceReducer;
import org.opencb.opencga.storage.hadoop.variant.archive.mr.VcfSliceWritable;
import org.opencb.opencga.storage.hadoop.variant.metadata.HBaseVariantFileMetadataDBAdaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.zip.GZIPInputStream;

/**
 * @author mh719
 */
public class ArchiveDriver extends Configured implements Tool {

    public static final String CONFIG_ARCHIVE_INPUT_FILE_VCF      = "opencga.archive.input.file.vcf";
    public static final String CONFIG_ARCHIVE_INPUT_FILE_VCF_META = "opencga.archive.input.file.vcf.meta";
    public static final String CONFIG_ARCHIVE_TABLE_NAME          = "opencga.archive.table.name";

    private static final Logger LOGGER = LoggerFactory.getLogger(ArchiveDriver.class);

    public ArchiveDriver() {
    }

    public ArchiveDriver(Configuration conf) {
        super(conf);
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        HBaseConfiguration.addHbaseResources(conf);

        URI inputFile = URI.create(conf.get(CONFIG_ARCHIVE_INPUT_FILE_VCF));
        URI inputMetaFile = URI.create(conf.get(CONFIG_ARCHIVE_INPUT_FILE_VCF_META));
        String tableName = conf.get(CONFIG_ARCHIVE_TABLE_NAME);
        int studyId = conf.getInt(HadoopVariantStorageEngine.STUDY_ID, -1);
        int fileId = conf.getInt(HadoopVariantStorageEngine.FILE_ID, -1);
        GenomeHelper genomeHelper = new GenomeHelper(conf);

/*  SERVER details  */
        if (ArchiveTableHelper.createArchiveTableIfNeeded(genomeHelper, tableName)) {
            LOGGER.info("Create table '{}' in hbase!", tableName);
        } else {
            LOGGER.info("Table '{}' exists in hbase!", tableName);
        }

        // add metadata config as string
        VariantFileMetadata meta = readMetaData(conf, inputMetaFile);
        // StudyID and FileID may not be correct. Use the given through the CLI and overwrite the values from meta.
        meta.setId(String.valueOf(fileId));
        storeMetaData(String.valueOf(studyId), meta, conf);

        /* JOB setup */
        final Job job = Job.getInstance(conf, "opencga: Load file [" + fileId + "] to ArchiveTable '" + tableName + "'");
        job.setJarByClass(getClass());
        conf = job.getConfiguration();
        conf.set("mapreduce.job.user.classpath.first", "true");

        // input
        FileInputFormat.addInputPath(job, new Path(inputFile));

        AvroJob.setInputKeySchema(job, VariantAvro.getClassSchema());
        job.setInputFormatClass(AvroKeyInputFormat.class);

        // mapper
        job.setMapperClass(VariantToVcfSliceMapper.class);

        // combiner
        job.setCombinerClass(VcfSliceCombiner.class);


        TableMapReduceUtil.initTableReducerJob(tableName, VcfSliceReducer.class, job, null, null, null, null,
                conf.getBoolean(HadoopVariantStorageEngine.MAPREDUCE_ADD_DEPENDENCY_JARS, true));
        job.setMapOutputValueClass(VcfSliceWritable.class);

        Thread hook = new Thread(() -> {
            try {
                if (!job.isComplete()) {
                    job.killJob();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        Runtime.getRuntime().addShutdownHook(hook);
        boolean succeed = job.waitForCompletion(true);
        Runtime.getRuntime().removeShutdownHook(hook);

        try (HBaseVariantFileMetadataDBAdaptor manager = new HBaseVariantFileMetadataDBAdaptor(conf)) {
            manager.updateLoadedFilesSummary(studyId, Collections.singletonList(fileId));
        }
        return succeed ? 0 : 1;
    }

    private void storeMetaData(String studyId, VariantFileMetadata meta, Configuration conf) throws IOException, StorageEngineException {
        try (HBaseVariantFileMetadataDBAdaptor manager = new HBaseVariantFileMetadataDBAdaptor(conf)) {
            manager.updateVariantFileMetadata(studyId, meta);
        }
    }

    private VariantFileMetadata readMetaData(Configuration conf, URI inputMetaFile) throws IOException {
        Path from = new Path(inputMetaFile);
        FileSystem fs = FileSystem.get(conf);
        DatumReader<VariantFileMetadata> userDatumReader = new SpecificDatumReader<>(VariantFileMetadata.class);
        VariantFileMetadata variantFileMetadata;
        if (inputMetaFile.toString().endsWith("json") || inputMetaFile.toString().endsWith("json.gz")) {
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);

            try (InputStream ids = inputMetaFile.toString().endsWith("json.gz") ? new GZIPInputStream(fs.open(from)) : fs.open(from)) {
                variantFileMetadata = objectMapper.readValue(ids, VariantFileMetadata.class);
            }
        } else {
            try (FSDataInputStream ids = fs.open(from);
                 DataFileStream<VariantFileMetadata> dataFileReader = new DataFileStream<>(ids, userDatumReader)) {
                Iterator<VariantFileMetadata> iter = dataFileReader.iterator();
                if (!iter.hasNext()) {
                    throw new IllegalStateException(String.format("No Meta data object found in %s !!!", inputMetaFile));
                }
                variantFileMetadata = iter.next();
                if (iter.hasNext()) {
                    LOGGER.warn(String.format("More than 1 entry found in metadata file %s", inputMetaFile));
                }
            }
        }
        return variantFileMetadata;
    }

    public static String buildCommandLineArgs(URI input, URI inputMeta, String server, String outputTable,
                                              int studyId, int fileId, ObjectMap other) {
        StringBuilder stringBuilder = new StringBuilder()
                .append(input).append(' ')
                .append(inputMeta).append(' ')
                .append(server).append(' ')
                .append(outputTable).append(' ')
                .append(studyId).append(' ')
                .append(fileId);
        AbstractVariantsTableDriver.addOtherParams(other, stringBuilder);
        return stringBuilder.toString();
    }

    public static void main(String[] args) throws Exception {
        try {
            System.exit(privateMain(args, null));
        } catch (Exception e) {
            LOGGER.error("Error: ", e);
            System.exit(1);
        }
    }

    public static int privateMain(String[] args, Configuration conf) throws Exception {
        if (conf == null) {
            conf = new Configuration();
        }
        ArchiveDriver driver = new ArchiveDriver();
        GenericOptionsParser parser = new GenericOptionsParser(conf, args);

        //get the args w/o generic hadoop args
        String[] toolArgs = parser.getRemainingArgs();

        int fixedSizeArgs = 6;
        if (toolArgs.length < fixedSizeArgs || (toolArgs.length - fixedSizeArgs) % 2 != 0) {
            System.err.printf("Usage: %s [generic options] <avro> <avro-meta> <server> <output-table> <study-id> <file-id>"
                    + " [<key> <value>]*\n",
                    ArchiveDriver.class.getSimpleName());
            System.err.println("Found argc:" + toolArgs.length + ", argv: " + Arrays.toString(toolArgs));
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        // Get first other args to avoid overwrite the fixed position args.
        for (int i = fixedSizeArgs; i < toolArgs.length; i = i + 2) {
            conf.set(toolArgs[i], toolArgs[i + 1]);
        }

        conf.set(CONFIG_ARCHIVE_INPUT_FILE_VCF, toolArgs[0]);
        conf.set(CONFIG_ARCHIVE_INPUT_FILE_VCF_META, toolArgs[1]);
        conf = HBaseManager.addHBaseSettings(conf, toolArgs[2]);
        conf.set(CONFIG_ARCHIVE_TABLE_NAME, toolArgs[3]);
        conf.set(HadoopVariantStorageEngine.STUDY_ID, toolArgs[4]);
        conf.set(HadoopVariantStorageEngine.FILE_ID, toolArgs[5]);
        //set the configuration back, so that Tool can configure itself
        driver.setConf(conf);

        /* Alternative to using tool runner */
//      int exitCode = ToolRunner.run(conf,new GenomeVariantDriver(), args);

        return driver.run(toolArgs);
    }

}
