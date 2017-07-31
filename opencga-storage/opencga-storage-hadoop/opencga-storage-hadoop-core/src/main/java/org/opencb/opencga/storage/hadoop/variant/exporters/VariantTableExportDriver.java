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

package org.opencb.opencga.storage.hadoop.variant.exporters;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.hadoop.variant.AbstractAnalysisTableDriver;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Created by mh719 on 21/11/2016.
 * @author Matthias Haimel
 */
public class VariantTableExportDriver extends AbstractAnalysisTableDriver {
    @Deprecated
    public static final String CONFIG_VARIANT_TABLE_EXPORT_AVRO_PATH = "opencga.variant.table.export.avro.path";
    @Deprecated
    public static final String CONFIG_VARIANT_TABLE_EXPORT_AVRO_GENOTYPE = "opencga.variant.table.export.avro.genotype";

    public static final String CONFIG_VARIANT_TABLE_EXPORT_PATH = "opencga.variant.table.export.path";
    public static final String CONFIG_VARIANT_TABLE_EXPORT_TYPE = "opencga.variant.table.export.type";
    public static final String CONFIG_VARIANT_TABLE_EXPORT_GENOTYPE = "opencga.variant.table.export.genotype";
    private String outFile;
    private ExportType type;

    public enum ExportType {AVRO, VCF};

    public VariantTableExportDriver() {
        super();
    }

    public VariantTableExportDriver(Configuration conf) {
        super(conf);
    }

    @Override
    protected void parseAndValidateParameters() {
        outFile = null;
        if (!Objects.isNull(getConf().get(CONFIG_VARIANT_TABLE_EXPORT_AVRO_PATH, null))) {
            outFile = getConf().get(CONFIG_VARIANT_TABLE_EXPORT_AVRO_PATH);
        }

        String typeString = getConf().get(CONFIG_VARIANT_TABLE_EXPORT_TYPE, ExportType.AVRO.name());
        this.type = ExportType.valueOf(typeString);

        outFile = getConf().get(CONFIG_VARIANT_TABLE_EXPORT_PATH, outFile);
        if (StringUtils.isEmpty(outFile)) {
            throw new IllegalArgumentException("No output file specified!!!");
        }
    }

    @Override
    protected Class<? extends TableMapper> getMapperClass() {
        return AnalysisToFileMapper.class;
    }

    @Override
    protected Job setupJob(Job job, String archiveTable, String variantTable, List<Integer> files) throws IOException {
        // QUERY design
        Scan scan = createVariantsTableScan();

        initMapReduceJob(job, getMapperClass(), variantTable, scan);

        FileOutputFormat.setOutputPath(job, new Path(this.outFile)); // set Path
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class); // compression
        switch (this.type) {
            case AVRO:
                job.setOutputFormatClass(AvroKeyOutputFormat.class);
                AvroJob.setOutputKeySchema(job, VariantAvro.getClassSchema()); // Set schema
                break;
            case VCF:
                job.setOutputFormatClass(HadoopVcfOutputFormat.class);
                break;
            default:
                throw new IllegalStateException("Type not known: " + this.type);
        }
        job.setNumReduceTasks(0);

        return job;
    }

    @Override
    protected String getJobOperationName() {
        return "Export";
    }

    @Override
    protected void postExecution(boolean succeed) throws IOException, StorageEngineException {
        super.postExecution(succeed);
        StudyConfiguration studyConfiguration = readStudyConfiguration();
        writeMetadata(studyConfiguration, this.outFile + ".studyConfiguration");
    }

    protected void writeMetadata(StudyConfiguration studyConfiguration, String output) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        Path path = new Path(output);
        FileSystem fs = FileSystem.get(getConf());
        try (FSDataOutputStream fos = fs.create(path)) {
            objectMapper.writeValue(fos, studyConfiguration);
        }
    }

    public static void main(String[] args) {
        try {
            System.exit(new VariantTableExportDriver().privateMain(args));
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

}
