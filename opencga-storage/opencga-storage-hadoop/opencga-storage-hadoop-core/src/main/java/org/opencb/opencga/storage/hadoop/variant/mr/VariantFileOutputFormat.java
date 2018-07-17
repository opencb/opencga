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

package org.opencb.opencga.storage.hadoop.variant.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.io.DataWriter;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory.VariantOutputFormat;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableHelper;
import org.opencb.opencga.storage.hadoop.variant.metadata.HBaseVariantStorageMetadataDBAdaptorFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import static org.opencb.opencga.storage.hadoop.variant.io.VariantExporterDriver.OUTPUT_FORMAT_PARAM;

/**
 * Created by mh719 on 21/12/2016.
 *
 * @author Matthias Haimel
 */
public class VariantFileOutputFormat extends FileOutputFormat<Variant, NullWritable> {

    public VariantFileOutputFormat() {
        // do nothing
    }

    @Override
    public RecordWriter<Variant, NullWritable> getRecordWriter(TaskAttemptContext job)
            throws IOException, InterruptedException {

        Configuration conf = job.getConfiguration();
        boolean isCompressed = getCompressOutput(job);
        CompressionCodec codec = null;
        String extension = "";
        if (isCompressed) {
            Class<?> file = getOutputCompressorClass(job, GzipCodec.class);
            codec = (CompressionCodec) ReflectionUtils.newInstance(file, conf);
            extension = codec.getDefaultExtension();
        }
        Path file = this.getDefaultWorkFile(job, extension);
        FileSystem fs = file.getFileSystem(conf);
        FSDataOutputStream fileOut = fs.create(file, false);
        if (!isCompressed) {
            return new VariantRecordWriter(configureWriter(job, fileOut));
        } else {
            DataOutputStream out = new DataOutputStream(codec.createOutputStream(fileOut));
            return new VariantRecordWriter(configureWriter(job, out));
        }
    }

    private DataWriter<Variant> configureWriter(final TaskAttemptContext job, OutputStream fileOut) throws IOException {
//        job.getCounter(VcfDataWriter.class.getName(), "failed").increment(0); // init
        final Configuration conf = job.getConfiguration();
        VariantOutputFormat outputFormat = VariantOutputFormat.valueOf(conf.get(OUTPUT_FORMAT_PARAM));

        DataWriter<Variant> dataWriter;
        VariantTableHelper helper = new VariantTableHelper(conf);
        try (StudyConfigurationManager scm = new StudyConfigurationManager(new HBaseVariantStorageMetadataDBAdaptorFactory(helper))) {
            VariantWriterFactory writerFactory = new VariantWriterFactory(scm);
            Query query = VariantMapReduceUtil.getQueryFromConfig(conf);
            QueryOptions options = VariantMapReduceUtil.getQueryOptionsFromConfig(conf);
            dataWriter = writerFactory.newDataWriter(outputFormat, fileOut, query, options);

//            dataWriter.setConverterErrorListener((v, e) ->
//                    job.getCounter(VcfDataWriter.class.getName(), "failed").increment(1));

            dataWriter.open();
            dataWriter.pre();
            return dataWriter;
        }
    }

    protected static class VariantRecordWriter extends RecordWriter<Variant, NullWritable> {
        private final DataWriter<Variant> writer;

        public VariantRecordWriter(DataWriter<Variant> writer) {
            this.writer = writer;
        }

        @Override
        public void write(Variant variant, NullWritable nullWritable) throws IOException, InterruptedException {
            writer.write(variant);
        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            writer.post();
            writer.close();
        }
    }


}
