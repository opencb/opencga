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
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory.VariantOutputFormat;
import org.opencb.opencga.storage.hadoop.variant.io.CountingOutputStream;
import org.opencb.opencga.storage.hadoop.variant.metadata.HBaseVariantStorageMetadataDBAdaptorFactory;

import java.io.DataOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import static org.opencb.opencga.storage.hadoop.variant.mr.VariantsTableMapReduceHelper.COUNTER_GROUP_NAME;


/**
 * Writes variants into any format supported by the {@link VariantWriterFactory}.
 *
 * @see VariantWriterFactory
 * @see VariantOutputFormat
 */
public class VariantFileOutputFormat extends FileOutputFormat<Variant, NullWritable> {

    private static Class<?> abfsOutputStreamClass;

    static {
        try {
            abfsOutputStreamClass = Class.forName("org.apache.hadoop.fs.azurebfs.services.AbfsOutputStream");
        } catch (ClassNotFoundException e) {
            abfsOutputStreamClass = null;
        }
    }

    public static final String VARIANT_OUTPUT_FORMAT = "variant.output_format";

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
        FSDataOutputStream fsOs = fs.create(file, false);
        OutputStream out;
        if (isAbfsOutputStream(fsOs)) {
            // Disable flush on ABFS. See HADOOP-16548
            out = new FilterOutputStream(fsOs) {
                @Override
                public void flush() throws IOException {
                    // Do nothing
                }
            };
        } else {
            out = fsOs;
        }
        if (isCompressed) {
            out = new DataOutputStream(codec.createOutputStream(out));
        }
        CountingOutputStream countingOut = new CountingOutputStream(out);
        return new VariantRecordWriter(configureWriter(job, countingOut), countingOut);
    }

    private static boolean isAbfsOutputStream(FSDataOutputStream fsOs) {
        return abfsOutputStreamClass != null && abfsOutputStreamClass.isInstance(fsOs.getWrappedStream());
    }

    private DataWriter<Variant> configureWriter(final TaskAttemptContext job, OutputStream fileOut) throws IOException {
//        job.getCounter(VcfDataWriter.class.getName(), "failed").increment(0); // init
        final Configuration conf = job.getConfiguration();
        VariantOutputFormat outputFormat = VariantOutputFormat.valueOf(conf.get(VARIANT_OUTPUT_FORMAT));

        DataWriter<Variant> dataWriter;
        VariantTableHelper helper = new VariantTableHelper(conf);
        HBaseVariantStorageMetadataDBAdaptorFactory dbAdaptorFactory = new HBaseVariantStorageMetadataDBAdaptorFactory(helper);
        try (VariantStorageMetadataManager scm = new VariantStorageMetadataManager(dbAdaptorFactory)) {
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
        private final CountingOutputStream outputStream;

        public VariantRecordWriter(DataWriter<Variant> writer, CountingOutputStream outputStream) {
            this.writer = writer;
            this.outputStream = outputStream;
        }

        @Override
        public void write(Variant variant, NullWritable nullWritable) throws IOException, InterruptedException {
            writer.write(variant);
        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            writer.post();
            writer.close();
            outputStream.close();
            taskAttemptContext.getCounter(COUNTER_GROUP_NAME, "bytes_written").increment(outputStream.getByteCount());
        }
    }


}
