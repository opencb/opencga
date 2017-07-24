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
package org.opencb.opencga.storage.hadoop.variant;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAvro;

/**
 * @author Matthias Haimel mh719+git@cam.ac.uk
 *
 */
public class Echo extends Mapper<AvroKey<VariantAvro>, NullWritable, ImmutableBytesWritable, Put> {

    /**
     *
     */
    public Echo() {
        // TODO Auto-generated constructor stub
    }

    @Override
    protected void map(AvroKey<VariantAvro> key, NullWritable value,
            Mapper<AvroKey<VariantAvro>, NullWritable, ImmutableBytesWritable, Put>.Context context) throws IOException,
            InterruptedException {
//        VariantAvro varAvro = key.datum();
        // Variant variant = new Variant(varAvro);
        context.getCounter("Echo", "Count").increment(1);
    }

    /**
     * @param args
     *            abc
     * @throws Exception
     *             x
     */
    public static void main(String[] args) throws Exception {
        System.out.println(String.join(",", args));
        if (args.length > 1) {
            System.exit(localMain(args));
        } else {
            System.exit(privateMain(args));
        }
    }

    private static int localMain(String[] args) {
        File f = new File(args[0]);
        DatumReader<VariantAvro> reader = new SpecificDatumReader<VariantAvro>(VariantAvro.class);
        int cnt = 0;
        try (DataFileReader<VariantAvro> freader = new DataFileReader<VariantAvro>(f, reader)) {
            while (freader.hasNext()) {
                VariantAvro next = freader.next();
                Variant variant = new Variant(next);
                ++cnt;
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        System.out.println("Read " + cnt + " entries !!!");
        return 0;
    }

    private static int privateMain(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        Echo echo = new Echo();

        /* Alternative to using tool runner */
        // int exitCode = ToolRunner.run(conf,new GenomeVariantDriver(), args);

        return echo.run(conf, otherArgs);
    }

    public int run(Configuration conf, String[] args) throws Exception {
        URI inputFile = URI.create(args[0]);

        /* JOB setup */
        Job job = Job.getInstance(conf, "Echo");
        job.setJarByClass(getClass());
        conf = job.getConfiguration();
        conf.set("mapreduce.job.user.classpath.first", "true");

        // input
        FileInputFormat.addInputPath(job, new Path(inputFile));

        AvroJob.setInputKeySchema(job, VariantAvro.getClassSchema());
        job.setInputFormatClass(AvroKeyInputFormat.class);

        // mapper
        job.setMapperClass(Echo.class);

        job.setOutputFormatClass(NullOutputFormat.class);
        job.setNumReduceTasks(0);

        // TODO: Update list of indexed files
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
