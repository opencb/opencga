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

package org.opencb.opencga.storage.hadoop.variant.annotation.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.phoenix.mapreduce.util.PhoenixMapReduceUtil;
import org.opencb.opencga.storage.hadoop.variant.AbstractVariantsTableDriver;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper.VariantColumn;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantMapReduceUtil;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by mh719 on 15/12/2016.
 */
public class VariantsTableAnnotateDriver extends AbstractVariantsTableDriver {
    public static final String CONFIG_VARIANT_TABLE_ANNOTATE_PARALLEL = "opencga.variant.table.annotate.parallel";

    public VariantsTableAnnotateDriver() {
        super();
    }

    public VariantsTableAnnotateDriver(Configuration conf) {
        super(conf);
    }

    @Override
    protected void parseAndValidateParameters() throws IOException {
        super.parseAndValidateParameters();
        int parallel = getConf().getInt(CONFIG_VARIANT_TABLE_ANNOTATE_PARALLEL, 5);
        getConf().setInt("mapreduce.job.running.map.limit", parallel);
        getConf().setLong("phoenix.upsert.batch.size", 200L);
    }

    @Override
    protected Class<AnalysisAnnotateMapper> getMapperClass() {
        return AnalysisAnnotateMapper.class;
    }

    @Override
    protected Job setupJob(Job job, String archiveTable, String variantTable) throws IOException {
        // QUERY design
        Scan scan = createVariantsTableScan();

        // set other scan attrs
        TableMapReduceUtil.setScannerCaching(job, 200);
        VariantMapReduceUtil.initTableMapperJob(job, variantTable, scan, getMapperClass());

        String[] fieldNames = Arrays.stream(VariantColumn.values()).map(VariantColumn::toString).toArray(String[]::new);
        PhoenixMapReduceUtil.setOutput(job, VariantPhoenixHelper.getEscapedFullTableName(variantTable, getConf()), fieldNames);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(PhoenixVariantAnnotationWritable.class);
        job.setNumReduceTasks(0);

        return job;
    }

    @Override
    protected String getJobOperationName() {
        return "Annotate";
    }

    public static void main(String[] args) throws Exception {
        try {
            System.exit(new VariantsTableAnnotateDriver().privateMain(args));
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
