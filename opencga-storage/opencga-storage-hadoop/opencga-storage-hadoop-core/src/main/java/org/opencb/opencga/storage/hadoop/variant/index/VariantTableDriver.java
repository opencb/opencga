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
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Job;
import org.opencb.opencga.storage.hadoop.variant.AbstractAnalysisTableDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * @author Matthias Haimel mh719+git@cam.ac.uk
 */
public class VariantTableDriver extends AbstractAnalysisTableDriver {
    protected static final Logger LOG = LoggerFactory.getLogger(VariantTableDriver.class);

    public static final String JOB_OPERATION_NAME = "Load";

    public VariantTableDriver() { /* nothing */ }

    public VariantTableDriver(Configuration conf) {
        super(conf);
    }

    @Override
    protected void parseAndValidateParameters() {
        // Set parallel pool size
        String fjpKey = "java.util.concurrent.ForkJoinPool.common.parallelism";
        boolean hasForkJoinPool = false;
        Integer vCores = getConf().getInt("mapreduce.map.cpu.vcores", 1);
        Collection<String> opts = getConf().getStringCollection("opencga.variant.table.mapreduce.map.java.opts");
        String optString = StringUtils.EMPTY;
        if (!opts.isEmpty()) {
            for (String opt : opts) {
                if (opt.contains(fjpKey)) {
                    hasForkJoinPool = true;
                }
                optString += opt + " ";
            }
        }
        if (!hasForkJoinPool && vCores > 1) {
            optString += " -D" + fjpKey + "=" + vCores;
            LOG.warn("Force ForkJoinPool to {}", vCores);
        }
        if (StringUtils.isNotBlank(optString)) {
            LOG.info("Set mapreduce java opts: {}", optString);
            getConf().set("mapreduce.map.java.opts", optString);
        }
    }

    @Override
    protected Job setupJob(Job job, String archiveTable, String variantTable, List<Integer> files) throws IOException {
        // QUERY design
        Scan scan = createArchiveTableScan(files);

        // set other scan attrs
        initMapReduceJob(job, getMapperClass(), archiveTable, variantTable, scan);
        job.setOutputFormatClass(MultiTableOutputFormat.class);

        return job;
    }

    @Override
    protected Class<? extends TableMapper> getMapperClass() {
        return VariantMergerTableMapper.class;
    }

    @Override
    protected String getJobOperationName() {
        return JOB_OPERATION_NAME;
    }

    public static void main(String[] args) throws Exception {
        try {
            System.exit(new VariantTableDriver().privateMain(args));
        } catch (Exception e) {
            LOG.error("Problems", e);
            e.printStackTrace();
            System.exit(1);
        }
    }

}
