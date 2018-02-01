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

package org.opencb.opencga.storage.hadoop.variant.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Job;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.AbstractAnalysisTableDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author Matthias Haimel mh719+git@cam.ac.uk
 *
 */
public class VariantTableRemoveFileDriver extends AbstractAnalysisTableDriver {

    public static final String JOB_OPERATION_NAME = VariantStorageEngine.REMOVE_OPERATION_NAME;
    protected final Logger logger = LoggerFactory.getLogger(VariantTableRemoveFileDriver.class);

    public VariantTableRemoveFileDriver() {
        super();
    }

    public VariantTableRemoveFileDriver(Configuration conf) {
        super(conf);
    }

    @Override
    protected void parseAndValidateParameters() {
    }

    @Override
    protected Job setupJob(Job job, String archiveTable, String variantTable) throws IOException {
        // QUERY design
        Scan scan = createArchiveTableScan(getFiles());
        if (VariantStorageEngine.MergeMode.from(readStudyConfiguration().getAttributes()).equals(VariantStorageEngine.MergeMode.BASIC)) {
            // If basic, add "KeyOnlyFilter" as we don't care about the content of the VCFSlices
            scan.setFilter(new FilterList(FilterList.Operator.MUST_PASS_ALL, new KeyOnlyFilter(), scan.getFilter()));
        }
        // set other scan attrs
        initMapReduceJob(job, getMapperClass(), archiveTable, variantTable, scan);
        job.setOutputFormatClass(MultiTableOutputFormat.class);

        return job;
    }

    @Override
    protected Class<? extends TableMapper> getMapperClass() {
        return VariantTableRemoveMapper.class;
    }

    @Override
    protected String getJobOperationName() {
        return JOB_OPERATION_NAME;
    }

    public static void main(String[] args) throws Exception {
        try {
            System.exit(new VariantTableRemoveFileDriver().privateMain(args));
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
