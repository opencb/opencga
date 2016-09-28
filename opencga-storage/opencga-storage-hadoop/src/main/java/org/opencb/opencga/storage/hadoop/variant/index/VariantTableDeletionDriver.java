/*
 * Copyright 2015-2016 OpenCB
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
import org.apache.hadoop.hbase.mapreduce.TableMapper;

/**
 * @author Matthias Haimel mh719+git@cam.ac.uk
 *
 */
public class VariantTableDeletionDriver extends AbstractVariantTableDriver {

    public static final String JOB_OPERATION_NAME = "Delete";

    public VariantTableDeletionDriver() { /* nothing */}

    public VariantTableDeletionDriver(Configuration conf) {
        super(conf);
    }

    @Override
    protected Class<? extends TableMapper> getMapperClass() {
        return VariantTableDeletionMapReduce.class;
    }

    @Override
    protected String getJobOperationName() {
        return JOB_OPERATION_NAME;
    }

    public static void main(String[] args) throws Exception {
        System.exit(privateMain(args, null));
    }

    public static int privateMain(String[] args, Configuration conf) throws Exception {
        if (conf == null) {
            conf = new Configuration();
        }
        VariantTableDeletionDriver driver = new VariantTableDeletionDriver();
        driver.setConf(conf);
        String[] toolArgs = configure(args, driver);
        if (null == toolArgs) {
            return -1;
        }

        /* Alternative to using tool runner */
//      int exitCode = ToolRunner.run(conf,new GenomeVariantDriver(), args);
        int exitCode = driver.run(toolArgs);
        return exitCode;
    }
}
