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

/**
 *
 */
package org.opencb.opencga.storage.hadoop.variant.index;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.util.ToolRunner;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * @author Matthias Haimel mh719+git@cam.ac.uk
 */
public class VariantTableDriver extends AbstractVariantTableDriver {
    protected static final Logger LOG = LoggerFactory.getLogger(VariantTableDriver.class);

    public static final String JOB_OPERATION_NAME = "Load";

    public VariantTableDriver() { /* nothing */ }

    public VariantTableDriver(Configuration conf) {
        super(conf);
    }

    @Override
    public int run(String[] args) throws Exception {
        int fixedSizeArgs = 5;
        getConf().set(ArchiveDriver.CONFIG_ARCHIVE_TABLE_NAME, args[1]);
        getConf().set(CONFIG_VARIANT_TABLE_NAME, args[2]);
        getConf().set(GenomeHelper.CONFIG_STUDY_ID, args[3]);
        getConf().setStrings(CONFIG_VARIANT_FILE_IDS, args[4].split(","));
        for (int i = fixedSizeArgs; i < args.length; i = i + 2) {
            getConf().set(args[i], args[i + 1]);
        }

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
        return super.run(args);
    }

    @SuppressWarnings ("rawtypes")
    @Override
    protected Class<? extends TableMapper> getMapperClass() {
        return VariantTableMapper.class;
    }

    @Override
    protected String getJobOperationName() {
        return JOB_OPERATION_NAME;
    }

    public static void main(String[] args) throws Exception {
        try {
            System.exit(privateMain(args, null, new VariantTableDriver()));
        } catch (Exception e) {
            LOG.error("Problems", e);
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static int privateMain(String[] args, Configuration conf, VariantTableDriver driver) throws Exception {
        // info https://code.google.com/p/temapred/wiki/HbaseWithJava
        if (conf == null) {
            conf = HBaseConfiguration.create();
        }
        driver.setConf(conf);
        int exitCode = ToolRunner.run(driver, args);
        return exitCode;
    }

}
