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

package org.opencb.opencga.storage.hadoop.variant;

import org.apache.hadoop.conf.Configuration;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.io.VariantReaderUtils;
import org.opencb.opencga.storage.hadoop.auth.HBaseCredentials;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveDriver;
import org.opencb.opencga.storage.hadoop.variant.executors.MRExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine.HADOOP_BIN;
import static org.opencb.opencga.storage.core.variant.VariantStorageEngine.Options;

/**
 * Created on 31/03/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HadoopVariantStoragePipeline extends AbstractHadoopVariantStoragePipeline {

    private final Logger logger = LoggerFactory.getLogger(HadoopVariantStoragePipeline.class);

    public HadoopVariantStoragePipeline(
            StorageConfiguration configuration,
            VariantHadoopDBAdaptor dbAdaptor, MRExecutor mrExecutor,
            Configuration conf, HBaseCredentials archiveCredentials,
            VariantReaderUtils variantReaderUtils, ObjectMap options) {
        super(configuration, dbAdaptor, variantReaderUtils,
                options, archiveCredentials, mrExecutor, conf);
    }


    protected void loadArch(URI input) throws StorageEngineException {
        int studyId = getStudyId();
        URI vcfMeta = URI.create(VariantReaderUtils.getMetaFromTransformedFile(input.toString()));
        int fileId = options.getInt(Options.FILE_ID.key());

        String hadoopRoute = options.getString(HADOOP_BIN, "hadoop");
        String jar = getJarWithDependencies();

        Class execClass = ArchiveDriver.class;
        String executable = hadoopRoute + " jar " + jar + " " + execClass.getName();
        String args = ArchiveDriver.buildCommandLineArgs(input, vcfMeta,
                archiveTableCredentials.toString(), archiveTableCredentials.getTable(), studyId,
                fileId, options);

        long startTime = System.currentTimeMillis();
        logger.info("------------------------------------------------------");
        logger.info("Loading file {} into archive table '{}'", fileId, archiveTableCredentials.getTable());
        logger.debug(executable + " " + args);
        logger.info("------------------------------------------------------");
        int exitValue = mrExecutor.run(executable, args);
        logger.info("------------------------------------------------------");
        logger.info("Exit value: {}", exitValue);
        logger.info("Total time: {}s", (System.currentTimeMillis() - startTime) / 1000.0);
        if (exitValue != 0) {
            throw new StorageEngineException("Error loading file " + input + " into archive table \""
                    + archiveTableCredentials.getTable() + "\"");
        }
    }


    @Override
    protected boolean needLoadFromHdfs() {
        return true;
    }
}
