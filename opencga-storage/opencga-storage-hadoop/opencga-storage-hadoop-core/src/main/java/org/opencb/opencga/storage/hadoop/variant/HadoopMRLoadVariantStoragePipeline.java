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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.io.VariantReaderUtils;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveDriver;
import org.opencb.opencga.storage.hadoop.variant.executors.MRExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;

import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine.HADOOP_BIN;
import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine.INTERMEDIATE_HDFS_DIRECTORY;

/**
 * Created on 31/03/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
// FIXME! This class is broken!
public class HadoopMRLoadVariantStoragePipeline extends HadoopVariantStoragePipeline {

    private final Logger logger = LoggerFactory.getLogger(HadoopMRLoadVariantStoragePipeline.class);

    public HadoopMRLoadVariantStoragePipeline(
            StorageConfiguration configuration,
            VariantHadoopDBAdaptor dbAdaptor, MRExecutor mrExecutor,
            Configuration conf,
            VariantReaderUtils variantReaderUtils, ObjectMap options) {
        super(configuration, dbAdaptor, variantReaderUtils,
                options, mrExecutor, conf);
        throw new IllegalStateException("Unable to load from hdfs using a MR job");
    }

    @Override
    public URI preLoad(URI input, URI output) throws StorageEngineException {

        if (!input.getScheme().equals("hdfs")) {
            if (!StringUtils.isEmpty(options.getString(INTERMEDIATE_HDFS_DIRECTORY))) {
                output = URI.create(options.getString(INTERMEDIATE_HDFS_DIRECTORY));
            }
            if (output.getScheme() != null && !output.getScheme().equals("hdfs")) {
                throw new StorageEngineException("Output must be in HDFS");
            }

            try {
                long startTime = System.currentTimeMillis();
//                    Configuration conf = getHadoopConfiguration(options);
                FileSystem fs = FileSystem.get(conf);
                org.apache.hadoop.fs.Path variantsOutputPath = new org.apache.hadoop.fs.Path(
                        output.resolve(Paths.get(input.getPath()).getFileName().toString()));
                logger.info("Copy from {} to {}", new org.apache.hadoop.fs.Path(input).toUri(), variantsOutputPath.toUri());
                fs.copyFromLocalFile(false, new org.apache.hadoop.fs.Path(input), variantsOutputPath);
                logger.info("Copied to hdfs in {}s", (System.currentTimeMillis() - startTime) / 1000.0);

                startTime = System.currentTimeMillis();
                URI fileInput = URI.create(VariantReaderUtils.getMetaFromTransformedFile(input.toString()));
                org.apache.hadoop.fs.Path fileOutputPath = new org.apache.hadoop.fs.Path(
                        output.resolve(Paths.get(fileInput.getPath()).getFileName().toString()));
                logger.info("Copy from {} to {}", new org.apache.hadoop.fs.Path(fileInput).toUri(), fileOutputPath.toUri());
                fs.copyFromLocalFile(false, new org.apache.hadoop.fs.Path(fileInput), fileOutputPath);
                logger.info("Copied to hdfs in {}s", (System.currentTimeMillis() - startTime) / 1000.0);

                input = variantsOutputPath.toUri();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return super.preLoad(input, output);
    }

    protected void load(URI input, int studyId, int fileId) throws StorageEngineException {
        URI vcfMeta = URI.create(VariantReaderUtils.getMetaFromTransformedFile(input.toString()));

        String hadoopRoute = options.getString(HADOOP_BIN, "hadoop");
        String jar = getJarWithDependencies();

        Class execClass = ArchiveDriver.class;
        String executable = hadoopRoute + " jar " + jar + " " + execClass.getName();
        String args = ArchiveDriver.buildCommandLineArgs(input, vcfMeta,
                variantsTableCredentials.toString(), getArchiveTable(), studyId,
                fileId, options);

        long startTime = System.currentTimeMillis();
        logger.info("------------------------------------------------------");
        logger.info("Loading file {} into archive table '{}'", fileId, getArchiveTable());
        logger.debug(executable + " " + args);
        logger.info("------------------------------------------------------");
        int exitValue = mrExecutor.run(executable, args);
        logger.info("------------------------------------------------------");
        logger.info("Exit value: {}", exitValue);
        logger.info("Total time: {}s", (System.currentTimeMillis() - startTime) / 1000.0);
        if (exitValue != 0) {
            throw new StorageEngineException("Error loading file " + input + " into archive table \""
                    + getArchiveTable() + "\"");
        }
    }

}
