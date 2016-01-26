/*
 * Copyright 2015 OpenCB
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

package org.opencb.opencga.storage.hadoop.alignment;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.opencb.biodata.formats.io.FileFormatException;
import org.opencb.biodata.models.alignment.AlignmentRegion;
import org.opencb.biodata.tools.alignment.AlignmentFileUtils;
import org.opencb.commons.io.DataWriter;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.hpg.bigdata.tools.alignment.Bam2AvroMR;
import org.opencb.hpg.bigdata.tools.alignment.stats.ReadAlignmentDepthMR;
import org.opencb.opencga.storage.core.StorageManagerException;
import org.opencb.opencga.storage.core.alignment.AlignmentStorageManager;
import org.opencb.opencga.storage.core.alignment.adaptors.AlignmentDBAdaptor;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;


/**
 * Created by imedina on 16/06/15.
 */
public class HadoopAlignmentStorageManager extends AlignmentStorageManager {

    public static final String STORAGE_ENGINE_ID = "hadoop";
    private Configuration conf;

    public HadoopAlignmentStorageManager(StorageConfiguration configuration) {
        super(STORAGE_ENGINE_ID, configuration);
        logger = LoggerFactory.getLogger(HadoopAlignmentStorageManager.class);
    }

    public HadoopAlignmentStorageManager() {
    }

    private Configuration getConf() throws IOException {
        ObjectMap options = configuration.getStorageEngine(STORAGE_ENGINE_ID).getAlignment().getOptions();
        if (conf == null) {
            conf = new Configuration();
            conf.addResource(new File(options.getString("core-site.path")).toURI().toURL().openStream(), "core-site.path");
            conf.addResource(new File(options.getString("hdfs-site.path")).toURI().toURL().openStream(), "hdfs-site.path");
//            System.out.println(conf.get("fs.defaultFS"));
//            System.out.println(options.getString("core-site.path"));
//            System.out.println(options.getString("hdfs-site.path"));
        }
        return conf;
    }

    @Override
    public void setConfiguration(StorageConfiguration configuration, String storageEngineId) {
        conf = null;
        super.setConfiguration(configuration, storageEngineId);
    }

    @Override
    public URI extract(URI input, URI ouput) throws StorageManagerException {
        if (input.getScheme() == null || input.getScheme().isEmpty() || !input.getScheme().equals("hdfs")) {
            logger.warn("Input file '{}' not in hdfs", input);
//            throw new StorageManagerException("Input file must be in hdfs");
        }
        //TODO: Move file to HDFS if needed

        return super.extract(input, ouput);
    }

    @Override
    public URI preTransform(URI inputUri) throws IOException, FileFormatException {

        ObjectMap options = configuration.getStorageEngine(STORAGE_ENGINE_ID).getAlignment().getOptions();
        System.out.println(new File(options.getString("core-site.path")).toURI().toURL());
        System.out.println(new File(options.getString("hdfs-site.path")).toURI().toURL());

        Configuration conf = getConf();

        FileSystem fileSystem = FileSystem.get(conf);
        AlignmentFileUtils.checkBamOrCramFile(fileSystem.open(new Path(inputUri)), inputUri.getPath(), false);
        return inputUri;
    }

    @Override
    public URI transform(URI inputUri, URI pedigree, URI outputUri) throws IOException, FileFormatException, StorageManagerException {

        ObjectMap options = configuration.getStorageEngine(STORAGE_ENGINE_ID).getAlignment().getOptions();
        String codec = options.getString("transform.avro.codec", "deflate");
//        String hpg_bigdata_bin = options.getString("hpg-bigdata.bin", System.getProperty("user.home") +
// "/appl/hpg-bigdata/build/bin/hpg-bigdata.sh");
        Configuration conf = getConf();


        codec = codec.equals("gzip") ? "deflate" : codec;

        boolean includeCoverage = options.getBoolean(Options.INCLUDE_COVERAGE.key(), Options.INCLUDE_COVERAGE.<Boolean>defaultValue());
        boolean adjustQuality = options.getBoolean(Options.ADJUST_QUALITY.key(), Options.ADJUST_QUALITY.<Boolean>defaultValue());
//        int regionSize = options.getInt(Options.TRANSFORM_REGION_SIZE.key(), Options.TRANSFORM_REGION_SIZE.<Integer>defaultValue());

        URI alignmentAvroFile = outputUri.resolve(Paths.get(inputUri.getPath()).getFileName().toString() + ".avro");
        URI coverageAvroFile = outputUri.resolve(Paths.get(inputUri.getPath()).getFileName().toString() + ".coverage.avro");

        try {
            logger.info("Transforming file {} -> {} ....", inputUri, alignmentAvroFile);
//            String cli = hpg_bigdata_bin +
//                    " alignment convert " +
//                    " --compression " + codec +
//                    " --input " + inputUri +
//                    " --output " + alignmentAvroFile;
//            System.out.println("cli = " + cli);

//            Command command = new Command(cli);
//            command.run();
//            int exitValue = command.getExitValue();
//            if (exitValue != 0) {
//                throw new Exception("Transform cli error: Exit value = " + exitValue);
//            }
            Bam2AvroMR.run(inputUri.toString(), alignmentAvroFile.toString(), codec, adjustQuality, conf);
        } catch (Exception e) {
            throw new StorageManagerException("Error while transforming file", e);
        }
        if (includeCoverage) {
            try {
                logger.info("Calculating coverage {} -> {} ....", alignmentAvroFile, coverageAvroFile);
//                String cli = hpg_bigdata_bin +
//                        " alignment depth " +
//                        " --input " + alignmentAvroFile + "/part-r-00000.avro" +
//                        " --output " + coverageAvroFile;
//                System.out.println("cli = " + cli);
//
//                Command command = new Command(cli);
//                command.run();
//                int exitValue = command.getExitValue();
//                if (exitValue != 0) {
//                    throw new Exception("Calculating coverage cli error: Exit value = " + exitValue);
//                }

                ReadAlignmentDepthMR.run(alignmentAvroFile.toString() + "/part-r-00000.avro", coverageAvroFile.toString(), conf);
            } catch (Exception e) {
                throw new StorageManagerException("Error while computing coverage", e);
            }
        }
        return alignmentAvroFile;
    }

    @Override
    public URI preLoad(URI input, URI output) throws IOException, StorageManagerException {
        throw new UnsupportedOperationException("Unimplemented");
    }

    @Override
    public URI load(URI input) throws IOException, StorageManagerException {
        throw new UnsupportedOperationException("Unimplemented");
    }

    @Override
    public URI postLoad(URI input, URI output) throws IOException, StorageManagerException {
        throw new UnsupportedOperationException("Unimplemented");
    }

    @Override
    public DataWriter<AlignmentRegion> getDBWriter(String dbName) throws StorageManagerException {
        throw new UnsupportedOperationException("Unimplemented");
    }

    @Override
    public AlignmentDBAdaptor getDBAdaptor(String dbName) throws StorageManagerException {
        throw new UnsupportedOperationException("Unimplemented");
    }

}
