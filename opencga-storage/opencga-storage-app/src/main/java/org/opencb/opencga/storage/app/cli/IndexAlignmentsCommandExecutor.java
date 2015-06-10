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

package org.opencb.opencga.storage.app.cli;

import org.opencb.biodata.formats.io.FileFormatException;
import org.opencb.commons.utils.FileUtils;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.storage.core.StorageManagerException;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.alignment.AlignmentStorageManager;
import org.opencb.opencga.storage.core.config.StorageEngineConfiguration;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;

/**
 * Created by imedina on 22/05/15.
 */
public class IndexAlignmentsCommandExecutor extends CommandExecutor {

    private CliOptionsParser.IndexAlignmentsCommandOptions indexAlignmentsCommandOptions;

//    public static final String OPENCGA_STORAGE_ANNOTATOR = "OPENCGA.STORAGE.ANNOTATOR";

    public IndexAlignmentsCommandExecutor(CliOptionsParser.IndexAlignmentsCommandOptions indexAlignmentsCommandOptions) {
        super(indexAlignmentsCommandOptions.logLevel, indexAlignmentsCommandOptions.verbose,
                indexAlignmentsCommandOptions.configFile);

        this.indexAlignmentsCommandOptions = indexAlignmentsCommandOptions;
    }


    @Override
    public void execute() {
        logger.debug("In IndexAlignmentsCommandExecutor");

        try {
            // We need to find out the Storage Engine Id to be used
            // If not storage engine is passed then the default is taken from storage-configuration.yml file
            String storageEngine = (indexAlignmentsCommandOptions.storageEngine != null && !indexAlignmentsCommandOptions.storageEngine.isEmpty())
                    ? indexAlignmentsCommandOptions.storageEngine
                    : configuration.getDefaultStorageEngineId();
            logger.debug("storageEngine set to '{}'", storageEngine);

            StorageEngineConfiguration storageConfiguration = configuration.getStorageEngine(storageEngine);


            URI input = UriUtils.createUri(indexAlignmentsCommandOptions.input);
            URI outdir;
            if (indexAlignmentsCommandOptions.outdir != null && !indexAlignmentsCommandOptions.outdir.isEmpty()) {
                outdir = UriUtils.createDirectoryUri(indexAlignmentsCommandOptions.outdir);
            } else {
                // Get parent folder form input file
                outdir = input.resolve(".");
            }

//            assertDirectoryExists(outdir);
            FileUtils.checkDirectory(Paths.get(outdir));

            ObjectMap options = storageConfiguration.getAlignment().getOptions();
            if(indexAlignmentsCommandOptions.params != null) {
                options.putAll(indexAlignmentsCommandOptions.params);
            }

            if (Integer.parseInt(indexAlignmentsCommandOptions.fileId) != 0) {
                options.put(AlignmentStorageManager.FILE_ID, indexAlignmentsCommandOptions.fileId);
            }

            if(indexAlignmentsCommandOptions.dbName != null && !indexAlignmentsCommandOptions.dbName.isEmpty()) {
                options.put(AlignmentStorageManager.DB_NAME, indexAlignmentsCommandOptions.dbName);
            }


            options.put(AlignmentStorageManager.PLAIN, false);
            options.put(AlignmentStorageManager.INCLUDE_COVERAGE, indexAlignmentsCommandOptions.calculateCoverage);
            if (indexAlignmentsCommandOptions.meanCoverage != null && !indexAlignmentsCommandOptions.meanCoverage.isEmpty()) {
                options.put(AlignmentStorageManager.MEAN_COVERAGE_SIZE_LIST, indexAlignmentsCommandOptions.meanCoverage);
            }
            options.put(AlignmentStorageManager.COPY_FILE, false);
            options.put(AlignmentStorageManager.ENCRYPT, "null");

            StorageManagerFactory storageManagerFactory = new StorageManagerFactory(configuration);
            AlignmentStorageManager alignmentStorageManager;
            if (storageEngine == null || storageEngine.isEmpty()) {
                alignmentStorageManager = storageManagerFactory.getAlignmentStorageManager();
            } else {
                alignmentStorageManager = storageManagerFactory.getAlignmentStorageManager(storageEngine);
            }


            boolean extract, transform, load;
            URI nextFileUri = input;

            if (!indexAlignmentsCommandOptions.load && !indexAlignmentsCommandOptions.transform) {  // if not present --transform nor --load, do both
                extract = true;
                transform = true;
                load = true;
            } else {
                extract = indexAlignmentsCommandOptions.transform;
                transform = indexAlignmentsCommandOptions.transform;
                load = indexAlignmentsCommandOptions.load;
            }

            if (extract) {
                logger.info("-- Extract alignments -- {}", input);
                nextFileUri = alignmentStorageManager.extract(input, outdir);
            }

            if (transform) {
                logger.info("-- PreTransform alignments -- {}", nextFileUri);
                nextFileUri = alignmentStorageManager.preTransform(nextFileUri);
                logger.info("-- Transform alignments -- {}", nextFileUri);
                nextFileUri = alignmentStorageManager.transform(nextFileUri, null, outdir);
                logger.info("-- PostTransform alignments -- {}", nextFileUri);
                nextFileUri = alignmentStorageManager.postTransform(nextFileUri);
            }

            if (load) {
                logger.info("-- PreLoad alignments -- {}", nextFileUri);
                nextFileUri = alignmentStorageManager.preLoad(nextFileUri, outdir);
                logger.info("-- Load alignments -- {}", nextFileUri);
                nextFileUri = alignmentStorageManager.load(nextFileUri);
                logger.info("-- PostLoad alignments -- {}", nextFileUri);
                nextFileUri = alignmentStorageManager.postLoad(nextFileUri, outdir);
            }
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (StorageManagerException e) {
            e.printStackTrace();
        } catch (FileFormatException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

}
