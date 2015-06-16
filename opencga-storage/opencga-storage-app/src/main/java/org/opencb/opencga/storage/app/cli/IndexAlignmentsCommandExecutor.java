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
        logger.debug("Executing index-alignments command line");

        try {
            // We need to find out the Storage Engine Id to be used
            // If not storage engine is passed then the default is taken from storage-configuration.yml file
            String storageEngine = (indexAlignmentsCommandOptions.storageEngine != null && !indexAlignmentsCommandOptions.storageEngine.isEmpty())
                    ? indexAlignmentsCommandOptions.storageEngine
                    : configuration.getDefaultStorageEngineId();
            logger.debug("Storage Engine set to '{}'", storageEngine);

            StorageEngineConfiguration storageConfiguration = configuration.getStorageEngine(storageEngine);

            StorageManagerFactory storageManagerFactory = new StorageManagerFactory(configuration);
            AlignmentStorageManager alignmentStorageManager;
            if (storageEngine == null || storageEngine.isEmpty()) {
                alignmentStorageManager = storageManagerFactory.getAlignmentStorageManager();
            } else {
                alignmentStorageManager = storageManagerFactory.getAlignmentStorageManager(storageEngine);
            }

            /*
             * Getting URIs and checking Paths
             */
            URI inputUri = UriUtils.createUri(indexAlignmentsCommandOptions.input);
            FileUtils.checkFile(Paths.get(inputUri));

            URI outdirUri = (indexAlignmentsCommandOptions.outdir != null && !indexAlignmentsCommandOptions.outdir.isEmpty())
                    ? UriUtils.createDirectoryUri(indexAlignmentsCommandOptions.outdir)
                    // Get parent folder from input file
                    : inputUri.resolve(".");
            FileUtils.checkDirectory(Paths.get(outdirUri));
            logger.debug("All files and directories exist");

            /*
             * Add CLI options to the alignmentOptions
             */
            ObjectMap alignmentOptions = storageConfiguration.getAlignment().getOptions();
            if (Integer.parseInt(indexAlignmentsCommandOptions.fileId) != 0) {
                alignmentOptions.put(AlignmentStorageManager.Options.FILE_ID.key(), indexAlignmentsCommandOptions.fileId);
            }
            if(indexAlignmentsCommandOptions.dbName != null && !indexAlignmentsCommandOptions.dbName.isEmpty()) {
                alignmentOptions.put(AlignmentStorageManager.Options.DB_NAME.key(), indexAlignmentsCommandOptions.dbName);
            }
            if(indexAlignmentsCommandOptions.params != null) {
                alignmentOptions.putAll(indexAlignmentsCommandOptions.params);
            }

            alignmentOptions.put(AlignmentStorageManager.Options.PLAIN.key(), false);
            alignmentOptions.put(AlignmentStorageManager.Options.INCLUDE_COVERAGE.key(), indexAlignmentsCommandOptions.calculateCoverage);
            if (indexAlignmentsCommandOptions.meanCoverage != null && !indexAlignmentsCommandOptions.meanCoverage.isEmpty()) {
                alignmentOptions.put(AlignmentStorageManager.Options.MEAN_COVERAGE_SIZE_LIST.key(), indexAlignmentsCommandOptions.meanCoverage);
            }
            alignmentOptions.put(AlignmentStorageManager.Options.COPY_FILE.key(), false);
            alignmentOptions.put(AlignmentStorageManager.Options.ENCRYPT.key(), "null");
            logger.debug("Configuration options: {}", alignmentOptions.toJson());


            boolean extract, transform, load;
            URI nextFileUri = inputUri;

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
                logger.info("-- Extract alignments -- {}", inputUri);
                nextFileUri = alignmentStorageManager.extract(inputUri, outdirUri);
            }

            if (transform) {
                logger.info("-- PreTransform alignments -- {}", nextFileUri);
                nextFileUri = alignmentStorageManager.preTransform(nextFileUri);
                logger.info("-- Transform alignments -- {}", nextFileUri);
                nextFileUri = alignmentStorageManager.transform(nextFileUri, null, outdirUri);
                logger.info("-- PostTransform alignments -- {}", nextFileUri);
                nextFileUri = alignmentStorageManager.postTransform(nextFileUri);
            }

            if (load) {
                logger.info("-- PreLoad alignments -- {}", nextFileUri);
                nextFileUri = alignmentStorageManager.preLoad(nextFileUri, outdirUri);
                logger.info("-- Load alignments -- {}", nextFileUri);
                nextFileUri = alignmentStorageManager.load(nextFileUri);
                logger.info("-- PostLoad alignments -- {}", nextFileUri);
                nextFileUri = alignmentStorageManager.postLoad(nextFileUri, outdirUri);
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
