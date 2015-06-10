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
import org.opencb.datastore.core.ObjectMap;
import org.opencb.opencga.core.common.Config;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.storage.core.StorageManagerException;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.config.StorageEngineConfiguration;
import org.opencb.opencga.storage.core.config.StorageEtlConfiguration;
import org.opencb.opencga.storage.core.variant.FileStudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

/**
 * Created by imedina on 02/03/15.
 */
public class IndexVariantsCommandExecutor extends CommandExecutor {

    private CliOptionsParser.IndexVariantsCommandOptions indexVariantsCommandOptions;

    public static final String OPENCGA_STORAGE_ANNOTATOR = "OPENCGA.STORAGE.ANNOTATOR";

    public IndexVariantsCommandExecutor(CliOptionsParser.IndexVariantsCommandOptions indexVariantsCommandOptions) {
        super(indexVariantsCommandOptions.logLevel, indexVariantsCommandOptions.verbose,
                indexVariantsCommandOptions.configFile);

        this.indexVariantsCommandOptions = indexVariantsCommandOptions;
    }
    @Override
    public void execute() {
        logger.info("in IndexVariantsCommandExecutor");
        Config.setOpenCGAHome();
        try {
            /** Get VariantStorageManager **/
            // We need to find out the Storage Engine Id to be used
            // If not storage engine is passed then the default is taken from configuration.yml file
            String storageEngine = (indexVariantsCommandOptions.storageEngine != null && !indexVariantsCommandOptions.storageEngine.isEmpty())
                    ? indexVariantsCommandOptions.storageEngine
                    : configuration.getDefaultStorageEngineId();
            logger.debug("storageEngine set to '{}'", storageEngine);

            StorageEngineConfiguration storageConfiguration = configuration.getStorageEngine(storageEngine);
            StorageEtlConfiguration storageEtlConfiguration = storageConfiguration.getAlignment();

            StorageManagerFactory storageManagerFactory = new StorageManagerFactory(configuration);
            VariantStorageManager variantStorageManager;
            if (storageEngine == null || storageEngine.isEmpty()) {
                variantStorageManager = storageManagerFactory.getVariantStorageManager();
            } else {
                variantStorageManager = storageManagerFactory.getVariantStorageManager(storageEngine);
            }

//            if(indexVariantsCommandOptions.credentials != null && !indexVariantsCommandOptions.credentials.isEmpty()) {
//                variantStorageManager.addConfigUri(new URI(null, indexVariantsCommandOptions.credentials, null));
//            }

            /** Get URIs **/
            URI variantsUri = UriUtils.createUri(indexVariantsCommandOptions.input);
            URI pedigreeUri = indexVariantsCommandOptions.pedigree != null && !indexVariantsCommandOptions.pedigree.isEmpty()
                    ? UriUtils.createUri(indexVariantsCommandOptions.pedigree)
                    : null;
            URI outdirUri = indexVariantsCommandOptions.outdir != null && !indexVariantsCommandOptions.outdir.isEmpty()
                    ? UriUtils.createUri(indexVariantsCommandOptions.outdir + (indexVariantsCommandOptions.outdir.endsWith("/") ? "" : "/")).resolve(".")
                    : variantsUri.resolve(".");

//            assertDirectoryExists(outdirUri);

//            String fileName = variantsUri.resolve(".").relativize(variantsUri).toString();
//            VariantSource source = new VariantSource(fileName, indexVariantsCommandOptions.fileId,
//                    indexVariantsCommandOptions.studyId, indexVariantsCommandOptions.study, indexVariantsCommandOptions.studyType, indexVariantsCommandOptions.aggregated);

            /** Add cli options to the variant options **/
            ObjectMap variantOptions = storageConfiguration.getVariant().getOptions();
            variantOptions.put(VariantStorageManager.STUDY_NAME, indexVariantsCommandOptions.study);
            variantOptions.put(VariantStorageManager.STUDY_ID, indexVariantsCommandOptions.studyId);
            variantOptions.put(VariantStorageManager.FILE_ID, indexVariantsCommandOptions.fileId);
            variantOptions.put(VariantStorageManager.SAMPLE_IDS, indexVariantsCommandOptions.sampleIds);
            variantOptions.put(VariantStorageManager.CALCULATE_STATS, indexVariantsCommandOptions.calculateStats);
            variantOptions.put(VariantStorageManager.INCLUDE_STATS, indexVariantsCommandOptions.includeStats);
            variantOptions.put(VariantStorageManager.INCLUDE_GENOTYPES, indexVariantsCommandOptions.includeGenotype);   // TODO rename samples to genotypes
            variantOptions.put(VariantStorageManager.INCLUDE_SRC, indexVariantsCommandOptions.includeSrc);
            variantOptions.put(VariantStorageManager.COMPRESS_GENOTYPES, indexVariantsCommandOptions.compressGenotypes);
            variantOptions.put(VariantStorageManager.AGGREGATED_TYPE, indexVariantsCommandOptions.aggregated);
            if (indexVariantsCommandOptions.dbName != null) variantOptions.put(VariantStorageManager.DB_NAME, indexVariantsCommandOptions.dbName);
            variantOptions.put(VariantStorageManager.ANNOTATE, indexVariantsCommandOptions.annotate);
            variantOptions.put(VariantStorageManager.OVERWRITE_ANNOTATIONS, indexVariantsCommandOptions.overwriteAnnotations);
            if (indexVariantsCommandOptions.studyConfigurationFile != null && !indexVariantsCommandOptions.studyConfigurationFile.isEmpty()) {
                variantOptions.put(FileStudyConfigurationManager.STUDY_CONFIGURATION_PATH, indexVariantsCommandOptions.studyConfigurationFile);
            }

            if(indexVariantsCommandOptions.annotate) {
                //Get annotator config
                Properties annotatorProperties = Config.getStorageProperties();
                if(indexVariantsCommandOptions.annotatorConfigFile != null && !indexVariantsCommandOptions.annotatorConfigFile.isEmpty()) {
                    annotatorProperties.load(new FileInputStream(indexVariantsCommandOptions.annotatorConfigFile));
                }
                variantOptions.put(VariantStorageManager.ANNOTATOR_PROPERTIES, annotatorProperties);

                //Get annotation source
                VariantAnnotationManager.AnnotationSource annotatorSource = indexVariantsCommandOptions.annotator;
                if(annotatorSource == null) {
                    annotatorSource = org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager.AnnotationSource.valueOf(
                            annotatorProperties.getProperty(
                                    OPENCGA_STORAGE_ANNOTATOR,
                                    org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager.AnnotationSource.CELLBASE_REST.name()
                            ).toUpperCase()
                    );
                }
                variantOptions.put(VariantStorageManager.ANNOTATION_SOURCE, annotatorSource);
            }

            variantOptions.putAll(indexVariantsCommandOptions.params);


            /** Execute ETL steps **/
            ObjectMap params = null;
            URI nextFileUri = variantsUri;
            boolean extract, transform, load;

            if (!indexVariantsCommandOptions.load && !indexVariantsCommandOptions.transform) {
                extract = true;
                transform = true;
                load = true;
            } else {
                extract = indexVariantsCommandOptions.transform;
                transform = indexVariantsCommandOptions.transform;
                load = indexVariantsCommandOptions.load;
            }

            if (extract) {
                logger.info("-- Extract variants -- {}", variantsUri);
                nextFileUri = variantStorageManager.extract(variantsUri, outdirUri, params);
            }

            if (transform) {
                logger.info("-- PreTransform variants -- {}", nextFileUri);
                nextFileUri = variantStorageManager.preTransform(nextFileUri, params);
                logger.info("-- Transform variants -- {}", nextFileUri);
                nextFileUri = variantStorageManager.transform(nextFileUri, pedigreeUri, outdirUri, params);
                logger.info("-- PostTransform variants -- {}", nextFileUri);
                nextFileUri = variantStorageManager.postTransform(nextFileUri, params);
            }

            if (load) {
                logger.info("-- PreLoad variants -- {}", nextFileUri);
                nextFileUri = variantStorageManager.preLoad(nextFileUri, outdirUri, params);
                logger.info("-- Load variants -- {}", nextFileUri);
                nextFileUri = variantStorageManager.load(nextFileUri, params);
                logger.info("-- PostLoad variants -- {}", nextFileUri);
                nextFileUri = variantStorageManager.postLoad(nextFileUri, outdirUri, params);
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (FileFormatException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (StorageManagerException e) {
            e.printStackTrace();
        }
    }



}
