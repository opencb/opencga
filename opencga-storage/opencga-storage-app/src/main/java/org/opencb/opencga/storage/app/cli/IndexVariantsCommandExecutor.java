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

import com.beust.jcommander.ParameterException;
import org.opencb.commons.utils.FileUtils;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.config.StorageEngineConfiguration;
import org.opencb.opencga.storage.core.variant.FileStudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * Created by imedina on 02/03/15.
 */
public class IndexVariantsCommandExecutor extends CommandExecutor {

    private CliOptionsParser.IndexVariantsCommandOptions indexVariantsCommandOptions;

//    public static final String OPENCGA_STORAGE_ANNOTATOR = "OPENCGA.STORAGE.ANNOTATOR";

    public IndexVariantsCommandExecutor(CliOptionsParser.IndexVariantsCommandOptions indexVariantsCommandOptions) {
        super(indexVariantsCommandOptions.logLevel, indexVariantsCommandOptions.verbose,
                indexVariantsCommandOptions.configFile);

        this.logFile = indexVariantsCommandOptions.logFile;
        this.indexVariantsCommandOptions = indexVariantsCommandOptions;
    }

    @Override
    public void execute() throws Exception {
        logger.debug("Executing index-variants command line");

        /**
         * Getting VariantStorageManager
         * We need to find out the Storage Engine Id to be used
         * If not storage engine is passed then the default is taken from storage-configuration.yml file
         **/
        String storageEngine = (indexVariantsCommandOptions.storageEngine != null && !indexVariantsCommandOptions.storageEngine.isEmpty())
                ? indexVariantsCommandOptions.storageEngine
                : configuration.getDefaultStorageEngineId();
        logger.debug("Storage Engine set to '{}'", storageEngine);

        StorageEngineConfiguration storageConfiguration = configuration.getStorageEngine(storageEngine);

        StorageManagerFactory storageManagerFactory = new StorageManagerFactory(configuration);
        VariantStorageManager variantStorageManager;
        if (storageEngine == null || storageEngine.isEmpty()) {
            variantStorageManager = storageManagerFactory.getVariantStorageManager();
        } else {
            variantStorageManager = storageManagerFactory.getVariantStorageManager(storageEngine);
        }

            /*
             * Getting URIs and checking Paths
             */
        URI variantsUri = UriUtils.createUri(indexVariantsCommandOptions.input);
        if (variantsUri.getScheme().startsWith("file") || variantsUri.getScheme().isEmpty()) {
            FileUtils.checkFile(Paths.get(variantsUri));
        }

        URI pedigreeUri = (indexVariantsCommandOptions.pedigree != null && !indexVariantsCommandOptions.pedigree.isEmpty())
                ? UriUtils.createUri(indexVariantsCommandOptions.pedigree)
                : null;
        if (pedigreeUri != null) {
            FileUtils.checkFile(Paths.get(pedigreeUri));
        }

        URI outdirUri = (indexVariantsCommandOptions.outdir != null && !indexVariantsCommandOptions.outdir.isEmpty())
                ? UriUtils.createDirectoryUri(indexVariantsCommandOptions.outdir)
                // Get parent folder from input file
                : variantsUri.resolve(".");
        if (outdirUri.getScheme().startsWith("file") || outdirUri.getScheme().isEmpty()) {
            FileUtils.checkDirectory(Paths.get(outdirUri), true);
        }
        logger.debug("All files and directories exist");

//            VariantSource source = new VariantSource(fileName, indexVariantsCommandOptions.fileId,
//                    indexVariantsCommandOptions.studyId, indexVariantsCommandOptions.study, indexVariantsCommandOptions.studyType,
// indexVariantsCommandOptions.aggregated);

        /** Add CLi options to the variant options **/
        ObjectMap variantOptions = storageConfiguration.getVariant().getOptions();
        variantOptions.put(VariantStorageManager.Options.STUDY_NAME.key(), indexVariantsCommandOptions.study);
        variantOptions.put(VariantStorageManager.Options.STUDY_ID.key(), indexVariantsCommandOptions.studyId);
        variantOptions.put(VariantStorageManager.Options.FILE_ID.key(), indexVariantsCommandOptions.fileId);
        variantOptions.put(VariantStorageManager.Options.SAMPLE_IDS.key(), indexVariantsCommandOptions.sampleIds);
        variantOptions.put(VariantStorageManager.Options.CALCULATE_STATS.key(), indexVariantsCommandOptions.calculateStats);
        variantOptions.put(VariantStorageManager.Options.INCLUDE_STATS.key(), indexVariantsCommandOptions.includeStats);
        variantOptions.put(VariantStorageManager.Options.INCLUDE_GENOTYPES.key(), indexVariantsCommandOptions.includeGenotype);
        variantOptions.put(VariantStorageManager.Options.EXTRA_GENOTYPE_FIELDS.key(), indexVariantsCommandOptions.extraFields);
        variantOptions.put(VariantStorageManager.Options.INCLUDE_SRC.key(), indexVariantsCommandOptions.includeSrc);
//        variantOptions.put(VariantStorageManager.Options.COMPRESS_GENOTYPES.key(), indexVariantsCommandOptions.compressGenotypes);
        variantOptions.put(VariantStorageManager.Options.AGGREGATED_TYPE.key(), indexVariantsCommandOptions.aggregated);
        if (indexVariantsCommandOptions.dbName != null) {
            variantOptions.put(VariantStorageManager.Options.DB_NAME.key(), indexVariantsCommandOptions.dbName);
        }
        variantOptions.put(VariantStorageManager.Options.ANNOTATE.key(), indexVariantsCommandOptions.annotate);
        if (indexVariantsCommandOptions.annotator != null) {
            variantOptions.put(VariantAnnotationManager.ANNOTATION_SOURCE, indexVariantsCommandOptions.annotator);
        }
        variantOptions.put(VariantAnnotationManager.OVERWRITE_ANNOTATIONS, indexVariantsCommandOptions.overwriteAnnotations);
        if (indexVariantsCommandOptions.studyConfigurationFile != null && !indexVariantsCommandOptions.studyConfigurationFile.isEmpty()) {
            variantOptions.put(FileStudyConfigurationManager.STUDY_CONFIGURATION_PATH, indexVariantsCommandOptions.studyConfigurationFile);
        }

        if (indexVariantsCommandOptions.aggregationMappingFile != null) {
            // TODO move this options to new configuration.yml
            Properties aggregationMappingProperties = new Properties();
            try {
                aggregationMappingProperties.load(new FileInputStream(indexVariantsCommandOptions.aggregationMappingFile));
                variantOptions.put(VariantStorageManager.Options.AGGREGATION_MAPPING_PROPERTIES.key(), aggregationMappingProperties);
            } catch (FileNotFoundException e) {
                logger.error("Aggregation mapping file {} not found. Population stats won't be parsed.", indexVariantsCommandOptions
                        .aggregationMappingFile);
            }
        }

        if (indexVariantsCommandOptions.params != null) {
            variantOptions.putAll(indexVariantsCommandOptions.params);
        }
        logger.debug("Configuration options: {}", variantOptions.toJson());


        /** Execute ETL steps **/
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

        // Check the database connection before we start
        if (load) {
            if (!variantStorageManager.testConnection(variantOptions.getString(VariantStorageManager.Options.DB_NAME.key()))) {
                logger.error("Connection to database '{}' failed", variantOptions.getString(VariantStorageManager.Options.DB_NAME.key()));
                throw new ParameterException("Database connection test failed");
            }
        }

        if (extract) {
            logger.info("Extract variants '{}'", variantsUri);
            nextFileUri = variantStorageManager.extract(variantsUri, outdirUri);
        }

        if (transform) {
            logger.info("PreTransform variants '{}'", nextFileUri);
            nextFileUri = variantStorageManager.preTransform(nextFileUri);
            logger.info("Transform variants '{}'", nextFileUri);
            nextFileUri = variantStorageManager.transform(nextFileUri, pedigreeUri, outdirUri);
            logger.info("PostTransform variants '{}'", nextFileUri);
            nextFileUri = variantStorageManager.postTransform(nextFileUri);
        }

        if (load) {
            logger.info("PreLoad variants '{}'", nextFileUri);
            nextFileUri = variantStorageManager.preLoad(nextFileUri, outdirUri);
            logger.info("Load variants '{}'", nextFileUri);
            nextFileUri = variantStorageManager.load(nextFileUri);
            logger.info("PostLoad variants '{}'", nextFileUri);
            nextFileUri = variantStorageManager.postLoad(nextFileUri, outdirUri);
        }
    }

}
