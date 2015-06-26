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
import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.storage.core.StorageManagerException;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.config.StorageEngineConfiguration;
import org.opencb.opencga.storage.core.variant.FileStudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.io.VariantExporter;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Created by imedina on 02/03/15.
 */
public class ExportVariantsCommandExecutor extends CommandExecutor {

    private final CliOptionsParser.ExportVariantsCommandOptions exportVariantsCommandOptions;

//    public static final String OPENCGA_STORAGE_ANNOTATOR = "OPENCGA.STORAGE.ANNOTATOR";

    public ExportVariantsCommandExecutor(CliOptionsParser.ExportVariantsCommandOptions exportVariantsCommandOptions) {
        super(exportVariantsCommandOptions.logLevel, exportVariantsCommandOptions.verbose,
                exportVariantsCommandOptions.configFile);

        this.exportVariantsCommandOptions = exportVariantsCommandOptions;
    }
    @Override
    public void execute() {
        logger.debug("Executing export-variants command line");

        try {
            /**
             * Getting VariantStorageManager
             * We need to find out the Storage Engine Id to be used
             * If not storage engine is passed then the default is taken from storage-configuration.yml file
             **/
            String storageEngine = (exportVariantsCommandOptions.storageEngine != null && !exportVariantsCommandOptions.storageEngine.isEmpty())
                    ? exportVariantsCommandOptions.storageEngine
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

            VariantDBAdaptor dbAdaptor = variantStorageManager.getDBAdaptor(exportVariantsCommandOptions.dbName);
        /*
         * Getting URIs and checking Paths
         */
            URI outputUri = UriUtils.createUri(exportVariantsCommandOptions.outdir);

            /** Add CLi options to the variant options **/
            ObjectMap variantOptions = storageConfiguration.getVariant().getOptions();
            if (exportVariantsCommandOptions.studyConfigurationFile != null && !exportVariantsCommandOptions.studyConfigurationFile.isEmpty()) {
                variantOptions.put(FileStudyConfigurationManager.STUDY_CONFIGURATION_PATH, exportVariantsCommandOptions.studyConfigurationFile);
            }

            StudyConfiguration studyConfiguration = variantStorageManager.getStudyConfiguration(variantOptions);
            logger.debug("Configuration options: {}", variantOptions.toJson());


            /** Execute ETL steps **/
            VariantExporter variantExporter = new VariantExporter();

            QueryOptions queryOptions = new QueryOptions();
            List<String> include = Arrays.asList("chromosome", "start", "end", "alternative", "reference", "ids", "sourceEntries");
            queryOptions.add("include", include);
            variantExporter.vcfExport(dbAdaptor, studyConfiguration, outputUri, queryOptions);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
