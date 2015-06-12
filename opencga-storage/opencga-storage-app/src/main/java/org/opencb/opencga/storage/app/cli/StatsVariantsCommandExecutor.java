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

import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.storage.core.StorageManagerException;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.FileStudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.stats.VariantStatisticsManager;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by imedina on 25/05/15.
 */
public class StatsVariantsCommandExecutor extends CommandExecutor {

    private CliOptionsParser.StatsVariantsCommandOptions statsVariantsCommandOptions;


    public StatsVariantsCommandExecutor(CliOptionsParser.StatsVariantsCommandOptions statsVariantsCommandOptions) {
        super(statsVariantsCommandOptions.logLevel, statsVariantsCommandOptions.verbose,
                statsVariantsCommandOptions.configFile);

        this.statsVariantsCommandOptions = statsVariantsCommandOptions;
    }


    @Override
    public void execute() {

        try {
            /**
             * query options
             */
            QueryOptions queryOptions = new QueryOptions();
            queryOptions.put(VariantStorageManager.Options.DB_NAME.key(), statsVariantsCommandOptions.dbName);
            queryOptions.put(VariantStorageManager.Options.OVERWRITE_STATS.key(), statsVariantsCommandOptions.overwriteStats);
            queryOptions.put(VariantStorageManager.Options.FILE_ID.key(), statsVariantsCommandOptions.fileId);
            queryOptions.put(VariantStorageManager.Options.STUDY_ID.key(), statsVariantsCommandOptions.studyId);
            if (statsVariantsCommandOptions.studyConfigurationFile != null && !statsVariantsCommandOptions.studyConfigurationFile.isEmpty()) {
                queryOptions.put(FileStudyConfigurationManager.STUDY_CONFIGURATION_PATH, statsVariantsCommandOptions.studyConfigurationFile);
            }

            Map<String, Set<String>> cohorts = null;
            if (statsVariantsCommandOptions.cohort != null && !statsVariantsCommandOptions.cohort.isEmpty()) {
                cohorts = new LinkedHashMap<>(statsVariantsCommandOptions.cohort.size());
                for (Map.Entry<String, String> entry : statsVariantsCommandOptions.cohort.entrySet()) {
                    cohorts.put(entry.getKey(), new HashSet<>(Arrays.asList(entry.getValue().split(","))));
                }
            }

            /**
             * Create DBAdaptor
             */
            VariantStorageManager variantStorageManager = new StorageManagerFactory(configuration).getVariantStorageManager(statsVariantsCommandOptions.storageEngine);
            VariantDBAdaptor dbAdaptor = variantStorageManager.getDBAdaptor(statsVariantsCommandOptions.dbName);
//        dbAdaptor.setConstantSamples(Integer.toString(statsVariantsCommandOptions.fileId));    // TODO jmmut: change to studyId when we remove fileId
            StudyConfiguration studyConfiguration = variantStorageManager.getStudyConfiguration(queryOptions);
            if (studyConfiguration == null) {
                studyConfiguration = new StudyConfiguration(statsVariantsCommandOptions.studyId, statsVariantsCommandOptions.dbName);
            }
            /**
             * Create and load stats
             */
            URI outputUri = UriUtils.createUri(statsVariantsCommandOptions.fileName == null? "" : statsVariantsCommandOptions.fileName);
            URI directoryUri = outputUri.resolve(".");
            String filename = outputUri.equals(directoryUri) ? VariantStorageManager.buildFilename(studyConfiguration.getStudyId(), statsVariantsCommandOptions.fileId)
                    : Paths.get(outputUri.getPath()).getFileName().toString();
            assertDirectoryExists(directoryUri);
            VariantStatisticsManager variantStatisticsManager = new VariantStatisticsManager();

            boolean doCreate = true;
            boolean doLoad = true;
//        doCreate = statsVariantsCommandOptions.create;
//        doLoad = statsVariantsCommandOptions.load != null;
//        if (!statsVariantsCommandOptions.create && statsVariantsCommandOptions.load == null) {
//            doCreate = doLoad = true;
//        } else if (statsVariantsCommandOptions.load != null) {
//            filename = statsVariantsCommandOptions.load;
//        }

            try {

                Map<String, Integer> cohortIds = statsVariantsCommandOptions.cohortIds.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> Integer.parseInt(e.getValue())));
                /** Check and update StudyConfiguration **/
                variantStatisticsManager.checkAndUpdateStudyConfigurationCohorts(studyConfiguration, cohorts, cohortIds);

                if (doCreate) {
                    filename += "." + TimeUtils.getTime();
                    outputUri = outputUri.resolve(filename);
                    outputUri = variantStatisticsManager.createStats(dbAdaptor, outputUri, cohorts, studyConfiguration, queryOptions);
                }

                if (doLoad) {
                    outputUri = outputUri.resolve(filename);
                    variantStatisticsManager.loadStats(dbAdaptor, outputUri, studyConfiguration, queryOptions);
                    variantStorageManager.checkStudyConfiguration(studyConfiguration, dbAdaptor);
                    variantStorageManager.getStudyConfigurationManager(queryOptions).updateStudyConfiguration(studyConfiguration, queryOptions);
                }
            } catch (Exception e) {   // file not found? wrong file id or study id? bad parameters to ParallelTaskRunner?
                e.printStackTrace();
                logger.error(e.getMessage());
            }

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (StorageManagerException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }
}
