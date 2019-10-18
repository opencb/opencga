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

package org.opencb.opencga.app.cli.analysis.executors;

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.interpretation.ClinicalProperty;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.hpg.bigdata.analysis.tools.ExecutorMonitor;
import org.opencb.hpg.bigdata.analysis.tools.Status;
import org.opencb.opencga.analysis.clinical.interpretation.TeamInterpretationAnalysis;
import org.opencb.opencga.analysis.clinical.interpretation.TeamInterpretationConfiguration;
import org.opencb.opencga.analysis.clinical.interpretation.TieringInterpretationAnalysis;
import org.opencb.opencga.analysis.clinical.interpretation.TieringInterpretationConfiguration;
import org.opencb.opencga.analysis.exceptions.AnalysisException;
import org.opencb.opencga.app.cli.analysis.options.InterpretationCommandOptions;
import org.opencb.opencga.catalog.db.api.JobDBAdaptor;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.models.Interpretation;
import org.opencb.opencga.core.models.Job;
import org.opencb.oskar.analysis.result.AnalysisResult;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InterpretationCommandExecutor extends AnalysisCommandExecutor {

    private final InterpretationCommandOptions interpretationCommandOptions;

    private static Map<String, Map<String, List<String>>> actionableVariantsByAssembly = new HashMap<>();
    private static Map<String, ClinicalProperty.RoleInCancer> roleInCancer = new HashMap<>();

    public InterpretationCommandExecutor(InterpretationCommandOptions options) {
        super(options.analysisCommonOptions);
        interpretationCommandOptions = options;
    }

    @Override
    public void execute() throws Exception {
        logger.debug("Executing interpretation command line");

        String subCommandString = getParsedSubCommand(interpretationCommandOptions.jCommander);
        configure();
        switch (subCommandString) {
            case "team":
                team();
                break;
            case "tiering":
                tiering();
                break;
            case "custom":
//                custom();
                break;
            default:
                logger.error("Subcommand not valid");
                break;

        }
    }

    private void team() throws Exception {
        InterpretationCommandOptions.TeamCommandOptions options = interpretationCommandOptions.teamCommandOptions;

        String token = options.commonOptions.sessionId;

        String studyStr;
        String clinicalAnalysisId;
        List<String> panelList;
        TeamInterpretationConfiguration config = new TeamInterpretationConfiguration();

        if (StringUtils.isNotEmpty(options.job)) {
            Query query = new Query(JobDBAdaptor.QueryParams.UID.key(), Long.parseLong(options.job));
            QueryResult<Job> jobQueryResult = catalogManager.getJobManager().get(options.study, query, QueryOptions.empty(), token);

            if (jobQueryResult.getNumResults() == 0) {
                throw new AnalysisException("Job '" + options.job + "' not found");
            }

            Map<String, String> params = jobQueryResult.first().getParams();
            clinicalAnalysisId = params.get("clinicalAnalysisId");
            panelList = Arrays.asList(StringUtils.split(params.get("panelIds"), ","));
//            config.setIncludeLowCoverage(params.get("includeLowCoverage"));
//            config.setMaxLowCoverage(params.get("maxLowCoverage"));
//            config.setSkipUntieredVariants(params.get("includeNoTier"))
            studyStr = String.valueOf(jobQueryResult.first().getAttributes().get(Job.OPENCGA_STUDY));
        } else {
            studyStr = options.study;
            clinicalAnalysisId = options.clinicalAnalysisId;
            if (StringUtils.isEmpty(options.panelIds)) {
                throw new AnalysisException("Missing panel ids");
            }
            panelList = Arrays.asList(StringUtils.split(options.panelIds, ","));
            config.setIncludeLowCoverage(options.includeLowCoverage);
            config.setMaxLowCoverage(options.maxLowCoverage);
            config.setSkipUntieredVariants(!options.includeNoTier);
        }

        // Initialize monitor
        ExecutorMonitor monitor = new ExecutorMonitor();
        Thread hook = new Thread(() -> {
            monitor.stop(new Status(Status.ERROR));
            logger.info("Running ShutdownHook. Tool execution has being aborted.");
        });

        Path outDir = Paths.get(options.outdirId);
        Path opencgaHome = Paths.get(configuration.getDataDir()).getParent();

        Runtime.getRuntime().addShutdownHook(hook);
        monitor.start(outDir);

        ClinicalProperty.ModeOfInheritance moi;
        try {
            moi = ClinicalProperty.ModeOfInheritance.valueOf(options.segregation);
        } catch (IllegalArgumentException e) {
            throw new AnalysisException("Unknown '--family-segregation' value: " + options.segregation);
        }

        // Run interpretation
        TeamInterpretationAnalysis teamAnalysis = new TeamInterpretationAnalysis(clinicalAnalysisId, studyStr, panelList, moi, outDir,
                opencgaHome, config, token);

        AnalysisResult result = teamAnalysis.execute();

        // Store team analysis in DB
        org.opencb.biodata.models.clinical.interpretation.Interpretation interpretation = null;
        catalogManager.getInterpretationManager().create(studyStr, clinicalAnalysisId, new Interpretation(interpretation),
                QueryOptions.empty(), token);

        // Stop monitor
        Runtime.getRuntime().removeShutdownHook(hook);
        monitor.stop(new Status(Status.DONE));
    }

    private void tiering() throws Exception {
        InterpretationCommandOptions.TieringCommandOptions options = interpretationCommandOptions.tieringCommandOptions;
        TieringInterpretationConfiguration config = new TieringInterpretationConfiguration();

        String token = options.commonOptions.sessionId;

        String studyStr;
        String clinicalAnalysisId;
        List<String> panelList;

        if (StringUtils.isNotEmpty(options.job)) {
            Query query = new Query(JobDBAdaptor.QueryParams.UID.key(), Long.parseLong(options.job));
            QueryResult<Job> jobQueryResult = catalogManager.getJobManager().get(options.study, query, QueryOptions.empty(), token);

            if (jobQueryResult.getNumResults() == 0) {
                throw new AnalysisException("Job '" + options.job + "' not found");
            }

            Map<String, String> params = jobQueryResult.first().getParams();
            clinicalAnalysisId = params.get("clinicalAnalysisId");
            panelList = Arrays.asList(StringUtils.split(params.get("panelIds"), ","));
//            config.setIncludeLowCoverage(params.get("includeLowCoverage"));
//            config.setMaxLowCoverage(params.get("maxLowCoverage"));
//            config.setSkipUntieredVariants(params.get("includeNoTier"))
            studyStr = String.valueOf(jobQueryResult.first().getAttributes().get(Job.OPENCGA_STUDY));

        } else {
            studyStr = options.study;
            clinicalAnalysisId = options.clinicalAnalysisId;
            if (StringUtils.isEmpty(options.panelIds)) {
                throw new AnalysisException("Missing panel ids");
            }
            panelList = Arrays.asList(StringUtils.split(options.panelIds, ","));
            config.setIncludeLowCoverage(options.includeLowCoverage);
            config.setMaxLowCoverage(options.maxLowCoverage);
            config.setSkipUntieredVariants(!options.includeNoTier);
        }

        // Initialize monitor
        ExecutorMonitor monitor = new ExecutorMonitor();
        Thread hook = new Thread(() -> {
            monitor.stop(new Status(Status.ERROR));
            logger.info("Running ShutdownHook. Tool execution has being aborted.");
        });

        Path outDir = Paths.get(options.outdirId);
        Path opencgaHome = Paths.get(configuration.getDataDir()).getParent();

        Runtime.getRuntime().addShutdownHook(hook);
        monitor.start(outDir);

        // Run interpretation
        TieringInterpretationAnalysis tieringAnalysis = new TieringInterpretationAnalysis(clinicalAnalysisId, studyStr, panelList,
                options.penetrance, outDir, opencgaHome, config, token);

        AnalysisResult analysisResult = tieringAnalysis.execute();

        // Store tiering analysis in DB
        org.opencb.biodata.models.clinical.interpretation.Interpretation interpretation = null;
        if (options.commonOptions.params.getOrDefault("skipSave", "false").equals("true")) {
            logger.info("Skip save");
            System.out.println(JacksonUtils.getDefaultObjectMapper().writerWithDefaultPrettyPrinter()
                    .writeValueAsString(interpretation));
        } else {
            catalogManager.getInterpretationManager().create(studyStr, clinicalAnalysisId,
                    new Interpretation(interpretation), QueryOptions.empty(), token);
        }

        // Stop monitor
        Runtime.getRuntime().removeShutdownHook(hook);
        monitor.stop(new Status(Status.DONE));
    }

}
