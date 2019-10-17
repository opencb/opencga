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
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.hpg.bigdata.analysis.tools.ExecutorMonitor;
import org.opencb.hpg.bigdata.analysis.tools.Status;
import org.opencb.opencga.analysis.clinical.ClinicalUtils;
import org.opencb.opencga.analysis.clinical.interpretation.InterpretationResult;
import org.opencb.opencga.analysis.clinical.interpretation.TeamInterpretationAnalysis;
import org.opencb.opencga.analysis.clinical.interpretation.TieringInterpretationAnalysis;
import org.opencb.opencga.app.cli.analysis.options.InterpretationCommandOptions;
import org.opencb.opencga.catalog.db.api.JobDBAdaptor;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.models.Interpretation;
import org.opencb.opencga.core.models.Job;
import org.opencb.oskar.analysis.exceptions.AnalysisException;

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
        ObjectMap teamAnalysisOptions;

        if (StringUtils.isNotEmpty(options.job)) {
            Query query = new Query(JobDBAdaptor.QueryParams.UID.key(), Long.parseLong(options.job));
            DataResult<Job> jobQueryResult = catalogManager.getJobManager().search(options.study, query, QueryOptions.empty(), token);

            if (jobQueryResult.getNumResults() == 0) {
                throw new AnalysisException("Job '" + options.job + "' not found");
            }

            Map<String, String> params = jobQueryResult.first().getParams();
            clinicalAnalysisId = params.get("clinicalAnalysisId");
            panelList = Arrays.asList(StringUtils.split(params.get("panelIds"), ","));
            teamAnalysisOptions = new ObjectMap()
                    .append("includeLowCoverage", params.get("includeLowCoverage"))
                    .append("maxLowCoverage", params.get("maxLowCoverage"))
                    .append("includeNoTier", params.get("includeNoTier"));
            studyStr = String.valueOf(jobQueryResult.first().getAttributes().get(Job.OPENCGA_STUDY));

        } else {
            studyStr = options.study;
            clinicalAnalysisId = options.clinicalAnalysisId;
            if (StringUtils.isEmpty(options.panelIds)) {
                throw new AnalysisException("Missing panel ids");
            }
            panelList = Arrays.asList(StringUtils.split(options.panelIds, ","));
            teamAnalysisOptions = new ObjectMap()
                    .append("includeLowCoverage", options.includeLowCoverage)
                    .append("maxLowCoverage", options.maxLowCoverage)
                    .append("includeNoTier", options.includeNoTier);
        }

        // Initialize monitor
        ExecutorMonitor monitor = new ExecutorMonitor();
        Thread hook = new Thread(() -> {
            monitor.stop(new Status(Status.ERROR));
            logger.info("Running ShutdownHook. Tool execution has being aborted.");
        });

        Path outDirPath = Paths.get(options.outdirId);
        String opencgaHome = Paths.get(configuration.getDataDir()).getParent().toString();
        String assembly = ClinicalUtils.getAssembly(catalogManager, studyStr, token);

        Runtime.getRuntime().addShutdownHook(hook);
        monitor.start(outDirPath);

        ClinicalProperty.ModeOfInheritance moi;
        try {
            moi = ClinicalProperty.ModeOfInheritance.valueOf(options.segregation);
        } catch (IllegalArgumentException e) {
            throw new AnalysisException("Unknown '--family-segregation' value: " + options.segregation);
        }

        // Run interpretation
        TeamInterpretationAnalysis teamAnalysis = new TeamInterpretationAnalysis(clinicalAnalysisId, studyStr, panelList, moi,
                teamAnalysisOptions, opencgaHome, token);

        InterpretationResult interpretationResult = teamAnalysis.compute();

        // Store team analysis in DB
        catalogManager.getInterpretationManager().create(studyStr, clinicalAnalysisId, new Interpretation(interpretationResult.getResult()),
                QueryOptions.empty(), token);

        // Stop monitor
        Runtime.getRuntime().removeShutdownHook(hook);
        monitor.stop(new Status(Status.DONE));
    }

    private void tiering() throws Exception {
        InterpretationCommandOptions.TieringCommandOptions options = interpretationCommandOptions.tieringCommandOptions;

        String token = options.commonOptions.sessionId;

        String studyStr;
        String clinicalAnalysisId;
        List<String> panelList;
        ObjectMap tieringAnalysisOptions;

        if (StringUtils.isNotEmpty(options.job)) {
            Query query = new Query(JobDBAdaptor.QueryParams.UID.key(), Long.parseLong(options.job));
            DataResult<Job> jobQueryResult = catalogManager.getJobManager().search(options.study, query, QueryOptions.empty(), token);

            if (jobQueryResult.getNumResults() == 0) {
                throw new AnalysisException("Job '" + options.job + "' not found");
            }

            Map<String, String> params = jobQueryResult.first().getParams();
            clinicalAnalysisId = params.get("clinicalAnalysisId");
            panelList = Arrays.asList(StringUtils.split(params.get("panelIds"), ","));
            tieringAnalysisOptions = new ObjectMap()
                    .append("includeLowCoverage", params.get("includeLowCoverage"))
                    .append("maxLowCoverage", params.get("maxLowCoverage"))
                    .append("includeNoTier", params.get("includeNoTier"));
            studyStr = String.valueOf(jobQueryResult.first().getAttributes().get(Job.OPENCGA_STUDY));

        } else {
            studyStr = options.study;
            clinicalAnalysisId = options.clinicalAnalysisId;
            if (StringUtils.isEmpty(options.panelIds)) {
                throw new AnalysisException("Missing panel ids");
            }
            panelList = Arrays.asList(StringUtils.split(options.panelIds, ","));
            tieringAnalysisOptions = new ObjectMap()
                    .append("includeLowCoverage", options.includeLowCoverage)
                    .append("maxLowCoverage", options.maxLowCoverage)
                    .append("includeNoTier", options.includeNoTier);
        }

        // Initialize monitor
        ExecutorMonitor monitor = new ExecutorMonitor();
        Thread hook = new Thread(() -> {
            monitor.stop(new Status(Status.ERROR));
            logger.info("Running ShutdownHook. Tool execution has being aborted.");
        });

        Path outDirPath = Paths.get(options.outdirId);
        String opencgaHome = Paths.get(configuration.getDataDir()).getParent().toString();
        String assembly = ClinicalUtils.getAssembly(catalogManager, studyStr, token);

        Runtime.getRuntime().addShutdownHook(hook);
        monitor.start(outDirPath);

        // Run interpretation
        TieringInterpretationAnalysis tieringAnalysis = new TieringInterpretationAnalysis(clinicalAnalysisId, studyStr, panelList,
                options.penetrance, tieringAnalysisOptions, opencgaHome, token);

        InterpretationResult interpretationResult = tieringAnalysis.compute();

        // Store tiering analysis in DB
        if (options.commonOptions.params.getOrDefault("skipSave", "false").equals("true")) {
            logger.info("Skip save");
            System.out.println(JacksonUtils.getDefaultObjectMapper().writerWithDefaultPrettyPrinter()
                    .writeValueAsString(interpretationResult));
        } else {
            catalogManager.getInterpretationManager().create(studyStr, clinicalAnalysisId,
                    new Interpretation(interpretationResult.getResult()), QueryOptions.empty(), token);
        }

        // Stop monitor
        Runtime.getRuntime().removeShutdownHook(hook);
        monitor.stop(new Status(Status.DONE));
    }

}
