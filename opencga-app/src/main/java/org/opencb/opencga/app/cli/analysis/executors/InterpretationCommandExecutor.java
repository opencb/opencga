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

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.interpretation.ClinicalProperty;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.hpg.bigdata.analysis.tools.ExecutorMonitor;
import org.opencb.hpg.bigdata.analysis.tools.Status;
import org.opencb.opencga.analysis.clinical.ClinicalUtils;
import org.opencb.opencga.analysis.clinical.interpretation.*;
import org.opencb.opencga.app.cli.analysis.options.InterpretationCommandOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.analysis.result.AnalysisResult;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exception.AnalysisException;
import org.opencb.opencga.core.models.Interpretation;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.opencb.opencga.analysis.clinical.interpretation.InterpretationAnalysis.*;


public class InterpretationCommandExecutor extends AnalysisCommandExecutor {

    private final InterpretationCommandOptions interpretationCommandOptions;

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
            case TeamInterpretationAnalysis.ID:
                team();
                break;
            case TieringInterpretationAnalysis.ID:
                tiering();
                break;
            case CancerTieringInterpretationAnalysis.ID:
                cancerTiering();
                break;
            case CustomInterpretationAnalysis.ID:
                custom();
                break;
            default:
                logger.error("Subcommand not valid");
                break;

        }
    }

    private void team() throws Exception {
        InterpretationCommandOptions.TeamCommandOptions options = interpretationCommandOptions.teamCommandOptions;

        String token = options.commonOptions.sessionId;

        String studyId = options.studyId;
        String clinicalAnalysisId = options.clinicalAnalysisId;

        if (StringUtils.isEmpty(options.panelIds)) {
            throw new AnalysisException("Missing panel ids");
        }
        List<String> panelList = Arrays.asList(StringUtils.split(options.panelIds, ","));

        ClinicalProperty.ModeOfInheritance moi;
        try {
            moi = ClinicalProperty.ModeOfInheritance.valueOf(options.familySegregation);
        } catch (IllegalArgumentException e) {
            throw new AnalysisException("Unknown family segregation value: " + options.familySegregation);
        }

        TeamInterpretationConfiguration config = new TeamInterpretationConfiguration();
        config.setIncludeLowCoverage(options.includeLowCoverage);
        config.setMaxLowCoverage(options.maxLowCoverage);
        config.setSkipUntieredVariants(!options.includeUntieredVariants);

        // Initialize monitor
        ExecutorMonitor monitor = new ExecutorMonitor();
        Thread hook = new Thread(() -> {
            monitor.stop(new Status(Status.ERROR));
            logger.info("Running ShutdownHook. Tool execution has being aborted.");
        });

        Path outDir = Paths.get(options.outdirId);
        Path opencgaHome = Paths.get(configuration.getWorkspace()).getParent();

        Runtime.getRuntime().addShutdownHook(hook);
        monitor.start(outDir);

        // Execute TEAM analysis
        TeamInterpretationAnalysis teamAnalysis = new TeamInterpretationAnalysis();
        teamAnalysis.setUp(opencgaHome.toString(), new ObjectMap(), outDir, token);
        teamAnalysis.setStudyId(studyId)
                .setClinicalAnalysisId(clinicalAnalysisId)
                .setDiseasePanelIds(panelList)
                .setMoi(moi)
                .setConfig(config);
        AnalysisResult result = teamAnalysis.start();

        // Read interpretation from the output dir
        org.opencb.biodata.models.clinical.interpretation.Interpretation interpretation = readInterpretation(result, outDir);

        // Store interpretation analysis in DB
        try {
            catalogManager.getInterpretationManager().create(studyId, clinicalAnalysisId, new Interpretation(interpretation),
                    QueryOptions.empty(), token);
        } catch (CatalogException e) {
            throw new AnalysisException("Error saving interpretation into database", e);
        }

        // Stop monitor
        Runtime.getRuntime().removeShutdownHook(hook);
        monitor.stop(new Status(Status.DONE));
    }

    private void tiering() throws Exception {
        InterpretationCommandOptions.TieringCommandOptions options = interpretationCommandOptions.tieringCommandOptions;

        String token = options.commonOptions.sessionId;

        String studyId = options.studyId;
        String clinicalAnalysisId = options.clinicalAnalysisId;

        if (StringUtils.isEmpty(options.panelIds)) {
            throw new AnalysisException("Missing panel ids");
        }
        List<String> panelList = Arrays.asList(StringUtils.split(options.panelIds, ","));

        ClinicalProperty.Penetrance penetrance = options.penetrance;

        TieringInterpretationConfiguration config = new TieringInterpretationConfiguration();
        config.setIncludeLowCoverage(options.includeLowCoverage);
        config.setMaxLowCoverage(options.maxLowCoverage);
        config.setSkipUntieredVariants(!options.includeUntieredVariants);

        // Initialize monitor
        ExecutorMonitor monitor = new ExecutorMonitor();
        Thread hook = new Thread(() -> {
            monitor.stop(new Status(Status.ERROR));
            logger.info("Running ShutdownHook. Tool execution has being aborted.");
        });

        Path outDir = Paths.get(options.outdirId);
        Path opencgaHome = Paths.get(configuration.getWorkspace()).getParent();

        Runtime.getRuntime().addShutdownHook(hook);
        monitor.start(outDir);

        // Execute tiering analysis
        TieringInterpretationAnalysis tieringAnalysis = new TieringInterpretationAnalysis();
        tieringAnalysis.setUp(opencgaHome.toString(), new ObjectMap(), outDir, token);
        tieringAnalysis.setStudyId(studyId)
                .setClinicalAnalysisId(clinicalAnalysisId)
                .setDiseasePanelIds(panelList)
                .setPenetrance(penetrance)
                .setConfig(config);
        AnalysisResult result = tieringAnalysis.start();

        // Read interpretation from the output dir
        org.opencb.biodata.models.clinical.interpretation.Interpretation interpretation = readInterpretation(result, outDir);

        // Store interpretation analysis in DB
        try {
            catalogManager.getInterpretationManager().create(studyId, clinicalAnalysisId, new Interpretation(interpretation),
                    QueryOptions.empty(), token);
        } catch (CatalogException e) {
            throw new AnalysisException("Error saving interpretation into database", e);
        }

        // Stop monitor
        Runtime.getRuntime().removeShutdownHook(hook);
        monitor.stop(new Status(Status.DONE));
    }

    private void cancerTiering() throws Exception {
        InterpretationCommandOptions.CancerTieringCommandOptions options = interpretationCommandOptions.cancerTieringCommandOptions;

        String token = options.commonOptions.sessionId;

        String studyId = options.studyId;
        String clinicalAnalysisId = options.clinicalAnalysisId;

        List<String> variantIdsToDiscard = new ArrayList<>();
        if (StringUtils.isNotEmpty(options.variantIdsToDiscard)) {
            variantIdsToDiscard = Arrays.asList(StringUtils.split(options.variantIdsToDiscard, ","));
        }

        CancerTieringInterpretationConfiguration config = new CancerTieringInterpretationConfiguration();
        config.setSkipUntieredVariants(!options.includeUntieredVariants);

        // Initialize monitor
        ExecutorMonitor monitor = new ExecutorMonitor();
        Thread hook = new Thread(() -> {
            monitor.stop(new Status(Status.ERROR));
            logger.info("Running ShutdownHook. Tool execution has being aborted.");
        });

        Path outDir = Paths.get(options.outdirId);
        Path opencgaHome = Paths.get(configuration.getWorkspace()).getParent();

        Runtime.getRuntime().addShutdownHook(hook);
        monitor.start(outDir);

        // Execute cancer tiering analysis
        CancerTieringInterpretationAnalysis cancerTieringAnalysis = new CancerTieringInterpretationAnalysis();
        cancerTieringAnalysis.setUp(opencgaHome.toString(), new ObjectMap(), outDir, token);
        cancerTieringAnalysis.setStudyId(studyId)
                .setClinicalAnalysisId(clinicalAnalysisId)
                .setVariantIdsToDiscard(variantIdsToDiscard)
                .setConfig(config);
        AnalysisResult result = cancerTieringAnalysis.start();

        // Read interpretation from the output dir
        org.opencb.biodata.models.clinical.interpretation.Interpretation interpretation = readInterpretation(result, outDir);

        // Store interpretation analysis in DB
        try {
            catalogManager.getInterpretationManager().create(studyId, clinicalAnalysisId, new Interpretation(interpretation),
                    QueryOptions.empty(), token);
        } catch (CatalogException e) {
            throw new AnalysisException("Error saving interpretation into database", e);
        }

        // Stop monitor
        Runtime.getRuntime().removeShutdownHook(hook);
        monitor.stop(new Status(Status.DONE));
    }

    private void custom() throws Exception {
        InterpretationCommandOptions.CustomCommandOptions options = interpretationCommandOptions.customCommandOptions;

        String token = options.commonOptions.sessionId;

        String studyId = options.studyId;
        String clinicalAnalysisId = options.clinicalAnalysisId;

        Map<Long, String> studyIds = getStudyIds(token);
        Query query = VariantQueryCommandUtils.parseQuery(options.variantQueryCommandOptions, studyIds, clientConfiguration);
        QueryOptions queryOptions = VariantQueryCommandUtils.parseQueryOptions(options.variantQueryCommandOptions);

        CustomInterpretationConfiguration config = new CustomInterpretationConfiguration();
        config.setIncludeLowCoverage(options.includeLowCoverage);
        config.setMaxLowCoverage(options.maxLowCoverage);
        config.setSkipUntieredVariants(!options.includeUntieredVariants);

        // Initialize monitor
        ExecutorMonitor monitor = new ExecutorMonitor();
        Thread hook = new Thread(() -> {
            monitor.stop(new Status(Status.ERROR));
            logger.info("Running ShutdownHook. Tool execution has being aborted.");
        });

        Path outDir = Paths.get(options.outdirId);
        Path opencgaHome = Paths.get(configuration.getWorkspace()).getParent();

        Runtime.getRuntime().addShutdownHook(hook);
        monitor.start(outDir);

        // Execute custom interpretation analysis
        CustomInterpretationAnalysis customAnalysis = new CustomInterpretationAnalysis();
        customAnalysis.setUp(opencgaHome.toString(), new ObjectMap(), outDir, token);
        customAnalysis.setStudyId(studyId)
                .setClinicalAnalysisId(clinicalAnalysisId)
                .setQuery(query)
                .setQueryOptions(queryOptions)
                .setConfig(config);
        AnalysisResult result = customAnalysis.start();

        // Read interpretation from the output dir
        org.opencb.biodata.models.clinical.interpretation.Interpretation interpretation = readInterpretation(result, outDir);

        // Store interpretation analysis in DB
        try {
            catalogManager.getInterpretationManager().create(studyId, clinicalAnalysisId, new Interpretation(interpretation),
                    QueryOptions.empty(), token);
        } catch (CatalogException e) {
            throw new AnalysisException("Error saving interpretation into database", e);
        }

        // Stop monitor
        Runtime.getRuntime().removeShutdownHook(hook);
        monitor.stop(new Status(Status.DONE));
    }

    private org.opencb.biodata.models.clinical.interpretation.Interpretation readInterpretation(AnalysisResult result,
                                                                                                java.nio.file.Path outDir)
            throws AnalysisException {
        java.io.File file = new java.io.File(outDir + "/" + INTERPRETATION_FILENAME);
        if (file.exists()) {
            return ClinicalUtils.readInterpretation(file.toPath());
        }
        String msg = "Interpretation file not found for " + result.getId() + " analysis";
        if (CollectionUtils.isNotEmpty(result.getEvents())) {
            msg += (": " + StringUtils.join(result.getEvents(), ". "));
        }
        throw new AnalysisException(msg);
    }
}
