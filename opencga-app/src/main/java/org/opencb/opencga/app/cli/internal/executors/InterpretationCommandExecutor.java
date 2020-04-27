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

package org.opencb.opencga.app.cli.internal.executors;

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.interpretation.ClinicalProperty;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.clinical.zetta.ZettaInterpretationAnalysis;
import org.opencb.opencga.analysis.clinical.zetta.ZettaInterpretationConfiguration;
import org.opencb.opencga.analysis.clinical.team.TeamInterpretationAnalysis;
import org.opencb.opencga.analysis.clinical.team.TeamInterpretationConfiguration;
import org.opencb.opencga.analysis.clinical.tiering.CancerTieringInterpretationAnalysis;
import org.opencb.opencga.analysis.clinical.tiering.CancerTieringInterpretationConfiguration;
import org.opencb.opencga.app.cli.internal.options.InterpretationCommandOptions;
import org.opencb.opencga.core.exceptions.ToolException;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;


public class InterpretationCommandExecutor extends InternalCommandExecutor {

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
            case TeamInterpretationAnalysis.ID + "-run":
                team();
                break;
//            case TieringInterpretationAnalysis.ID + "-run":
//                tiering();
//                break;
            case CancerTieringInterpretationAnalysis.ID + "-run":
                cancerTiering();
                break;
            case ZettaInterpretationAnalysis.ID + "-run":
                custom();
                break;
            default:
                logger.error("Subcommand not valid");
                break;

        }
    }

    private void team() throws Exception {
        InterpretationCommandOptions.TeamCommandOptions options = interpretationCommandOptions.teamCommandOptions;

        // Prepare analysis parameters and config
        String token = options.commonOptions.token;

        String studyId = options.studyId;
        String clinicalAnalysisId = options.clinicalAnalysisId;

        if (StringUtils.isEmpty(options.panelIds)) {
            throw new ToolException("Missing panel ids");
        }
        List<String> panelList = Arrays.asList(StringUtils.split(options.panelIds, ","));

        ClinicalProperty.ModeOfInheritance moi;
        try {
            moi = ClinicalProperty.ModeOfInheritance.valueOf(options.familySegregation);
        } catch (IllegalArgumentException e) {
            throw new ToolException("Unknown family segregation value: " + options.familySegregation);
        }

        TeamInterpretationConfiguration config = new TeamInterpretationConfiguration();
        config.setIncludeLowCoverage(options.includeLowCoverage);
        config.setMaxLowCoverage(options.maxLowCoverage);
        config.setSkipUntieredVariants(!options.includeUntieredVariants);

        Path outDir = Paths.get(options.outDir);
        Path opencgaHome = Paths.get(configuration.getWorkspace()).getParent();

        // Execute TEAM analysis
        TeamInterpretationAnalysis teamAnalysis = new TeamInterpretationAnalysis();
        teamAnalysis.setUp(opencgaHome.toString(), new ObjectMap(), outDir, token);
        teamAnalysis.setStudyId(studyId)
                .setClinicalAnalysisId(clinicalAnalysisId)
                .setDiseasePanelIds(panelList)
                .setMoi(moi)
                .setConfig(config);
        teamAnalysis.start();
    }

//    private void tiering() throws Exception {
//        InterpretationCommandOptions.TieringCommandOptions options = interpretationCommandOptions.tieringCommandOptions;
//
//        // Prepare analysis parameters and config
//        String token = options.commonOptions.token;
//
//        String studyId = options.studyId;
//        String clinicalAnalysisId = options.clinicalAnalysisId;
//
//        if (StringUtils.isEmpty(options.panelIds)) {
//            throw new ToolException("Missing panel ids");
//        }
//        List<String> panelList = Arrays.asList(StringUtils.split(options.panelIds, ","));
//
//        ClinicalProperty.Penetrance penetrance = options.penetrance;
//
//        TieringInterpretationConfiguration config = new TieringInterpretationConfiguration();
//        config.setIncludeLowCoverage(options.includeLowCoverage);
//        config.setMaxLowCoverage(options.maxLowCoverage);
//        config.setSkipUntieredVariants(!options.includeUntieredVariants);
//
//        Path outDir = Paths.get(options.outDir);
//        Path opencgaHome = Paths.get(configuration.getWorkspace()).getParent();
//
//        // Execute tiering analysis
//        TieringInterpretationAnalysis tieringAnalysis = new TieringInterpretationAnalysis();
//        tieringAnalysis.setUp(opencgaHome.toString(), new ObjectMap(), outDir, token);
//        tieringAnalysis.setStudyId(studyId)
//                .setClinicalAnalysis(clinicalAnalysisId)
//                .setDiseasePanelIds(panelList)
//                .setPenetrance(penetrance)
//                .setConfig(config);
//        tieringAnalysis.start();
//    }

    private void cancerTiering() throws Exception {
        InterpretationCommandOptions.CancerTieringCommandOptions options = interpretationCommandOptions.cancerTieringCommandOptions;

        // Prepare analysis parameters and config
        String token = options.commonOptions.token;

        String studyId = options.studyId;
        String clinicalAnalysisId = options.clinicalAnalysisId;

        List<String> variantIdsToDiscard = new ArrayList<>();
        if (StringUtils.isNotEmpty(options.variantIdsToDiscard)) {
            variantIdsToDiscard = Arrays.asList(StringUtils.split(options.variantIdsToDiscard, ","));
        }

        CancerTieringInterpretationConfiguration config = new CancerTieringInterpretationConfiguration();
        config.setSkipUntieredVariants(!options.includeUntieredVariants);

        Path outDir = Paths.get(options.outDir);
        Path opencgaHome = Paths.get(configuration.getWorkspace()).getParent();

        // Execute cancer tiering analysis
        CancerTieringInterpretationAnalysis cancerTieringAnalysis = new CancerTieringInterpretationAnalysis();
        cancerTieringAnalysis.setUp(opencgaHome.toString(), new ObjectMap(), outDir, token);
        cancerTieringAnalysis.setStudyId(studyId)
                .setClinicalAnalysisId(clinicalAnalysisId)
                .setVariantIdsToDiscard(variantIdsToDiscard)
                .setConfig(config);
        cancerTieringAnalysis.start();
    }

    private void custom() throws Exception {
        InterpretationCommandOptions.ZettaInterpretationCommandOptions options = interpretationCommandOptions.zettaInterpretationCommandOptions;

        // Prepare analysis parameters and config
        String token = options.commonOptions.token;

        String studyId = options.studyId;
        String clinicalAnalysisId = options.clinicalAnalysisId;

        Map<Long, String> studyIds = getStudyIds(token);
        Query query = VariantQueryCommandUtils.parseQuery(options.variantQueryCommandOptions, studyIds, clientConfiguration);
        QueryOptions queryOptions = VariantQueryCommandUtils.parseQueryOptions(options.variantQueryCommandOptions);

        ZettaInterpretationConfiguration config = new ZettaInterpretationConfiguration();
        config.setIncludeLowCoverage(options.includeLowCoverage);
        config.setMaxLowCoverage(options.maxLowCoverage);
        config.setSkipUntieredVariants(!options.includeUntieredVariants);

        Path outDir = Paths.get(options.outDir);
        Path opencgaHome = Paths.get(configuration.getWorkspace()).getParent();

        // Execute custom interpretation analysis
        ZettaInterpretationAnalysis customAnalysis = new ZettaInterpretationAnalysis();
        customAnalysis.setUp(opencgaHome.toString(), new ObjectMap(), outDir, token);
        customAnalysis.setStudyId(studyId)
                .setClinicalAnalysisId(clinicalAnalysisId)
                .setQuery(query)
                .setQueryOptions(queryOptions)
                .setConfig(config);
        customAnalysis.start();
    }
}
