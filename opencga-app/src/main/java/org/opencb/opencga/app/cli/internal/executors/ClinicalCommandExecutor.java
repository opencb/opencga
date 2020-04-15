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

import org.apache.commons.collections.CollectionUtils;
import org.opencb.biodata.models.clinical.interpretation.ClinicalProperty;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.analysis.clinical.tiering.TieringInterpretationAnalysis;
import org.opencb.opencga.analysis.clinical.tiering.TieringInterpretationConfiguration;
import org.opencb.opencga.app.cli.internal.options.ClinicalCommandOptions;
import org.opencb.opencga.core.exceptions.ToolException;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

import static org.opencb.opencga.app.cli.main.options.ClinicalCommandOptions.InterpretationTieringCommandOptions.TIERING_RUN_COMMAND;

/**
 * Created on 01/04/20
 *
 * @author Joaquin Tarraga Gimenez &lt;joaquintarraga@gmail.com&gt;
 */
public class ClinicalCommandExecutor extends InternalCommandExecutor {

    private final ClinicalCommandOptions clinicalCommandOptions;

    public ClinicalCommandExecutor(ClinicalCommandOptions options) {
        super(options.commonCommandOptions);
        clinicalCommandOptions = options;
    }

    @Override
    public void execute() throws Exception {
        logger.debug("Executing clinical command line");

        String subCommandString = getParsedSubCommand(clinicalCommandOptions.jCommander);
        configure();
        switch (subCommandString) {
            case TIERING_RUN_COMMAND:
                tiering();
                break;
            default:
                logger.error("Subcommand not valid");
                break;

        }
    }


    private void tiering() throws Exception {
        ClinicalCommandOptions.TieringCommandOptions cliOptions = clinicalCommandOptions.tieringCommandOptions;
        ObjectMap params = new ObjectMap();
        params.putAll(cliOptions.commonOptions.params);

        // Prepare analysis parameters and config
        String token = cliOptions.commonOptions.token;

        String studyId = cliOptions.study;
        String clinicalAnalysisId = cliOptions.clinicalAnalysis;

        if (CollectionUtils.isEmpty(cliOptions.panels)) {
            throw new ToolException("Missing disease panel IDs");
        }
//        List<String> panelList = Arrays.asList(StringUtils.split(options.panelIds, ","));

        ClinicalProperty.Penetrance penetrance = cliOptions.penetrance;

        TieringInterpretationConfiguration config = new TieringInterpretationConfiguration();
        config.setIncludeLowCoverage(cliOptions.includeLowCoverage);
        config.setMaxLowCoverage(cliOptions.maxLowCoverage);
//        config.setSkipUntieredVariants(!cliOptions.includeUntieredVariants);

        Path outDir = Paths.get(cliOptions.outdir);
        Path opencgaHome = Paths.get(configuration.getWorkspace()).getParent();

        // Execute tiering analysis
        TieringInterpretationAnalysis tieringAnalysis = new TieringInterpretationAnalysis();
        tieringAnalysis.setUp(opencgaHome.toString(), new ObjectMap(), outDir, token);
        tieringAnalysis.setStudyId(studyId)
                .setClinicalAnalysisId(clinicalAnalysisId)
                .setDiseasePanelIds(cliOptions.panels)
                .setPenetrance(penetrance)
                .setConfig(config);
        tieringAnalysis.start();
    }
}
