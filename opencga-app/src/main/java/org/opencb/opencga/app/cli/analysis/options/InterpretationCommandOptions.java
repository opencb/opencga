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

package org.opencb.opencga.app.cli.analysis.options;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.opencb.biodata.models.clinical.interpretation.ClinicalProperty;
import org.opencb.opencga.app.cli.GeneralCliOptions;
import org.opencb.opencga.analysis.variant.VariantCatalogQueryUtils;

@Parameters(commandNames = {"interpretation"}, commandDescription = "Implement several interpretation analysis")
public class InterpretationCommandOptions {

    public TeamCommandOptions teamCommandOptions;
    public TieringCommandOptions tieringCommandOptions;

    public GeneralCliOptions.CommonCommandOptions analysisCommonOptions;
    public JCommander jCommander;

    public InterpretationCommandOptions(GeneralCliOptions.CommonCommandOptions analysisCommonCommandOptions, JCommander jCommander) {
        this.analysisCommonOptions = analysisCommonCommandOptions;
        this.jCommander = jCommander;

        this.teamCommandOptions = new TeamCommandOptions();
        this.tieringCommandOptions = new TieringCommandOptions();
    }

    @Parameters(commandNames = {"team"}, commandDescription = "Team interpretation analysis")
    public class TeamCommandOptions {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = analysisCommonOptions;

        @Parameter(names = {"-j", "--job"}, description = "Job id containing the team interpretation parameters", arity = 1)
        public String job;


        @Parameter(names = {"--include-low-coverage"}, description = "Include low coverage regions", arity = 1)
        public boolean includeLowCoverage;

        @Parameter(names = {"--max-low-coverage"}, description = "Max. low coverage", arity = 1)
        public int maxLowCoverage;

        @Parameter(names = {"--include-no-tier"}, description = "Reported variants without tier", arity = 1)
        public boolean includeNoTier;

        @Parameter(names = {"--clinical-analysis-id"}, description = "Clinical Analysis ID", arity = 1)
        public String clinicalAnalysisId;

        @Parameter(names = {"--panel-ids"}, description = "Comma separated list of disease panel IDs", arity = 1)
        public String panelIds;

        @Parameter(names = {"--family-segregation"}, description = VariantCatalogQueryUtils.FAMILY_SEGREGATION_DESCR, arity = 1)
        public String segregation;


        @Parameter(names = {"-s", "--study"}, description = "Study [[user@]project:]study.", required = true, arity = 1)
        public String study;

        @Parameter(names = {"-o", "--outdir"}, description = "Directory where output files will be saved", required = true, arity = 1)
        public String outdirId;
    }

    @Parameters(commandNames = {"tiering"}, commandDescription = "Tiering interpretation analysis")
    public class TieringCommandOptions {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = analysisCommonOptions;

        @Parameter(names = {"-j", "--job"}, description = "Job id containing the team interpretation parameters", arity = 1)
        public String job;


        @Parameter(names = {"--include-low-coverage"}, description = "Include low coverage regions", arity = 1)
        public boolean includeLowCoverage;

        @Parameter(names = {"--max-low-coverage"}, description = "Max. low coverage", arity = 1)
        public int maxLowCoverage;

        @Parameter(names = {"--include-no-tier"}, description = "Reported variants without tier", arity = 1)
        public boolean includeNoTier;

        @Parameter(names = {"--clinical-analysis-id"}, description = "Clinical Analysis ID", arity = 1)
        public String clinicalAnalysisId;

        @Parameter(names = {"--panel-ids"}, description = "Comma separated list of disease panel IDs", arity = 1)
        public String panelIds;

        @Parameter(names = {"--penetrance"}, description = "Penetrance. Accepted values: COMPLETE, INCOMPLETE", arity = 1)
        public ClinicalProperty.Penetrance penetrance = ClinicalProperty.Penetrance.COMPLETE;

        @Parameter(names = {"-s", "--study"}, description = "Study [[user@]project:]study.", required = true, arity = 1)
        public String study;

        @Parameter(names = {"-o", "--outdir"}, description = "Directory where output files will be saved", required = true, arity = 1)
        public String outdirId;
    }
}
