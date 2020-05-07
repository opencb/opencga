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

package org.opencb.opencga.app.cli.main.options;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.opencb.opencga.app.cli.main.options.commons.AclCommandOptions;
import org.opencb.opencga.app.cli.main.options.commons.AnnotationCommandOptions;
import org.opencb.opencga.core.models.common.Enums;

import java.util.List;

import static org.opencb.opencga.app.cli.GeneralCliOptions.*;

/**
 * Created by sgallego on 6/14/16.
 */
@Parameters(commandNames = {"cohorts"}, commandDescription = "Cohorts commands")
public class CohortCommandOptions {

    public CreateCommandOptions createCommandOptions;
    public InfoCommandOptions infoCommandOptions;
    public SearchCommandOptions searchCommandOptions;
    public UpdateCommandOptions updateCommandOptions;
    public DeleteCommandOptions deleteCommandOptions;
    public StatsCommandOptions statsCommandOptions;

    public AclCommandOptions.AclsCommandOptions aclsCommandOptions;
    public AclCommandOptions.AclsUpdateCommandOptions aclsUpdateCommandOptions;

    public AnnotationCommandOptions.AnnotationSetsUpdateCommandOptions annotationUpdateCommandOptions;

    public JCommander jCommander;
    public CommonCommandOptions commonCommandOptions;
    public DataModelOptions commonDataModelOptions;
    public NumericOptions commonNumericOptions;

    private AnnotationCommandOptions annotationCommandOptions;

    public CohortCommandOptions(CommonCommandOptions commonCommandOptions, DataModelOptions dataModelOptions, NumericOptions numericOptions,
                                JCommander jCommander) {

        this.commonCommandOptions = commonCommandOptions;
        this.commonDataModelOptions = dataModelOptions;
        this.commonNumericOptions = numericOptions;
        this.jCommander = jCommander;

        this.createCommandOptions = new CreateCommandOptions();
        this.infoCommandOptions = new InfoCommandOptions();
        this.searchCommandOptions = new SearchCommandOptions();
        this.updateCommandOptions = new UpdateCommandOptions();
        this.deleteCommandOptions = new DeleteCommandOptions();
        this.statsCommandOptions = new StatsCommandOptions();

        this.annotationCommandOptions = new AnnotationCommandOptions(commonCommandOptions);
        this.annotationUpdateCommandOptions = this.annotationCommandOptions.getUpdateCommandOptions();

        AclCommandOptions aclCommandOptions = new AclCommandOptions(commonCommandOptions);
        this.aclsCommandOptions = aclCommandOptions.getAclsCommandOptions();
        this.aclsUpdateCommandOptions = aclCommandOptions.getAclsUpdateCommandOptions();
    }

    public class BaseCohortsCommand extends StudyOption {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--cohort"}, description = "Cohort id", required = true, arity = 1)
        public String cohort;

    }

    @Parameters(commandNames = {"create"}, commandDescription = "Create a cohort")
    public class CreateCommandOptions extends StudyOption {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-n", "--name"}, description = "Cohort name.", required = true, arity = 1)
        public String name;

        @Parameter(names = {"--type"}, description = "Cohort type", arity = 1)
        public Enums.CohortType type = Enums.CohortType.COLLECTION;

        @Parameter(names = {"--variable-set"}, description = "Variable set id or name", arity = 1)
        public String variableSetId;

        @Parameter(names = {"-d", "--description"}, description = "cohort description", arity = 1)
        public String description;

        @Parameter(names = {"--samples"}, description = "Comma separated list of sample ids", arity = 1)
        public List<String> sampleIds;

        @Parameter(names = {"--variable"}, description = "Categorical variable name to use to create cohorts, must go together the "
                + "parameter variable-set", arity = 1)
        public String variable;
    }

    @Parameters(commandNames = {"search"}, commandDescription = "Search cohorts")
    public class SearchCommandOptions extends StudyOption {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public DataModelOptions dataModelOptions = commonDataModelOptions;

        @ParametersDelegate
        public NumericOptions numericOptions = commonNumericOptions;

        @Parameter(names = {"--name"}, description = "Comma separated list of file names", arity = 1)
        public String name;

        @Parameter(names = {"--type"}, description = "Cohort type.", arity = 1)
        public String type;

        @Parameter(names = {"--status"}, description = "Status.", arity = 1)
        public String status;

        @Parameter(names = {"--samples"}, description = "Comma separated list of sample ids", arity = 1)
        public String samples;

    }

    @Parameters(commandNames = {"info"}, commandDescription = "Get cohort information")
    public class InfoCommandOptions extends BaseCohortsCommand {

        @ParametersDelegate
        public DataModelOptions dataModelOptions = commonDataModelOptions;

    }

    @Parameters(commandNames = {"update"}, commandDescription = "Update cohort")
    public class UpdateCommandOptions extends BaseCohortsCommand {

        @Parameter(names = {"-n", "--name"}, description = "New cohort name.", arity = 1)
        public String name;

        @Parameter(names = {"--creation-date"}, description = "Creation date", arity = 1)
        public String creationDate;

        @Parameter(names = {"-d", "--description"}, description = "Description", arity = 1)
        public String description;

        @Parameter(names = {"--samples"}, description = "Comma separated values of sampleIds. Will replace all existing sampleIds", arity = 0)
        public List<String> samples;
    }

    @Parameters(commandNames = {"delete"}, commandDescription = "Delete cohort")
    public class DeleteCommandOptions extends BaseCohortsCommand {

    }

    @Parameters(commandNames = {"stats"}, commandDescription = "Cohort stats")
    public class StatsCommandOptions extends StudyOption {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--default"}, description = "Flag to calculate default stats", arity = 0)
        public boolean defaultStats;

        @Parameter(names = {"--creation-year"}, description = "Creation year.", arity = 1)
        public String creationYear;

        @Parameter(names = {"--creation-month"}, description = "Creation month (JANUARY, FEBRUARY...).", arity = 1)
        public String creationMonth;

        @Parameter(names = {"--creation-day"}, description = "Creation day.", arity = 1)
        public String creationDay;

        @Parameter(names = {"--creation-day-of-week"}, description = "Creation day of week (MONDAY, TUESDAY...).", arity = 1)
        public String creationDayOfWeek;

        @Parameter(names = {"--type"}, description = "Cohort type.", arity = 1)
        public String type;

        @Parameter(names = {"--status"}, description = "Status.", arity = 1)
        public String status;

        @Parameter(names = {"--num-samples"}, description = "Number of samples", arity = 1)
        public String numSamples;

        @Parameter(names = {"--release"}, description = "Release.", arity = 1)
        public String release;

        @Parameter(names = {"--annotation"}, description = "Annotation. See documentation to see the options.", arity = 1)
        public String annotation;

        @Parameter(names = {"--field"}, description = "List of fields separated by semicolons, e.g.: studies;type. For nested "
                + "fields use >>, e.g.: studies>>biotype;type;numSamples[0..10]:1.", arity = 1)
        public String field;
    }

}
