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
import org.opencb.opencga.catalog.utils.ParamUtils;

import static org.opencb.opencga.app.cli.GeneralCliOptions.*;

/**
 * Created by sgallego on 6/14/16.
 */
@Parameters(commandNames = {"samples"}, commandDescription = "Samples commands")
public class SampleCommandOptions {

    public CreateCommandOptions createCommandOptions;
    public LoadCommandOptions loadCommandOptions;
    public InfoCommandOptions infoCommandOptions;
    public SearchCommandOptions searchCommandOptions;
    public UpdateCommandOptions updateCommandOptions;
    public DeleteCommandOptions deleteCommandOptions;
    public GroupByCommandOptions groupByCommandOptions;
    public IndividualCommandOptions individualCommandOptions;
    public StatsCommandOptions statsCommandOptions;

    public AclCommandOptions.AclsCommandOptions aclsCommandOptions;
    public SampleAclCommandOptions.AclsUpdateCommandOptions aclsUpdateCommandOptions;

    public AnnotationCommandOptions.AnnotationSetsCreateCommandOptions annotationCreateCommandOptions;
    public AnnotationCommandOptions.AnnotationSetsSearchCommandOptions annotationSearchCommandOptions;
    public AnnotationCommandOptions.AnnotationSetsDeleteCommandOptions annotationDeleteCommandOptions;
    public AnnotationCommandOptions.AnnotationSetsInfoCommandOptions annotationInfoCommandOptions;
    public AnnotationCommandOptions.AnnotationSetsUpdateCommandOptions annotationUpdateCommandOptions;

    public JCommander jCommander;
    public CommonCommandOptions commonCommandOptions;
    public DataModelOptions commonDataModelOptions;
    public NumericOptions commonNumericOptions;

    protected static final String DEPRECATED = "[DEPRECATED] ";

    public SampleCommandOptions(CommonCommandOptions commonCommandOptions, DataModelOptions dataModelOptions, NumericOptions numericOptions,
                                JCommander jCommander) {

        this.commonCommandOptions = commonCommandOptions;
        this.commonDataModelOptions = dataModelOptions;
        this.commonNumericOptions = numericOptions;
        this.jCommander = jCommander;

        this.createCommandOptions = new CreateCommandOptions();
        this.loadCommandOptions = new LoadCommandOptions();
        this.infoCommandOptions = new InfoCommandOptions();
        this.searchCommandOptions = new SearchCommandOptions();
        this.updateCommandOptions = new UpdateCommandOptions();
        this.deleteCommandOptions = new DeleteCommandOptions();
        this.groupByCommandOptions = new GroupByCommandOptions();
        this.individualCommandOptions = new IndividualCommandOptions();
        this.statsCommandOptions = new StatsCommandOptions();

        AnnotationCommandOptions annotationCommandOptions = new AnnotationCommandOptions(commonCommandOptions);
        this.annotationCreateCommandOptions = annotationCommandOptions.getCreateCommandOptions();
        this.annotationSearchCommandOptions = annotationCommandOptions.getSearchCommandOptions();
        this.annotationDeleteCommandOptions = annotationCommandOptions.getDeleteCommandOptions();
        this.annotationInfoCommandOptions = annotationCommandOptions.getInfoCommandOptions();
        this.annotationUpdateCommandOptions = annotationCommandOptions.getUpdateCommandOptions();

        SampleAclCommandOptions aclCommandOptions = new SampleAclCommandOptions(commonCommandOptions);
        this.aclsCommandOptions = aclCommandOptions.getAclsCommandOptions();
        this.aclsUpdateCommandOptions = aclCommandOptions.getAclsUpdateCommandOptions();
    }

    public class BaseSampleCommand extends StudyOption {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--sample"}, description = "Sample id or name", required = true, arity = 1)
        public String sample;

    }

    @Parameters(commandNames = {"info"}, commandDescription = "Get samples information")
    public class InfoCommandOptions extends BaseSampleCommand {

        @ParametersDelegate
        public DataModelOptions dataModelOptions = commonDataModelOptions;

        @Parameter(names = {"--no-lazy"}, description = "Obtain the entire related job and experiment objects", arity = 0)
        public boolean noLazy;

        @Parameter(names = {"--flatten-annotations"}, description = "Flag indicating whether nested annotations should be returned flattened",
                arity = 0)
        public boolean flattenAnnotations;
    }

    @Parameters(commandNames = {"create"}, commandDescription = "Create a sample")
    public class CreateCommandOptions extends StudyOption {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--id"}, description = "Sample id", required = true, arity = 1)
        public String id;

        @Parameter(names = {"--json"}, description = "JSON file containing the rest of the sample fields", arity = 1)
        public String json;

        @Parameter(names = {"-n", "--name"}, description = DEPRECATED + "Use --json instead.", arity = 1)
        public String name;

        @Parameter(names = {"--source"}, description = DEPRECATED + "Use --json instead.", arity = 1)
        public String source;

        @Parameter(names = {"-d", "--description"}, description = DEPRECATED + "Use --json instead.", arity = 1)
        public String description;

        @Parameter(names = {"--individual"}, description = DEPRECATED + "Use --json instead.", arity = 1)
        public String individual;

        @Parameter(names = {"--somatic"}, description = DEPRECATED + "Use --json instead.", arity = 0)
        public boolean somatic;

        @Parameter(names = {"--type"}, description = DEPRECATED + "Use --json instead.", arity = 1)
        public String type;
    }

    @Parameters(commandNames = {"load"}, commandDescription = "Load samples from a pedigree file")
    public class LoadCommandOptions extends StudyOption {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--ped-file"}, description = "Pedigree file id already loaded in OpenCGA", required = true, arity = 1)
        public String pedFile;

        @Parameter(names = {"--variable-set"}, description = "VariableSetId that represents the pedigree file", arity = 1)
        public String variableSetId;

    }

    @Parameters(commandNames = {"search"}, commandDescription = "Search samples")
    public class SearchCommandOptions extends StudyOption {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public DataModelOptions dataModelOptions = commonDataModelOptions;

        @ParametersDelegate
        public NumericOptions numericOptions = commonNumericOptions;

        @Parameter(names = {"-n", "--name"}, description = "List of id or names separated by commas", required = false, arity = 1)
        public String name;

        @Parameter(names = {"--source"}, description = "Filter by the sample source such as the VCF file name from which this sample was created", arity = 1)
        public String source;

        @Parameter(names = {"--individual"}, description = "Filter by id or name of the individual", required = false, arity = 1)
        public String individual;

        @Parameter(names = {"--type"}, description = "Sample type", arity = 1)
        public String type;

        @Parameter(names = {"--somatic"}, description = "Flag indicating if the sample comes from somatic cells", arity = 1)
        public Boolean somatic;

        @Parameter(names = {"--annotation"}, description = "Annotation filters. Example: age>30;gender=FEMALE. For more information, " +
                "please visit http://docs.opencb.org/display/opencga/AnnotationSets+1.4.0", arity = 1)
        public String annotation;

        @Parameter(names = {"--flatten-annotations"}, description = "Flag indicating whether nested annotations should be returned flattened",
                arity = 0)
        public boolean flattenAnnotations;
    }


    @Parameters(commandNames = {"update"}, commandDescription = "Update sample")
    public class UpdateCommandOptions extends BaseSampleCommand {

        @Parameter(names = {"--json"}, description = "JSON file containing the sample fields to be updated", arity = 1)
        public String json;

        @Parameter(names = {"-n", "--name"}, description = DEPRECATED + "Use --json instead.", required = false, arity = 1)
        public String name;

        @Parameter(names = {"--individual"}, description = DEPRECATED + "Use --json instead.", required = false, arity = 1)
        public String individual;

        @Parameter(names = {"--source"}, description = DEPRECATED + "Use --json instead.", required = false, arity = 1)
        public String source;

        @Parameter(names = {"-d", "--description"}, description = DEPRECATED + "Use --json instead.", required = false, arity = 1)
        public String description;

        @Parameter(names = {"--somatic"}, description = DEPRECATED + "Use --json instead.", arity = 1)
        public Boolean somatic;

        @Parameter(names = {"--type"}, description = DEPRECATED + "Use --json instead.", arity = 1)
        public String type;

        @Parameter(names = {"--annotation-sets-action"}, description = "Action to be performed if the array of annotationSets is being updated. (ADD, SET, REMOVE)",
                arity = 1)
        public ParamUtils.UpdateAction annotationSetsAction;
    }

    @Parameters(commandNames = {"delete"}, commandDescription = "Delete the selected sample")
    public class DeleteCommandOptions extends BaseSampleCommand {

    }

    @Parameters(commandNames = {"group-by"}, commandDescription = "Group samples")
    public class GroupByCommandOptions extends StudyOption {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @Deprecated
        @Parameter(names = {"--ids"}, description = "[DEPRECATED] Comma separated list of ids.", required = false, arity = 1)
        public String id;

        @Parameter(names = {"-f", "--fields"}, description = "Comma separated list of fields by which to group by.", required = true, arity = 1)
        public String fields;

        @Parameter(names = {"-n", "--name"}, description = "Comma separated list of names.", required = false, arity = 0)
        public String name;

        @Parameter(names = {"--source"}, description = "Source.", required = false, arity = 0)
        public String source;

        @Parameter(names = {"--individual"}, description = "Individual id or name", required = false, arity = 0)
        public String individual;

        @Parameter(names = {"-a", "--annotation"}, description = "SampleAnnotations values. <variableName>:<annotationValue>(,<annotationValue>)*", arity = 1)
        public String annotation;
    }

    @Parameters(commandNames = {"individuals"}, commandDescription = "Get the individuals of a list of samples.")
    public class IndividualCommandOptions extends StudyOption {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public DataModelOptions dataModelOptions = commonDataModelOptions;

        @Parameter(names = {"--sample"}, description = "List of sample ids or aliases", required = true, arity = 1)
        public String sample;
    }

    public class SampleAclCommandOptions extends AclCommandOptions {

        private AclsUpdateCommandOptions aclsUpdateCommandOptions;

        public SampleAclCommandOptions(CommonCommandOptions commonCommandOptions) {
            super(commonCommandOptions);
        }

        @Parameters(commandNames = {"acl-update"}, commandDescription = "Update the permissions set for a member")
        public class AclsUpdateCommandOptions extends AclCommandOptions.AclsUpdateCommandOptions {

//            @Parameter(names = {"--sample"}, description = "Comma separated list of sample ids or names", arity = 1)
//            public String sample;

            @Parameter(names = {"--file"}, description = "Comma separated list of file ids, names or paths or file containing the list of "
                    + "ids (one per line)", arity = 1)
            public String file;

            @Parameter(names = {"--individual"}, description = "Comma separated list of individual ids or file containing the list of ids "
                    + "(one per line)", arity = 1)
            public String individual;

            @Parameter(names = {"--cohort"}, description = "Comma separated list of cohort ids or file containing the list of ids "
                    + "(one per line)", arity = 1)
            public String cohort;

            @Parameter(names = {"--propagate"}, description = "Flag parameter indicating whether to propagate the permissions to the " +
                    "individuals related to the sample(s).", arity = 0)
            public boolean propagate;
        }

        public AclsUpdateCommandOptions getAclsUpdateCommandOptions() {
            if (this.aclsUpdateCommandOptions == null) {
                this.aclsUpdateCommandOptions = new AclsUpdateCommandOptions();
            }
            return aclsUpdateCommandOptions;
        }
    }

    @Parameters(commandNames = {"stats"}, commandDescription = "Sample stats")
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

        @Parameter(names = {"--type"}, description = "Type.", arity = 1)
        public String type;

        @Parameter(names = {"--status"}, description = "Status.", arity = 1)
        public String status;

        @Parameter(names = {"--source"}, description = "Source", arity = 1)
        public String source;

        @Parameter(names = {"--release"}, description = "Release.", arity = 1)
        public String release;

        @Parameter(names = {"--version"}, description = "Version.", arity = 1)
        public String version;

        @Parameter(names = {"--phenotypes"}, description = "Phenotypes.", arity = 1)
        public String phenotypes;

        @Parameter(names = {"--somatic"}, description = "Somatic.", arity = 1)
        public Boolean somatic;

        @Parameter(names = {"--annotation"}, description = "Annotation. See documentation to see the options.", arity = 1)
        public String annotation;

        @Parameter(names = {"--field"}, description = "List of fields separated by semicolons, e.g.: studies;type. For nested "
                + "fields use >>, e.g.: studies>>biotype;type;numSamples[0..10]:1.", arity = 1)
        public String field;
    }

}
