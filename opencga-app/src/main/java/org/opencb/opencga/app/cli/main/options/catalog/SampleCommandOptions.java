/*
 * Copyright 2015-2016 OpenCB
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

package org.opencb.opencga.app.cli.main.options.catalog;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.opencb.opencga.app.cli.main.options.catalog.commons.AclCommandOptions;
import org.opencb.opencga.app.cli.main.options.catalog.commons.AnnotationCommandOptions;

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

    public AclCommandOptions.AclsCommandOptions aclsCommandOptions;
    public AclCommandOptions.AclsCreateCommandOptions aclsCreateCommandOptions;
    public AclCommandOptions.AclsMemberDeleteCommandOptions aclsMemberDeleteCommandOptions;
    public AclCommandOptions.AclsMemberInfoCommandOptions aclsMemberInfoCommandOptions;
    public AclCommandOptions.AclsMemberUpdateCommandOptions aclsMemberUpdateCommandOptions;

    public AnnotationCommandOptions.AnnotationSetsCreateCommandOptions annotationCreateCommandOptions;
    public AnnotationCommandOptions.AnnotationSetsAllInfoCommandOptions annotationAllInfoCommandOptions;
    public AnnotationCommandOptions.AnnotationSetsSearchCommandOptions annotationSearchCommandOptions;
    public AnnotationCommandOptions.AnnotationSetsDeleteCommandOptions annotationDeleteCommandOptions;
    public AnnotationCommandOptions.AnnotationSetsInfoCommandOptions annotationInfoCommandOptions;
    public AnnotationCommandOptions.AnnotationSetsUpdateCommandOptions annotationUpdateCommandOptions;

    public JCommander jCommander;
    public CommonCommandOptions commonCommandOptions;
    public DataModelOptions commonDataModelOptions;
    public NumericOptions commonNumericOptions;

    private AclCommandOptions aclCommandOptions;
    private AnnotationCommandOptions annotationCommandOptions;

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

        this.annotationCommandOptions = new AnnotationCommandOptions(commonCommandOptions);
        this.annotationCreateCommandOptions = this.annotationCommandOptions.getCreateCommandOptions();
        this.annotationAllInfoCommandOptions = this.annotationCommandOptions.getAllInfoCommandOptions();
        this.annotationSearchCommandOptions = this.annotationCommandOptions.getSearchCommandOptions();
        this.annotationDeleteCommandOptions = this.annotationCommandOptions.getDeleteCommandOptions();
        this.annotationInfoCommandOptions = this.annotationCommandOptions.getInfoCommandOptions();
        this.annotationUpdateCommandOptions = this.annotationCommandOptions.getUpdateCommandOptions();

        aclCommandOptions = new AclCommandOptions(commonCommandOptions);
        this.aclsCommandOptions = aclCommandOptions.getAclsCommandOptions();
        this.aclsCreateCommandOptions = aclCommandOptions.getAclsCreateCommandOptions();
        this.aclsMemberDeleteCommandOptions = aclCommandOptions.getAclsMemberDeleteCommandOptions();
        this.aclsMemberInfoCommandOptions = aclCommandOptions.getAclsMemberInfoCommandOptions();
        this.aclsMemberUpdateCommandOptions = aclCommandOptions.getAclsMemberUpdateCommandOptions();
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
    }

    @Parameters(commandNames = {"create"}, commandDescription = "Create a sample")
    public class CreateCommandOptions extends StudyOption {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-n", "--name"}, description = "Name for the sample, this must be unique in the study", required = true, arity = 1)
        public String name;

        @Parameter(names = {"--source"}, description = "Source from which this sample is created such as VCF file", arity = 1)
        public String source;

        @Parameter(names = {"-d", "--description"}, description = "Description of the sample", arity = 1)
        public String description;

    }

    @Parameters(commandNames = {"load"}, commandDescription = "Load samples from a pedigree file")
    public class LoadCommandOptions extends StudyOption {

        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--ped-file"}, description = "Pedigree file id already loaded in OpenCGA", required = true, arity = 1)
        public String pedFile;

        @Parameter(names = {"--variable-set-id"}, description = "VariableSetId that represents the pedigree file", arity = 1)
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

        @Parameter(names = {"-a", "--annotation"}, description = "SampleAnnotations values. <variableName>:<annotationValue>(,<annotationValue>)*", arity = 1)
        public String annotation;

        @Parameter(names = {"--annotation-set-name"}, description = "Annotation set name.", required = false, arity = 1)
        public String annotationSetName;

        @Parameter(names = {"--variable-set-id"}, description = "Variable set id.", required = false, arity = 1)
        public String variableSetId;
    }


    @Parameters(commandNames = {"update"}, commandDescription = "Update cohort")
    public class UpdateCommandOptions extends BaseSampleCommand {

        @Parameter(names = {"-n", "--name"}, description = "Cohort set name.", required = false, arity = 1)
        public String name;

        @Parameter(names = {"--individual"}, description = "Individual id or name", required = false, arity = 1)
        public String individual;

        @Parameter(names = {"--source"}, description = "Source", required = false, arity = 1)
        public String source;

        @Parameter(names = {"-d", "--description"}, description = "Description", required = false, arity = 1)
        public String description;

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

        @Parameter(names = {"--annotation"}, description = "Annotation", required = false, arity = 1)
        public String annotation;

        @Parameter(names = {"--annotation-set-name"}, description = "Annotation set name.", required = false, arity = 0)
        public String annotationSetName;

        @Parameter(names = {"--variable-set-id"}, description = "Variable set ids", required = false, arity = 1)
        public String variableSetId;
    }

}
