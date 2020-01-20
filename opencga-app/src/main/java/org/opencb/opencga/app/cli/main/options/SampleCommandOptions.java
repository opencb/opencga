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
    public StatsCommandOptions statsCommandOptions;

    public AclCommandOptions.AclsCommandOptions aclsCommandOptions;
    public SampleAclCommandOptions.AclsUpdateCommandOptions aclsUpdateCommandOptions;

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
        this.statsCommandOptions = new StatsCommandOptions();

        AnnotationCommandOptions annotationCommandOptions = new AnnotationCommandOptions(commonCommandOptions);
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

        @Parameter(names = {"-n", "--name"}, description = "Sample name.", arity = 1)
        public String name;

        @Parameter(names = {"--source"}, description = "Source from which this sample is created such as VCF file", arity = 1)
        public String source;

        @Parameter(names = {"-d", "--description"}, description = "Description of the sample", arity = 1)
        public String description;

        @Parameter(names = {"--individual"}, description = "Individual name or id to whom the sample belongs to", arity = 1)
        public String individual;

        @Parameter(names = {"--somatic"}, description = "Flag indicating that the sample comes from somatic cells", arity = 0)
        public boolean somatic;

        @Parameter(names = {"--type"}, description = "Sample type", arity = 1)
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

        public static final String ANNOTATION_DOC_URL = "http://docs.opencb.org/display/opencga/AnnotationSets+1.4.0";
        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public DataModelOptions dataModelOptions = commonDataModelOptions;

        @ParametersDelegate
        public NumericOptions numericOptions = commonNumericOptions;

        @Parameter(names = {"-n", "--name"}, description = "List of id or names separated by commas", arity = 1)
        public String name;

        @Parameter(names = {"--source"}, description = "Filter by the sample source such as the VCF file name from which this sample was created", arity = 1)
        public String source;

        @Parameter(names = {"--individual"}, description = "Filter by id or name of the individual", arity = 1)
        public String individual;

        @Parameter(names = {"--type"}, description = "Sample type", arity = 1)
        public String type;

        @Parameter(names = {"--somatic"}, description = "Flag indicating if the sample comes from somatic cells", arity = 1)
        public Boolean somatic;

        @Parameter(names = {"--annotation"}, description = "Annotation filters. Example: age>30;gender=FEMALE. For more information, " +
                "please visit " + ANNOTATION_DOC_URL, arity = 1)
        public String annotation;

        @Parameter(names = {"--flatten-annotations"}, description = "Flag indicating whether nested annotations should be returned flattened",
                arity = 0)
        public boolean flattenAnnotations;
    }


    @Parameters(commandNames = {"update"}, commandDescription = "Update sample")
    public class UpdateCommandOptions extends BaseSampleCommand {

        @Parameter(names = {"--id"}, description = "New sample id.", arity = 1)
        public String id;

        @Parameter(names = {"--individual"}, description = "Individual id or name", arity = 1)
        public String individual;

        @Parameter(names = {"--source"}, description = "Source", arity = 1)
        public String source;

        @Parameter(names = {"-d", "--description"}, description = "Description", arity = 1)
        public String description;

        @Parameter(names = {"--somatic"}, description = "Boolean indicating whether the sample comes from somatic cells or not", arity = 1)
        public Boolean somatic;

        @Parameter(names = {"--type"}, description = "Sample type", arity = 1)
        public String type;
    }

    @Parameters(commandNames = {"delete"}, commandDescription = "Delete the selected sample")
    public class DeleteCommandOptions extends BaseSampleCommand {

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
