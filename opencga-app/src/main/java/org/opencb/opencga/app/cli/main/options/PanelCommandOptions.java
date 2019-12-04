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
import org.opencb.opencga.app.cli.GeneralCliOptions;
import org.opencb.opencga.app.cli.main.options.commons.AclCommandOptions;

/**
 * Created by sgallego on 6/15/16.
 */
@Parameters(commandNames = {"panels"}, commandDescription = "Panel commands")
public class PanelCommandOptions {

//    public CreateCommandOptions createCommandOptions;
    public InfoCommandOptions infoCommandOptions;
    public SearchCommandOptions searchCommandOptions;

    public AclCommandOptions.AclsCommandOptions aclsCommandOptions;
    public AclCommandOptions.AclsUpdateCommandOptions aclsUpdateCommandOptions;

    public JCommander jCommander;
    public GeneralCliOptions.CommonCommandOptions commonCommandOptions;
    public GeneralCliOptions.DataModelOptions commonDataModelOptions;
    public GeneralCliOptions.NumericOptions commonNumericOptions;

    public PanelCommandOptions(GeneralCliOptions.CommonCommandOptions commonCommandOptions,
                               GeneralCliOptions.DataModelOptions dataModelOptions, GeneralCliOptions.NumericOptions numericOptions,
                               JCommander jCommander) {

        this.commonCommandOptions = commonCommandOptions;
        this.commonDataModelOptions = dataModelOptions;
        this.commonNumericOptions = numericOptions;
        this.jCommander = jCommander;

//        this.createCommandOptions = new CreateCommandOptions();
        this.infoCommandOptions = new InfoCommandOptions();
        this.searchCommandOptions = new SearchCommandOptions();

        AclCommandOptions aclCommandOptions = new AclCommandOptions(commonCommandOptions);
        this.aclsCommandOptions = aclCommandOptions.getAclsCommandOptions();
        this.aclsUpdateCommandOptions = aclCommandOptions.getAclsUpdateCommandOptions();
    }

    class BasePanelsCommand extends GeneralCliOptions.StudyOption {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--id"}, description = "Panel id", required = true, arity = 1)
        public String id;
    }

//    @Parameters(commandNames = {"create"}, commandDescription = "Create a panel")
//    public class CreateCommandOptions {
//
//        @ParametersDelegate
//        public GeneralCliOptions.CommonCommandOptions commonOptions = commonOptions;
//
//        @Parameter(names = {"-s","--study-id"}, description = "Study id", required = true, arity = 1)
//        public String studyId;
//
//        @Parameter(names = {"--name"}, description = "Panel name", required = true, arity = 1)
//        public String name;
//
//        @Parameter(names = {"--disease"}, description = "Disease", required = true, arity = 1)
//        public String disease;
//
//        @Parameter(names = {"--description"}, description = "Panel description", required = false, arity = 1)
//        public String description;
//
//        @Parameter(names = {"--genes"}, description = "Genes", required = false, arity = 1)
//        public String genes;
//
//        @Parameter(names = {"--regions"}, description = "Regions", required = false, arity = 1)
//        public String regions;
//
//        @Parameter(names = {"--variants"}, description = "Variants", required = false, arity = 1)
//        public String variants;
//
//    }

    @Parameters(commandNames = {"info"}, commandDescription = "Get panel information")
    public class InfoCommandOptions extends BasePanelsCommand {
        @ParametersDelegate
        public GeneralCliOptions.DataModelOptions dataModelOptions = commonDataModelOptions;
    }

    @Parameters(commandNames = {"search"}, commandDescription = "Search for panels")
    public class SearchCommandOptions extends GeneralCliOptions.StudyOption {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public GeneralCliOptions.DataModelOptions dataModelOptions = commonDataModelOptions;

        @ParametersDelegate
        public GeneralCliOptions.NumericOptions numericOptions = commonNumericOptions;

        @Parameter(names = {"--name"}, description = "name", arity = 1)
        public String name;

        @Parameter(names = {"--phenotypes"}, description = "phenotypes", arity = 1)
        public String phenotypes;

        @Parameter(names = {"--variants"}, description = "variants", arity = 1)
        public String variants;

        @Parameter(names = {"--regions"}, description = "regions", arity = 1)
        public String regions;

        @Parameter(names = {"--genes"}, description = "genes", arity = 1)
        public String genes;

        @Parameter(names = {"--description"}, description = "description", arity = 1)
        public String description;

        @Parameter(names = {"--author"}, description = "author", arity = 1)
        public String author;

        @Parameter(names = {"--tags"}, description = "tags", arity = 1)
        public String tags;

        @Parameter(names = {"--categories"}, description = "categories", arity = 1)
        public String categories;

        @Parameter(names = {"--creationDate"}, description = "creationDate", arity = 1)
        public String creationDate;

        @Parameter(names = {"--release"}, description = "release", arity = 1)
        public String release;

        @Parameter(names = {"--snapshot"}, description = "snapshot", arity = 1)
        public Integer snapshot;
    }

}
