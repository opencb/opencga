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

package org.opencb.opencga.app.cli.main.options.commons;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.opencb.opencga.app.cli.GeneralCliOptions;

/**
 * Created by pfurio on 27/07/16.
 */
public class AnnotationCommandOptions {
    protected static final String DEPRECATED = "[DEPRECATED] ";

    private GeneralCliOptions.CommonCommandOptions commonCommandOptions;

    private AnnotationSetsUpdateCommandOptions updateCommandOptions;

    public AnnotationCommandOptions(GeneralCliOptions.CommonCommandOptions commonCommandOptions) {
        this.commonCommandOptions = commonCommandOptions;
    }

    public class BaseCommandOptions extends GeneralCliOptions.StudyListOption {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--id"}, description = "Id of the resource", required = true, arity = 1)
        public String id;
    }

    @Parameters(commandNames = {"annotation-sets-update"}, commandDescription = "Update the value of some annotations")
    public class AnnotationSetsUpdateCommandOptions extends BaseCommandOptions {

        public String annotationSetId;

        @Parameter(names = {"--annotation-set-name"}, description = DEPRECATED + "Use --annotation-set", arity = 1)
        public void setAnnotationSetId(String value) {
            this.annotationSetId = value;
        }

        @Parameter(names = {"--annotation-set"}, description = "Annotation set id", required = true, arity = 1)
        public void setAnnotationSet(String annotationSetName) {
            this.annotationSetId = annotationSetName;
        }

        @Parameter(names = {"--annotations"}, description = "Json containing the map of annotations to be updated", required = true,
                arity = 1)
        public String annotations;

//        @Parameter(names = {"--action"}, description = "Action to be performed: ADD to add new annotations; REPLACE to replace the value "
//                + "of an already existing annotation; SET to set the new list of annotations removing any possible old annotations; REMOVE "
//                + "to remove some annotations; RESET to set some annotations to the default value configured in the corresponding "
//                + "variables of the VariableSet if any.", arity = 1)
//        public ParamUtils.CompleteUpdateAction action;
    }

    public AnnotationSetsUpdateCommandOptions getUpdateCommandOptions() {
        if (this.updateCommandOptions == null) {
            this.updateCommandOptions = new AnnotationSetsUpdateCommandOptions();
        }
        return updateCommandOptions;
    }
}
