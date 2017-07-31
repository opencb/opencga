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

    private GeneralCliOptions.CommonCommandOptions commonCommandOptions;

    private AnnotationSetsCreateCommandOptions createCommandOptions;
    private AnnotationSetsSearchCommandOptions searchCommandOptions;
    private AnnotationSetsDeleteCommandOptions deleteCommandOptions;
    private AnnotationSetsInfoCommandOptions infoCommandOptions;
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

    @Parameters(commandNames = {"annotation-sets-create"}, commandDescription = "Create a new annotation set")
    public class AnnotationSetsCreateCommandOptions extends BaseCommandOptions {
        @Parameter(names = {"--variable-set-id"}, description = "Variable set id", required = true, arity = 1)
        public String variableSetId;

        @Parameter(names = {"--annotation-set-name"}, description = "Annotation set name", required = true, arity = 1)
        public String annotationSetName;

        @Parameter(names = {"--annotations"}, description = "Json file containing the annotations", required = true, arity = 1)
        public String annotations;
    }

    @Parameters(commandNames = {"annotation-sets-search"}, commandDescription = "Search annotation sets from the resource")
    public class AnnotationSetsSearchCommandOptions extends BaseCommandOptions {

        @Parameter(names = {"--variable-set"}, description = "Variable set id or name", arity = 1)
        public String variableSetId;

        @Parameter(names = {"--annotation"}, description = "Annotation, e.g: key1=value(,key2=value)",  arity = 1)
        public String annotation;
    }

    @Parameters(commandNames = {"annotation-sets-delete"}, commandDescription = "Remove an entire annotation set or just some annotations")
    public class AnnotationSetsDeleteCommandOptions extends BaseCommandOptions {

        @Parameter(names = {"--annotation-set-name"}, description = "Annotation set name", required = true, arity = 1)
        public String annotationSetName;

        @Parameter(names = {"--annotations"}, description = "Comma separated list of annotations to be removed. If any, only the "
                + "annotations will be removed", arity = 1)
        public String annotations;
    }

    @Parameters(commandNames = {"annotation-sets"}, commandDescription = "Retrieve all the annotation sets from the resource")
    public class AnnotationSetsInfoCommandOptions extends BaseCommandOptions {

        @Parameter(names = {"--name"}, description = "Annotation set name. If present, it will only fetch the required "
                + "annotation set", arity = 1)
        public String annotationSetName;
    }

    @Parameters(commandNames = {"annotation-sets-update"}, commandDescription = "Update the value of some annotations")
    public class AnnotationSetsUpdateCommandOptions extends BaseCommandOptions {

        @Parameter(names = {"--annotation-set-name"}, description = "Annotation set name", required = true, arity = 1)
        public String annotationSetName;

        @Parameter(names = {"--annotations"}, description = "Json file containing the annotations to be updated", required = true,
                arity = 1)
        public String annotations;
    }

    public AnnotationSetsCreateCommandOptions getCreateCommandOptions() {
        if (this.createCommandOptions == null) {
            this.createCommandOptions = new AnnotationSetsCreateCommandOptions();
        }
        return createCommandOptions;
    }

    public AnnotationSetsSearchCommandOptions getSearchCommandOptions() {
        if (this.searchCommandOptions == null) {
            this.searchCommandOptions = new AnnotationSetsSearchCommandOptions();
        }
        return searchCommandOptions;
    }

    public AnnotationSetsDeleteCommandOptions getDeleteCommandOptions() {
        if (this.deleteCommandOptions == null) {
            this.deleteCommandOptions = new AnnotationSetsDeleteCommandOptions();
        }
        return deleteCommandOptions;
    }

    public AnnotationSetsInfoCommandOptions getInfoCommandOptions() {
        if (this.infoCommandOptions == null) {
            this.infoCommandOptions = new AnnotationSetsInfoCommandOptions();
        }
        return infoCommandOptions;
    }

    public AnnotationSetsUpdateCommandOptions getUpdateCommandOptions() {
        if (this.updateCommandOptions == null) {
            this.updateCommandOptions = new AnnotationSetsUpdateCommandOptions();
        }
        return updateCommandOptions;
    }
}
