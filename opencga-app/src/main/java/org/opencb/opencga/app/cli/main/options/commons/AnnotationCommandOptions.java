package org.opencb.opencga.app.cli.main.options.commons;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.opencb.opencga.app.cli.main.OpencgaCliOptionsParser;

/**
 * Created by pfurio on 27/07/16.
 */
public class AnnotationCommandOptions {

    private OpencgaCliOptionsParser.OpencgaCommonCommandOptions commonCommandOptions;

    private AnnotationSetsCreateCommandOptions createCommandOptions;
    private AnnotationSetsAllInfoCommandOptions allInfoCommandOptions;
    private AnnotationSetsSearchCommandOptions searchCommandOptions;
    private AnnotationSetsDeleteCommandOptions deleteCommandOptions;
    private AnnotationSetsInfoCommandOptions infoCommandOptions;
    private AnnotationSetsUpdateCommandOptions updateCommandOptions;

    public AnnotationCommandOptions(OpencgaCliOptionsParser.OpencgaCommonCommandOptions commonCommandOptions) {
        this.commonCommandOptions = commonCommandOptions;
    }

    public class BaseCommandOptions {
        @ParametersDelegate
        public OpencgaCliOptionsParser.OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

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

    @Parameters(commandNames = {"annotation-sets-all-info"}, commandDescription = "Retrieve all the annotation sets from the resource")
    public class AnnotationSetsAllInfoCommandOptions extends BaseCommandOptions {
        @Parameter(names = {"--as-map"}, description = "Boolean indicating whether to show the annotations as key-value pairs", arity = 0)
        public boolean asMap;
    }

    @Parameters(commandNames = {"annotation-sets-search"}, commandDescription = "Search annotation sets from the resource")
    public class AnnotationSetsSearchCommandOptions extends BaseCommandOptions {

        @Parameter(names = {"--variable-set-id"}, description = "Variable set id", arity = 1)
        public String variableSetId;

        @Parameter(names = {"--annotation"}, description = "Annotation",  arity = 1)
        public String annotation;

        @Parameter(names = {"--as-map"}, description = "Boolean indicating whether to show the annotations as key-value pairs", arity = 0)
        public boolean asMap;
    }

    @Parameters(commandNames = {"annotation-sets-delete"}, commandDescription = "Remove an entire annotation set or just some annotations")
    public class AnnotationSetsDeleteCommandOptions extends BaseCommandOptions {

        @Parameter(names = {"--annotation-set-name"}, description = "Annotation set name", required = true, arity = 1)
        public String annotationSetName;

        @Parameter(names = {"--annotations"}, description = "Comma separated list of annotations to be removed. If any, only the "
                + "annotations will be removed", arity = 1)
        public String annotations;
    }

    @Parameters(commandNames = {"annotation-sets-info"}, commandDescription = "Retrieve one annotation set")
    public class AnnotationSetsInfoCommandOptions extends BaseCommandOptions {

        @Parameter(names = {"--annotation-set-name"}, description = "Annotation set name", required = true, arity = 1)
        public String annotationSetName;

        @Parameter(names = {"--as-map"}, description = "Boolean indicating whether to show the annotations as key-value pairs", arity = 0)
        public boolean asMap;
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

    public AnnotationSetsAllInfoCommandOptions getAllInfoCommandOptions() {
        if (this.allInfoCommandOptions == null) {
            this.allInfoCommandOptions = new AnnotationSetsAllInfoCommandOptions();
        }
        return allInfoCommandOptions;
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
