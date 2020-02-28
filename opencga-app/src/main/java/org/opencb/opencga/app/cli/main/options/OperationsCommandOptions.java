package org.opencb.opencga.app.cli.main.options;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.opencb.opencga.app.cli.GeneralCliOptions;
import org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;

import java.util.List;

import static org.opencb.opencga.analysis.variant.manager.VariantCatalogQueryUtils.PROJECT_DESC;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.FamilyIndexCommandOptions.FAMILY_INDEX_COMMAND_DESCRIPTION;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.SampleIndexCommandOptions.SAMPLE_INDEX_COMMAND_DESCRIPTION;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.AggregateFamilyCommandOptions.AGGREGATE_FAMILY_COMMAND_DESCRIPTION;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.*;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.GenericAggregateCommandOptions.AGGREGATE_COMMAND_DESCRIPTION;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.GenericAnnotationDeleteCommandOptions.ANNOTATION_DELETE_COMMAND_DESCRIPTION;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.GenericAnnotationSaveCommandOptions.ANNOTATION_SAVE_COMMAND_DESCRIPTION;

@Parameters(commandNames = {OperationsCommandOptions.OPERATIONS_COMMAND}, commandDescription = "Operations commands")
public class OperationsCommandOptions {

    public static final String OPERATIONS_COMMAND = "operations";

    public static final String VARIANT_CONFIGURE = "variant-configure";

    public static final String VARIANT_SECONDARY_INDEX = "variant-secondary-index";
    public static final String VARIANT_SECONDARY_INDEX_DELETE = "variant-secondary-index-delete";

    public static final String VARIANT_ANNOTATION_INDEX = "variant-annotation-index";
    public static final String VARIANT_ANNOTATION_SAVE = "variant-annotation-save";
    public static final String VARIANT_ANNOTATION_DELETE = "variant-annotation-delete";

    public static final String VARIANT_SCORE_INDEX = "variant-score-index";
    public static final String VARIANT_SCORE_DELETE = "variant-score-delete";

    public static final String VARIANT_SAMPLE_GENOTYPE_INDEX = "variant-sample-genotype-index";
    public static final String VARIANT_FAMILY_GENOTYPE_INDEX = "variant-family-genotype-index";

    public static final String VARIANT_FAMILY_AGGREGATE = "variant-family-aggregate";
    public static final String VARIANT_AGGREGATE = "variant-aggregate";

    public final VariantConfigureCommandOptions variantConfigure;

    public final VariantSecondaryIndexCommandOptions variantSecondaryIndex;
    public final VariantSecondaryIndexDeleteCommandOptions variantSecondaryIndexDelete;

    public final VariantAnnotationIndexCommandOptions variantAnnotation;
    public final VariantAnnotationSaveCommandOptions variantAnnotationSave;
    public final VariantAnnotationDeleteCommandOptions variantAnnotationDelete;

    public final VariantScoreIndexCommandOptions variantScoreIndex;
    public final VariantScoreDeleteCommandOptions variantScoreDelete;

    public final VariantSampleGenotypeIndexCommandOptions variantSampleIndex;
    public final VariantFamilyGenotypeIndexCommandOptions variantFamilyIndex;

    public final VariantFamilyAggregateCommandOptions variantAggregateFamily;
    public final VariantAggregateCommandOptions variantAggregate;


    public JCommander jCommander;
    public GeneralCliOptions.CommonCommandOptions commonCommandOptions;
//    public GeneralCliOptions.DataModelOptions commonDataModelOptions;
//    public GeneralCliOptions.NumericOptions commonNumericOptions;
    public GeneralCliOptions.JobOptions commonJobOptions;

    public OperationsCommandOptions(GeneralCliOptions.CommonCommandOptions commonCommandOptions,
                                    GeneralCliOptions.DataModelOptions dataModelOptions,
                                    GeneralCliOptions.NumericOptions numericOptions,
                                    JCommander jCommander) {

        this.commonCommandOptions = commonCommandOptions;
//        this.commonDataModelOptions = dataModelOptions;
//        this.commonNumericOptions = numericOptions;
        this.jCommander = jCommander;
        commonJobOptions = new GeneralCliOptions.JobOptions();

        variantConfigure = new VariantConfigureCommandOptions();
        variantSecondaryIndex = new VariantSecondaryIndexCommandOptions();
        variantSecondaryIndexDelete = new VariantSecondaryIndexDeleteCommandOptions();
        variantAnnotation = new VariantAnnotationIndexCommandOptions();
        variantAnnotationSave = new VariantAnnotationSaveCommandOptions();
        variantAnnotationDelete = new VariantAnnotationDeleteCommandOptions();
        variantScoreIndex = new VariantScoreIndexCommandOptions();
        variantScoreDelete = new VariantScoreDeleteCommandOptions();
        variantSampleIndex = new VariantSampleGenotypeIndexCommandOptions();
        variantFamilyIndex = new VariantFamilyGenotypeIndexCommandOptions();
        variantAggregateFamily = new VariantFamilyAggregateCommandOptions();
        variantAggregate = new VariantAggregateCommandOptions();
    }

    @Parameters(commandNames = {VARIANT_CONFIGURE}, commandDescription = "Modify variant storage configuration")
    public class VariantConfigureCommandOptions extends GeneralCliOptions.StudyOption {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-p", "--project"}, description = "Project to index.", arity = 1)
        public String project;
    }

    @Parameters(commandNames = {VARIANT_SECONDARY_INDEX}, commandDescription = "Creates a secondary index using a search engine")
    public class VariantSecondaryIndexCommandOptions extends GeneralCliOptions.StudyOption {

        @ParametersDelegate
        public GeneralCliOptions.JobOptions jobOptions = commonJobOptions;

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-p", "--project"}, description = "Project to index.", arity = 1)
        public String project;

        @Parameter(names = {"-r", "--region"}, description = VariantQueryParam.REGION_DESCR)
        public String region;

        @Parameter(names = {"--sample"}, description = "Samples to index."
                + " If provided, all sample data will be added to the secondary index.", arity = 1)
        public List<String> sample;

        @Parameter(names = {"--overwrite"}, description = "Overwrite search index for all files and variants. Repeat operation for already processed variants.")
        public boolean overwrite;
    }

    @Parameters(commandNames = {VARIANT_SECONDARY_INDEX_DELETE}, commandDescription = "Remove a secondary index from the search engine")
    public class VariantSecondaryIndexDeleteCommandOptions extends GeneralCliOptions.StudyOption {

        @ParametersDelegate
        public GeneralCliOptions.JobOptions jobOptions = commonJobOptions;

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--sample"}, description = "Samples to remove. Needs to provide all the samples in the secondary index.",
                required = true, arity = 1)
        public List<String> sample;
    }


    @Parameters(commandNames = {VARIANT_ANNOTATION_INDEX}, commandDescription = GenericVariantAnnotateOptions.ANNOTATE_DESCRIPTION)
    public class VariantAnnotationIndexCommandOptions extends GeneralCliOptions.StudyOption {

        @ParametersDelegate
        public GeneralCliOptions.JobOptions jobOptions = commonJobOptions;

        @ParametersDelegate
        public GenericVariantAnnotateOptions genericVariantAnnotateOptions = new GenericVariantAnnotateOptions();

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-p", "--project-id"}, description = "Project to annotate.")
        public String project;

        @Parameter(names = {"-o", "--outdir"}, description = "Output directory.")
        public String outdir;
    }

    @Parameters(commandNames = {VARIANT_ANNOTATION_SAVE}, commandDescription = ANNOTATION_SAVE_COMMAND_DESCRIPTION)
    public class VariantAnnotationSaveCommandOptions extends StorageVariantCommandOptions.GenericAnnotationSaveCommandOptions {

        @ParametersDelegate
        public GeneralCliOptions.JobOptions jobOptions = commonJobOptions;

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-p", "--project"}, description = PROJECT_DESC, arity = 1)
        public String project;
    }

    @Parameters(commandNames = {VARIANT_ANNOTATION_DELETE}, commandDescription = ANNOTATION_DELETE_COMMAND_DESCRIPTION)
    public class VariantAnnotationDeleteCommandOptions extends StorageVariantCommandOptions.GenericAnnotationDeleteCommandOptions {

        @ParametersDelegate
        public GeneralCliOptions.JobOptions jobOptions = commonJobOptions;

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-p", "--project"}, description = PROJECT_DESC, arity = 1)
        public String project;

    }

    @Parameters(commandNames = {VARIANT_SCORE_INDEX}, commandDescription = GenericVariantScoreIndexCommandOptions.SCORE_INDEX_COMMAND_DESCRIPTION)
    public class VariantScoreIndexCommandOptions extends GenericVariantScoreIndexCommandOptions {

        @ParametersDelegate
        public GeneralCliOptions.JobOptions jobOptions = commonJobOptions;

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public GeneralCliOptions.StudyOption study = new GeneralCliOptions.StudyOption();
    }

    @Parameters(commandNames = {VARIANT_SCORE_DELETE}, commandDescription = GenericVariantScoreDeleteCommandOptions.SCORE_DELETE_COMMAND_DESCRIPTION)
    public class VariantScoreDeleteCommandOptions extends GenericVariantScoreDeleteCommandOptions {

        @ParametersDelegate
        public GeneralCliOptions.JobOptions jobOptions = commonJobOptions;

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public GeneralCliOptions.StudyOption study = new GeneralCliOptions.StudyOption();
    }

    @Parameters(commandNames = {VARIANT_SAMPLE_GENOTYPE_INDEX}, commandDescription = SAMPLE_INDEX_COMMAND_DESCRIPTION)
    public class VariantSampleGenotypeIndexCommandOptions extends GeneralCliOptions.StudyOption {

        @ParametersDelegate
        public GeneralCliOptions.JobOptions jobOptions = commonJobOptions;

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--sample"}, required = true, description = "Samples to include in the index. " +
                "Use \"" + VariantQueryUtils.ALL + "\" to annotate the index for all samples in the study.")
        public List<String> sample;

        @Parameter(names = {"--build-index"}, description = "Build sample index.", arity = 0)
        public boolean buildIndex;

        @Parameter(names = {"--annotate"}, description = "Annotate sample index", arity = 0)
        public boolean annotate;

//        @Parameter(names = {"--overwrite"}, description = "Overwrite mendelian errors")
//        public boolean overwrite = false;
    }

    @Parameters(commandNames = {VARIANT_FAMILY_GENOTYPE_INDEX}, commandDescription = FAMILY_INDEX_COMMAND_DESCRIPTION)
    public class VariantFamilyGenotypeIndexCommandOptions extends GeneralCliOptions.StudyOption {

        @ParametersDelegate
        public GeneralCliOptions.JobOptions jobOptions = commonJobOptions;

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--family"}, required = true, description = "Families to index. " +
                "Use \"" + VariantQueryUtils.ALL + "\" to index all families in the study.")
        public List<String> family;

        @Parameter(names = {"--overwrite"}, description = "Overwrite existing values")
        public boolean overwrite = false;

        @Parameter(names = {"--skip-incomplete-families"}, description = "Do not process incomplete families.")
        public boolean skipIncompleteFamilies = false;
    }


    @Parameters(commandNames = {VARIANT_FAMILY_AGGREGATE}, commandDescription = AGGREGATE_FAMILY_COMMAND_DESCRIPTION)
    public class VariantFamilyAggregateCommandOptions extends GeneralCliOptions.StudyOption {

        @ParametersDelegate
        public GeneralCliOptions.JobOptions jobOptions = commonJobOptions;

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public GenericAggregateFamilyOptions genericAggregateFamilyOptions = new GenericAggregateFamilyOptions();
    }

    @Parameters(commandNames = {VARIANT_AGGREGATE}, commandDescription = AGGREGATE_COMMAND_DESCRIPTION)
    public class VariantAggregateCommandOptions extends GeneralCliOptions.StudyOption {

        @ParametersDelegate
        public GeneralCliOptions.JobOptions jobOptions = commonJobOptions;

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public GenericAggregateCommandOptions aggregateCommandOptions = new GenericAggregateCommandOptions();

    }

}
