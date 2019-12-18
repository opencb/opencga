package org.opencb.opencga.app.cli.main.executors.operations;

import org.opencb.opencga.app.cli.main.executors.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.options.OperationsCommandOptions;
import org.opencb.opencga.core.api.operations.variant.*;
import org.opencb.opencga.core.models.Job;
import org.opencb.opencga.core.rest.RestResponse;

import java.io.IOException;

import static org.opencb.opencga.app.cli.main.options.OperationsCommandOptions.*;

public class OperationsCommandExecutor extends OpencgaCommandExecutor {

    private OperationsCommandOptions operationsCommandOptions;

    public OperationsCommandExecutor(OperationsCommandOptions operationsCommandOptions) {
        super(operationsCommandOptions.commonCommandOptions);
        this.operationsCommandOptions = operationsCommandOptions;
    }


    @Override
    public void execute() throws Exception {
        logger.debug("Executing variant command line");

        String subCommandString = getParsedSubCommand(operationsCommandOptions.jCommander);
        RestResponse queryResponse = null;
        switch (subCommandString) {
            case VARIANT_SECONDARY_INDEX:
                queryResponse = variantSecondaryIndex();
                break;
            case VARIANT_SECONDARY_INDEX_DELETE:
                queryResponse = variantSecondaryIndexDelete();
                break;
            case VARIANT_ANNOTATION_INDEX:
                queryResponse = variantAnnotationIndex();
                break;
            case VARIANT_ANNOTATION_SAVE:
                queryResponse = variantAnnotationSave();
                break;
            case VARIANT_ANNOTATION_DELETE:
                queryResponse = variantAnnotationDelete();
                break;
            case VARIANT_SCORE_INDEX:
                queryResponse = variantScoreIndex();
                break;
            case VARIANT_SCORE_DELETE:
                queryResponse = variantScoreDelete();
                break;
            case VARIANT_FAMILY_GENOTYPE_INDEX:
                queryResponse = variantFamilyIndex();
                break;
            case VARIANT_SAMPLE_GENOTYPE_INDEX:
                queryResponse = variantSampleIndex();
                break;
            case VARIANT_AGGREGATE:
                queryResponse = variantAggregate();
                break;
            case VARIANT_FAMILY_AGGREGATE:
                queryResponse = variantAggregateFamily();
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }

//        ObjectMapper objectMapper = new ObjectMapper();
//        System.out.println(objectMapper.writeValueAsString(queryResponse.getResponse()));

        createOutput(queryResponse);

    }

    private RestResponse<Job> variantSecondaryIndex() throws IOException {
        OperationsCommandOptions.VariantSecondaryIndexCommandOptions cliOptions = operationsCommandOptions.variantSecondaryIndex;
        return openCGAClient.getOperationClient().variantSecondaryIndex(cliOptions.project, cliOptions.study,
                new VariantSecondaryIndexParams(
                        cliOptions.region,
                        cliOptions.sample,
                        cliOptions.overwrite),
                cliOptions.commonOptions.params);
    }

    private RestResponse variantSecondaryIndexDelete() throws IOException {
        OperationsCommandOptions.VariantSecondaryIndexDeleteCommandOptions cliOptions = operationsCommandOptions.variantSecondaryIndexDelete;
        return openCGAClient.getOperationClient().variantSecondaryIndexDelete(
                cliOptions.study,
                cliOptions.sample,
                cliOptions.commonOptions.params);
    }

    private RestResponse<Job> variantAnnotationIndex() throws IOException {
        OperationsCommandOptions.VariantAnnotationIndexCommandOptions cliOptions = operationsCommandOptions.variantAnnotation;
        VariantAnnotationIndexParams body = new VariantAnnotationIndexParams(
                cliOptions.outdir,
                cliOptions.genericVariantAnnotateOptions.outputFileName,
                cliOptions.genericVariantAnnotateOptions.annotator == null
                        ? null
                        : cliOptions.genericVariantAnnotateOptions.annotator.toString(),
                cliOptions.genericVariantAnnotateOptions.overwriteAnnotations,
                cliOptions.genericVariantAnnotateOptions.region,
                cliOptions.genericVariantAnnotateOptions.create,
                cliOptions.genericVariantAnnotateOptions.load,
                cliOptions.genericVariantAnnotateOptions.customName
        );
        return openCGAClient.getOperationClient()
                .variantAnnotationIndex(cliOptions.project, cliOptions.study, body, cliOptions.commonOptions.params);
    }

    private RestResponse variantAnnotationSave() throws IOException {
        OperationsCommandOptions.VariantAnnotationSaveCommandOptions cliOptions = operationsCommandOptions.variantAnnotationSave;
        return openCGAClient.getOperationClient().variantAnnotationSave(
                cliOptions.project,
                new VariantAnnotationSaveParams(
                        cliOptions.annotationId
                ),
                cliOptions.commonOptions.params);
    }

    private RestResponse variantAnnotationDelete() throws IOException {
        OperationsCommandOptions.VariantAnnotationDeleteCommandOptions cliOptions = operationsCommandOptions.variantAnnotationDelete;
        return openCGAClient.getOperationClient().variantAnnotationDelete(
                cliOptions.project,
                new VariantAnnotationDeleteParams(cliOptions.annotationId),
                cliOptions.commonOptions.params);
    }

    private RestResponse variantScoreIndex() throws IOException {
        OperationsCommandOptions.VariantScoreIndexCommandOptions cliOptions = operationsCommandOptions.variantScoreIndex;
        return openCGAClient.getOperationClient().variantScoreIndex(
                cliOptions.study.study,
                new VariantScoreIndexParams(
                        cliOptions.scoreName,
                        cliOptions.cohort1,
                        cliOptions.cohort2,
                        cliOptions.input,
                        cliOptions.columns,
                        cliOptions.resume
                ),
                cliOptions.commonOptions.params);
    }

    private RestResponse variantScoreDelete() throws IOException {
        OperationsCommandOptions.VariantScoreDeleteCommandOptions cliOptions = operationsCommandOptions.variantScoreDelete;
        return openCGAClient.getOperationClient().variantScoreDelete(
                cliOptions.study.study,
                new VariantScoreDeleteParams(
                        cliOptions.scoreName,
                        cliOptions.resume,
                        cliOptions.force),
                cliOptions.commonOptions.params
        );
    }

    private RestResponse<Job> variantFamilyIndex() throws IOException {
        OperationsCommandOptions.VariantFamilyGenotypeIndexCommandOptions cliOptions = operationsCommandOptions.variantFamilyIndex;

        return openCGAClient.getOperationClient().variantFamilyGenotypeIndex(
                cliOptions.study,
                new VariantFamilyIndexParams(
                        cliOptions.family,
                        cliOptions.overwrite,
                        cliOptions.skipIncompleteFamilies
                ),
                cliOptions.commonOptions.params);
    }

    private RestResponse<Job> variantSampleIndex() throws IOException {
        OperationsCommandOptions.VariantSampleGenotypeIndexCommandOptions cliOptions = operationsCommandOptions.variantSampleIndex;

        return openCGAClient.getOperationClient().variantSampleGenotypeIndex(cliOptions.study,
                new VariantSampleIndexParams(
                        cliOptions.sample,
                        cliOptions.buildIndex,
                        cliOptions.annotate
                ),
                cliOptions.commonOptions.params);
    }

    private RestResponse<Job> variantAggregate() throws IOException {
        OperationsCommandOptions.VariantAggregateCommandOptions cliOptions = operationsCommandOptions.variantAggregate;

        return openCGAClient.getOperationClient().variantAggregate(cliOptions.study,
                new VariantAggregateParams(
                        cliOptions.aggregateCommandOptions.overwrite,
                        cliOptions.aggregateCommandOptions.resume
                ),
                cliOptions.commonOptions.params);
    }

    private RestResponse<Job> variantAggregateFamily() throws IOException {
        OperationsCommandOptions.VariantFamilyAggregateCommandOptions cliOptions = operationsCommandOptions.variantAggregateFamily;

        return openCGAClient.getOperationClient().variantAggregateFamily(cliOptions.study,
                new VariantAggregateFamilyParams(
                        cliOptions.genericAggregateFamilyOptions.samples, cliOptions.genericAggregateFamilyOptions.resume
                ),
                cliOptions.commonOptions.params);
    }

}
