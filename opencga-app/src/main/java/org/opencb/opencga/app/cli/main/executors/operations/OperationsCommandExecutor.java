package org.opencb.opencga.app.cli.main.executors.operations;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.app.cli.main.executors.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.options.OperationsCommandOptions;
import org.opencb.opencga.core.api.operations.variant.*;
import org.opencb.opencga.core.models.Job;
import org.opencb.opencga.core.rest.RestResponse;

import java.io.IOException;
import java.util.Map;

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
            case VARIANT_FILE_DELETE:
                queryResponse = variantIndexDelete();
                break;
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

    private RestResponse variantIndexDelete() throws IOException {
        OperationsCommandOptions.VariantFileDeleteCommandOptions variantIndexDelete = operationsCommandOptions.variantIndexDelete;
        return openCGAClient.getOperationClient().variantFileDelete(
                variantIndexDelete.study,
                variantIndexDelete.genericVariantDeleteOptions.file,
                variantIndexDelete.genericVariantDeleteOptions.resume,
                getDynamicParams());
    }

    private RestResponse<Job> variantSecondaryIndex() throws IOException {
        OperationsCommandOptions.VariantSecondaryIndexCommandOptions variantSecondaryIndex = operationsCommandOptions.variantSecondaryIndex;
        ObjectMap params = new VariantSecondaryIndexParams(
                variantSecondaryIndex.region,
                variantSecondaryIndex.sample,
                variantSecondaryIndex.overwrite
        ).toObjectMap();
        addDynamicParams(params);
        return openCGAClient.getOperationClient().variantSecondaryIndex(variantSecondaryIndex.project, variantSecondaryIndex.study, params);
    }

    private RestResponse variantSecondaryIndexDelete() throws IOException {
        return openCGAClient.getOperationClient().variantSecondaryIndexDelete(
                operationsCommandOptions.variantSecondaryIndexDelete.study,
                operationsCommandOptions.variantSecondaryIndexDelete.sample,
                addDynamicParams(new ObjectMap()));
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
                .variantAnnotationIndex(cliOptions.project, cliOptions.study, body, getDynamicParams());
    }

    private RestResponse variantAnnotationSave() throws IOException {
        ObjectMap params = new VariantAnnotationSaveParams(
                operationsCommandOptions.variantAnnotationSave.annotationId
        ).toObjectMap();
        addDynamicParams(params);
        return openCGAClient.getOperationClient().variantAnnotationSave(operationsCommandOptions.variantAnnotationSave.project, params);
    }

    private RestResponse variantAnnotationDelete() throws IOException {
        OperationsCommandOptions.VariantAnnotationDeleteCommandOptions variantAnnotationDelete = operationsCommandOptions.variantAnnotationDelete;
        return openCGAClient.getOperationClient().variantAnnotationDelete(
                variantAnnotationDelete.project,
                variantAnnotationDelete.annotationId,
                addDynamicParams(new ObjectMap()));
    }

    private RestResponse variantScoreIndex() throws IOException {
        OperationsCommandOptions.VariantScoreIndexCommandOptions variantScoreIndex = operationsCommandOptions.variantScoreIndex;
        ObjectMap params = new VariantScoreIndexParams(
                variantScoreIndex.scoreName,
                variantScoreIndex.cohort1,
                variantScoreIndex.cohort2,
                variantScoreIndex.input,
                variantScoreIndex.columns,
                variantScoreIndex.resume
        ).toObjectMap();
        addDynamicParams(params);
        return openCGAClient.getOperationClient().variantScoreIndex(variantScoreIndex.study.study, params);
    }

    private RestResponse variantScoreDelete() throws IOException {
        OperationsCommandOptions.VariantScoreDeleteCommandOptions variantScoreDelete = operationsCommandOptions.variantScoreDelete;
        return openCGAClient.getOperationClient().variantScoreDelete(
                variantScoreDelete.study.study,
                variantScoreDelete.scoreName,
                variantScoreDelete.resume,
                variantScoreDelete.force,
                addDynamicParams(new ObjectMap())
        );
    }

    private RestResponse<Job> variantFamilyIndex() throws IOException {
        OperationsCommandOptions.VariantFamilyGenotypeIndexCommandOptions variantFamilyIndex = operationsCommandOptions.variantFamilyIndex;
        ObjectMap params = new VariantFamilyIndexParams(
                variantFamilyIndex.family,
                variantFamilyIndex.overwrite,
                variantFamilyIndex.skipIncompleteFamilies
        ).toObjectMap();
        addDynamicParams(params);
        return openCGAClient.getOperationClient().variantFamilyGenotypeIndex(variantFamilyIndex.study, params);
    }

    private RestResponse<Job> variantSampleIndex() throws IOException {
        OperationsCommandOptions.VariantSampleGenotypeIndexCommandOptions variantSampleIndex = operationsCommandOptions.variantSampleIndex;
        ObjectMap params = new VariantSampleIndexParams(
                variantSampleIndex.sample,
                variantSampleIndex.buildIndex,
                variantSampleIndex.annotate
        ).toObjectMap();
        addDynamicParams(params);
        return openCGAClient.getOperationClient().variantSampleGenotypeIndex(variantSampleIndex.study, params);
    }

    private RestResponse<Job> variantAggregate() throws IOException {
        OperationsCommandOptions.VariantAggregateCommandOptions variantAggregate = operationsCommandOptions.variantAggregate;
        ObjectMap params = new VariantAggregateParams(
                variantAggregate.aggregateCommandOptions.overwrite,
                variantAggregate.aggregateCommandOptions.resume
        ).toObjectMap();
        addDynamicParams(params);
        return openCGAClient.getOperationClient().variantAggregate(variantAggregate.study, params);
    }

    private RestResponse<Job> variantAggregateFamily() throws IOException {
        OperationsCommandOptions.VariantFamilyAggregateCommandOptions variantAggregateFamily = operationsCommandOptions.variantAggregateFamily;
        ObjectMap params = new VariantAggregateFamilyParams(
                variantAggregateFamily.genericAggregateFamilyOptions.samples, variantAggregateFamily.genericAggregateFamilyOptions.resume
        ).toObjectMap();
        addDynamicParams(params);
        return openCGAClient.getOperationClient().variantAggregateFamily(variantAggregateFamily.study, params);
    }

    @Deprecated
    public ObjectMap addDynamicParams(ObjectMap params) {
        for (Map.Entry<String, String> entry : operationsCommandOptions.commonCommandOptions.params.entrySet()) {
            params.put("dynamic_" + entry.getKey(), entry.getKey());
        }
        return params;
    }

    public ObjectMap getDynamicParams() {
        ObjectMap params = new ObjectMap();
        for (Map.Entry<String, String> entry : operationsCommandOptions.commonCommandOptions.params.entrySet()) {
            params.put("dynamic_" + entry.getKey(), entry.getKey());
        }
        return params;
    }
}
