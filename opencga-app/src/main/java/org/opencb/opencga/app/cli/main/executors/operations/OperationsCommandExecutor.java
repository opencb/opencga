package org.opencb.opencga.app.cli.main.executors.operations;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.app.cli.main.executors.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.options.OperationsCommandOptions;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.operations.variant.*;
import org.opencb.opencga.core.response.RestResponse;

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

    private RestResponse<ObjectMap> variantSecondaryIndex() throws ClientException {
        OperationsCommandOptions.VariantSecondaryIndexCommandOptions cliOptions = operationsCommandOptions.variantSecondaryIndex;

        ObjectMap params = new ObjectMap(cliOptions.commonOptions.params)
                .append(ParamConstants.STUDY_PARAM, cliOptions.study)
                .append(ParamConstants.PROJECT_PARAM, cliOptions.project);

        return openCGAClient.getOperationClient().secondaryIndexVariant(
                new VariantSecondaryIndexParams(
                        cliOptions.region,
                        cliOptions.sample,
                        cliOptions.overwrite), params);
    }

    private RestResponse variantSecondaryIndexDelete() throws ClientException {
        OperationsCommandOptions.VariantSecondaryIndexDeleteCommandOptions cliOptions = operationsCommandOptions.variantSecondaryIndexDelete;

        ObjectMap params = new ObjectMap(cliOptions.commonOptions.params)
                .append(ParamConstants.STUDY_PARAM, cliOptions.study)
                .append("sample", cliOptions.sample);

        return openCGAClient.getOperationClient().deleteVariantSecondaryIndex(params);
    }

    private RestResponse<Job> variantAnnotationIndex() throws ClientException {
        OperationsCommandOptions.VariantAnnotationIndexCommandOptions cliOptions = operationsCommandOptions.variantAnnotation;

        ObjectMap params = new ObjectMap(cliOptions.commonOptions.params)
                .append(ParamConstants.STUDY_PARAM, cliOptions.study)
                .append(ParamConstants.PROJECT_PARAM, cliOptions.project);

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
        return openCGAClient.getOperationClient().indexVariantAnnotation(body, params);
    }

    private RestResponse variantAnnotationSave() throws ClientException {
        OperationsCommandOptions.VariantAnnotationSaveCommandOptions cliOptions = operationsCommandOptions.variantAnnotationSave;

        ObjectMap params = new ObjectMap(cliOptions.commonOptions.params)
                .append(ParamConstants.PROJECT_PARAM, cliOptions.project);

        return openCGAClient.getOperationClient().saveVariantAnnotation(new VariantAnnotationSaveParams(cliOptions.annotationId), params);
    }

    private RestResponse variantAnnotationDelete() throws ClientException {
        OperationsCommandOptions.VariantAnnotationDeleteCommandOptions cliOptions = operationsCommandOptions.variantAnnotationDelete;

        ObjectMap params = new ObjectMap(cliOptions.commonOptions.params)
                .append(ParamConstants.PROJECT_PARAM, cliOptions.project)
                .appendAll(new VariantAnnotationDeleteParams(cliOptions.annotationId).toObjectMap());

        return openCGAClient.getOperationClient().deleteVariantAnnotation(params);
    }

    private RestResponse variantScoreIndex() throws ClientException {
        OperationsCommandOptions.VariantScoreIndexCommandOptions cliOptions = operationsCommandOptions.variantScoreIndex;

        ObjectMap params = new ObjectMap(cliOptions.commonOptions.params)
                .append(ParamConstants.STUDY_PARAM, cliOptions.study);

        return openCGAClient.getOperationClient().indexVariantScore(
                new VariantScoreIndexParams(
                        cliOptions.scoreName,
                        cliOptions.cohort1,
                        cliOptions.cohort2,
                        cliOptions.input,
                        cliOptions.columns,
                        cliOptions.resume
                ), params);
    }

    private RestResponse variantScoreDelete() throws ClientException {
        OperationsCommandOptions.VariantScoreDeleteCommandOptions cliOptions = operationsCommandOptions.variantScoreDelete;

        ObjectMap params = new ObjectMap(cliOptions.commonOptions.params)
                .append(ParamConstants.STUDY_PARAM, cliOptions.study)
                .appendAll(new VariantScoreDeleteParams(
                        cliOptions.scoreName,
                        cliOptions.resume,
                        cliOptions.force).toObjectMap());

        return openCGAClient.getOperationClient().deleteVariantScore(params);
    }

    private RestResponse<Job> variantFamilyIndex() throws ClientException {
        OperationsCommandOptions.VariantFamilyGenotypeIndexCommandOptions cliOptions = operationsCommandOptions.variantFamilyIndex;

        ObjectMap params = new ObjectMap(cliOptions.commonOptions.params)
                .append(ParamConstants.STUDY_PARAM, cliOptions.study);

        return openCGAClient.getOperationClient().indexFamilyGenotype(
                new VariantFamilyIndexParams(
                        cliOptions.family,
                        cliOptions.overwrite,
                        cliOptions.skipIncompleteFamilies
                ), params);
    }

    private RestResponse<Job> variantSampleIndex() throws ClientException {
        OperationsCommandOptions.VariantSampleGenotypeIndexCommandOptions cliOptions = operationsCommandOptions.variantSampleIndex;

        ObjectMap params = new ObjectMap(cliOptions.commonOptions.params)
                .append(ParamConstants.STUDY_PARAM, cliOptions.study);

        return openCGAClient.getOperationClient().indexSampleGenotype(
                new VariantSampleIndexParams(
                        cliOptions.sample,
                        cliOptions.buildIndex,
                        cliOptions.annotate
                ), params);
    }

    private RestResponse<ObjectMap> variantAggregate() throws ClientException {
        OperationsCommandOptions.VariantAggregateCommandOptions cliOptions = operationsCommandOptions.variantAggregate;

        ObjectMap params = new ObjectMap(cliOptions.commonOptions.params)
                .append(ParamConstants.STUDY_PARAM, cliOptions.study);

        return openCGAClient.getOperationClient().aggregateVariant(
                new VariantAggregateParams(
                        cliOptions.aggregateCommandOptions.overwrite,
                        cliOptions.aggregateCommandOptions.resume
                ), params);
    }

    private RestResponse<ObjectMap> variantAggregateFamily() throws ClientException {
        OperationsCommandOptions.VariantFamilyAggregateCommandOptions cliOptions = operationsCommandOptions.variantAggregateFamily;

        ObjectMap params = new ObjectMap(cliOptions.commonOptions.params)
                .append(ParamConstants.STUDY_PARAM, cliOptions.study);

        return openCGAClient.getOperationClient().aggregateVariantFamily(
                new VariantAggregateFamilyParams(
                        cliOptions.genericAggregateFamilyOptions.samples, cliOptions.genericAggregateFamilyOptions.resume
                ), params);
    }

}
