package org.opencb.opencga.app.cli.main.executors.operations;

import org.opencb.commons.datastore.core.DataResponse;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.app.cli.main.executors.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.options.OperationsCommandOptions;
import org.opencb.opencga.server.rest.operations.OperationsWSService;

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
        DataResponse queryResponse = null;
        switch (subCommandString) {
            case VARIANT_FILE_INDEX:
                queryResponse = variantIndex();
                break;
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
                queryResponse = variantAnnotation();
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

    private DataResponse variantIndex() throws IOException {
        OperationsWSService.VariantIndexParams variantIndexParams = new OperationsWSService.VariantIndexParams(
                operationsCommandOptions.variantIndex.study,
                operationsCommandOptions.variantIndex.fileId,
                operationsCommandOptions.variantIndex.genericVariantIndexOptions.resume,
                operationsCommandOptions.variantIndex.outdir,
                operationsCommandOptions.variantIndex.genericVariantIndexOptions.transform,
                operationsCommandOptions.variantIndex.genericVariantIndexOptions.gvcf,
                operationsCommandOptions.variantIndex.genericVariantIndexOptions.load,
                operationsCommandOptions.variantIndex.genericVariantIndexOptions.loadSplitData,
                operationsCommandOptions.variantIndex.genericVariantIndexOptions.skipPostLoadCheck,
                operationsCommandOptions.variantIndex.genericVariantIndexOptions.excludeGenotype,
                operationsCommandOptions.variantIndex.genericVariantIndexOptions.includeExtraFields,
                operationsCommandOptions.variantIndex.genericVariantIndexOptions.merge,
                operationsCommandOptions.variantIndex.genericVariantIndexOptions.calculateStats,
                operationsCommandOptions.variantIndex.genericVariantIndexOptions.aggregated,
                operationsCommandOptions.variantIndex.genericVariantIndexOptions.aggregationMappingFile,
                operationsCommandOptions.variantIndex.genericVariantIndexOptions.annotate,
                operationsCommandOptions.variantIndex.genericVariantIndexOptions.annotator,
                operationsCommandOptions.variantIndex.genericVariantIndexOptions.overwriteAnnotations,
                operationsCommandOptions.variantIndex.genericVariantIndexOptions.indexSearch,
                operationsCommandOptions.commonCommandOptions.params
        );
        return openCGAClient.getOperationClient().variantFileIndex(variantIndexParams.toObjectMap());
    }

    private DataResponse variantIndexDelete() throws IOException {
        return openCGAClient.getOperationClient().variantFileDelete(
                operationsCommandOptions.variantIndexDelete.study,
                operationsCommandOptions.variantIndexDelete.genericVariantDeleteOptions.files,
                operationsCommandOptions.variantIndexDelete.genericVariantDeleteOptions.resume,
                new ObjectMap(operationsCommandOptions.variantIndexDelete.commonOptions.params));
    }

    private DataResponse variantSecondaryIndex() throws IOException {
        ObjectMap params = new OperationsWSService.VariantSecondaryIndexParams(
                operationsCommandOptions.variantSecondaryIndex.study,
                operationsCommandOptions.variantSecondaryIndex.project,
                operationsCommandOptions.variantSecondaryIndex.region,
                operationsCommandOptions.variantSecondaryIndex.sample,
                operationsCommandOptions.variantSecondaryIndex.overwrite,
                operationsCommandOptions.variantSecondaryIndex.commonOptions.params
        ).toObjectMap();
        return openCGAClient.getOperationClient().variantSecondaryIndex(params);
    }

    private DataResponse variantSecondaryIndexDelete() throws IOException {
        return openCGAClient.getOperationClient().variantSecondaryIndexDelete(
                operationsCommandOptions.variantSecondaryIndexDelete.study,
                operationsCommandOptions.variantSecondaryIndexDelete.sample,
                new ObjectMap(operationsCommandOptions.variantIndexDelete.commonOptions.params));
    }

    private DataResponse variantAnnotation() throws IOException {
        ObjectMap params = new OperationsWSService.VariantAnnotationParams(
                operationsCommandOptions.variantAnnotation.study,
//                operationsCommandOptions.variantAnnotation.project,
                operationsCommandOptions.variantAnnotation.outdir,
                operationsCommandOptions.variantAnnotation.genericVariantAnnotateOptions.annotator,
                operationsCommandOptions.variantAnnotation.genericVariantAnnotateOptions.overwriteAnnotations,
                operationsCommandOptions.variantAnnotation.genericVariantAnnotateOptions.region,
                operationsCommandOptions.variantAnnotation.genericVariantAnnotateOptions.create,
                operationsCommandOptions.variantAnnotation.genericVariantAnnotateOptions.load,
                operationsCommandOptions.variantAnnotation.genericVariantAnnotateOptions.customAnnotationKey,
                operationsCommandOptions.variantAnnotation.commonOptions.params
        ).toObjectMap();
        return openCGAClient.getOperationClient().variantAnnotationIndex(params);
    }

    private DataResponse variantAnnotationSave() throws IOException {
        ObjectMap params = new OperationsWSService.VariantAnnotationSaveParams(
                operationsCommandOptions.variantAnnotationSave.project,
                operationsCommandOptions.variantAnnotationSave.annotationId,
                operationsCommandOptions.variantAnnotationSave.commonOptions.params
        ).toObjectMap();
        return openCGAClient.getOperationClient().variantAnnotationSave(params);
    }

    private DataResponse variantAnnotationDelete() throws IOException {
        return openCGAClient.getOperationClient().variantAnnotationDelete(
                operationsCommandOptions.variantAnnotationDelete.project,
                operationsCommandOptions.variantAnnotationDelete.annotationId,
                new ObjectMap(operationsCommandOptions.variantAnnotationDelete.commonOptions.params));
    }

    private DataResponse variantScoreIndex() throws IOException {
        ObjectMap params = new OperationsWSService.VariantScoreIndexParams(
                operationsCommandOptions.variantScoreIndex.study.study,
                operationsCommandOptions.variantScoreIndex.cohort1,
                operationsCommandOptions.variantScoreIndex.cohort2,
                operationsCommandOptions.variantScoreIndex.input,
                operationsCommandOptions.variantScoreIndex.columns,
                operationsCommandOptions.variantScoreIndex.resume,
                operationsCommandOptions.variantScoreIndex.commonOptions.params
        ).toObjectMap();
        return openCGAClient.getOperationClient().variantScoreIndex(params);
    }

    private DataResponse variantScoreDelete() throws IOException {
        return openCGAClient.getOperationClient().variantScoreDelete(
                operationsCommandOptions.variantScoreDelete.study.study,
                operationsCommandOptions.variantScoreDelete.scoreName,
                operationsCommandOptions.variantScoreDelete.resume,
                operationsCommandOptions.variantScoreDelete.force,
                new ObjectMap(operationsCommandOptions.variantScoreDelete.commonOptions.params)
        );
    }

    private DataResponse variantFamilyIndex() throws IOException {
        ObjectMap params = new OperationsWSService.VariantFamilyIndexParams(
                operationsCommandOptions.variantFamilyIndex.study,
                operationsCommandOptions.variantFamilyIndex.family,
                operationsCommandOptions.variantFamilyIndex.overwrite,
                operationsCommandOptions.variantFamilyIndex.commonOptions.params
        ).toObjectMap();
        return openCGAClient.getOperationClient().variantFamilyGenotypeIndex(params);
    }

    private DataResponse variantSampleIndex() throws IOException {
        ObjectMap params = new OperationsWSService.VariantSampleIndexParams(
                operationsCommandOptions.variantSampleIndex.study,
                operationsCommandOptions.variantSampleIndex.sample,
                operationsCommandOptions.variantSampleIndex.commonOptions.params
        ).toObjectMap();
        return openCGAClient.getOperationClient().variantSampleGenotypeIndex(params);
    }

    private DataResponse variantAggregate() throws IOException {
        ObjectMap params = new OperationsWSService.VariantAggregateParams(
                operationsCommandOptions.variantAggregate.study,
                null,
                operationsCommandOptions.variantAggregate.aggregateCommandOptions.overwrite,
                operationsCommandOptions.variantAggregate.aggregateCommandOptions.resume,
                operationsCommandOptions.variantAggregate.commonOptions.params
        ).toObjectMap();
        return openCGAClient.getOperationClient().variantSampleGenotypeIndex(params);
    }

    private DataResponse variantAggregateFamily() throws IOException {
        ObjectMap params = new OperationsWSService.VariantAggregateFamilyParams(
                operationsCommandOptions.variantAggregateFamily.study,
                operationsCommandOptions.variantAggregateFamily.genericAggregateFamilyOptions.resume,
                operationsCommandOptions.variantAggregateFamily.genericAggregateFamilyOptions.samples,
                operationsCommandOptions.variantAggregateFamily.commonOptions.params
        ).toObjectMap();
        return openCGAClient.getOperationClient().variantSampleGenotypeIndex(params);
    }
}
