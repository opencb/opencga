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

package org.opencb.opencga.app.cli.main.executors.analysis;

import com.google.protobuf.util.JsonFormat;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.common.protobuf.service.ServiceTypesModel;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.biodata.models.variant.metadata.SampleVariantStats;
import org.opencb.biodata.models.variant.metadata.VariantMetadata;
import org.opencb.biodata.models.variant.metadata.VariantSetStats;
import org.opencb.biodata.models.variant.protobuf.VariantProto;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.analysis.variant.manager.VariantCatalogQueryUtils;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.app.cli.internal.executors.VariantQueryCommandUtils;
import org.opencb.opencga.app.cli.internal.options.VariantCommandOptions;
import org.opencb.opencga.app.cli.main.executors.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.io.VcfOutputWriter;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.variant.*;
import org.opencb.opencga.core.response.RestResponse;
import org.opencb.opencga.core.response.VariantQueryResult;
import org.opencb.opencga.core.tools.ToolParams;
import org.opencb.opencga.server.grpc.AdminServiceGrpc;
import org.opencb.opencga.server.grpc.GenericServiceModel;
import org.opencb.opencga.server.grpc.VariantServiceGrpc;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;

import java.io.IOException;
import java.io.PrintStream;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.CohortVariantStatsCommandOptions.COHORT_VARIANT_STATS_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.CohortVariantStatsQueryCommandOptions.COHORT_VARIANT_STATS_QUERY_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.FamilyQcCommandOptions.FAMILY_QC_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.GatkCommandOptions.GATK_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.GwasCommandOptions.GWAS_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.IndividualQcCommandOptions.INDIVIDUAL_QC_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.InferredSexCommandOptions.INFERRED_SEX_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.MendelianErrorCommandOptions.MENDELIAN_ERROR_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.MutationalSignatureCommandOptions.MUTATIONAL_SIGNATURE_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.PlinkCommandOptions.PLINK_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.RelatednessCommandOptions.RELATEDNESS_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.RvtestsCommandOptions.RVTESTS_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.SampleEligibilityCommandOptions.SAMPLE_ELIGIBILITY_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.SampleQcCommandOptions.SAMPLE_QC_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.SampleVariantStatsCommandOptions.SAMPLE_VARIANT_STATS_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.SampleVariantStatsQueryCommandOptions.SAMPLE_VARIANT_STATS_QUERY_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.VariantExportCommandOptions.EXPORT_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.VariantIndexCommandOptions.INDEX_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.VariantSampleQueryCommandOptions.SAMPLE_QUERY_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.VariantSamplesFilterCommandOptions.SAMPLE_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.VariantStatsCommandOptions.STATS_RUN_COMMAND;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.GenericAnnotationMetadataCommandOptions.ANNOTATION_METADATA_COMMAND;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.GenericAnnotationQueryCommandOptions.ANNOTATION_QUERY_COMMAND;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.VariantDeleteCommandOptions.VARIANT_DELETE_COMMAND;


/**
 * Created by pfurio on 15/08/16.
 */
public class VariantCommandExecutor extends OpencgaCommandExecutor {

    private VariantCommandOptions variantCommandOptions;
    private ManagedChannel channel = null;

    public VariantCommandExecutor(VariantCommandOptions variantCommandOptions) {
        super(variantCommandOptions.commonCommandOptions);
        this.variantCommandOptions = variantCommandOptions;
    }


    @Override
    public void execute() throws Exception {
        logger.debug("Executing variant command line");

        String subCommandString = getParsedSubCommand(variantCommandOptions.jCommander);
        RestResponse queryResponse = null;
        switch (subCommandString) {

            case INDEX_RUN_COMMAND:
                queryResponse = index();
                break;

            case VARIANT_DELETE_COMMAND:
                queryResponse = fileDelete();
                break;

            case "query":
                queryResponse = query();
                break;

            case EXPORT_RUN_COMMAND:
                queryResponse = export();
                break;

            case ANNOTATION_QUERY_COMMAND:
                queryResponse = annotationQuery();
                break;

            case ANNOTATION_METADATA_COMMAND:
                queryResponse = annotationMetadata();
                break;

            case STATS_RUN_COMMAND:
                queryResponse = stats();
                break;

            case SAMPLE_VARIANT_STATS_RUN_COMMAND:
                queryResponse = sampleStats();
                break;

            case SAMPLE_QUERY_COMMAND:
                queryResponse = sampleQuery();
                break;

            case SAMPLE_RUN_COMMAND:
                queryResponse = sampleRun();
                break;

            case SAMPLE_VARIANT_STATS_QUERY_COMMAND:
                queryResponse = sampleStatsQuery();
                break;

            case COHORT_VARIANT_STATS_RUN_COMMAND:
                queryResponse = cohortStats();
                break;

            case COHORT_VARIANT_STATS_QUERY_COMMAND:
                queryResponse = cohortStatsQuery();
                break;

            case VariantCommandOptions.KnockoutCommandOptions.KNOCKOUT_RUN_COMMAND:
                queryResponse = knockout();
                break;

            case SAMPLE_ELIGIBILITY_RUN_COMMAND:
                queryResponse = sampleEligibility();
                break;
//            case "family-stats":
//                queryResponse = familyStats();
//                break;
//            case "family-stats-query":
//                queryResponse = familyStatsQuery();
//                break;

            case GWAS_RUN_COMMAND:
                queryResponse = gwas();
                break;

            case MUTATIONAL_SIGNATURE_RUN_COMMAND:
                queryResponse = mutationalSignature();
                break;

            case MENDELIAN_ERROR_RUN_COMMAND:
                queryResponse = mendelianError();
                break;

            case INFERRED_SEX_RUN_COMMAND:
                queryResponse = inferredSex();
                break;

            case RELATEDNESS_RUN_COMMAND:
                queryResponse = relatedness();
                break;

            case FAMILY_QC_RUN_COMMAND:
                queryResponse = familyQc();
                break;

            case INDIVIDUAL_QC_RUN_COMMAND:
                queryResponse = individualQc();
                break;

            case SAMPLE_QC_RUN_COMMAND:
                queryResponse = sampleQc();
                break;

            case PLINK_RUN_COMMAND:
                queryResponse = plink();
                break;

            case RVTESTS_RUN_COMMAND:
                queryResponse = rvtests();
                break;

            case GATK_RUN_COMMAND:
                queryResponse = gatk();
                break;

            default:
                logger.error("Subcommand not valid");
                break;
        }

//        ObjectMapper objectMapper = new ObjectMapper();
//        System.out.println(objectMapper.writeValueAsString(queryResponse.getResponse()));

        createOutput(queryResponse);

    }

    private RestResponse stats() throws ClientException {
        return openCGAClient.getVariantClient().runStats(
                new VariantStatsAnalysisParams(
                        variantCommandOptions.statsVariantCommandOptions.cohort,
                        variantCommandOptions.statsVariantCommandOptions.samples,
                        variantCommandOptions.statsVariantCommandOptions.region,
                        variantCommandOptions.statsVariantCommandOptions.gene,
                        variantCommandOptions.statsVariantCommandOptions.outdir,
                        variantCommandOptions.statsVariantCommandOptions.fileName,
                        variantCommandOptions.statsVariantCommandOptions.aggregated,
                        variantCommandOptions.statsVariantCommandOptions.aggregationMappingFile
                ), getParams(variantCommandOptions.statsVariantCommandOptions.study));
    }

    private RestResponse<Job> sampleRun() throws ClientException {
        return openCGAClient.getVariantClient().runSample(
                variantCommandOptions.samplesFilterCommandOptions.toolParams,
                getParams(variantCommandOptions.samplesFilterCommandOptions.study)
        );
    }

    private RestResponse<Variant> sampleQuery() throws ClientException {
        ObjectMap options = getParams(variantCommandOptions.sampleQueryCommandOptions.study)
                .append("variant", variantCommandOptions.sampleQueryCommandOptions.variant)
                .append("genotype", variantCommandOptions.sampleQueryCommandOptions.genotype)
                .append(QueryOptions.LIMIT, variantCommandOptions.sampleQueryCommandOptions.numericOptions.limit)
                .append(QueryOptions.SKIP, variantCommandOptions.sampleQueryCommandOptions.numericOptions.skip);

        return openCGAClient.getVariantClient().querySample(options);
    }

    private RestResponse<Job> sampleStats() throws ClientException {
        // Build variant query from cli options
        Query variantQuery = new Query();
        variantQuery.putAll(variantCommandOptions.sampleVariantStatsCommandOptions.variantQuery);

        return openCGAClient.getVariantClient().runSampleStats(
                new SampleVariantStatsAnalysisParams(
                        variantCommandOptions.sampleVariantStatsCommandOptions.sample,
                        variantCommandOptions.sampleVariantStatsCommandOptions.individual,
                        variantCommandOptions.sampleVariantStatsCommandOptions.outdir,
                        variantCommandOptions.sampleVariantStatsCommandOptions.index,
                        variantCommandOptions.sampleVariantStatsCommandOptions.indexOverwrite,
                        variantCommandOptions.sampleVariantStatsCommandOptions.indexId,
                        variantCommandOptions.sampleVariantStatsCommandOptions.indexDescription,
                        variantCommandOptions.sampleVariantStatsCommandOptions.batchSize,
                        variantQuery
                ),
                getParams(variantCommandOptions.sampleVariantStatsCommandOptions.study));
    }

    private RestResponse<SampleVariantStats> sampleStatsQuery() throws ClientException {
        return openCGAClient.getVariantClient()
                .querySampleStats(String.join(",", variantCommandOptions.sampleVariantStatsQueryCommandOptions.sample),
                        getParams(variantCommandOptions.sampleVariantStatsQueryCommandOptions.study));
    }

    private RestResponse<Job> cohortStats() throws ClientException {
        return openCGAClient.getVariantClient().runCohortStats(
                new CohortVariantStatsAnalysisParams(
                        variantCommandOptions.cohortVariantStatsCommandOptions.cohort,
                        variantCommandOptions.cohortVariantStatsCommandOptions.samples,
                        variantCommandOptions.cohortVariantStatsCommandOptions.index,
                        variantCommandOptions.cohortVariantStatsCommandOptions.samplesAnnotation,
                        variantCommandOptions.cohortVariantStatsCommandOptions.outdir
                ),
                getParams(variantCommandOptions.cohortVariantStatsCommandOptions.study));
    }

    private RestResponse<VariantSetStats> cohortStatsQuery() throws ClientException {
        return openCGAClient.getVariantClient().infoCohortStats(
                String.join(",",variantCommandOptions.cohortVariantStatsQueryCommandOptions.cohort),
                getParams(variantCommandOptions.cohortVariantStatsQueryCommandOptions.study)
        );
    }

    private RestResponse<Job> knockout() throws ClientException {
        return openCGAClient.getVariantClient().runKnockout(
                new KnockoutAnalysisParams(
                        variantCommandOptions.knockoutCommandOptions.sample,
                        variantCommandOptions.knockoutCommandOptions.gene,
                        variantCommandOptions.knockoutCommandOptions.panel,
                        variantCommandOptions.knockoutCommandOptions.biotype,
                        variantCommandOptions.knockoutCommandOptions.consequenceType,
                        variantCommandOptions.knockoutCommandOptions.filter,
                        variantCommandOptions.knockoutCommandOptions.qual,
                        variantCommandOptions.knockoutCommandOptions.skipGenesFile,
                        variantCommandOptions.knockoutCommandOptions.outdir,
                        variantCommandOptions.knockoutCommandOptions.index
                ),
                getParams(variantCommandOptions.knockoutCommandOptions.study)
        );
    }

    private RestResponse<Job> sampleEligibility() throws ClientException {
        return openCGAClient.getVariantClient().runSampleEligibility(
                new SampleEligibilityAnalysisParams(
                        variantCommandOptions.sampleEligibilityCommandOptions.query,
                        variantCommandOptions.sampleEligibilityCommandOptions.index,
                        variantCommandOptions.sampleEligibilityCommandOptions.cohortId),
                getParams(variantCommandOptions.sampleEligibilityCommandOptions.study)
        );
    }

//    private RestResponse<Job> familyStats() {
//        openCGAClient.getVariantClient().familyStats()
//    }
//
//    private RestResponse<Job> familyStatsQuery() {
//        openCGAClient.getVariantClient().familyStatsQuery()
//    }

    private RestResponse<Job> mutationalSignature() throws ClientException {
        return openCGAClient.getVariantClient().runMutationalSignature(
                new MutationalSignatureAnalysisParams(
                        variantCommandOptions.mutationalSignatureCommandOptions.sample,
                        variantCommandOptions.mutationalSignatureCommandOptions.outdir
                ),
                getParams(variantCommandOptions.mutationalSignatureCommandOptions.study)
        );
    }

    private RestResponse<Job> mendelianError() throws ClientException {
        return openCGAClient.getVariantClient().runMendelianError(
                new MendelianErrorAnalysisParams(
                        variantCommandOptions.mendelianErrorCommandOptions.family,
                        variantCommandOptions.mendelianErrorCommandOptions.individual,
                        variantCommandOptions.mendelianErrorCommandOptions.sample,
                        variantCommandOptions.mendelianErrorCommandOptions.outdir
                ),
                getParams(variantCommandOptions.mendelianErrorCommandOptions.study)
        );
    }

    private RestResponse<Job> inferredSex() throws ClientException {
        return openCGAClient.getVariantClient().runInferredSex(
                new InferredSexAnalysisParams(
                        variantCommandOptions.inferredSexCommandOptions.individual,
                        variantCommandOptions.inferredSexCommandOptions.sample,
                        variantCommandOptions.inferredSexCommandOptions.outdir
                ),
                getParams(variantCommandOptions.inferredSexCommandOptions.study)
        );
    }

    private RestResponse<Job> relatedness() throws ClientException {
        return openCGAClient.getVariantClient().runRelatedness(
                new RelatednessAnalysisParams(
                        variantCommandOptions.relatednessCommandOptions.individuals,
                        variantCommandOptions.relatednessCommandOptions.samples,
                        variantCommandOptions.relatednessCommandOptions.minorAlleleFreq,
                        variantCommandOptions.relatednessCommandOptions.method,
                        variantCommandOptions.relatednessCommandOptions.outdir
                ),
                getParams(variantCommandOptions.relatednessCommandOptions.study)
        );
    }

    private RestResponse<Job> familyQc() throws ClientException {
        return openCGAClient.getVariantClient().runFamilyQc(
                new FamilyQcAnalysisParams(
                        variantCommandOptions.familyQcCommandOptions.family,
                        variantCommandOptions.familyQcCommandOptions.relatednessMethod,
                        variantCommandOptions.familyQcCommandOptions.relatednessMaf,
                        variantCommandOptions.familyQcCommandOptions.outdir
                ),
                getParams(variantCommandOptions.familyQcCommandOptions.study)
        );
    }

    private RestResponse<Job> individualQc() throws ClientException {
        return openCGAClient.getVariantClient().runIndividualQc(
                new IndividualQcAnalysisParams(
                        variantCommandOptions.individualQcCommandOptions.individual,
                        variantCommandOptions.individualQcCommandOptions.sample,
                        variantCommandOptions.individualQcCommandOptions.inferredSexMethod,
                        variantCommandOptions.individualQcCommandOptions.outdir
                ),
                getParams(variantCommandOptions.individualQcCommandOptions.study)
        );
    }

    private RestResponse<Job> sampleQc() throws ClientException {
        VariantCommandOptions.SampleQcCommandOptions cliOptions = variantCommandOptions.sampleQcCommandOptions;

        // Build variant query from cli options
        AnnotationVariantQueryParams variantStatsQuery = ToolParams.fromParams(AnnotationVariantQueryParams.class,
                cliOptions.variantStatsQuery);

        // Build signature query from cli options
        SampleQcSignatureQueryParams signatureQuery = ToolParams.fromParams(SampleQcSignatureQueryParams.class, cliOptions.signatureQuery);

//        // Build list of genes from cli options
//        List<String> genesForCoverageStats = StringUtils.isEmpty(variantCommandOptions.sampleQcCommandOptions.genesForCoverageStats)
//                ? new ArrayList<>()
//                : Arrays.asList(variantCommandOptions.sampleQcCommandOptions.genesForCoverageStats.split(","));

        return openCGAClient.getVariantClient().runSampleQc(
                new SampleQcAnalysisParams(
                        variantCommandOptions.sampleQcCommandOptions.sample,
                        variantCommandOptions.sampleQcCommandOptions.variantStatsId,
                        variantCommandOptions.sampleQcCommandOptions.variantStatsDecription,
                        variantStatsQuery,
                        variantCommandOptions.sampleQcCommandOptions.signatureId,
                        signatureQuery,
                        //genesForCoverageStats,
                        null,
                        null,
                        variantCommandOptions.sampleQcCommandOptions.outdir
                ),
                getParams(variantCommandOptions.sampleQcCommandOptions.study)
        );
    }

    private RestResponse<Job> gwas() throws ClientException {
        return openCGAClient.getVariantClient().runGwas(
                new GwasAnalysisParams(
                        variantCommandOptions.gwasCommandOptions.phenotype,
                        variantCommandOptions.gwasCommandOptions.index,
                        variantCommandOptions.gwasCommandOptions.indexScoreId,
                        variantCommandOptions.gwasCommandOptions.method,
                        variantCommandOptions.gwasCommandOptions.fisherMode,
                        variantCommandOptions.gwasCommandOptions.caseCohort,
                        variantCommandOptions.gwasCommandOptions.caseCohortSamplesAnnotation,
                        variantCommandOptions.gwasCommandOptions.caseCohortSamples,
                        variantCommandOptions.gwasCommandOptions.controlCohort,
                        variantCommandOptions.gwasCommandOptions.controlCohortSamplesAnnotation,
                        variantCommandOptions.gwasCommandOptions.controlCohortSamples,
                        variantCommandOptions.gwasCommandOptions.outdir
                ), getParams(variantCommandOptions.gwasCommandOptions.study));
    }

    private RestResponse<Job> export() throws ClientException, IOException {
        VariantCommandOptions.VariantExportCommandOptions c = variantCommandOptions.exportVariantCommandOptions;

        List<String> studies = new ArrayList<>();
        if (cliSession != null && ListUtils.isNotEmpty(cliSession.getStudies())) {
            studies = cliSession.getStudies();
        }
        Query query = VariantQueryCommandUtils.parseQuery(c, studies, clientConfiguration);
        QueryOptions options = VariantQueryCommandUtils.parseQueryOptions(c);

        return openCGAClient.getVariantClient()
                .runExport(new VariantExportParams(
                        query,
                        c.outdir,
                        c.outputFileName,
                        c.commonOptions.outputFormat,
                        c.compress,
                        c.variantsFile), getParams(c.project, c.study).appendAll(options));
    }

    private RestResponse<Job> index() throws ClientException {
        VariantCommandOptions.VariantIndexCommandOptions variantIndex = variantCommandOptions.indexVariantCommandOptions;
        return openCGAClient.getVariantClient().runIndex(
                new VariantIndexParams(
                        variantIndex.fileId,
                        variantIndex.genericVariantIndexOptions.resume,
                        variantIndex.outdir,
                        variantIndex.genericVariantIndexOptions.transform,
                        variantIndex.genericVariantIndexOptions.gvcf,
                        variantIndex.genericVariantIndexOptions.normalizationSkip,
                        variantIndex.genericVariantIndexOptions.referenceGenome,
                        variantIndex.genericVariantIndexOptions.family,
                        variantIndex.genericVariantIndexOptions.somatic,
                        variantIndex.genericVariantIndexOptions.load,
                        variantIndex.genericVariantIndexOptions.loadSplitData,
                        variantIndex.genericVariantIndexOptions.loadMultiFileData,
                        variantIndex.genericVariantIndexOptions.loadSampleIndex,
                        variantIndex.genericVariantIndexOptions.loadArchive,
                        variantIndex.genericVariantIndexOptions.loadHomRef,
                        variantIndex.genericVariantIndexOptions.postLoadCheck,
                        variantIndex.genericVariantIndexOptions.excludeGenotype,
                        variantIndex.genericVariantIndexOptions.includeSampleData,
                        variantIndex.genericVariantIndexOptions.merge,
                        variantIndex.genericVariantIndexOptions.deduplicationPolicy,
                        variantIndex.genericVariantIndexOptions.calculateStats,
                        variantIndex.genericVariantIndexOptions.aggregated,
                        variantIndex.genericVariantIndexOptions.aggregationMappingFile,
                        variantIndex.genericVariantIndexOptions.annotate,
                        variantIndex.genericVariantIndexOptions.annotator,
                        variantIndex.genericVariantIndexOptions.overwriteAnnotations,
                        variantIndex.genericVariantIndexOptions.indexSearch,
                        variantIndex.skipIndexedFiles),
                getParams(variantIndex.study));
    }

    private RestResponse<Job> fileDelete() throws ClientException {
        VariantCommandOptions.VariantDeleteCommandOptions cliOptions = variantCommandOptions.variantDeleteCommandOptions;

        return openCGAClient.getVariantClient().deleteFile(
                new VariantFileDeleteParams(
                        cliOptions.genericVariantDeleteOptions.file,
                        cliOptions.genericVariantDeleteOptions.resume).toObjectMap()
                        .appendAll(getParams(cliOptions.study)));
    }

    private RestResponse query() throws CatalogException, ClientException, InterruptedException, IOException {
        logger.debug("Listing variants of a study.");

        VariantCommandOptions.VariantQueryCommandOptions queryCommandOptions = variantCommandOptions.queryVariantCommandOptions;

        queryCommandOptions.study = queryCommandOptions.study;
        queryCommandOptions.genericVariantQueryOptions.includeStudy = queryCommandOptions.genericVariantQueryOptions.includeStudy;

        List<String> studies = new ArrayList<>();
        if (cliSession != null && ListUtils.isNotEmpty(cliSession.getStudies())) {
            studies = cliSession.getStudies();
        }
        Query query = VariantQueryCommandUtils.parseQuery(queryCommandOptions, studies, clientConfiguration);
        QueryOptions options = VariantQueryCommandUtils.parseQueryOptions(queryCommandOptions);

        List<String> annotations = queryCommandOptions.genericVariantQueryOptions.annotations == null
                ? Collections.singletonList("gene")
                : Arrays.asList(queryCommandOptions.genericVariantQueryOptions.annotations.split(","));


        // Let's hide some STDOUT verbose messages from ManagedChannelImpl class
//        Logger.getLogger(ManagedChannelImpl.class.getName()).setLevel(java.util.logging.Level.WARNING);


        ObjectMap params = new ObjectMap(query);
        params.putAll(options);
        boolean grpc = usingGrpcMode(queryCommandOptions.mode);
        if (!grpc) {
            params.put(VariantQueryParam.SAMPLE_METADATA.key(), true);
            if (queryCommandOptions.commonOptions.outputFormat.equalsIgnoreCase("vcf")
                    || queryCommandOptions.commonOptions.outputFormat.equalsIgnoreCase("text")) {
                RestResponse<Variant> queryResponse = openCGAClient.getVariantClient().query(params);

                VcfOutputWriter vcfOutputWriter = initVcfOutputWriter(query, options, annotations);

                vcfOutputWriter.print(queryResponse);
                return null;
            } else {
                return openCGAClient.getVariantClient().query(params);
            }

        } else {
            ManagedChannel channel = getManagedChannel();

            // We use a blocking stub to execute the query to gRPC
            VariantServiceGrpc.VariantServiceBlockingStub variantServiceBlockingStub = VariantServiceGrpc.newBlockingStub(channel);

            query = VariantStorageManager.getVariantQuery(params);
            Map<String, String> queryMap = new HashMap<>();
            Map<String, String> queryOptionsMap = new HashMap<>();
            for (String key : params.keySet()) {
                if (query.containsKey(key)) {
                    queryMap.put(key, query.getString(key));
                } else {
                    queryOptionsMap.put(key, params.getString(key));
                }
            }

            // We create the OpenCGA gRPC request object with the query, queryOptions and sessionId
            GenericServiceModel.Request request = GenericServiceModel.Request.newBuilder()
                    .putAllQuery(queryMap)
                    .putAllOptions(queryOptionsMap)
                    .setSessionId(token == null ? "" : token)
                    .build();


            Iterator<VariantProto.Variant> variantIterator = variantServiceBlockingStub.get(request);
            if (queryCommandOptions.commonOptions.outputFormat.equalsIgnoreCase("vcf")
                    || queryCommandOptions.commonOptions.outputFormat.equalsIgnoreCase("text")) {
                options.put(QueryOptions.LIMIT, 1);

                VcfOutputWriter vcfOutputWriter = initVcfOutputWriter(query, options, annotations);

                vcfOutputWriter.print(variantIterator);
            } else {
                JsonFormat.Printer printer = JsonFormat.printer();
                try (PrintStream printStream = new PrintStream(System.out)) {
                    while (variantIterator.hasNext()) {
                        VariantProto.Variant next = variantIterator.next();
                        printStream.println(printer.print(next));
                    }
                }
            }

            channel.shutdown().awaitTermination(2, TimeUnit.SECONDS);
            return null;
        }
    }

    private VcfOutputWriter initVcfOutputWriter(Query query, QueryOptions options, List<String> annotations) throws ClientException {
        QueryOptions metadataQueryOptions = new QueryOptions(options);
        metadataQueryOptions.putAll(query);
        metadataQueryOptions.addToListOption(QueryOptions.EXCLUDE, "files");
        metadataQueryOptions.append("basic", true);
        VariantMetadata metadata = openCGAClient.getVariantClient().metadata(metadataQueryOptions).firstResult();
        return new VcfOutputWriter(metadata, annotations, System.out);
    }

    private boolean usingGrpcMode(String mode) {
        final boolean grpc;
        switch (mode.toUpperCase()) {
            case "AUTO":
                grpc = isGrpcAvailable() == null;
                if (grpc) {
                    logger.debug("Using gRPC mode");
                } else {
                    logger.debug("Using REST mode");
                }
                break;
            case "GRPC":
                RuntimeException exception = isGrpcAvailable();
                if (exception != null) {
                    throw exception;
                }
                grpc = true;
                break;
            case "REST":
                grpc = false;
                break;
            default:
                throw new IllegalArgumentException("Unknown mode " + mode);
        }
        return grpc;
    }

    protected synchronized ManagedChannel getManagedChannel() {
        if (channel == null) {
            // Connecting to the server host and port
            String grpcServerHost = clientConfiguration.getGrpc().getHost();
            logger.debug("Connecting to gRPC server at '{}'", grpcServerHost);
//            com.google.common.base.Preconditions
            // We create the gRPC channel to the specified server host and port
            channel = ManagedChannelBuilder.forTarget(grpcServerHost)
                    .usePlaintext(true)
                    .build();
        }
        return channel;
    }

    protected RuntimeException isGrpcAvailable() {
        // Connecting to the server host and port
        try {
            ManagedChannel channel = getManagedChannel();
            AdminServiceGrpc.AdminServiceBlockingStub stub = AdminServiceGrpc.newBlockingStub(channel);
            ServiceTypesModel.MapResponse status = stub.status(GenericServiceModel.Request.getDefaultInstance());
            return null;
        } catch (RuntimeException e) {
            return e;
        } catch (NoSuchMethodError | NoClassDefFoundError e) {
            return new RuntimeException(e);
        }
    }

    private List<String> getSamplesFromVariantQueryResult(VariantQueryResult<Variant> variantQueryResult, String study) {
        Map<String, List<String>> samplePerStudy = new HashMap<>();
        // Aggregated studies do not contain samples
        if (variantQueryResult.getSamples() != null) {
            // We have to remove the user and project from the Study name
            variantQueryResult.getSamples().forEach((st, sampleList) -> {
                String study1 = st.split(":")[1];
                samplePerStudy.put(study1, sampleList);
            });
        }

        // Prepare samples for the VCF header
        List<String> samples = null;
        if (StringUtils.isEmpty(study)) {
            if (samplePerStudy.size() == 1) {
                study = samplePerStudy.keySet().iterator().next();
                samples = samplePerStudy.get(study);
            }
        } else {
            if (study.contains(":")) {
                study = study.split(":")[1];
            }
            samples = samplePerStudy.get(study);
        }

        // TODO move this to biodata
        if (samples == null) {
            samples = new ArrayList<>();
        }
        return samples;
    }

    public RestResponse<VariantAnnotation> annotationQuery() throws ClientException {
        VariantCommandOptions.AnnotationQueryCommandOptions cliOptions = variantCommandOptions.annotationQueryCommandOptions;


        QueryOptions options = new QueryOptions();
        options.put(QueryOptions.LIMIT, cliOptions.limit);
        options.put(QueryOptions.SKIP, cliOptions.skip);
        options.put(QueryOptions.INCLUDE, cliOptions.dataModelOptions.include);
        options.put(QueryOptions.EXCLUDE, cliOptions.dataModelOptions.exclude);
        options.putAll(cliOptions.commonOptions.params);

        Query query = new Query();
        query.put(VariantCatalogQueryUtils.PROJECT.key(), cliOptions.project);
        query.put(VariantQueryParam.REGION.key(), cliOptions.region);
        query.put(VariantQueryParam.ID.key(), cliOptions.id);

        return openCGAClient.getVariantClient().queryAnnotation(options.appendAll(query).append("annotationId", cliOptions.annotationId));
    }

    public RestResponse<ObjectMap> annotationMetadata() throws ClientException {
        VariantCommandOptions.AnnotationMetadataCommandOptions cliOptions = variantCommandOptions.annotationMetadataCommandOptions;


        return openCGAClient.getVariantClient().metadataAnnotation(getParams(cliOptions.project, null).append("annotationId", cliOptions.annotationId));
    }

    //-------------------------------------------------------------------------
    // W R A P P E R S     A N A L Y S I S
    //-------------------------------------------------------------------------

    // Gatk

    private RestResponse<Job> gatk() throws ClientException {
        VariantCommandOptions.GatkCommandOptions cliOptions = variantCommandOptions.gatkCommandOptions;

        ObjectMap params = new ObjectMap(FileDBAdaptor.QueryParams.STUDY.key(), cliOptions.study);

        return openCGAClient.getVariantClient().runGatk(new GatkWrapperParams(cliOptions.command, cliOptions.outdir, cliOptions.gatkParams),
                params);
    }

    private RestResponse<Job> plink() throws ClientException {
        VariantCommandOptions.PlinkCommandOptions cliOptions = variantCommandOptions.plinkCommandOptions;

        ObjectMap params = new ObjectMap(FileDBAdaptor.QueryParams.STUDY.key(), cliOptions.study);

        return openCGAClient.getVariantClient().runPlink(new PlinkWrapperParams(cliOptions.outdir, cliOptions.plinkParams), params);
    }

    // RvTests

    private RestResponse<Job> rvtests() throws ClientException {
        VariantCommandOptions.RvtestsCommandOptions cliOptions = variantCommandOptions.rvtestsCommandOptions;

        ObjectMap params = new ObjectMap(FileDBAdaptor.QueryParams.STUDY.key(), cliOptions.study);

        return openCGAClient.getVariantClient().runRvtests(new RvtestsWrapperParams(cliOptions.command, cliOptions.outdir,
                        cliOptions.rvtestsParams), params);
    }

    private ObjectMap getParams(String study) {
        return getParams(null, study);
    }

    private ObjectMap getParams(String project, String study) {
        ObjectMap params = getCommonParams(project, study, variantCommandOptions.commonCommandOptions.params);
        addJobParams(variantCommandOptions.commonJobOptions, params);
        addNumericParams(variantCommandOptions.commonNumericOptions, params);
        return params;
    }
}
