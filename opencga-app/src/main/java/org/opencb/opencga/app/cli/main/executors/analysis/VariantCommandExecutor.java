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
import org.opencb.commons.datastore.core.Event;
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
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.api.operations.variant.VariantFileDeleteParams;
import org.opencb.opencga.core.api.variant.*;
import org.opencb.opencga.core.models.Job;
import org.opencb.opencga.core.response.RestResponse;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.response.VariantQueryResult;
import org.opencb.opencga.server.grpc.AdminServiceGrpc;
import org.opencb.opencga.server.grpc.GenericServiceModel;
import org.opencb.opencga.server.grpc.VariantServiceGrpc;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;

import java.io.IOException;
import java.io.PrintStream;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.CohortVariantStatsCommandOptions.COHORT_VARIANT_STATS_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.CohortVariantStatsQueryCommandOptions.COHORT_VARIANT_STATS_QUERY_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.GatkCommandOptions.GATK_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.GwasCommandOptions.GWAS_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.PlinkCommandOptions.PLINK_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.RvtestsCommandOptions.RVTEST_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.SampleVariantStatsCommandOptions.SAMPLE_VARIANT_STATS_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.SampleVariantStatsQueryCommandOptions.SAMPLE_VARIANT_STATS_QUERY_COMMAND;
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

            case "index":
                queryResponse = index();
                break;
            case VARIANT_DELETE_COMMAND:
                queryResponse = fileDelete();
                break;
            case "query":
                queryResponse = query();
                break;
            case "export":
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
//            case "family-stats":
//                queryResponse = familyStats();
//                break;
//            case "family-stats-query":
//                queryResponse = familyStatsQuery();
//                break;
            case GWAS_RUN_COMMAND:
                queryResponse = gwas();
                break;

            case PLINK_RUN_COMMAND:
                queryResponse = plink();
                break;

            case RVTEST_RUN_COMMAND:
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

    private RestResponse stats() throws IOException {
        return openCGAClient.getVariantClient().statsRun(variantCommandOptions.statsVariantCommandOptions.study,
                new VariantStatsAnalysisParams(
                        variantCommandOptions.statsVariantCommandOptions.cohort,
                        variantCommandOptions.statsVariantCommandOptions.samples,
                        variantCommandOptions.statsVariantCommandOptions.index,
                        variantCommandOptions.statsVariantCommandOptions.outdir,
                        variantCommandOptions.statsVariantCommandOptions.genericVariantStatsOptions.fileName,
                        variantCommandOptions.statsVariantCommandOptions.genericVariantStatsOptions.region,
                        variantCommandOptions.statsVariantCommandOptions.genericVariantStatsOptions.gene,
                        variantCommandOptions.statsVariantCommandOptions.genericVariantStatsOptions.overwriteStats,
                        variantCommandOptions.statsVariantCommandOptions.genericVariantStatsOptions.updateStats,
                        variantCommandOptions.statsVariantCommandOptions.genericVariantStatsOptions.resume,
                        variantCommandOptions.statsVariantCommandOptions.genericVariantStatsOptions.aggregated,
                        variantCommandOptions.statsVariantCommandOptions.genericVariantStatsOptions.aggregationMappingFile
                ));
    }

    private RestResponse<Job> sampleRun() throws IOException {
        return openCGAClient.getVariantClient().sampleRun(
                variantCommandOptions.samplesFilterCommandOptions.toolParams.getStudy(),
                variantCommandOptions.samplesFilterCommandOptions.toolParams
        );
    }

    private RestResponse<Variant> sampleQuery() throws IOException {
        QueryOptions options = new QueryOptions();
        options.putAll(variantCommandOptions.sampleQueryCommandOptions.commonOptions.params);

        return openCGAClient.getVariantClient().sampleQuery(
                variantCommandOptions.sampleQueryCommandOptions.variant,
                variantCommandOptions.sampleQueryCommandOptions.study,
                variantCommandOptions.sampleQueryCommandOptions.genotype,
                variantCommandOptions.sampleQueryCommandOptions.numericOptions.limit,
                variantCommandOptions.sampleQueryCommandOptions.numericOptions.skip,
                options);
    }

    private RestResponse<Job> sampleStats() throws IOException {
        return openCGAClient.getVariantClient().sampleStatsRun(variantCommandOptions.sampleVariantStatsCommandOptions.study,
                new SampleVariantStatsAnalysisParams(
                        variantCommandOptions.sampleVariantStatsCommandOptions.sample,
                        variantCommandOptions.sampleVariantStatsCommandOptions.family,
                        variantCommandOptions.sampleVariantStatsCommandOptions.index,
                        variantCommandOptions.sampleVariantStatsCommandOptions.samplesAnnotation,
                        variantCommandOptions.sampleVariantStatsCommandOptions.outdir
                ));
    }

    private RestResponse<SampleVariantStats> sampleStatsQuery() throws IOException {
        return openCGAClient.getVariantClient()
                .sampleStatsInfo(variantCommandOptions.sampleVariantStatsQueryCommandOptions.study,
                        variantCommandOptions.sampleVariantStatsQueryCommandOptions.sample);
    }

    private RestResponse<Job> cohortStats() throws IOException {
        return openCGAClient.getVariantClient().cohortStatsRun(variantCommandOptions.cohortVariantStatsCommandOptions.study,
                new CohortVariantStatsAnalysisParams(
                        variantCommandOptions.cohortVariantStatsCommandOptions.cohort,
                        variantCommandOptions.cohortVariantStatsCommandOptions.samples,
                        variantCommandOptions.cohortVariantStatsCommandOptions.index,
                        variantCommandOptions.cohortVariantStatsCommandOptions.samplesAnnotation,
                        variantCommandOptions.cohortVariantStatsCommandOptions.outdir
                ));
    }

    private RestResponse<VariantSetStats> cohortStatsQuery() throws IOException {
        return openCGAClient.getVariantClient().cohortStatsInfo(
                variantCommandOptions.cohortVariantStatsQueryCommandOptions.study,
                variantCommandOptions.cohortVariantStatsQueryCommandOptions.cohort
        );
    }

//    private RestResponse<Job> familyStats() {
//        openCGAClient.getVariantClient().familyStats()
//    }
//
//    private RestResponse<Job> familyStatsQuery() {
//        openCGAClient.getVariantClient().familyStatsQuery()
//    }

    private RestResponse<Job> gwas() throws IOException {
        return openCGAClient.getVariantClient().gwasRun(variantCommandOptions.gwasCommandOptions.study,
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
                ));
    }

    private List<String> asList(String s) {
        return StringUtils.isEmpty(s)
                ? Collections.emptyList()
                : Arrays.asList(s.split(","));
    }

    private RestResponse<Job> export() throws IOException {
        VariantCommandOptions.VariantExportCommandOptions c = variantCommandOptions.exportVariantCommandOptions;

        c.study = resolveStudy(c.study);
        c.genericVariantQueryOptions.includeStudy = resolveStudy(c.genericVariantQueryOptions.includeStudy);

        List<String> studies = new ArrayList<>();
        if (cliSession != null && ListUtils.isNotEmpty(cliSession.getStudies())) {
            studies = cliSession.getStudies();
        }
        Query query = VariantQueryCommandUtils.parseQuery(c, studies, clientConfiguration);
        QueryOptions options = VariantQueryCommandUtils.parseQueryOptions(c);
        String study = query.getString(VariantQueryParam.STUDY.key());
        return openCGAClient.getVariantClient()
                .export(study, query, options, c.outdir, c.outputFileName, c.commonOptions.outputFormat, c.compress);
    }

    private RestResponse<Job> index() throws IOException {
        VariantCommandOptions.VariantIndexCommandOptions variantIndex = variantCommandOptions.indexVariantCommandOptions;
        return openCGAClient.getVariantClient().index(variantIndex.study,
                new VariantIndexParams(
                        variantIndex.fileId,
                        variantIndex.genericVariantIndexOptions.resume,
                        variantIndex.outdir,
                        variantIndex.genericVariantIndexOptions.transform,
                        variantIndex.genericVariantIndexOptions.gvcf,
                        variantIndex.genericVariantIndexOptions.load,
                        variantIndex.genericVariantIndexOptions.loadSplitData,
                        variantIndex.genericVariantIndexOptions.skipPostLoadCheck,
                        variantIndex.genericVariantIndexOptions.excludeGenotype,
                        variantIndex.genericVariantIndexOptions.includeExtraFields,
                        variantIndex.genericVariantIndexOptions.merge,
                        variantIndex.genericVariantIndexOptions.calculateStats,
                        variantIndex.genericVariantIndexOptions.aggregated,
                        variantIndex.genericVariantIndexOptions.aggregationMappingFile,
                        variantIndex.genericVariantIndexOptions.annotate,
                        variantIndex.genericVariantIndexOptions.annotator,
                        variantIndex.genericVariantIndexOptions.overwriteAnnotations,
                        variantIndex.genericVariantIndexOptions.indexSearch
                ));
    }

    private RestResponse<Job> fileDelete() throws IOException {
        VariantCommandOptions.VariantDeleteCommandOptions cliOptions = variantCommandOptions.variantDeleteCommandOptions;

        return openCGAClient.getVariantClient().fileDelete(
                cliOptions.study,
                new VariantFileDeleteParams(
                        cliOptions.genericVariantDeleteOptions.file,
                        cliOptions.genericVariantDeleteOptions.resume));
    }

    private RestResponse query() throws CatalogException, IOException, InterruptedException {
        logger.debug("Listing variants of a study.");

        VariantCommandOptions.VariantQueryCommandOptions queryCommandOptions = variantCommandOptions.queryVariantCommandOptions;

        queryCommandOptions.study = resolveStudy(queryCommandOptions.study);
        queryCommandOptions.genericVariantQueryOptions.includeStudy = resolveStudy(queryCommandOptions.genericVariantQueryOptions.includeStudy);

        List<String> studies = new ArrayList<>();
        if (cliSession != null && ListUtils.isNotEmpty(cliSession.getStudies())) {
            studies = cliSession.getStudies();
        }
        Query query = VariantQueryCommandUtils.parseQuery(queryCommandOptions, studies, clientConfiguration);
        QueryOptions options = VariantQueryCommandUtils.parseQueryOptions(queryCommandOptions);

        options.putIfNotEmpty("groupBy", queryCommandOptions.genericVariantQueryOptions.groupBy);
        options.put("histogram", queryCommandOptions.genericVariantQueryOptions.histogram);
        options.put("interval", queryCommandOptions.genericVariantQueryOptions.interval);
        options.put("rank", queryCommandOptions.genericVariantQueryOptions.rank);

        List<String> annotations = queryCommandOptions.genericVariantQueryOptions.annotations == null
                ? Collections.singletonList("gene")
                : Arrays.asList(queryCommandOptions.genericVariantQueryOptions.annotations.split(","));


        // Let's hide some STDOUT verbose messages from ManagedChannelImpl class
//        Logger.getLogger(ManagedChannelImpl.class.getName()).setLevel(java.util.logging.Level.WARNING);


        ObjectMap params = new ObjectMap(query);
        QueryOptions metadataQueryOptions = new QueryOptions(options);
        metadataQueryOptions.addToListOption(QueryOptions.EXCLUDE, "files");
        metadataQueryOptions.append("basic", true);
        VariantMetadata metadata = openCGAClient.getVariantClient().metadata(params, metadataQueryOptions).firstResult();
        VcfOutputWriter vcfOutputWriter = new VcfOutputWriter(metadata, annotations, System.out);

        boolean grpc = usingGrpcMode(queryCommandOptions.mode);
        if (!grpc) {

            if (queryCommandOptions.numericOptions.count) {
                return openCGAClient.getVariantClient().count(params, options);
            } else if (StringUtils.isNoneEmpty(queryCommandOptions.genericVariantQueryOptions.groupBy)
                    || queryCommandOptions.genericVariantQueryOptions.histogram
                    || StringUtils.isNoneEmpty(queryCommandOptions.genericVariantQueryOptions.rank)) {
                return openCGAClient.getVariantClient().genericQuery(params, options);
            } else {
                options.put(QueryOptions.SKIP_COUNT, true);
                params.put(VariantQueryParam.SAMPLE_METADATA.key(), true);
                if (queryCommandOptions.commonOptions.outputFormat.equalsIgnoreCase("vcf")
                        || queryCommandOptions.commonOptions.outputFormat.equalsIgnoreCase("text")) {
                    RestResponse<Variant> queryResponse = openCGAClient.getVariantClient().query(params, options);

                    vcfOutputWriter.print(queryResponse);
                    return null;
                } else {
                    return openCGAClient.getVariantClient().query(params, options);
                }
            }
        } else {
            ManagedChannel channel = getManagedChannel();

            // We use a blocking stub to execute the query to gRPC
            VariantServiceGrpc.VariantServiceBlockingStub variantServiceBlockingStub = VariantServiceGrpc.newBlockingStub(channel);

            params.putAll(options);
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

            RestResponse queryResponse = null;
            if (queryCommandOptions.numericOptions.count) {
                ServiceTypesModel.LongResponse countResponse = variantServiceBlockingStub.count(request);
                ServiceTypesModel.Response response = countResponse.getResponse();
                List<Event> events = new ArrayList<>();
                if (StringUtils.isNotEmpty(response.getWarning())) {
                    events.add(new Event(Event.Type.WARNING, response.getWarning()));
                }
                if (StringUtils.isNotEmpty(response.getError())) {
                    events.add(new Event(Event.Type.ERROR, response.getError()));
                }
                queryResponse = new RestResponse<>("", 0, events, new ObjectMap(params), Collections.singletonList(
                                new OpenCGAResult<>(0, Collections.emptyList(), 1, Collections.singletonList(countResponse.getValue()), 1)));
                return queryResponse;
            } else if (queryCommandOptions.genericVariantQueryOptions.samplesMetadata || StringUtils.isNoneEmpty(queryCommandOptions.genericVariantQueryOptions.groupBy) || queryCommandOptions.genericVariantQueryOptions.histogram) {
                queryResponse = openCGAClient.getVariantClient().genericQuery(params, options);
            } else {
                Iterator<VariantProto.Variant> variantIterator = variantServiceBlockingStub.get(request);
                if (queryCommandOptions.commonOptions.outputFormat.equalsIgnoreCase("vcf")
                        || queryCommandOptions.commonOptions.outputFormat.equalsIgnoreCase("text")) {
                    options.put(QueryOptions.SKIP_COUNT, true);
                    options.put(QueryOptions.LIMIT, 1);

                    vcfOutputWriter.print(variantIterator);
                } else {
                    JsonFormat.Printer printer = JsonFormat.printer();
                    try (PrintStream printStream = new PrintStream(System.out)) {
                        while (variantIterator.hasNext()) {
                            VariantProto.Variant next = variantIterator.next();
                            printStream.println(printer.print(next));
                        }
                    }
                    queryResponse = null;
                }
            }
            channel.shutdown().awaitTermination(2, TimeUnit.SECONDS);
            return queryResponse;
        }
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
            } else {
                if (clientConfiguration.getAlias() != null && clientConfiguration.getAlias().get(study) != null) {
                    study = clientConfiguration.getAlias().get(study);
                    if (study.contains(":")) {
                        study = study.split(":")[1];
                    }
                }
            }
            samples = samplePerStudy.get(study);
        }

        // TODO move this to biodata
        if (samples == null) {
            samples = new ArrayList<>();
        }
        return samples;
    }

    @Override
    protected String resolveStudy(String study) {
        if (StringUtils.isEmpty(study)) {
            if (StringUtils.isNotEmpty(clientConfiguration.getDefaultStudy())) {
                return clientConfiguration.getDefaultStudy();
            }
        } else {
            // study is not empty, let's check if it is an alias
            if (clientConfiguration.getAlias() != null && clientConfiguration.getAlias().size() > 0) {
                VariantQueryUtils.QueryOperation queryOperation = VariantQueryUtils.checkOperator(study);
                if (queryOperation == null) {
                    queryOperation = VariantQueryUtils.QueryOperation.AND;
                }
                List<String> studies = VariantQueryUtils.splitValue(study, queryOperation);
                List<String> studyList = new ArrayList<>(studies.size());
                for (String s : studies) {
                    boolean negated = VariantQueryUtils.isNegated(s);
                    if (negated) {
                        // Remove negation to search alias
                        s = VariantQueryUtils.removeNegation(s);
                    }
                    if (clientConfiguration.getAlias().containsKey(s)) {
                        s = clientConfiguration.getAlias().get(study);
                    }
                    if (negated) {
                        // restore negation
                        s = VariantQueryUtils.NOT + s;
                    }
                    studyList.add(s);
                }
                return StringUtils.join(studyList, queryOperation.separator());
            }
        }
        return study;
    }

    public RestResponse<VariantAnnotation> annotationQuery() throws IOException {
        VariantCommandOptions.AnnotationQueryCommandOptions cliOptions = variantCommandOptions.annotationQueryCommandOptions;


        QueryOptions options = new QueryOptions();
        options.put(QueryOptions.LIMIT, cliOptions.limit);
        options.put(QueryOptions.SKIP, cliOptions.skip);
        options.put(QueryOptions.INCLUDE, cliOptions.dataModelOptions.include);
        options.put(QueryOptions.EXCLUDE, cliOptions.dataModelOptions.exclude);
        options.put(QueryOptions.SKIP, cliOptions.skip);
        options.putAll(cliOptions.commonOptions.params);

        Query query = new Query();
        query.put(VariantCatalogQueryUtils.PROJECT.key(), cliOptions.project);
        query.put(VariantQueryParam.REGION.key(), cliOptions.region);
        query.put(VariantQueryParam.ID.key(), cliOptions.id);

        return openCGAClient.getVariantClient().annotationQuery(cliOptions.annotationId, query, options);
    }

    public RestResponse<ObjectMap> annotationMetadata() throws IOException {
        VariantCommandOptions.AnnotationMetadataCommandOptions cliOptions = variantCommandOptions.annotationMetadataCommandOptions;

        QueryOptions options = new QueryOptions();
        options.putAll(cliOptions.commonOptions.params);

        return openCGAClient.getVariantClient().annotationMetadata(cliOptions.annotationId, cliOptions.project, options);
    }

    // Wrappers

    private RestResponse<Job> plink() throws IOException {
        return openCGAClient.getVariantClient().plinkRun(variantCommandOptions.plinkCommandOptions.study,
                new PlinkRunParams(
                        variantCommandOptions.plinkCommandOptions.tpedFile,
                        variantCommandOptions.plinkCommandOptions.tfamFile,
                        variantCommandOptions.plinkCommandOptions.covarFile,
                        variantCommandOptions.plinkCommandOptions.outdir,
                        variantCommandOptions.plinkCommandOptions.basicOptions.params
                ));
    }

    private RestResponse<Job> rvtests() throws IOException {
        return openCGAClient.getVariantClient().rvtestsRun(variantCommandOptions.rvtestsCommandOptions.study,
                new RvtestsRunParams(
                        variantCommandOptions.rvtestsCommandOptions.executable,
                        variantCommandOptions.rvtestsCommandOptions.vcfFile,
                        variantCommandOptions.rvtestsCommandOptions.phenoFile,
                        variantCommandOptions.rvtestsCommandOptions.pedigreeFile,
                        variantCommandOptions.rvtestsCommandOptions.kinshipFile,
                        variantCommandOptions.rvtestsCommandOptions.covarFile,
                        variantCommandOptions.rvtestsCommandOptions.outdir,
                        variantCommandOptions.rvtestsCommandOptions.basicOptions.params
                ));
    }


    private RestResponse<Job> gatk() throws IOException {
        return openCGAClient.getVariantClient().gatkRun(variantCommandOptions.gatkCommandOptions.study,
                new GatkRunParams(
                        variantCommandOptions.gatkCommandOptions.command,
                        variantCommandOptions.gatkCommandOptions.fastaFile,
                        variantCommandOptions.gatkCommandOptions.bamFile,
                        variantCommandOptions.gatkCommandOptions.vcfFilename,
                        variantCommandOptions.gatkCommandOptions.outdir,
                        variantCommandOptions.gatkCommandOptions.basicOptions.params
                ));
    }
}
