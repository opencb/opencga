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
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.writer.VariantContextWriter;
import htsjdk.variant.vcf.VCFHeader;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.internal.ManagedChannelImpl;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.formats.variant.vcf4.VcfUtils;
import org.opencb.biodata.models.common.protobuf.service.ServiceTypesModel;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.metadata.VariantMetadata;
import org.opencb.biodata.models.variant.protobuf.VariantProto;
import org.opencb.biodata.tools.variant.converters.VariantContextToAvroVariantConverter;
import org.opencb.biodata.tools.variant.converters.VariantContextToProtoVariantConverter;
import org.opencb.biodata.tools.variant.converters.avro.VariantDatasetMetadataToVCFHeaderConverter;
import org.opencb.commons.datastore.core.*;
import org.opencb.opencga.app.cli.analysis.executors.VariantQueryCommandUtils;
import org.opencb.opencga.app.cli.analysis.options.VariantCommandOptions;
import org.opencb.opencga.app.cli.main.executors.OpencgaCommandExecutor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.results.VariantQueryResult;
import org.opencb.opencga.server.grpc.AdminServiceGrpc;
import org.opencb.opencga.server.grpc.GenericServiceModel;
import org.opencb.opencga.server.grpc.VariantServiceGrpc;
import org.opencb.opencga.storage.core.manager.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;

import java.io.IOException;
import java.io.PrintStream;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

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
        QueryResponse queryResponse = null;
        switch (subCommandString) {
            case "index":
                queryResponse = index();
                break;
            case "query":
                queryResponse = query();
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }

//        ObjectMapper objectMapper = new ObjectMapper();
//        System.out.println(objectMapper.writeValueAsString(queryResponse.getResponse()));

        createOutput(queryResponse);

    }

    private QueryResponse index() throws CatalogException, IOException {
        logger.debug("Indexing variant(s)");

        String fileIds = variantCommandOptions.indexVariantCommandOptions.fileId;

        ObjectMap o = new ObjectMap();
        o.putIfNotNull(VariantStorageEngine.Options.STUDY_ID.key(), variantCommandOptions.indexVariantCommandOptions.study);
        o.putIfNotNull("outDir", variantCommandOptions.indexVariantCommandOptions.outdir);
        o.putIfNotNull("transform", variantCommandOptions.indexVariantCommandOptions.genericVariantIndexOptions.transform);
        o.putIfNotNull("load", variantCommandOptions.indexVariantCommandOptions.genericVariantIndexOptions.load);
        o.putIfNotNull(VariantStorageEngine.Options.EXCLUDE_GENOTYPES.key(), variantCommandOptions.indexVariantCommandOptions.genericVariantIndexOptions.excludeGenotype);
        o.putIfNotNull("includeExtraFields", variantCommandOptions.indexVariantCommandOptions.genericVariantIndexOptions.extraFields);
        o.putIfNotNull("merge", variantCommandOptions.indexVariantCommandOptions.genericVariantIndexOptions.merge);
        o.putIfNotNull("aggregated", variantCommandOptions.indexVariantCommandOptions.genericVariantIndexOptions.aggregated);
        o.putIfNotNull(VariantStorageEngine.Options.CALCULATE_STATS.key(), variantCommandOptions.indexVariantCommandOptions.genericVariantIndexOptions.calculateStats);
        o.putIfNotNull(VariantStorageEngine.Options.ANNOTATE.key(), variantCommandOptions.indexVariantCommandOptions.genericVariantIndexOptions.annotate);
        o.putIfNotNull(VariantStorageEngine.Options.RESUME.key(), variantCommandOptions.indexVariantCommandOptions.genericVariantIndexOptions.resume);
        o.putIfNotNull(VariantAnnotationManager.OVERWRITE_ANNOTATIONS, variantCommandOptions.indexVariantCommandOptions.genericVariantIndexOptions.overwriteAnnotations);
        o.putAll(variantCommandOptions.commonCommandOptions.params);

//        return openCGAClient.getFileClient().index(fileIds, o);
        return openCGAClient.getVariantClient().index(fileIds, o);
    }

    private QueryResponse query() throws CatalogException, IOException, InterruptedException {
        logger.debug("Listing variants of a study.");

        VariantCommandOptions.VariantQueryCommandOptions queryCommandOptions = variantCommandOptions.queryVariantCommandOptions;

        String study = resolveStudy(queryCommandOptions.study);

        List<String> studies = new ArrayList<>();
        for (Map.Entry<String, List<String>> entry : cliSession.getProjectsAndStudies().entrySet()) {
            for (String s : entry.getValue()) {
                studies.add(entry.getKey() + ':' + s);
            }
        }
        Query query = VariantQueryCommandUtils.parseQuery(queryCommandOptions, studies);
        QueryOptions options = VariantQueryCommandUtils.parseQueryOptions(queryCommandOptions);

        options.putIfNotEmpty("groupBy", queryCommandOptions.genericVariantQueryOptions.groupBy);
        options.put("histogram", queryCommandOptions.genericVariantQueryOptions.histogram);
        options.put("interval", queryCommandOptions.genericVariantQueryOptions.interval);

        List<String> annotations = queryCommandOptions.genericVariantQueryOptions.annotations == null
                ? Collections.singletonList("gene")
                : Arrays.asList(queryCommandOptions.genericVariantQueryOptions.annotations.split(","));


        // Let's hide some STDOUT verbose messages from ManagedChannelImpl class
        Logger.getLogger(ManagedChannelImpl.class.getName()).setLevel(java.util.logging.Level.WARNING);


        boolean grpc = usingGrpcMode(queryCommandOptions.mode);
        if (!grpc) {
            ObjectMap params = new ObjectMap(query);

            if (queryCommandOptions.numericOptions.count) {
                return openCGAClient.getVariantClient().count(params, options);
            } else if (StringUtils.isNoneEmpty(queryCommandOptions.genericVariantQueryOptions.groupBy)
                    || queryCommandOptions.genericVariantQueryOptions.histogram) {
                return openCGAClient.getVariantClient().genericQuery(params, options);
            } else {
                options.put(QueryOptions.SKIP_COUNT, true);
                params.put(VariantQueryParam.SAMPLES_METADATA.key(), true);
                if (queryCommandOptions.commonOptions.outputFormat.equalsIgnoreCase("vcf")
                        || queryCommandOptions.commonOptions.outputFormat.equalsIgnoreCase("text")) {
                    VariantQueryResult<Variant> variantQueryResult = openCGAClient.getVariantClient().query2(params, options);

                    List<String> samples = getSamplesFromVariantQueryResult(variantQueryResult, study);
                    printVcf(variantQueryResult, null, study, samples, annotations, System.out);
                    return null;
                } else {
                    return openCGAClient.getVariantClient().query(params, options);
                }
            }
        } else {
            ManagedChannel channel = getManagedChannel();

            // We use a blocking stub to execute the query to gRPC
            VariantServiceGrpc.VariantServiceBlockingStub variantServiceBlockingStub = VariantServiceGrpc.newBlockingStub(channel);

            ObjectMap params = new ObjectMap(query);
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
                    .setSessionId(sessionId == null ? "" : sessionId)
                    .build();

            QueryResponse queryResponse = null;
            if (queryCommandOptions.numericOptions.count) {
                ServiceTypesModel.LongResponse countResponse = variantServiceBlockingStub.count(request);
                ServiceTypesModel.Response response = countResponse.getResponse();
                queryResponse = new QueryResponse<>("", 0, response.getWarning(), response.getError(), new QueryOptions(params),
                        Collections.singletonList(
                                new QueryResult<>(response.getId(), 0, 1, 1, "", "", Collections.singletonList(countResponse.getValue()))));
                return queryResponse;
            } else if (queryCommandOptions.genericVariantQueryOptions.samplesMetadata || StringUtils.isNoneEmpty(queryCommandOptions.genericVariantQueryOptions.groupBy) || queryCommandOptions.genericVariantQueryOptions.histogram) {
                queryResponse = openCGAClient.getVariantClient().genericQuery(params, options);
            } else {
                Iterator<VariantProto.Variant> variantIterator = variantServiceBlockingStub.get(request);
                if (queryCommandOptions.commonOptions.outputFormat.equalsIgnoreCase("vcf")
                        || queryCommandOptions.commonOptions.outputFormat.equalsIgnoreCase("text")) {
                    options.put(QueryOptions.SKIP_COUNT, true);
                    options.put(QueryOptions.LIMIT, 1);
                    VariantQueryResult<Variant> variantQueryResult = openCGAClient.getVariantClient().query2(params, options);

                    List<String> samples = getSamplesFromVariantQueryResult(variantQueryResult, study);
                    printVcf(null, variantIterator, study, samples, annotations, System.out);
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
                    logger.debug("Using GRPC mode");
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
        }
    }

    private void printVcf(VariantQueryResult<Variant> variantQueryResult, Iterator<VariantProto.Variant> variantIterator, String study,
                          List<String> samples, List<String> annotations, PrintStream outputStream) throws CatalogException, IOException {
//        logger.debug("Samples from variantQueryResult: {}", variantQueryResult.getSamples());
//
//        Map<String, List<String>> samplePerStudy = new HashMap<>();
//        // Aggregated studies do not contain samples
//        if (variantQueryResult.getSamples() != null) {
//            // We have to remove the user and project from the Study name
//            variantQueryResult.getSamples().forEach((st, sampleList) -> {
//                String study1 = st.split(":")[1];
//                samplePerStudy.put(study1, sampleList);
//            });
//        }
//
//        // Prepare samples for the VCF header
//        List<String> samples = null;
//        if (StringUtils.isEmpty(study)) {
//            if (samplePerStudy.size() == 1) {
//                study = samplePerStudy.keySet().iterator().next();
//                samples = samplePerStudy.get(study);
//            }
//        } else {
//            if (study.contains(":")) {
//                study = study.split(":")[1];
//            } else {
//                if (clientConfiguration.getAlias() != null && clientConfiguration.getAlias().get(study) != null) {
//                    study = clientConfiguration.getAlias().get(study);
//                    if (study.contains(":")) {
//                        study = study.split(":")[1];
//                    }
//                }
//            }
//            samples = samplePerStudy.get(study);
//        }
//
//        // TODO move this to biodata
//        if (samples == null) {
//            samples = new ArrayList<>();
//        }

        // Prepare other VCF fields
        List<String> cohorts = new ArrayList<>(); // Arrays.asList("ALL", "MXL");
        List<String> formats = new ArrayList<>();
        List<String> formatTypes = new ArrayList<>();
        List<Integer> formatArities = new ArrayList<>();
        List<String> formatDescriptions = new ArrayList<>();

        if (clientConfiguration.getVariant() != null && clientConfiguration.getVariant().getIncludeFormats() != null) {
            String studyConfigAlias = null;
            if (clientConfiguration.getVariant().getIncludeFormats().get(study) != null) {
                studyConfigAlias = study;
            } else {
                // Search for the study alias
                if (clientConfiguration.getAlias() != null) {
                    for (Map.Entry<String, String> stringStringEntry : clientConfiguration.getAlias().entrySet()) {
                        if (stringStringEntry.getValue().contains(study)) {
                            studyConfigAlias = stringStringEntry.getKey();
                            logger.debug("Updating study name by alias (key) when including formats: from " + study + " to " + studyConfigAlias);
                            break;
                        }
                    }
                }
            }

            // create format arrays (names, types, arities, descriptions)
            String formatFields = clientConfiguration.getVariant().getIncludeFormats().get(studyConfigAlias);
            if (formatFields != null) {
                String[] fields = formatFields.split(",");
                for (String field : fields) {
                    String[] subfields = field.split(":");
                    if (subfields.length == 4) {
                        formats.add(subfields[0]);
                        formatTypes.add(subfields[1]);
                        if (StringUtils.isEmpty(subfields[2]) || !StringUtils.isNumeric(subfields[2])) {
                            formatArities.add(1);
                            logger.debug("Invalid arity for format " + subfields[0] + ", updating arity to 1");
                        } else {
                            formatArities.add(Integer.parseInt(subfields[2]));
                        }
                        formatDescriptions.add(subfields[3]);
                    } else {
                        // We do not need the extra information fields for "GT", "AD", "DP", "GQ", "PL".
                        formats.add(subfields[0]);
                        formatTypes.add("");
                        formatArities.add(0);
                        formatDescriptions.add("");
                    }
                }
            } else {
                logger.debug("No formats found for: {}, setting default format: {}", study, VcfUtils.DEFAULT_SAMPLE_FORMAT);
                formats = VcfUtils.DEFAULT_SAMPLE_FORMAT;
            }
        } else {
            logger.debug("No formats found for: {}, setting default format: {}", study, VcfUtils.DEFAULT_SAMPLE_FORMAT);
            formats = VcfUtils.DEFAULT_SAMPLE_FORMAT;
        }

        VariantMetadata metadata = openCGAClient.getVariantClient().metadata(new ObjectMap(VariantQueryParam.STUDIES.key(), study)
                .append(VariantQueryParam.SAMPLES.key(), samples), new QueryOptions(QueryOptions.EXCLUDE, "files")).firstResult();

        VCFHeader vcfHeader = new VariantDatasetMetadataToVCFHeaderConverter().convert(metadata.getDatasets().get(0), annotations);
        VariantContextWriter variantContextWriter = VcfUtils.createVariantContextWriter(outputStream, vcfHeader.getSequenceDictionary(), null);
        variantContextWriter.writeHeader(vcfHeader);

        if (variantQueryResult != null) {
            VariantContextToAvroVariantConverter variantContextToAvroVariantConverter = new VariantContextToAvroVariantConverter(study, samples, formats, annotations);
            for (Variant variant : variantQueryResult.getResult()) {
                // FIXME: This should not be needed! VariantContextToAvroVariantConverter must be fixed
                if (variant.getStudies().isEmpty()) {
                    StudyEntry studyEntry = new StudyEntry(study);
                    studyEntry.getFiles().add(new FileEntry("", null, Collections.emptyMap()));
                    variant.addStudyEntry(studyEntry);
                }

                VariantContext variantContext = variantContextToAvroVariantConverter.from(variant);
                variantContextWriter.add(variantContext);
            }
        } else {
            VariantContextToProtoVariantConverter variantContextToProtoVariantConverter = new VariantContextToProtoVariantConverter(study, samples, formats, annotations);
                while (variantIterator.hasNext()) {
                    VariantProto.Variant next = variantIterator.next();
                    variantContextWriter.add(variantContextToProtoVariantConverter.from(next));
                }
        }
        variantContextWriter.close();
        outputStream.close();
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
}
