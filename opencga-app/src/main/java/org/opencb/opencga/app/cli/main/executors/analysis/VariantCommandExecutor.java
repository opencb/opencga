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
import org.opencb.biodata.models.variant.protobuf.VariantProto;
import org.opencb.biodata.tools.variant.converters.VariantContextToAvroVariantConverter;
import org.opencb.biodata.tools.variant.converters.VariantContextToProtoVariantConverter;
import org.opencb.commons.datastore.core.*;
import org.opencb.opencga.app.cli.analysis.options.VariantCommandOptions;
import org.opencb.opencga.app.cli.main.executors.OpencgaCommandExecutor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.results.VariantQueryResult;
import org.opencb.opencga.server.grpc.AdminServiceGrpc;
import org.opencb.opencga.server.grpc.GenericServiceModel;
import org.opencb.opencga.server.grpc.VariantServiceGrpc;
import org.opencb.opencga.storage.core.manager.variant.VariantCatalogQueryUtils;
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

        ObjectMap params = new ObjectMap();
        params.putIfNotNull(VariantQueryParam.ID.key(), queryCommandOptions.genericVariantQueryOptions.id);
        params.putIfNotEmpty(VariantQueryParam.REGION.key(), queryCommandOptions.genericVariantQueryOptions.region);
//        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.CHROMOSOME.key(),
//                queryCommandOptions.queryVariantsOptions.chromosome);
        params.putIfNotEmpty(VariantQueryParam.GENE.key(), queryCommandOptions.genericVariantQueryOptions.gene);
        params.putIfNotNull(VariantQueryParam.TYPE.key(), queryCommandOptions.genericVariantQueryOptions.type);
//        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.REFERENCE.key(), queryCommandOptions.queryVariantsOptions.reference);
//        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ALTERNATE.key(), queryCommandOptions.queryVariantsOptions.alternate);
        params.putIfNotEmpty(VariantQueryParam.RETURNED_STUDIES.key(), queryCommandOptions.genericVariantQueryOptions.returnStudy);
        params.putIfNotEmpty(VariantQueryParam.RETURNED_SAMPLES.key(), queryCommandOptions.genericVariantQueryOptions.returnSample);
//        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.RETURNED_FILES.key(), queryCommandOptions.queryVariantsOptions.returnFile);
        params.putIfNotEmpty(VariantQueryParam.STUDIES.key(), study);
        params.putIfNotEmpty(VariantQueryParam.FILES.key(), queryCommandOptions.genericVariantQueryOptions.file);
        params.putIfNotEmpty(VariantQueryParam.RETURNED_FILES.key(), queryCommandOptions.genericVariantQueryOptions.returnFile);
        params.putIfNotEmpty(VariantQueryParam.FILTER.key(), queryCommandOptions.genericVariantQueryOptions.filter);
        params.putIfNotEmpty(VariantQueryParam.STATS_MAF.key(), queryCommandOptions.genericVariantQueryOptions.maf);
        params.putIfNotEmpty(VariantQueryParam.STATS_MGF.key(), queryCommandOptions.genericVariantQueryOptions.mgf);
        params.putIfNotEmpty(VariantQueryParam.MISSING_ALLELES.key(), queryCommandOptions.genericVariantQueryOptions.missingAlleleCount);
        params.putIfNotEmpty(VariantQueryParam.MISSING_GENOTYPES.key(),
                queryCommandOptions.genericVariantQueryOptions.missingGenotypeCount);
//        queryOptions.put(VariantDBAdaptor.VariantQueryParams.ANNOTATION_EXISTS.key(),
//                queryCommandOptions.annotationExists);
        params.putIfNotEmpty(VariantQueryParam.GENOTYPE.key(), queryCommandOptions.genericVariantQueryOptions.sampleGenotype);
        params.putIfNotEmpty(VariantQueryParam.SAMPLES.key(), queryCommandOptions.genericVariantQueryOptions.samples);
        params.putIfNotEmpty(VariantQueryParam.INCLUDE_FORMAT.key(), queryCommandOptions.genericVariantQueryOptions.includeFormat);
        params.putIfNotEmpty(VariantQueryParam.INCLUDE_GENOTYPE.key(), queryCommandOptions.genericVariantQueryOptions.includeGenotype);
        params.putIfNotEmpty(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key(), queryCommandOptions.genericVariantQueryOptions.consequenceType);
        params.putIfNotEmpty(VariantQueryParam.ANNOT_XREF.key(), queryCommandOptions.genericVariantQueryOptions.annotXref);
        params.putIfNotEmpty(VariantQueryParam.ANNOT_BIOTYPE.key(), queryCommandOptions.genericVariantQueryOptions.geneBiotype);
        params.putIfNotEmpty(VariantQueryParam.ANNOT_PROTEIN_SUBSTITUTION.key(), queryCommandOptions.genericVariantQueryOptions.proteinSubstitution);
//        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_POLYPHEN.key(), queryCommandOptions.queryVariantsOptions.polyphen);
//        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_SIFT.key(), queryCommandOptions.queryVariantsOptions.sift);
        params.putIfNotEmpty(VariantQueryParam.ANNOT_CONSERVATION.key(), queryCommandOptions.genericVariantQueryOptions.conservation);
        params.putIfNotEmpty(VariantQueryParam.ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY.key(),
                queryCommandOptions.genericVariantQueryOptions.populationMaf);
        params.putIfNotEmpty(VariantQueryParam.ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(),
                queryCommandOptions.genericVariantQueryOptions.populationFreqs);
//        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_POPULATION_REFERENCE_FREQUENCY.key(),
//                queryCommandOptions.reference_frequency);
        params.putIfNotEmpty(VariantQueryParam.ANNOT_TRANSCRIPTION_FLAGS.key(),
                queryCommandOptions.genericVariantQueryOptions.flags);
        params.putIfNotEmpty(VariantQueryParam.ANNOT_GENE_TRAITS_ID.key(), queryCommandOptions.genericVariantQueryOptions.geneTraitId);
        params.putIfNotEmpty(VariantQueryParam.ANNOT_GENE_TRAITS_NAME.key(),
                queryCommandOptions.genericVariantQueryOptions.geneTraitName);
        params.putIfNotEmpty(VariantQueryParam.ANNOT_HPO.key(), queryCommandOptions.genericVariantQueryOptions.hpo);
        params.putIfNotEmpty(VariantQueryParam.ANNOT_GO.key(), queryCommandOptions.genericVariantQueryOptions.go);
//        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_EXPRESSION.key(), queryCommandOptions.genericVariantQueryOptions.expression);
        params.putIfNotEmpty(VariantQueryParam.ANNOT_PROTEIN_KEYWORDS.key(),
                queryCommandOptions.genericVariantQueryOptions.proteinKeywords);
        params.putIfNotEmpty(VariantQueryParam.ANNOT_DRUG.key(), queryCommandOptions.genericVariantQueryOptions.drugs);
        params.putIfNotEmpty(VariantQueryParam.ANNOT_FUNCTIONAL_SCORE.key(),
                queryCommandOptions.genericVariantQueryOptions.functionalScore);
        params.putIfNotEmpty(VariantQueryParam.UNKNOWN_GENOTYPE.key(), queryCommandOptions.genericVariantQueryOptions.unknownGenotype);
//        params.put(QueryOptions.SORT, queryCommandOptions.sort);
//        queryOptions.putIfNotEmpty("merge", queryCommandOptions.merge);
        params.putIfNotEmpty(VariantCatalogQueryUtils.SAMPLE_FILTER.key(), queryCommandOptions.sampleFilter);

        QueryOptions options = new QueryOptions();
        options.putIfNotEmpty(QueryOptions.INCLUDE, queryCommandOptions.dataModelOptions.include);
        options.putIfNotEmpty(QueryOptions.EXCLUDE, queryCommandOptions.dataModelOptions.exclude);
        options.put(QueryOptions.LIMIT, queryCommandOptions.numericOptions.limit);
        options.put(QueryOptions.SKIP, queryCommandOptions.numericOptions.skip);
        options.put(QueryOptions.COUNT, queryCommandOptions.numericOptions.count);
        options.putAll(variantCommandOptions.commonCommandOptions.params);

        params.put("samplesMetadata", queryCommandOptions.genericVariantQueryOptions.samplesMetadata);
        params.putIfNotEmpty("groupBy", queryCommandOptions.genericVariantQueryOptions.groupBy);
        params.put("histogram", queryCommandOptions.genericVariantQueryOptions.histogram);
        params.put("interval", queryCommandOptions.genericVariantQueryOptions.interval);

        List<String> annotations = queryCommandOptions.genericVariantQueryOptions.annotations == null
                ? Collections.singletonList("gene")
                : Arrays.asList(queryCommandOptions.genericVariantQueryOptions.annotations.split(","));


        // Let's hide some STDOUT verbose messages from ManagedChannelImpl class
        Logger.getLogger(ManagedChannelImpl.class.getName()).setLevel(java.util.logging.Level.WARNING);

        boolean grpc = usingGrpcMode(queryCommandOptions.mode);
        if (!grpc) {
            if (queryCommandOptions.numericOptions.count) {
                return openCGAClient.getVariantClient().count(params, options);
            } else if (queryCommandOptions.genericVariantQueryOptions.samplesMetadata || StringUtils.isNoneEmpty(queryCommandOptions.genericVariantQueryOptions.groupBy)
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

            params.putAll(options);
            Query query = VariantStorageManager.getVariantQuery(params);
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
                    params.put(VariantQueryParam.SAMPLES_METADATA.key(), true);
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
        boolean grpc;
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

    private void printVcf(VariantQueryResult<Variant> variantQueryResult, Iterator<VariantProto.Variant> variantIterator, String study, List<String> samples, List<String> annotations, PrintStream outputStream) {
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

        // TODO: modify VcfUtils in biodata project to take into account the formatArities
        VCFHeader vcfHeader = VcfUtils.createVCFHeader(cohorts, annotations, formats, formatTypes, formatDescriptions, samples, null);
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
