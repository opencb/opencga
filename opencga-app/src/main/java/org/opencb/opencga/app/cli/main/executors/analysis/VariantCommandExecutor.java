/*
 * Copyright 2015-2016 OpenCB
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
import org.opencb.biodata.models.variant.protobuf.VariantProto;
import org.opencb.commons.datastore.core.*;
import org.opencb.opencga.app.cli.analysis.options.VariantCommandOptions;
import org.opencb.opencga.app.cli.main.OpencgaCommandExecutor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.server.grpc.AdminServiceGrpc;
import org.opencb.opencga.server.grpc.GenericServiceModel;
import org.opencb.opencga.server.grpc.VariantServiceGrpc;
import org.opencb.opencga.storage.core.manager.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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

        createOutput(queryResponse);

    }

    private QueryResponse index() throws CatalogException, IOException {
        logger.debug("Indexing variant(s)");

        String fileIds = variantCommandOptions.indexCommandOptions.fileIds;

        ObjectMap o = new ObjectMap();
        o.putIfNotNull("study", variantCommandOptions.indexCommandOptions.study);
        o.putIfNotNull("outDir", variantCommandOptions.indexCommandOptions.outdirId);
        o.putIfNotNull("transform", variantCommandOptions.indexCommandOptions.transform);
        o.putIfNotNull("load", variantCommandOptions.indexCommandOptions.load);
        o.putIfNotNull(VariantStorageEngine.Options.EXCLUDE_GENOTYPES.key(), variantCommandOptions.indexCommandOptions.excludeGenotype);
        o.putIfNotNull("includeExtraFields", variantCommandOptions.indexCommandOptions.extraFields);
        o.putIfNotNull("aggregated", variantCommandOptions.indexCommandOptions.aggregated);
        o.putIfNotNull(VariantStorageEngine.Options.CALCULATE_STATS.key(), variantCommandOptions.indexCommandOptions.calculateStats);
        o.putIfNotNull(VariantStorageEngine.Options.ANNOTATE.key(), variantCommandOptions.indexCommandOptions.annotate);
        o.putIfNotNull(VariantStorageEngine.Options.RESUME.key(), variantCommandOptions.indexCommandOptions.resume);
//        o.putIfNotNull("overwrite", variantCommandOptions.indexCommandOptions.overwriteAnnotations);
        o.putAll(variantCommandOptions.commonCommandOptions.params);

//        return openCGAClient.getFileClient().index(fileIds, o);
        return openCGAClient.getVariantClient().index(fileIds, o);
    }

    private QueryResponse query() throws CatalogException, IOException, InterruptedException {
        logger.debug("Listing variants of a study.");

        VariantCommandOptions.QueryVariantCommandOptions queryCommandOptions = variantCommandOptions.queryCommandOptions;

        ObjectMap params = new ObjectMap();

        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ID.key(), queryCommandOptions.ids);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.REGION.key(), queryCommandOptions.region);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.CHROMOSOME.key(),
                queryCommandOptions.chromosome);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.GENE.key(), queryCommandOptions.gene);
        params.putIfNotNull(VariantDBAdaptor.VariantQueryParams.TYPE.key(), queryCommandOptions.type);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.REFERENCE.key(), queryCommandOptions.reference);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ALTERNATE.key(), queryCommandOptions.alternate);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.RETURNED_STUDIES.key(), queryCommandOptions.returnedStudies);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.RETURNED_SAMPLES.key(), queryCommandOptions.returnedSamples);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.RETURNED_FILES.key(), queryCommandOptions.returnedFiles);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.STUDIES.key(), queryCommandOptions.studies);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.FILES.key(), queryCommandOptions.files);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.STATS_MAF.key(), queryCommandOptions.maf);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.STATS_MGF.key(), queryCommandOptions.mgf);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.MISSING_ALLELES.key(), queryCommandOptions.missingAlleles);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.MISSING_GENOTYPES.key(),
                queryCommandOptions.missingGenotypes);
//        queryOptions.put(VariantDBAdaptor.VariantQueryParams.ANNOTATION_EXISTS.key(),
//                queryCommandOptions.annotationExists);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.GENOTYPE.key(), queryCommandOptions.genotype);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_CONSEQUENCE_TYPE.key(), queryCommandOptions.annot_ct);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_XREF.key(), queryCommandOptions.annot_xref);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_BIOTYPE.key(), queryCommandOptions.annot_biotype);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_POLYPHEN.key(), queryCommandOptions.polyphen);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_SIFT.key(), queryCommandOptions.sift);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_CONSERVATION.key(), queryCommandOptions.conservation);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY.key(),
                queryCommandOptions.annotPopulationMaf);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(),
                queryCommandOptions.alternate_frequency);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_POPULATION_REFERENCE_FREQUENCY.key(),
                queryCommandOptions.reference_frequency);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_TRANSCRIPTION_FLAGS.key(),
                queryCommandOptions.transcriptionFlags);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_GENE_TRAITS_ID.key(), queryCommandOptions.geneTraitId);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_GENE_TRAITS_NAME.key(),
                queryCommandOptions.geneTraitName);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_HPO.key(), queryCommandOptions.hpo);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_GO.key(), queryCommandOptions.go);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_EXPRESSION.key(), queryCommandOptions.expression);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_PROTEIN_KEYWORDS.key(),
                queryCommandOptions.proteinKeyword);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_DRUG.key(), queryCommandOptions.drug);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_FUNCTIONAL_SCORE.key(),
                queryCommandOptions.functionalScore);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.UNKNOWN_GENOTYPE.key(), queryCommandOptions.unknownGenotype);
        params.put(QueryOptions.SORT, queryCommandOptions.sort);
//        queryOptions.putIfNotEmpty("merge", queryCommandOptions.merge);

        QueryOptions options = new QueryOptions();
        options.putIfNotEmpty(QueryOptions.INCLUDE, queryCommandOptions.include);
        options.putIfNotEmpty(QueryOptions.EXCLUDE, queryCommandOptions.exclude);
        options.putIfNotEmpty(QueryOptions.LIMIT, queryCommandOptions.limit);
        options.putIfNotEmpty(QueryOptions.SKIP, queryCommandOptions.skip);
        options.put("count", queryCommandOptions.count);
        options.putAll(variantCommandOptions.commonCommandOptions.params);

        params.put("samplesMetadata", queryCommandOptions.samplesMetadata);
        params.putIfNotEmpty("groupBy", queryCommandOptions.groupBy);
        params.put("histogram", queryCommandOptions.histogram);
        params.putIfNotEmpty("interval", queryCommandOptions.interval);

        boolean grpc = usingGrpcMode(queryCommandOptions.mode);

        if (!grpc) {
            if (queryCommandOptions.count) {
                return openCGAClient.getVariantClient().count(params, options);
            } else if (queryCommandOptions.samplesMetadata || StringUtils.isNoneEmpty(queryCommandOptions.groupBy)
                    || queryCommandOptions.histogram) {
                return openCGAClient.getVariantClient().genericQuery(params, options);
            } else {
                return openCGAClient.getVariantClient().query(params, options);
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

            QueryResponse queryResponse;
            if (queryCommandOptions.count) {
                ServiceTypesModel.LongResponse countResponse = variantServiceBlockingStub.count(request);
                ServiceTypesModel.Response response = countResponse.getResponse();
                queryResponse = new QueryResponse<>("", 0, response.getWarning(), response.getError(), new QueryOptions(params),
                        Collections.singletonList(
                                new QueryResult<>(response.getId(), 0, 1, 1, "", "", Collections.singletonList(countResponse.getValue()))));
                return queryResponse;
            } else if (queryCommandOptions.samplesMetadata || StringUtils.isNoneEmpty(queryCommandOptions.groupBy) || queryCommandOptions.histogram) {
                queryResponse = openCGAClient.getVariantClient().genericQuery(params, options);
            } else {
                Iterator<VariantProto.Variant> variantIterator = variantServiceBlockingStub.get(request);
                JsonFormat.Printer printer = JsonFormat.printer();
                try (PrintStream printStream = new PrintStream(System.out)) {
                    while (variantIterator.hasNext()) {
                        VariantProto.Variant next = variantIterator.next();
                        printStream.println(printer.print(next));
                    }
                }
                queryResponse = null;
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
                    logger.info("Using GRPC mode");
                } else {
                    logger.info("Using REST mode");
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

}
