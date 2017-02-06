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
import org.opencb.opencga.app.cli.main.executors.OpencgaCommandExecutor;
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

        String fileIds = variantCommandOptions.indexVariantCommandOptions.fileId;

        ObjectMap o = new ObjectMap();
        o.putIfNotNull(VariantStorageEngine.Options.STUDY_ID.key(), variantCommandOptions.indexVariantCommandOptions.study);
        o.putIfNotNull("outDir", variantCommandOptions.indexVariantCommandOptions.outdir);
        o.putIfNotNull("transform", variantCommandOptions.indexVariantCommandOptions.genericVariantIndexOptions.transform);
        o.putIfNotNull("load", variantCommandOptions.indexVariantCommandOptions.genericVariantIndexOptions.load);
        o.putIfNotNull(VariantStorageEngine.Options.EXCLUDE_GENOTYPES.key(), variantCommandOptions.indexVariantCommandOptions.genericVariantIndexOptions.excludeGenotype);
        o.putIfNotNull("includeExtraFields", variantCommandOptions.indexVariantCommandOptions.genericVariantIndexOptions.extraFields);
        o.putIfNotNull("aggregated", variantCommandOptions.indexVariantCommandOptions.genericVariantIndexOptions.aggregated);
        o.putIfNotNull(VariantStorageEngine.Options.CALCULATE_STATS.key(), variantCommandOptions.indexVariantCommandOptions.genericVariantIndexOptions.calculateStats);
        o.putIfNotNull(VariantStorageEngine.Options.ANNOTATE.key(), variantCommandOptions.indexVariantCommandOptions.genericVariantIndexOptions.annotate);
        o.putIfNotNull(VariantStorageEngine.Options.RESUME.key(), variantCommandOptions.indexVariantCommandOptions.genericVariantIndexOptions.resume);
//        o.putIfNotNull("overwrite", variantCommandOptions.indexCommandOptions.overwriteAnnotations);
        o.putAll(variantCommandOptions.commonCommandOptions.params);

//        return openCGAClient.getFileClient().index(fileIds, o);
        return openCGAClient.getVariantClient().index(fileIds, o);
    }

    private QueryResponse query() throws CatalogException, IOException, InterruptedException {
        logger.debug("Listing variants of a study.");

        VariantCommandOptions.VariantQueryCommandOptions queryCommandOptions = variantCommandOptions.queryVariantCommandOptions;

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ID.key(), queryCommandOptions.genericVariantQueryOptions.id);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.REGION.key(), queryCommandOptions.genericVariantQueryOptions.region);
//        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.CHROMOSOME.key(),
//                queryCommandOptions.queryVariantsOptions.chromosome);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.GENE.key(), queryCommandOptions.genericVariantQueryOptions.gene);
        params.putIfNotNull(VariantDBAdaptor.VariantQueryParams.TYPE.key(), queryCommandOptions.genericVariantQueryOptions.type);
//        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.REFERENCE.key(), queryCommandOptions.queryVariantsOptions.reference);
//        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ALTERNATE.key(), queryCommandOptions.queryVariantsOptions.alternate);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.RETURNED_STUDIES.key(), queryCommandOptions.genericVariantQueryOptions.returnStudy);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.RETURNED_SAMPLES.key(), queryCommandOptions.genericVariantQueryOptions.returnSample);
//        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.RETURNED_FILES.key(), queryCommandOptions.queryVariantsOptions.returnFile);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.STUDIES.key(), queryCommandOptions.study);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.FILES.key(), queryCommandOptions.genericVariantQueryOptions.file);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.STATS_MAF.key(), queryCommandOptions.genericVariantQueryOptions.maf);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.STATS_MGF.key(), queryCommandOptions.genericVariantQueryOptions.mgf);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.MISSING_ALLELES.key(), queryCommandOptions.genericVariantQueryOptions.missingAlleleCount);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.MISSING_GENOTYPES.key(),
                queryCommandOptions.genericVariantQueryOptions.missingGenotypeCount);
//        queryOptions.put(VariantDBAdaptor.VariantQueryParams.ANNOTATION_EXISTS.key(),
//                queryCommandOptions.annotationExists);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.GENOTYPE.key(), queryCommandOptions.genericVariantQueryOptions.sampleGenotype);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_CONSEQUENCE_TYPE.key(), queryCommandOptions.genericVariantQueryOptions.consequenceType);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_XREF.key(), queryCommandOptions.genericVariantQueryOptions.annotXref);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_BIOTYPE.key(), queryCommandOptions.genericVariantQueryOptions.geneBiotype);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_PROTEIN_SUBSTITUTION.key(), queryCommandOptions.genericVariantQueryOptions.proteinSubstitution);
//        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_POLYPHEN.key(), queryCommandOptions.queryVariantsOptions.polyphen);
//        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_SIFT.key(), queryCommandOptions.queryVariantsOptions.sift);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_CONSERVATION.key(), queryCommandOptions.genericVariantQueryOptions.conservation);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY.key(),
                queryCommandOptions.genericVariantQueryOptions.populationMaf);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(),
                queryCommandOptions.genericVariantQueryOptions.populationFreqs);
//        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_POPULATION_REFERENCE_FREQUENCY.key(),
//                queryCommandOptions.reference_frequency);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_TRANSCRIPTION_FLAGS.key(),
                queryCommandOptions.genericVariantQueryOptions.flags);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_GENE_TRAITS_ID.key(), queryCommandOptions.genericVariantQueryOptions.geneTraitId);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_GENE_TRAITS_NAME.key(),
                queryCommandOptions.genericVariantQueryOptions.geneTraitName);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_HPO.key(), queryCommandOptions.genericVariantQueryOptions.hpo);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_GO.key(), queryCommandOptions.genericVariantQueryOptions.go);
//        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_EXPRESSION.key(), queryCommandOptions.genericVariantQueryOptions.expression);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_PROTEIN_KEYWORDS.key(),
                queryCommandOptions.genericVariantQueryOptions.proteinKeywords);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_DRUG.key(), queryCommandOptions.genericVariantQueryOptions.drugs);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.ANNOT_FUNCTIONAL_SCORE.key(),
                queryCommandOptions.genericVariantQueryOptions.functionalScore);
        params.putIfNotEmpty(VariantDBAdaptor.VariantQueryParams.UNKNOWN_GENOTYPE.key(), queryCommandOptions.genericVariantQueryOptions.unknownGenotype);
//        params.put(QueryOptions.SORT, queryCommandOptions.sort);
//        queryOptions.putIfNotEmpty("merge", queryCommandOptions.merge);

        QueryOptions options = new QueryOptions();
        options.putIfNotEmpty(QueryOptions.INCLUDE, queryCommandOptions.dataModelOptions.include);
        options.putIfNotEmpty(QueryOptions.EXCLUDE, queryCommandOptions.dataModelOptions.exclude);
        options.put(QueryOptions.LIMIT, queryCommandOptions.numericOptions.limit);
        options.put(QueryOptions.SKIP, queryCommandOptions.numericOptions.skip);
        options.put("count", queryCommandOptions.numericOptions.count);
        options.putAll(variantCommandOptions.commonCommandOptions.params);

        params.put("samplesMetadata", queryCommandOptions.genericVariantQueryOptions.samplesMetadata);
        params.putIfNotEmpty("groupBy", queryCommandOptions.genericVariantQueryOptions.groupBy);
        params.put("histogram", queryCommandOptions.genericVariantQueryOptions.histogram);
        params.put("interval", queryCommandOptions.genericVariantQueryOptions.interval);

        boolean grpc = usingGrpcMode(queryCommandOptions.mode);

        if (!grpc) {
            if (queryCommandOptions.numericOptions.count) {
                return openCGAClient.getVariantClient().count(params, options);
            } else if (queryCommandOptions.genericVariantQueryOptions.samplesMetadata || StringUtils.isNoneEmpty(queryCommandOptions.genericVariantQueryOptions.groupBy)
                    || queryCommandOptions.genericVariantQueryOptions.histogram) {
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
