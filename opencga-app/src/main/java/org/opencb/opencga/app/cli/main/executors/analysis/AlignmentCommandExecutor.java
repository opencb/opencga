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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import ga4gh.Reads;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.ga4gh.models.ReadAlignment;
import org.opencb.biodata.models.alignment.RegionCoverage;
import org.opencb.biodata.tools.alignment.converters.SAMRecordToAvroReadAlignmentBiConverter;
import org.opencb.commons.datastore.core.DataResponse;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.wrappers.BwaWrapperAnalysis;
import org.opencb.opencga.analysis.wrappers.DeeptoolsWrapperAnalysis;
import org.opencb.opencga.analysis.wrappers.SamtoolsWrapperAnalysis;
import org.opencb.opencga.app.cli.internal.options.AlignmentCommandOptions;
import org.opencb.opencga.app.cli.main.executors.OpencgaCommandExecutor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.client.rest.OpenCGAClient;
import org.opencb.opencga.core.models.Job;
import org.opencb.opencga.server.grpc.AlignmentServiceGrpc;
import org.opencb.opencga.server.grpc.GenericAlignmentServiceModel;
import org.opencb.opencga.server.grpc.ServiceTypesModel;
import org.opencb.opencga.server.rest.analysis.AlignmentAnalysisWSService;
import org.opencb.opencga.storage.core.alignment.AlignmentDBAdaptor;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by pfurio on 11/11/16.
 */
public class AlignmentCommandExecutor extends OpencgaCommandExecutor {

    private AlignmentCommandOptions alignmentCommandOptions;

    public AlignmentCommandExecutor(AlignmentCommandOptions  alignmentCommandOptions) {
        super(alignmentCommandOptions.analysisCommonOptions);
        this.alignmentCommandOptions = alignmentCommandOptions;
    }


    @Override
    public void execute() throws Exception {
        logger.debug("Executing alignment command line");

        String subCommandString = getParsedSubCommand(alignmentCommandOptions.jCommander);
        DataResponse queryResponse = null;
        switch (subCommandString) {
            case "index":
                queryResponse = index();
                break;
            case "query":
                query();
                break;
            case "stats":
                queryResponse = stats();
                break;
            case "coverage":
                queryResponse = coverage();
                break;
            case BwaWrapperAnalysis.ID:
                queryResponse = bwa();
                break;
            case SamtoolsWrapperAnalysis.ID:
                queryResponse = samtools();
                break;
            case DeeptoolsWrapperAnalysis.ID:
                queryResponse = deeptools();
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }

        createOutput(queryResponse);
    }

    private DataResponse index() throws CatalogException, IOException {
        logger.debug("Indexing alignment(s)");

        String fileIds = alignmentCommandOptions.indexAlignmentCommandOptions.fileId;

        ObjectMap o = new ObjectMap();
        o.putIfNotNull("study", alignmentCommandOptions.indexAlignmentCommandOptions.study);
        o.putIfNotNull("outDir", alignmentCommandOptions.indexAlignmentCommandOptions.outdirId);

        return openCGAClient.getAlignmentClient().index(fileIds, o);
    }

    private void query() throws CatalogException, IOException, InterruptedException {
        logger.debug("Querying alignment(s)");

        String rpc = alignmentCommandOptions.queryAlignmentCommandOptions.rpc;
        if (rpc == null) {
            rpc = "auto";
        }

        DataResponse<ReadAlignment> queryResponse = null;

        if (rpc.toLowerCase().equals("rest")) {
            queryResponse = queryRest(alignmentCommandOptions.queryAlignmentCommandOptions);
        } else if (rpc.toLowerCase().equals("grpc")) {
            queryGRPC(alignmentCommandOptions.queryAlignmentCommandOptions);
        } else {
            try {
                queryGRPC(alignmentCommandOptions.queryAlignmentCommandOptions);
            } catch(Exception e) {
                System.out.println("GRPC not available. Trying on REST.");
                queryResponse = queryRest(alignmentCommandOptions.queryAlignmentCommandOptions);
            }
        }

        // It will only enter this if when the query has been done via REST
        if (queryResponse != null) {
            if (alignmentCommandOptions.queryAlignmentCommandOptions.commonOptions.outputFormat.toLowerCase().contains("text")) {
                SAMRecordToAvroReadAlignmentBiConverter converter = new SAMRecordToAvroReadAlignmentBiConverter();
                for (ReadAlignment readAlignment : queryResponse.allResults()) {
                    System.out.print(converter.from(readAlignment).getSAMString());
                }
            } else {
                ObjectMapper objectMapper = new ObjectMapper();
                objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
                objectMapper.configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);
                ObjectWriter objectWriter = objectMapper.writerWithDefaultPrettyPrinter();
                try {
                    System.out.println(objectWriter.writeValueAsString(queryResponse.getResponses()));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    private DataResponse<ReadAlignment> queryRest(AlignmentCommandOptions.QueryAlignmentCommandOptions commandOptions)
            throws CatalogException, IOException {

        String study = resolveStudy(alignmentCommandOptions.queryAlignmentCommandOptions.study);

        String fileIds = commandOptions.fileId;

        ObjectMap o = new ObjectMap();
        o.putIfNotNull("study", study);
        o.putIfNotNull(AlignmentDBAdaptor.QueryParams.REGION.key(), commandOptions.region);
        o.putIfNotNull(AlignmentDBAdaptor.QueryParams.MIN_MAPQ.key(), commandOptions.minMappingQuality);
        o.putIfNotNull("limit", commandOptions.limit);

        DataResponse<ReadAlignment> query = openCGAClient.getAlignmentClient().query(fileIds, o);
        return query;
    }

    private void queryGRPC(AlignmentCommandOptions.QueryAlignmentCommandOptions commandOptions) throws InterruptedException {
//        StopWatch watch = new StopWatch();
//        watch.start();
        // We create the OpenCGA gRPC request object with the query, queryOptions, storageEngine and database

        String study = resolveStudy(alignmentCommandOptions.queryAlignmentCommandOptions.study);

        Map<String, String> query = new HashMap<>();
        addParam(query, "fileId", commandOptions.fileId);
        addParam(query, "sid", commandOptions.commonOptions.token);
        addParam(query, "study", study);
        addParam(query, AlignmentDBAdaptor.QueryParams.REGION.key(), commandOptions.region);
        addParam(query, AlignmentDBAdaptor.QueryParams.MIN_MAPQ.key(), commandOptions.minMappingQuality);

        Map<String, String> queryOptions = new HashMap<>();
        addParam(queryOptions, AlignmentDBAdaptor.QueryParams.CONTAINED.key(), commandOptions.contained);
        addParam(queryOptions, AlignmentDBAdaptor.QueryParams.MD_FIELD.key(), commandOptions.mdField);
        addParam(queryOptions, AlignmentDBAdaptor.QueryParams.BIN_QUALITIES.key(),commandOptions.binQualities);
        addParam(queryOptions, QueryOptions.LIMIT, commandOptions.limit);
        addParam(queryOptions, QueryOptions.SKIP, commandOptions.skip);

        GenericAlignmentServiceModel.Request request = GenericAlignmentServiceModel.Request.newBuilder()
                .putAllQuery(query)
                .putAllOptions(queryOptions)
                .build();

        // Connecting to the server host and port
        String[] split = clientConfiguration.getGrpc().getHost().split(":");
        String grpcServerHost = split[0];
        int grpcServerPort = 9091;
        if (split.length == 2) {
            grpcServerPort = Integer.parseInt(split[1]);
        }

        logger.debug("Connecting to gRPC server at {}:{}", grpcServerHost, grpcServerPort);

        // We create the gRPC channel to the specified server host and port
        ManagedChannel channel = ManagedChannelBuilder.forAddress(grpcServerHost, grpcServerPort)
                .usePlaintext(true)
                .build();

        // We use a blocking stub to execute the query to gRPC
        AlignmentServiceGrpc.AlignmentServiceBlockingStub serviceBlockingStub = AlignmentServiceGrpc.newBlockingStub(channel);

        if (commandOptions.count) {
            ServiceTypesModel.LongResponse count = serviceBlockingStub.count(request);
            String pretty = "";
            if (commandOptions.commonOptions.outputFormat.toLowerCase().equals("extended_text")) {
                pretty = "\nThe number of alignments is ";
            }
            System.out.println(pretty + count.getValue() + "\n");
        } else {
            if (commandOptions.commonOptions.outputFormat.toLowerCase().contains("text")) {
                // Output in SAM format
                Iterator<ServiceTypesModel.StringResponse> alignmentIterator = serviceBlockingStub.getAsSam(request);
//                watch.stop();
//                System.out.println("Time: " + watch.getTime());
                int limit = commandOptions.limit;
                if (limit > 0) {
                    long cont = 0;
                    while (alignmentIterator.hasNext() && cont < limit) {
                        ServiceTypesModel.StringResponse next = alignmentIterator.next();
                        cont++;
                        System.out.print(next.getValue());
                    }
                } else {
                    while (alignmentIterator.hasNext()) {
                        ServiceTypesModel.StringResponse next = alignmentIterator.next();
                        System.out.print(next.getValue());
                    }
                }

            } else {
                // Output in json format
                Iterator<Reads.ReadAlignment> alignmentIterator = serviceBlockingStub.get(request);
//                watch.stop();
//                System.out.println("Time: " + watch.getTime());
                int limit = commandOptions.limit;
                if (limit > 0) {
                    long cont = 0;
                    while (alignmentIterator.hasNext() && cont < limit) {
                        Reads.ReadAlignment next = alignmentIterator.next();
                        cont++;
                        System.out.println(next.toString());
                    }
                } else {
                    while (alignmentIterator.hasNext()) {
                        Reads.ReadAlignment next = alignmentIterator.next();
                        System.out.println(next.toString());
                    }
                }
            }
        }

        channel.shutdown().awaitTermination(1, TimeUnit.SECONDS);
    }


    private DataResponse stats() throws CatalogException, IOException {
//        ObjectMap objectMap = new ObjectMap();
////        objectMap.putIfNotNull("fileId", alignmentCommandOptions.statsAlignmentCommandOptions.fileId);
//        objectMap.putIfNotNull("sid", alignmentCommandOptions.statsAlignmentCommandOptions.commonOptions.token);
//        objectMap.putIfNotNull("study", alignmentCommandOptions.statsAlignmentCommandOptions.study);
//        objectMap.putIfNotNull("region", alignmentCommandOptions.statsAlignmentCommandOptions.region);
//        objectMap.putIfNotNull("minMapQ", alignmentCommandOptions.statsAlignmentCommandOptions.minMappingQuality);
//        if (alignmentCommandOptions.statsAlignmentCommandOptions.contained) {
//            objectMap.put("contained", alignmentCommandOptions.statsAlignmentCommandOptions.contained);
//        }
//
//        OpenCGAClient openCGAClient = new OpenCGAClient(clientConfiguration);
//        DataResponse<AlignmentGlobalStats> globalStats = openCGAClient.getAlignmentClient()
//                .stats(alignmentCommandOptions.statsAlignmentCommandOptions.inputFile, objectMap);
//
//        return globalStats;
////        for (AlignmentGlobalStats alignmentGlobalStats : globalStats.allResults()) {
////            System.out.println(alignmentGlobalStats.toJSON());
////        }
        return null;
    }

    private DataResponse coverage() throws CatalogException, IOException {
        ObjectMap objectMap = new ObjectMap();
//        objectMap.putIfNotNull("fileId", alignmentCommandOptions.coverageAlignmentCommandOptions.fileId);
        objectMap.putIfNotNull("sid", alignmentCommandOptions.coverageAlignmentCommandOptions.commonOptions.token);
        objectMap.putIfNotNull("study", alignmentCommandOptions.coverageAlignmentCommandOptions.study);
        objectMap.putIfNotNull("region", alignmentCommandOptions.coverageAlignmentCommandOptions.region);
        objectMap.putIfNotNull("minMapQ", alignmentCommandOptions.coverageAlignmentCommandOptions.minMappingQuality);
        if (alignmentCommandOptions.coverageAlignmentCommandOptions.contained) {
            objectMap.put("contained", alignmentCommandOptions.coverageAlignmentCommandOptions.contained);
        }

        OpenCGAClient openCGAClient = new OpenCGAClient(clientConfiguration);
        DataResponse<RegionCoverage> globalStats = openCGAClient.getAlignmentClient()
                .coverage(alignmentCommandOptions.coverageAlignmentCommandOptions.fileId, objectMap);

        return globalStats;
//        for (RegionCoverage regionCoverage : globalStats.allResults()) {
//            System.out.println(regionCoverage.toString());
//        }
    }

    //-------------------------------------------------------------------------
    // W R A P P E R S     A N A L Y S I S
    //-------------------------------------------------------------------------

    // BWA

    private DataResponse<Job> bwa() throws IOException {
        ObjectMap params = new AlignmentAnalysisWSService.BwaRunParams(
                alignmentCommandOptions.bwaCommandOptions.command,
                alignmentCommandOptions.bwaCommandOptions.fastaFile,
                alignmentCommandOptions.bwaCommandOptions.indexBaseFile,
                alignmentCommandOptions.bwaCommandOptions.fastq1File,
                alignmentCommandOptions.bwaCommandOptions.fastq2File,
                alignmentCommandOptions.bwaCommandOptions.samFile,
                alignmentCommandOptions.bwaCommandOptions.outdir,
                alignmentCommandOptions.bwaCommandOptions.commonOptions.params
        ).toObjectMap();
        return openCGAClient.getAlignmentClient().bwaRun(alignmentCommandOptions.bwaCommandOptions.study, params);
    }

    // Samtools

    private DataResponse<Job> samtools() throws IOException {
        ObjectMap params = new AlignmentAnalysisWSService.SamtoolsRunParams(
                alignmentCommandOptions.samtoolsCommandOptions.command,
                alignmentCommandOptions.samtoolsCommandOptions.inputFile,
                alignmentCommandOptions.samtoolsCommandOptions.outputFile,
                alignmentCommandOptions.samtoolsCommandOptions.outdir,
                alignmentCommandOptions.samtoolsCommandOptions.commonOptions.params
        ).toObjectMap();
        return openCGAClient.getAlignmentClient().samtoolsRun(alignmentCommandOptions.samtoolsCommandOptions.study, params);
    }

    // Deeptools

    private DataResponse<Job> deeptools() throws IOException {
        ObjectMap params = new AlignmentAnalysisWSService.DeeptoolsRunParams(
                alignmentCommandOptions.deeptoolsCommandOptions.executable,
                alignmentCommandOptions.deeptoolsCommandOptions.bamFile,
                alignmentCommandOptions.deeptoolsCommandOptions.coverageFile,
                alignmentCommandOptions.deeptoolsCommandOptions.outdir,
                alignmentCommandOptions.deeptoolsCommandOptions.commonOptions.params
        ).toObjectMap();
        return openCGAClient.getAlignmentClient().deeptoolsRun(alignmentCommandOptions.deeptoolsCommandOptions.study, params);
    }

    //-------------------------------------------------------------------------
    // M I S C E L A N E O U S     M E T H O D S
    //-------------------------------------------------------------------------

    private void addParam(Map<String, String> map, String key, Object value) {
        if (value == null) {
            return;
        }

        if (value instanceof String) {
            if (!((String) value).isEmpty()) {
                map.put(key, (String) value);
            }
        } else if (value instanceof Integer) {
            map.put(key, Integer.toString((int) value));
        } else if (value instanceof Boolean) {
            map.put(key, Boolean.toString((boolean) value));
        } else {
            throw new UnsupportedOperationException();
        }
    }
}
