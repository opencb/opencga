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

package org.opencb.opencga.storage.app.cli.client.executors;

import ga4gh.Reads;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.commons.lang3.StringUtils;
import org.ga4gh.models.ReadAlignment;
import org.opencb.biodata.models.alignment.RegionCoverage;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.tools.alignment.converters.SAMRecordToAvroReadAlignmentBiConverter;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.storage.app.cli.CommandExecutor;
import org.opencb.opencga.storage.app.cli.client.ClientCliOptionsParser;
import org.opencb.opencga.storage.app.cli.client.options.StorageAlignmentCommandOptions;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.StoragePipeline;
import org.opencb.opencga.storage.core.alignment.AlignmentDBAdaptor;
import org.opencb.opencga.storage.core.alignment.AlignmentStorageEngine;
import org.opencb.opencga.storage.core.config.StorageEngineConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.manager.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.BeaconResponse;
import org.opencb.opencga.storage.server.grpc.AlignmentGrpcService;
import org.opencb.opencga.storage.server.grpc.AlignmentServiceGrpc;
import org.opencb.opencga.storage.server.grpc.AlignmentServiceModel;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by imedina on 22/05/15.
 */
public class AlignmentCommandExecutor extends CommandExecutor {

    private StorageEngineConfiguration storageConfiguration;
    private AlignmentStorageEngine alignmentStorageEngine;

    private StorageAlignmentCommandOptions alignmentCommandOptions;

    public AlignmentCommandExecutor(StorageAlignmentCommandOptions alignmentCommandOptions) {
        super(alignmentCommandOptions.commonCommandOptions);
        this.alignmentCommandOptions = alignmentCommandOptions;
    }

    private void configure(ClientCliOptionsParser.CommonOptions commonOptions, String dbName) throws Exception {

        this.logFile = commonOptions.logFile;

        /**
         * Getting VariantStorageEngine
         * We need to find out the Storage Engine Id to be used
         * If not storage engine is passed then the default is taken from storage-configuration.yml file
         **/
        this.storageEngine = (storageEngine != null && !storageEngine.isEmpty())
                ? storageEngine
                : configuration.getDefaultStorageEngineId();
        logger.debug("Storage Engine set to '{}'", this.storageEngine);

        this.storageConfiguration = configuration.getStorageEngine(storageEngine);

        // TODO: Start passing catalogManager
        StorageEngineFactory storageEngineFactory = StorageEngineFactory.get(configuration);
        if (storageEngine == null || storageEngine.isEmpty()) {
            this.alignmentStorageEngine = storageEngineFactory.getAlignmentStorageEngine(null, dbName);
        } else {
            this.alignmentStorageEngine = storageEngineFactory.getAlignmentStorageEngine(storageEngine, dbName);
        }
    }


    @Override
    public void execute() throws Exception {
        logger.debug("Executing alignment command line");

        String subCommandString = getParsedSubCommand(alignmentCommandOptions.jCommander);
        switch (subCommandString) {
            case "index":
                configure(alignmentCommandOptions.indexAlignmentsCommandOptions.commonOptions, alignmentCommandOptions.indexAlignmentsCommandOptions.commonIndexOptions.dbName);
                index();
                break;
            case "query":
                configure(alignmentCommandOptions.queryAlignmentsCommandOptions.commonOptions, "");
                query();
                break;
            case "coverage":
                configure(alignmentCommandOptions.queryAlignmentsCommandOptions.commonOptions, "");
                coverage();
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }

    }

    private void index() throws Exception {
        StorageAlignmentCommandOptions.IndexAlignmentsCommandOptions indexAlignmentsCommandOptions = alignmentCommandOptions.indexAlignmentsCommandOptions;

        String inputs[] = indexAlignmentsCommandOptions.commonIndexOptions.input.split(",");
        URI inputUri = UriUtils.createUri(inputs[0]);
//        FileUtils.checkFile(Paths.get(inputUri.getPath()));

        URI outdirUri = (indexAlignmentsCommandOptions.commonIndexOptions.outdir != null && !indexAlignmentsCommandOptions.commonIndexOptions.outdir.isEmpty())
                ? UriUtils.createDirectoryUri(indexAlignmentsCommandOptions.commonIndexOptions.outdir)
                // Get parent folder from input file
                : inputUri.resolve(".");
//        FileUtils.checkDirectory(Paths.get(outdirUri.getPath()));
        logger.debug("All files and directories exist");

        /*
         * Add CLI options to the alignmentOptions
         */
        ObjectMap alignmentOptions = storageConfiguration.getAlignment().getOptions();
//        if (Integer.parseInt(indexAlignmentsCommandOptions.fileId) != 0) {
//            alignmentOptions.put(AlignmentStorageEngineOld.Options.FILE_ID.key(), indexAlignmentsCommandOptions.fileId);
//        }
//        if (indexAlignmentsCommandOptions.commonIndexOptions.dbName != null && !indexAlignmentsCommandOptions.commonIndexOptions.dbName.isEmpty()) {
//            alignmentOptions.put(AlignmentStorageEngineOld.Options.DB_NAME.key(), indexAlignmentsCommandOptions.commonIndexOptions.dbName);
//        }
        if (indexAlignmentsCommandOptions.commonOptions.params != null) {
            alignmentOptions.putAll(indexAlignmentsCommandOptions.commonOptions.params);
        }

//        alignmentOptions.put(AlignmentStorageEngineOld.Options.PLAIN.key(), false);
//        alignmentOptions.put(AlignmentStorageEngineOld.Options.INCLUDE_COVERAGE.key(), indexAlignmentsCommandOptions.calculateCoverage);
//        if (indexAlignmentsCommandOptions.meanCoverage != null && !indexAlignmentsCommandOptions.meanCoverage.isEmpty()) {
//            alignmentOptions.put(AlignmentStorageEngineOld.Options.MEAN_COVERAGE_SIZE_LIST.key(),
//                    indexAlignmentsCommandOptions.meanCoverage);
//        }
//        alignmentOptions.put(AlignmentStorageEngineOld.Options.COPY_FILE.key(), false);
//        alignmentOptions.put(AlignmentStorageEngineOld.Options.ENCRYPT.key(), "null");
        logger.debug("Configuration options: {}", alignmentOptions.toJson());


        boolean extract, transform, load;
        URI nextFileUri = inputUri;

        if (!indexAlignmentsCommandOptions.load && !indexAlignmentsCommandOptions.transform) {  // if not present --transform nor --load,
            // do both
            extract = true;
            transform = true;
            load = true;
        } else {
            extract = indexAlignmentsCommandOptions.transform;
            transform = indexAlignmentsCommandOptions.transform;
            load = indexAlignmentsCommandOptions.load;
        }

        try (StoragePipeline storagePipeline = alignmentStorageEngine.newStoragePipeline(true)) {

            if (extract) {
                logger.info("-- Extract alignments -- {}", inputUri);
                nextFileUri = storagePipeline.extract(inputUri, outdirUri);
            }

            if (transform) {
                logger.info("-- PreTransform alignments -- {}", nextFileUri);
                nextFileUri = storagePipeline.preTransform(nextFileUri);
                logger.info("-- Transform alignments -- {}", nextFileUri);
                nextFileUri = storagePipeline.transform(nextFileUri, null, outdirUri);
                logger.info("-- PostTransform alignments -- {}", nextFileUri);
                nextFileUri = storagePipeline.postTransform(nextFileUri);
            }

            if (load) {
                logger.info("-- PreLoad alignments -- {}", nextFileUri);
                nextFileUri = storagePipeline.preLoad(nextFileUri, outdirUri);
                logger.info("-- Load alignments -- {}", nextFileUri);
                nextFileUri = storagePipeline.load(nextFileUri);
                logger.info("-- PostLoad alignments -- {}", nextFileUri);
                nextFileUri = storagePipeline.postLoad(nextFileUri, outdirUri);
            }
        }
    }

    private void query() throws StorageEngineException, IOException, InterruptedException {
        StorageAlignmentCommandOptions.QueryAlignmentsCommandOptions queryAlignmentsCommandOptions = alignmentCommandOptions.queryAlignmentsCommandOptions;

        Path path = Paths.get(queryAlignmentsCommandOptions.filePath);
        FileUtils.checkFile(path);

        if (StringUtils.isEmpty(queryAlignmentsCommandOptions.region)) {
            logger.warn("'region' parameter cannot be empty");
            return;
        }

        Query query = new Query();
        query.putIfNotNull(AlignmentDBAdaptor.QueryParams.MIN_MAPQ.key(), queryAlignmentsCommandOptions.minMapq);
        query.putIfNotNull(AlignmentDBAdaptor.QueryParams.MAX_NM.key(), queryAlignmentsCommandOptions.maxNm);
        query.putIfNotNull(AlignmentDBAdaptor.QueryParams.MAX_NH.key(), queryAlignmentsCommandOptions.maxNH);
        query.putIfNotNull(AlignmentDBAdaptor.QueryParams.PROPERLY_PAIRED.key(), queryAlignmentsCommandOptions.properlyPaired);
        query.putIfNotNull(AlignmentDBAdaptor.QueryParams.MAX_INSERT_SIZE.key(), queryAlignmentsCommandOptions.maxInsertSize);
        query.putIfNotNull(AlignmentDBAdaptor.QueryParams.SKIP_UNMAPPED.key(), queryAlignmentsCommandOptions.skipUnmapped);
        query.putIfNotNull(AlignmentDBAdaptor.QueryParams.SKIP_DUPLICATED.key(), queryAlignmentsCommandOptions.skipDuplicated);

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.putIfNotNull(AlignmentDBAdaptor.QueryParams.CONTAINED.key(), queryAlignmentsCommandOptions.contained);
        queryOptions.putIfNotNull(AlignmentDBAdaptor.QueryParams.MD_FIELD.key(), queryAlignmentsCommandOptions.mdField);
        queryOptions.putIfNotNull(AlignmentDBAdaptor.QueryParams.BIN_QUALITIES.key(), queryAlignmentsCommandOptions.binQualities);
        queryOptions.putIfNotNull(QueryOptions.LIMIT, queryAlignmentsCommandOptions.limit);
        queryOptions.putIfNotNull(QueryOptions.COUNT, queryAlignmentsCommandOptions.count);


        PrintStream out = System.out;
        if (StringUtils.isNotEmpty(queryAlignmentsCommandOptions.output)) {
            out = new PrintStream(queryAlignmentsCommandOptions.output);
        }

        String[] regions = queryAlignmentsCommandOptions.region.split(",");
        switch (queryAlignmentsCommandOptions.mode.toLowerCase()) {
            case "rest":
                break;
            case "grpc":
                // Only one region is allowed in gRPC
                query.putIfNotNull(AlignmentDBAdaptor.QueryParams.REGION.key(), regions[0]);

                ManagedChannel channel = getManagedChannel(queryAlignmentsCommandOptions.serverUrl);
                AlignmentServiceGrpc.AlignmentServiceBlockingStub alignmentServiceBlockingStub =
                        AlignmentServiceGrpc.newBlockingStub(channel);
                AlignmentServiceModel.AlignmentRequest alignmentRequest =
                        getAlignmentRequest(queryAlignmentsCommandOptions.filePath, query, queryOptions);

                if (queryAlignmentsCommandOptions.count) {
                    AlignmentServiceModel.LongResponse count = alignmentServiceBlockingStub.count(alignmentRequest);
                    System.out.println(count.getValue());
                } else {
                    if (queryAlignmentsCommandOptions.outputFormat.equalsIgnoreCase("SAM")) {
                        Iterator<AlignmentServiceModel.StringResponse> samRecordString = alignmentServiceBlockingStub.getAsSam(alignmentRequest);
                        while (samRecordString.hasNext()) {
                            System.out.print(samRecordString.next().getValue());
                        }
                    } else {
                        Iterator<Reads.ReadAlignment> readAlignmentIterator = alignmentServiceBlockingStub.get(alignmentRequest);
                        while (readAlignmentIterator.hasNext()) {
                            System.out.print(readAlignmentIterator.next().getAlignment());
                        }
                    }
                }
                channel.shutdownNow().awaitTermination(2, TimeUnit.SECONDS);
                break;
            default:
                for (String region : regions) {
                    logger.debug("Processing region '{}'", region);
                    query.putIfNotNull(AlignmentDBAdaptor.QueryParams.REGION.key(), region);

                    if (queryAlignmentsCommandOptions.count) {
                        QueryResult<Long> longQueryResult = this.alignmentStorageEngine.getDBAdaptor().count(path, query, queryOptions);
                        if (longQueryResult != null) {
                            out.println(region + "\t" + longQueryResult.first());
                        }
                    } else {
                        QueryResult<ReadAlignment> readAlignmentQueryResult = this.alignmentStorageEngine.getDBAdaptor().get(path, query, queryOptions);
                        if (readAlignmentQueryResult != null) {
                            if (queryAlignmentsCommandOptions.outputFormat.equalsIgnoreCase("SAM")) {
                                SAMRecordToAvroReadAlignmentBiConverter samRecordToAvroReadAlignmentBiConverter =
                                        new SAMRecordToAvroReadAlignmentBiConverter(queryAlignmentsCommandOptions.binQualities);
                                for (ReadAlignment readAlignment : readAlignmentQueryResult.getResult()) {
                                    out.println(samRecordToAvroReadAlignmentBiConverter.from(readAlignment).getSAMString());
                                }
                            } else {
                                for (ReadAlignment readAlignment : readAlignmentQueryResult.getResult()) {
                                    out.println(readAlignment.toString());
                                }
                            }
                        }
                    }
                }
                break;
        }

        out.close();
    }

    private void coverage() throws Exception {
        StorageAlignmentCommandOptions.CoverageAlignmentsCommandOptions coverageAlignmentsCommandOptions = alignmentCommandOptions.coverageAlignmentsCommandOptions;

        switch (coverageAlignmentsCommandOptions.mode.toLowerCase()) {
            case "grpc":
                // Only one region is allowed in gRPC
                Query query = new Query();
                query.putIfNotNull(AlignmentDBAdaptor.QueryParams.REGION.key(), coverageAlignmentsCommandOptions.region);
                query.putIfNotNull(AlignmentDBAdaptor.QueryParams.WINDOW_SIZE.key(), coverageAlignmentsCommandOptions.windowSize);

                ManagedChannel channel = getManagedChannel(coverageAlignmentsCommandOptions.serverUrl);
                AlignmentServiceGrpc.AlignmentServiceBlockingStub alignmentServiceBlockingStub =
                        AlignmentServiceGrpc.newBlockingStub(channel);
                AlignmentServiceModel.AlignmentRequest alignmentRequest =
                        getAlignmentRequest(coverageAlignmentsCommandOptions.file, query, QueryOptions.empty());

                Iterator<AlignmentServiceModel.FloatResponse> coverageFloatResponse = alignmentServiceBlockingStub.coverage(alignmentRequest);
                while (coverageFloatResponse.hasNext()) {
                    System.out.println(coverageFloatResponse.next().getValue());
                }

                channel.shutdownNow().awaitTermination(2, TimeUnit.SECONDS);
                break;
            default:
                Path path = Paths.get(coverageAlignmentsCommandOptions.file);
                FileUtils.checkFile(path);

                Region region = Region.parseRegion(coverageAlignmentsCommandOptions.region);
                QueryResult<RegionCoverage> coverage = this.alignmentStorageEngine.getDBAdaptor().coverage(path, region,
                        coverageAlignmentsCommandOptions.windowSize);
                System.out.println("coverage = " + coverage);
                break;
        }
    }

    private ManagedChannel getManagedChannel(String serverUrl) {
        // We create the gRPC channel to the specified server host and port
        ManagedChannel channel = ManagedChannelBuilder.forTarget(serverUrl)
                .usePlaintext(true)
                .build();

        return channel;
    }

    private AlignmentServiceModel.AlignmentRequest getAlignmentRequest(String file, Query query, QueryOptions queryOptions) {
        Map<String, String> queryMap = new HashMap<>();
        Map<String, String> queryOptionsMap = new HashMap<>();
        for (String key : query.keySet()) {
            queryMap.put(key, query.getString(key));
        }
        for (String key : queryOptions.keySet()) {
            queryOptionsMap.put(key, queryOptions.getString(key));
        }

        // We create the OpenCGA gRPC request object with the query, queryOptions and sessionId
        AlignmentServiceModel.AlignmentRequest request = AlignmentServiceModel.AlignmentRequest.newBuilder()
                .setFile(file)
                .putAllQuery(queryMap)
                .putAllOptions(queryOptionsMap)
                .build();

        return request;
    }

}
