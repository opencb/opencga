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

package org.opencb.opencga.app.cli.analysis.executors;

import org.ga4gh.models.ReadAlignment;
import org.opencb.biodata.models.alignment.RegionCoverage;
import org.opencb.biodata.tools.alignment.stats.AlignmentGlobalStats;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.app.cli.analysis.options.AlignmentCommandOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.client.rest.OpenCGAClient;
import org.opencb.opencga.storage.core.alignment.AlignmentDBAdaptor;

import java.io.IOException;
import java.util.Map;

/**
 * Created on 09/05/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class AlignmentCommandExecutor extends AnalysisCommandExecutor {

    private final AlignmentCommandOptions alignmentCommandOptions;
//    private AlignmentStorageEngine alignmentStorageManager;

    public AlignmentCommandExecutor(AlignmentCommandOptions options) {
        super(options.analysisCommonOptions);
        alignmentCommandOptions = options;
    }

    @Override
    public void execute() throws Exception {
        logger.debug("Executing variant command line");

//        String subCommandString = alignmentCommandOptions.getParsedSubCommand();
        String subCommandString = getParsedSubCommand(alignmentCommandOptions.jCommander);
        configure();
        switch (subCommandString) {
            case "index":
                index();
                break;
            case "query":
                query();
                break;
            case "stats":
                stats();
                break;
            case "coverage":
                coverage();
                break;
            case "delete":
                delete();
                break;
            default:
                logger.error("Subcommand not valid");
                break;

        }
    }

    private void index() throws Exception {
        AlignmentCommandOptions.IndexAlignmentCommandOptions cliOptions = alignmentCommandOptions.indexAlignmentCommandOptions;

//        ObjectMap objectMap = new ObjectMap();
//        objectMap.putIfNotNull("fileId", cliOptions.fileId);

        ObjectMap params = new ObjectMap();
        if (!cliOptions.load && !cliOptions.transform) {  // if not present --transform nor --load,
            // do both
            params.put("extract", true);
            params.put("load", true);
            params.put("transform", true);
        } else {
            params.put("extract", cliOptions.transform);
            params.put("load", cliOptions.load);
            params.put("transform", cliOptions.transform);
        }

        String sessionId = cliOptions.commonOptions.sessionId;

        org.opencb.opencga.storage.core.manager.AlignmentStorageManager alignmentStorageManager =
                new org.opencb.opencga.storage.core.manager.AlignmentStorageManager(catalogManager, storageEngineFactory);
        alignmentStorageManager.index(cliOptions.study, cliOptions.fileId, params, sessionId);
    }

//    @Deprecated
//    private void index(Job job) throws CatalogException, IllegalAccessException, ClassNotFoundException, InstantiationException, StorageEngineException {
//
//        AnalysisCliOptionsParser.IndexAlignmentCommandOptions cliOptions = alignmentCommandOptions.indexAlignmentCommandOptions;
//
//
//        String sessionId = cliOptions.commonOptions.sessionId;
//        long inputFileId = catalogManager.getFileId(cliOptions.fileId);
//
//        // 1) Initialize VariantStorageEngine
//        long studyId = catalogManager.getStudyIdByFileId(inputFileId);
//        Study study = catalogManager.getStudy(studyId, sessionId).first();
//
//        /*
//         * Getting VariantStorageEngine
//         * We need to find out the Storage Engine Id to be used from Catalog
//         */
//        DataStore dataStore = AbstractFileIndexer.getDataStore(catalogManager, studyId, File.Bioformat.ALIGNMENT, sessionId);
//        initAlignmentStorageManager(dataStore);
//
//
//        // 2) Read and validate cli args. Configure options
////        ObjectMap alignmentOptions = alignmentStorageManager.getOptions();
//        ObjectMap alignmentOptions = new ObjectMap();
//        if (Integer.parseInt(cliOptions.fileId) != 0) {
//            alignmentOptions.put(AlignmentStorageEngineOld.Options.FILE_ID.key(), cliOptions.fileId);
//        }
//
//        alignmentOptions.put(AlignmentStorageEngineOld.Options.DB_NAME.key(), dataStore.getDbName());
//
//        if (cliOptions.commonOptions.params != null) {
//            alignmentOptions.putAll(cliOptions.commonOptions.params);
//        }
//
//        alignmentOptions.put(AlignmentStorageEngineOld.Options.PLAIN.key(), false);
//        alignmentOptions.put(AlignmentStorageEngineOld.Options.INCLUDE_COVERAGE.key(), cliOptions.calculateCoverage);
//        if (cliOptions.meanCoverage != null && !cliOptions.meanCoverage.isEmpty()) {
//            alignmentOptions.put(AlignmentStorageEngineOld.Options.MEAN_COVERAGE_SIZE_LIST.key(), cliOptions.meanCoverage);
//        }
//        alignmentOptions.put(AlignmentStorageEngineOld.Options.COPY_FILE.key(), false);
//        alignmentOptions.put(AlignmentStorageEngineOld.Options.ENCRYPT.key(), "null");
//        logger.debug("Configuration options: {}", alignmentOptions.toJson());
//
//
//        final boolean doExtract;
//        final boolean doTransform;
//        final boolean doLoad;
//        StoragePipelineResult storageETLResult = null;
//        Exception exception = null;
//
//        File file = catalogManager.getFile(inputFileId, sessionId).first();
//        URI inputUri = catalogManager.getFileUri(file);
////        FileUtils.checkFile(Paths.get(inputUri.getPath()));
//
////        URI outdirUri = job.getTmpOutDirUri();
//        URI outdirUri = IndexDaemon.getJobTemporaryFolder(job.getId(), catalogConfiguration.getTempJobsDir()).toUri();
////        FileUtils.checkDirectory(Paths.get(outdirUri.getPath()));
//
//
//        if (!cliOptions.load && !cliOptions.transform) {  // if not present --transform nor --load,
//            // do both
//            doExtract = true;
//            doTransform = true;
//            doLoad = true;
//        } else {
//            doExtract = cliOptions.transform;
//            doTransform = cliOptions.transform;
//            doLoad = cliOptions.load;
//        }
//
//        // 3) Execute indexation
//        try {
//            storageETLResult = alignmentStorageManager.index(Collections.singletonList(inputUri), outdirUri, doExtract, doTransform, doLoad).get(0);
//
//        } catch (StoragePipelineException e) {
//            storageETLResult = e.getResults().get(0);
//            exception = e;
//            e.printStackTrace();
//            throw e;
//        } catch (Exception e) {
//            exception = e;
//            e.printStackTrace();
//            throw e;
//        } finally {
//            // 4) Save indexation result.
//            // TODO: Uncomment this line
////            new ExecutionOutputRecorder(catalogManager, sessionId).saveStorageResult(job, storageETLResult);
//        }
//    }

    private void query() throws InterruptedException, CatalogException, IOException {
        ObjectMap objectMap = new ObjectMap();
//        objectMap.putIfNotNull("fileId", alignmentCommandOptions.queryAlignmentCommandOptions.fileId);
        objectMap.putIfNotNull("sid", alignmentCommandOptions.queryAlignmentCommandOptions.commonOptions.sessionId);
        objectMap.putIfNotNull("study", alignmentCommandOptions.queryAlignmentCommandOptions.study);
        objectMap.putIfNotNull(AlignmentDBAdaptor.QueryParams.REGION.key(), alignmentCommandOptions.queryAlignmentCommandOptions.region);
        objectMap.putIfNotNull(AlignmentDBAdaptor.QueryParams.MIN_MAPQ.key(),
                alignmentCommandOptions.queryAlignmentCommandOptions.minMappingQuality);
        objectMap.putIfNotNull(AlignmentDBAdaptor.QueryParams.CONTAINED.key(),
                alignmentCommandOptions.queryAlignmentCommandOptions.contained);
        objectMap.putIfNotNull(AlignmentDBAdaptor.QueryParams.MD_FIELD.key(),
                alignmentCommandOptions.queryAlignmentCommandOptions.mdField);
        objectMap.putIfNotNull(AlignmentDBAdaptor.QueryParams.BIN_QUALITIES.key(),
                alignmentCommandOptions.queryAlignmentCommandOptions.binQualities);
        objectMap.putIfNotNull("count", alignmentCommandOptions.queryAlignmentCommandOptions.count);
        objectMap.putIfNotNull(AlignmentDBAdaptor.QueryParams.LIMIT.key(), alignmentCommandOptions.queryAlignmentCommandOptions.limit);
        objectMap.putIfNotNull(AlignmentDBAdaptor.QueryParams.SKIP.key(), alignmentCommandOptions.queryAlignmentCommandOptions.skip);

        OpenCGAClient openCGAClient = new OpenCGAClient(clientConfiguration);
        QueryResponse<ReadAlignment> alignments = openCGAClient.getAlignmentClient()
                .query(alignmentCommandOptions.queryAlignmentCommandOptions.fileId, objectMap);

        for (ReadAlignment readAlignment : alignments.allResults()) {
            System.out.println(readAlignment);
        }
    }

//    private void query() throws FileFormatException, ClassNotFoundException, InstantiationException, CatalogException, IllegalAccessException, StorageEngineException, IOException, NoSuchMethodException {
//
//
//        AnalysisCliOptionsParser.QueryAlignmentCommandOptions cliOptions = alignmentCommandOptions.queryAlignmentCommandOptions;
//
//        String sessionId = cliOptions.commonOptions.sessionId;
//
//        long studyId;
//        if (StringUtils.isEmpty(cliOptions.study)) {
//            Map<Long, String> studyIds = getStudyIds(sessionId);
//            if (studyIds.size() != 1) {
//                throw new IllegalArgumentException("Missing study. Please select one from: " + studyIds.entrySet()
//                        .stream()
//                        .map(entry -> entry.getKey() + ":" + entry.getValue())
//                        .collect(Collectors.joining(",", "[", "]")));
//            }
//            else {
//                studyId = studyIds.entrySet().iterator().next().getKey();
//            }
//        } else {
//            studyId = catalogManager.getStudyId(cliOptions.study);
//        }
//
//        /*
//         * Getting VariantStorageEngine
//         * We need to find out the Storage Engine Id to be used from Catalog
//         */
//        DataStore dataStore = AbstractFileIndexer.getDataStore(catalogManager, studyId, File.Bioformat.ALIGNMENT, sessionId);
//        initAlignmentStorageManager(dataStore);
//
//        AlignmentDBAdaptor dbAdaptor = alignmentStorageManager.getDBAdaptor(dataStore.getDbName());
//
//        /**
//         * Parse Regions
//         */
//        GffReader gffReader = null;
//        List<Region> regions = Collections.emptyList();
//        if (StringUtils.isNotEmpty(cliOptions.region)) {
//            regions = Region.parseRegions(cliOptions.region);
//            logger.debug("Processed regions: '{}'", regions);
//        } else if (StringUtils.isNotEmpty(cliOptions.regionFile)) {
//            gffReader = new GffReader(cliOptions.regionFile);
//            //throw new UnsupportedOperationException("Unsuppoted GFF file");
//        }
//
//        /**
//         * Parse QueryOptions
//         */
//        QueryOptions options = new QueryOptions();
//
//        if (cliOptions.fileId != null && !cliOptions.fileId.isEmpty()) {
//            long fileId = catalogManager.getFileId(cliOptions.fileId);
//            File file = catalogManager.getFile(fileId, sessionId).first();
//            URI fileUri = catalogManager.getFileUri(file);
//            options.add(AlignmentDBAdaptor.QO_FILE_ID, cliOptions.fileId);
//            options.add(AlignmentDBAdaptor.QO_BAM_PATH, fileUri.getPath());
//        }
//        options.add(AlignmentDBAdaptor.QO_INCLUDE_COVERAGE, cliOptions.coverage);
//        options.add(AlignmentDBAdaptor.QO_VIEW_AS_PAIRS, cliOptions.asPairs);
//        options.add(AlignmentDBAdaptor.QO_PROCESS_DIFFERENCES, cliOptions.processDifferences);
//        if (cliOptions.histogram) {
//            options.add(AlignmentDBAdaptor.QO_INCLUDE_COVERAGE, true);
//            options.add(AlignmentDBAdaptor.QO_HISTOGRAM, true);
//            options.add(AlignmentDBAdaptor.QO_INTERVAL_SIZE, cliOptions.histogram);
//        }
////        if (cliOptions.filePath != null && !cliOptions.filePath.isEmpty()) {
////            options.add(AlignmentDBAdaptor.QO_BAM_PATH, cliOptions.filePath);
////        }
//
//
//        if (cliOptions.stats != null && !cliOptions.stats.isEmpty()) {
//            for (String csvStat : cliOptions.stats) {
//                for (String stat : csvStat.split(",")) {
//                    int index = stat.indexOf("<");
//                    index = index >= 0 ? index : stat.indexOf("!");
//                    index = index >= 0 ? index : stat.indexOf("~");
//                    index = index >= 0 ? index : stat.indexOf("<");
//                    index = index >= 0 ? index : stat.indexOf(">");
//                    index = index >= 0 ? index : stat.indexOf("=");
//                    if (index < 0) {
//                        throw new UnsupportedOperationException("Unknown stat filter operation: " + stat);
//                    }
//                    String name = stat.substring(0, index);
//                    String cond = stat.substring(index);
//
//                    if (name.matches("")) {
//                        options.put(name, cond);
//                    } else {
//                        throw new UnsupportedOperationException("Unknown stat filter name: " + name);
//                    }
//                    logger.info("Parsed stat filter: {} {}", name, cond);
//                }
//            }
//        }
//
//
//        /**
//         * Run query
//         */
//        int subListSize = 20;
//        logger.info("options = {}", options.toJson());
//        if (cliOptions.histogram) {
//            for (Region region : regions) {
//                System.out.println(dbAdaptor.getAllIntervalFrequencies(region, options));
//            }
//        } else if (regions != null && !regions.isEmpty()) {
//            for (int i = 0; i < (regions.size() + subListSize - 1) / subListSize; i++) {
//                List<Region> subRegions = regions.subList(
//                        i * subListSize,
//                        Math.min((i + 1) * subListSize, regions.size()));
//
//                logger.info("subRegions = " + subRegions);
//                QueryResult queryResult = dbAdaptor.getAllAlignmentsByRegion(subRegions, options);
//                logger.info("{}", queryResult);
//                System.out.println(new ObjectMap("queryResult", queryResult).toJson());
//            }
//        } else if (gffReader != null) {
//            List<Gff> gffList;
//            List<Region> subRegions;
//            while ((gffList = gffReader.read(subListSize)) != null) {
//                subRegions = new ArrayList<>(subListSize);
//                for (Gff gff : gffList) {
//                    subRegions.add(new Region(gff.getSequenceName(), gff.getStart(), gff.getEnd()));
//                }
//
//                logger.info("subRegions = " + subRegions);
//                QueryResult queryResult = dbAdaptor.getAllAlignmentsByRegion(subRegions, options);
//                logger.info("{}", queryResult);
//                System.out.println(new ObjectMap("queryResult", queryResult).toJson());
//            }
//        } else {
//            throw new UnsupportedOperationException("Unable to fetch over all the genome");
////                System.out.println(dbAdaptor.getAllAlignments(options));
//        }
//
//    }

    private void stats() throws CatalogException, IOException {
        ObjectMap objectMap = new ObjectMap();
//        objectMap.putIfNotNull("fileId", alignmentCommandOptions.statsAlignmentCommandOptions.fileId);
        objectMap.putIfNotNull("sid", alignmentCommandOptions.statsAlignmentCommandOptions.commonOptions.sessionId);
        objectMap.putIfNotNull("study", alignmentCommandOptions.statsAlignmentCommandOptions.study);
        objectMap.putIfNotNull("region", alignmentCommandOptions.statsAlignmentCommandOptions.region);
        objectMap.putIfNotNull("minMapQ", alignmentCommandOptions.statsAlignmentCommandOptions.minMappingQuality);
        if (alignmentCommandOptions.statsAlignmentCommandOptions.contained) {
            objectMap.put("contained", alignmentCommandOptions.statsAlignmentCommandOptions.contained);
        }

        OpenCGAClient openCGAClient = new OpenCGAClient(clientConfiguration);
        QueryResponse<AlignmentGlobalStats> globalStats = openCGAClient.getAlignmentClient()
                .stats(alignmentCommandOptions.statsAlignmentCommandOptions.fileId, objectMap);

        for (AlignmentGlobalStats alignmentGlobalStats : globalStats.allResults()) {
            System.out.println(alignmentGlobalStats.toJSON());
        }
    }

    private void coverage() throws CatalogException, IOException {
        ObjectMap objectMap = new ObjectMap();
//        objectMap.putIfNotNull("fileId", alignmentCommandOptions.coverageAlignmentCommandOptions.fileId);
        objectMap.putIfNotNull("sid", alignmentCommandOptions.coverageAlignmentCommandOptions.commonOptions.sessionId);
        objectMap.putIfNotNull("study", alignmentCommandOptions.coverageAlignmentCommandOptions.study);
        objectMap.putIfNotNull("region", alignmentCommandOptions.coverageAlignmentCommandOptions.region);
        objectMap.putIfNotNull("minMapQ", alignmentCommandOptions.coverageAlignmentCommandOptions.minMappingQuality);
        if (alignmentCommandOptions.coverageAlignmentCommandOptions.contained) {
            objectMap.put("contained", alignmentCommandOptions.coverageAlignmentCommandOptions.contained);
        }

        OpenCGAClient openCGAClient = new OpenCGAClient(clientConfiguration);
        QueryResponse<RegionCoverage> globalStats = openCGAClient.getAlignmentClient()
                .coverage(alignmentCommandOptions.coverageAlignmentCommandOptions.fileId, objectMap);

        for (RegionCoverage regionCoverage : globalStats.allResults()) {
            System.out.println(regionCoverage.toString());
        }
    }

    private void delete() {
        throw new UnsupportedOperationException();
    }

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
