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

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.formats.feature.gff.io.GffReader;
import org.opencb.biodata.formats.io.FileFormatException;
import org.opencb.biodata.models.alignment.RegionCoverage;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.tools.alignment.BamManager;
import org.opencb.biodata.tools.alignment.exceptions.AlignmentCoverageException;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
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

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * Created by imedina on 22/05/15.
 */
public class AlignmentCommandExecutor extends CommandExecutor {

    private StorageEngineConfiguration storageConfiguration;
    private AlignmentStorageEngine alignmentStorageManager;

    private StorageAlignmentCommandOptions alignmentCommandOptions;

    public AlignmentCommandExecutor(StorageAlignmentCommandOptions alignmentCommandOptions) {
        super(alignmentCommandOptions.commonCommandOptions);
        this.alignmentCommandOptions = alignmentCommandOptions;
    }

    private void configure(ClientCliOptionsParser.CommonOptions commonOptions, String dbName) throws Exception {

        this.logFile = commonOptions.logFile;

        this.storageConfiguration = configuration.getAlignment();

        // TODO: Start passing catalogManager
        StorageEngineFactory storageEngineFactory = StorageEngineFactory.get(configuration);
        this.alignmentStorageManager = storageEngineFactory.getAlignmentStorageEngine(dbName);
    }


    @Override
    public void execute() throws Exception {
        logger.debug("Executing alignment command line");

//        String subCommandString = alignmentCommandOptions.getParsedSubCommand();
        String subCommandString = getParsedSubCommand(alignmentCommandOptions.jCommander);
        switch (subCommandString) {
            case "index":
                configure(alignmentCommandOptions.indexAlignmentsCommandOptions.commonOptions, alignmentCommandOptions.indexAlignmentsCommandOptions.commonIndexOptions.dbName);
                index();
                break;
            case "query":
                configure(alignmentCommandOptions.queryAlignmentsCommandOptions.commonOptions, alignmentCommandOptions.queryAlignmentsCommandOptions.commonQueryOptions.dbName);
                query();
                break;
            case "coverage":
                configure(alignmentCommandOptions.queryAlignmentsCommandOptions.commonOptions, alignmentCommandOptions.queryAlignmentsCommandOptions.commonQueryOptions.dbName);
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
        ObjectMap alignmentOptions = storageConfiguration.getOptions();
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

        try (StoragePipeline storagePipeline = alignmentStorageManager.newStoragePipeline(true)) {

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

    private void query() throws StorageEngineException, FileFormatException {
        StorageAlignmentCommandOptions.QueryAlignmentsCommandOptions queryAlignmentsCommandOptions = alignmentCommandOptions.queryAlignmentsCommandOptions;
        AlignmentDBAdaptor dbAdaptor = alignmentStorageManager.getDBAdaptor();

        /**
         * Parse Regions
         */
        GffReader gffReader = null;
        List<Region> regions = null;
        if (queryAlignmentsCommandOptions.region != null && !queryAlignmentsCommandOptions.region.isEmpty()) {
            regions = Region.parseRegions(queryAlignmentsCommandOptions.region);
            logger.debug("Processed regions: '{}'", regions);
//            regions = new LinkedList<>();
//            for (String csvRegion : queryAlignmentsCommandOptions.regions) {
//                for (String strRegion : csvRegion.split(",")) {
//                    Region region = new Region(strRegion);
//                    regions.add(region);
//                    logger.info("Parsed region: {}", region);
//                }
//            }
        } else if (queryAlignmentsCommandOptions.regionFile != null && !queryAlignmentsCommandOptions.regionFile.isEmpty()) {
            try {
                gffReader = new GffReader(queryAlignmentsCommandOptions.regionFile);
            } catch (NoSuchMethodException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            //throw new UnsupportedOperationException("Unsuppoted GFF file");
        }

        /**
         * Parse QueryOptions
         */
        QueryOptions options = new QueryOptions();

        if (queryAlignmentsCommandOptions.fileId != null && !queryAlignmentsCommandOptions.fileId.isEmpty()) {
            options.add(AlignmentDBAdaptor.QO_FILE_ID, queryAlignmentsCommandOptions.fileId);
        }
        options.add(AlignmentDBAdaptor.QO_INCLUDE_COVERAGE, queryAlignmentsCommandOptions.coverage);
        options.add(AlignmentDBAdaptor.QO_VIEW_AS_PAIRS, queryAlignmentsCommandOptions.asPairs);
        options.add(AlignmentDBAdaptor.QO_PROCESS_DIFFERENCES, queryAlignmentsCommandOptions.processDifferences);
        if (queryAlignmentsCommandOptions.histogram) {
            options.add(AlignmentDBAdaptor.QO_INCLUDE_COVERAGE, true);
            options.add(AlignmentDBAdaptor.QO_HISTOGRAM, true);
            options.add(AlignmentDBAdaptor.QO_INTERVAL_SIZE, queryAlignmentsCommandOptions.histogram);
        }
        if (queryAlignmentsCommandOptions.filePath != null && !queryAlignmentsCommandOptions.filePath.isEmpty()) {
            options.add(AlignmentDBAdaptor.QO_BAM_PATH, queryAlignmentsCommandOptions.filePath);
        }


        if (queryAlignmentsCommandOptions.stats != null && !queryAlignmentsCommandOptions.stats.isEmpty()) {
            for (String csvStat : queryAlignmentsCommandOptions.stats) {
                for (String stat : csvStat.split(",")) {
                    int index = stat.indexOf("<");
                    index = index >= 0 ? index : stat.indexOf("!");
                    index = index >= 0 ? index : stat.indexOf("~");
                    index = index >= 0 ? index : stat.indexOf("<");
                    index = index >= 0 ? index : stat.indexOf(">");
                    index = index >= 0 ? index : stat.indexOf("=");
                    if (index < 0) {
                        throw new UnsupportedOperationException("Unknown stat filter operation: " + stat);
                    }
                    String name = stat.substring(0, index);
                    String cond = stat.substring(index);

                    if (name.matches("")) {
                        options.put(name, cond);
                    } else {
                        throw new UnsupportedOperationException("Unknown stat filter name: " + name);
                    }
                    logger.info("Parsed stat filter: {} {}", name, cond);
                }
            }
        }

        /**
         * Run query
         */
//        int subListSize = 20;
//        logger.info("options = {}", options.toJson());
//        if (queryAlignmentsCommandOptions.histogram) {
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
    }

    private void coverage() throws IOException, AlignmentCoverageException {
        StorageAlignmentCommandOptions.CoverageAlignmentsCommandOptions coverageAlignmentsCommandOptions
                = alignmentCommandOptions.coverageAlignmentsCommandOptions;

        String fileId = coverageAlignmentsCommandOptions.fileId;
        int windowSize = coverageAlignmentsCommandOptions.windowSize;
        String region = coverageAlignmentsCommandOptions.region;

        Path path = Paths.get(fileId);
        FileUtils.checkFile(path);

        BamManager bamManager = new BamManager(path);

        if (coverageAlignmentsCommandOptions.create) {
            Path bigWigPath = Paths.get(fileId + ".coverage.bw");
            bamManager.calculateBigWigCoverage(bigWigPath, windowSize);
        }

        if (StringUtils.isNotEmpty(region)) {
            RegionCoverage coverage = bamManager.coverage(Region.parseRegion(region), windowSize);
            if (coverage != null) {
                System.out.println(coverage);
            } else {
                System.out.println("BigWig file does not exist or region is too big");
            }
        }
    }
}
