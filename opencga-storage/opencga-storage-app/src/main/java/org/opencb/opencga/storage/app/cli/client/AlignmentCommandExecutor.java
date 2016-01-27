/*
 * Copyright 2015 OpenCB
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

package org.opencb.opencga.storage.app.cli.client;

import org.opencb.biodata.formats.feature.gff.Gff;
import org.opencb.biodata.formats.feature.gff.io.GffReader;
import org.opencb.biodata.formats.io.FileFormatException;
import org.opencb.biodata.models.core.Region;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.storage.app.cli.CommandExecutor;
import org.opencb.opencga.storage.core.StorageManagerException;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.alignment.AlignmentStorageManager;
import org.opencb.opencga.storage.core.alignment.adaptors.AlignmentDBAdaptor;
import org.opencb.opencga.storage.core.config.StorageEngineConfiguration;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by imedina on 22/05/15.
 */
public class AlignmentCommandExecutor extends CommandExecutor {

    private StorageEngineConfiguration storageConfiguration;
    private AlignmentStorageManager alignmentStorageManager;

    private CliOptionsParser.AlignmentCommandOptions commandOptions;

    public AlignmentCommandExecutor(CliOptionsParser.AlignmentCommandOptions commandOptions) {
        super(commandOptions.commonOptions);
        this.commandOptions = commandOptions;
    }

    private void configure(CliOptionsParser.CommonOptions commonOptions) throws Exception {

        this.logFile = commonOptions.logFile;

        /**
         * Getting VariantStorageManager
         * We need to find out the Storage Engine Id to be used
         * If not storage engine is passed then the default is taken from storage-configuration.yml file
         **/
        this.storageEngine = (storageEngine != null && !storageEngine.isEmpty())
                ? storageEngine
                : configuration.getDefaultStorageEngineId();
        logger.debug("Storage Engine set to '{}'", this.storageEngine);

        this.storageConfiguration = configuration.getStorageEngine(storageEngine);

        StorageManagerFactory storageManagerFactory = new StorageManagerFactory(configuration);
        if (storageEngine == null || storageEngine.isEmpty()) {
            this.alignmentStorageManager = storageManagerFactory.getAlignmentStorageManager();
        } else {
            this.alignmentStorageManager = storageManagerFactory.getAlignmentStorageManager(storageEngine);
        }
    }


    @Override
    public void execute() throws Exception {
        logger.debug("Executing alignment command line");

        String subCommandString = commandOptions.getParsedSubCommand();
        switch (subCommandString) {
            case "index":
                configure(commandOptions.indexAlignmentsCommandOptions.commonOptions);
                index();
                break;
            case "query":
                configure(commandOptions.queryAlignmentsCommandOptions.commonOptions);
                query();
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }

    }

    private void index() throws URISyntaxException, StorageManagerException, IOException, FileFormatException {
        CliOptionsParser.IndexAlignmentsCommandOptions indexAlignmentsCommandOptions = commandOptions.indexAlignmentsCommandOptions;

        URI inputUri = UriUtils.createUri(indexAlignmentsCommandOptions.input);
//        FileUtils.checkFile(Paths.get(inputUri.getPath()));

        URI outdirUri = (indexAlignmentsCommandOptions.outdir != null && !indexAlignmentsCommandOptions.outdir.isEmpty())
                ? UriUtils.createDirectoryUri(indexAlignmentsCommandOptions.outdir)
                // Get parent folder from input file
                : inputUri.resolve(".");
//        FileUtils.checkDirectory(Paths.get(outdirUri.getPath()));
        logger.debug("All files and directories exist");

            /*
             * Add CLI options to the alignmentOptions
             */
        ObjectMap alignmentOptions = storageConfiguration.getAlignment().getOptions();
        if (Integer.parseInt(indexAlignmentsCommandOptions.fileId) != 0) {
            alignmentOptions.put(AlignmentStorageManager.Options.FILE_ID.key(), indexAlignmentsCommandOptions.fileId);
        }
        if (indexAlignmentsCommandOptions.dbName != null && !indexAlignmentsCommandOptions.dbName.isEmpty()) {
            alignmentOptions.put(AlignmentStorageManager.Options.DB_NAME.key(), indexAlignmentsCommandOptions.dbName);
        }
        if (indexAlignmentsCommandOptions.commonOptions.params != null) {
            alignmentOptions.putAll(indexAlignmentsCommandOptions.commonOptions.params);
        }

        alignmentOptions.put(AlignmentStorageManager.Options.PLAIN.key(), false);
        alignmentOptions.put(AlignmentStorageManager.Options.INCLUDE_COVERAGE.key(), indexAlignmentsCommandOptions.calculateCoverage);
        if (indexAlignmentsCommandOptions.meanCoverage != null && !indexAlignmentsCommandOptions.meanCoverage.isEmpty()) {
            alignmentOptions.put(AlignmentStorageManager.Options.MEAN_COVERAGE_SIZE_LIST.key(), indexAlignmentsCommandOptions.meanCoverage);
        }
        alignmentOptions.put(AlignmentStorageManager.Options.COPY_FILE.key(), false);
        alignmentOptions.put(AlignmentStorageManager.Options.ENCRYPT.key(), "null");
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

        if (extract) {
            logger.info("-- Extract alignments -- {}", inputUri);
            nextFileUri = alignmentStorageManager.extract(inputUri, outdirUri);
        }

        if (transform) {
            logger.info("-- PreTransform alignments -- {}", nextFileUri);
            nextFileUri = alignmentStorageManager.preTransform(nextFileUri);
            logger.info("-- Transform alignments -- {}", nextFileUri);
            nextFileUri = alignmentStorageManager.transform(nextFileUri, null, outdirUri);
            logger.info("-- PostTransform alignments -- {}", nextFileUri);
            nextFileUri = alignmentStorageManager.postTransform(nextFileUri);
        }

        if (load) {
            logger.info("-- PreLoad alignments -- {}", nextFileUri);
            nextFileUri = alignmentStorageManager.preLoad(nextFileUri, outdirUri);
            logger.info("-- Load alignments -- {}", nextFileUri);
            nextFileUri = alignmentStorageManager.load(nextFileUri);
            logger.info("-- PostLoad alignments -- {}", nextFileUri);
            nextFileUri = alignmentStorageManager.postLoad(nextFileUri, outdirUri);
        }
    }

    private void query() throws StorageManagerException, FileFormatException {
        CliOptionsParser.QueryAlignmentsCommandOptions queryAlignmentsCommandOptions = commandOptions.queryAlignmentsCommandOptions;
        AlignmentDBAdaptor dbAdaptor = alignmentStorageManager.getDBAdaptor(queryAlignmentsCommandOptions.dbName);

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
        int subListSize = 20;
        logger.info("options = {}", options.toJson());
        if (queryAlignmentsCommandOptions.histogram) {
            for (Region region : regions) {
                System.out.println(dbAdaptor.getAllIntervalFrequencies(region, options));
            }
        } else if (regions != null && !regions.isEmpty()) {
            for (int i = 0; i < (regions.size() + subListSize - 1) / subListSize; i++) {
                List<Region> subRegions = regions.subList(
                        i * subListSize,
                        Math.min((i + 1) * subListSize, regions.size()));

                logger.info("subRegions = " + subRegions);
                QueryResult queryResult = dbAdaptor.getAllAlignmentsByRegion(subRegions, options);
                logger.info("{}", queryResult);
                System.out.println(new ObjectMap("queryResult", queryResult).toJson());
            }
        } else if (gffReader != null) {
            List<Gff> gffList;
            List<Region> subRegions;
            while ((gffList = gffReader.read(subListSize)) != null) {
                subRegions = new ArrayList<>(subListSize);
                for (Gff gff : gffList) {
                    subRegions.add(new Region(gff.getSequenceName(), gff.getStart(), gff.getEnd()));
                }

                logger.info("subRegions = " + subRegions);
                QueryResult queryResult = dbAdaptor.getAllAlignmentsByRegion(subRegions, options);
                logger.info("{}", queryResult);
                System.out.println(new ObjectMap("queryResult", queryResult).toJson());
            }
        } else {
            throw new UnsupportedOperationException("Unable to fetch over all the genome");
//                System.out.println(dbAdaptor.getAllAlignments(options));
        }
    }
}
