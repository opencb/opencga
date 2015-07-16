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

package org.opencb.opencga.storage.app.cli;

import org.opencb.biodata.formats.feature.gff.Gff;
import org.opencb.biodata.formats.feature.gff.io.GffReader;
import org.opencb.biodata.models.feature.Region;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.alignment.AlignmentStorageManager;
import org.opencb.opencga.storage.core.alignment.adaptors.AlignmentDBAdaptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by imedina on 25/05/15.
 */
public class FetchAlignmentsCommandExecutor extends CommandExecutor {

    private CliOptionsParser.QueryAlignmentsCommandOptions queryAlignmentsCommandOptions;


    public FetchAlignmentsCommandExecutor(CliOptionsParser.QueryAlignmentsCommandOptions queryAlignmentsCommandOptions) {
        super(queryAlignmentsCommandOptions.logLevel, queryAlignmentsCommandOptions.verbose,
                queryAlignmentsCommandOptions.configFile);

        this.queryAlignmentsCommandOptions = queryAlignmentsCommandOptions;
    }


    @Override
    public void execute() throws Exception {
        /**
         * Open connection
         */
        AlignmentStorageManager alignmentStorageManager = new StorageManagerFactory(configuration).getAlignmentStorageManager(queryAlignmentsCommandOptions.backend);
//            if (queryAlignmentsCommandOptions.credentials != null && !queryAlignmentsCommandOptions.credentials.isEmpty()) {
//                alignmentStorageManager.addConfigUri(new URI(null, queryAlignmentsCommandOptions.credentials, null));
//            }

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
