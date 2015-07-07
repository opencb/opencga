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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.biodata.formats.feature.gff.Gff;
import org.opencb.biodata.formats.feature.gff.io.GffReader;
import org.opencb.biodata.formats.io.FileFormatException;
import org.opencb.biodata.models.feature.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantSourceEntry;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.StorageManagerException;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.config.StorageEngineConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.io.json.VariantSourceEntryJsonMixin;
import org.opencb.opencga.storage.core.variant.io.json.VariantSourceJsonMixin;
import org.opencb.opencga.storage.core.variant.io.json.VariantStatsJsonMixin;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

/**
 * Created by imedina on 25/05/15.
 */
public class FetchVariantsCommandExecutor extends CommandExecutor {

    private CliOptionsParser.QueryVariantsCommandOptions queryVariantsCommandOptions;


    public FetchVariantsCommandExecutor(CliOptionsParser.QueryVariantsCommandOptions queryVariantsCommandOptions) {
        super(queryVariantsCommandOptions.logLevel, queryVariantsCommandOptions.verbose,
                queryVariantsCommandOptions.configFile);

        this.queryVariantsCommandOptions = queryVariantsCommandOptions;
    }


    @Override
    public void execute() throws Exception {

        /**
        * Getting VariantStorageManager
        * We need to find out the Storage Engine Id to be used
        * If not storage engine is passed then the default is taken from storage-configuration.yml file
        **/
        String storageEngine = (queryVariantsCommandOptions.storageEngine != null && !queryVariantsCommandOptions.storageEngine.isEmpty())
                ? queryVariantsCommandOptions.storageEngine
                : configuration.getDefaultStorageEngineId();
        logger.debug("Storage Engine set to '{}'", storageEngine);

        StorageEngineConfiguration storageConfiguration = configuration.getStorageEngine(storageEngine);

        StorageManagerFactory storageManagerFactory = new StorageManagerFactory(configuration);
        VariantStorageManager variantStorageManager;
        if (storageEngine == null || storageEngine.isEmpty()) {
            variantStorageManager = storageManagerFactory.getVariantStorageManager();
        } else {
            variantStorageManager = storageManagerFactory.getVariantStorageManager(storageEngine);
        }
        storageConfiguration.getVariant().getOptions().putAll(queryVariantsCommandOptions.params);
//        VariantStorageManager variantStorageManager = new StorageManagerFactory(configuration).getVariantStorageManager(queryVariantsCommandOptions.backend);


        ObjectMap params = new ObjectMap();

        VariantDBAdaptor dbAdaptor = variantStorageManager.getDBAdaptor(queryVariantsCommandOptions.dbName);

        /**
         * Parse Regions
         */
        List<Region> regions = null;
        GffReader gffReader = null;
        if (queryVariantsCommandOptions.regions != null && !queryVariantsCommandOptions.regions.isEmpty()) {
            regions = new LinkedList<>();
            for (String csvRegion : queryVariantsCommandOptions.regions) {
                for (String strRegion : csvRegion.split(",")) {
                    Region region = new Region(strRegion);
                    regions.add(region);
                    logger.info("Parsed region: {}", region);
                }
            }
        } else if (queryVariantsCommandOptions.gffFile != null && !queryVariantsCommandOptions.gffFile.isEmpty()) {
            try {
                gffReader = new GffReader(queryVariantsCommandOptions.gffFile);
            } catch (NoSuchMethodException e) {
                e.printStackTrace();
            }
//                throw new UnsupportedOperationException("Unsuppoted GFF file");
        }

        /**
         * Parse QueryOptions
         */
        QueryOptions options = new QueryOptions(new HashMap<>(queryVariantsCommandOptions.params));

        if (queryVariantsCommandOptions.studyAlias != null && !queryVariantsCommandOptions.studyAlias.isEmpty()) {
            options.add("studies", Arrays.asList(queryVariantsCommandOptions.studyAlias.split(",")));
        }
        if (queryVariantsCommandOptions.fileId != null && !queryVariantsCommandOptions.fileId.isEmpty()) {
            options.add("files", Arrays.asList(queryVariantsCommandOptions.fileId.split(",")));
        }
        if (queryVariantsCommandOptions.effect != null && !queryVariantsCommandOptions.effect.isEmpty()) {
            options.add("effect", Arrays.asList(queryVariantsCommandOptions.effect.split(",")));
        }

        if (queryVariantsCommandOptions.stats != null && !queryVariantsCommandOptions.stats.isEmpty()) {
            for (String csvStat : queryVariantsCommandOptions.stats) {
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

//                        if("maf".equals(name) || "mgf".equals(name) || "missingAlleles".equals(name) || "missingGenotypes".equals(name)) {
                    if (name.matches("maf|mgf|missingAlleles|missingGenotypes")) {
                        options.put(name, cond);
                    } else {
                        throw new UnsupportedOperationException("Unknown stat filter name: " + name);
                    }
                    logger.info("Parsed stat filter: {} {}", name, cond);
                }
            }
        }
        if (queryVariantsCommandOptions.id != null && !queryVariantsCommandOptions.id.isEmpty()) {   //csv
            options.add("id", queryVariantsCommandOptions.id);
        }
        if (queryVariantsCommandOptions.gene != null && !queryVariantsCommandOptions.gene.isEmpty()) {   //csv
            options.add("gene", queryVariantsCommandOptions.gene);
        }
        if (queryVariantsCommandOptions.type != null && !queryVariantsCommandOptions.type.isEmpty()) {   //csv
            options.add("type", queryVariantsCommandOptions.type);
        }
        if (queryVariantsCommandOptions.reference != null && !queryVariantsCommandOptions.reference.isEmpty()) {   //csv
            options.add("reference", queryVariantsCommandOptions.reference);
        }

        /**
         * Run query
         */
        int subListSize = 20;
        logger.info("options = " + options.toJson());
        if (regions != null && !regions.isEmpty()) {
            for (int i = 0; i < (regions.size() + subListSize - 1) / subListSize; i++) {
                List<Region> subRegions = regions.subList(
                        i * subListSize,
                        Math.min((i + 1) * subListSize, regions.size()));

                logger.info("subRegions = " + subRegions);
//                    List<QueryResult<Variant>> queryResults = dbAdaptor.getAllVariants(subRegions, options);
                List<QueryResult<Variant>> queryResults = dbAdaptor.getAllVariantsByRegionList(subRegions, options);
                StringBuilder sb = new StringBuilder();
                for (QueryResult<Variant> queryResult : queryResults) {
                    printQueryResult(queryResult, sb);
                }
                System.out.println(sb);
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
                List<QueryResult<Variant>> queryResults = dbAdaptor.getAllVariantsByRegionList(subRegions, options);
                StringBuilder sb = new StringBuilder();
                for (QueryResult<Variant> queryResult : queryResults) {
                    printQueryResult(queryResult, sb);
                }
                System.out.println(sb);
            }
        } else {
            System.out.println(printQueryResult(dbAdaptor.getAllVariants(options), null));
        }
    }

    public StringBuilder printQueryResult(QueryResult queryResult, StringBuilder sb) throws JsonProcessingException {
        if (sb == null) {
            sb = new StringBuilder();
        }
        ObjectMapper jsonObjectMapper;

        JsonFactory factory = new JsonFactory();
        jsonObjectMapper = new ObjectMapper(factory);

        jsonObjectMapper.addMixIn(VariantSourceEntry.class, VariantSourceEntryJsonMixin.class);
        jsonObjectMapper.addMixIn(VariantSource.class, VariantSourceJsonMixin.class);
        jsonObjectMapper.addMixIn(VariantStats.class, VariantStatsJsonMixin.class);

        jsonObjectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
//                objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        jsonObjectMapper.setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);

        sb.append(jsonObjectMapper.writeValueAsString(queryResult)).append("\n");
        return sb;
    }
}
