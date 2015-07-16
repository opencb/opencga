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

import com.beust.jcommander.ParameterException;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantSourceEntry;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.commons.utils.FileUtils;
import org.opencb.datastore.core.Query;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.config.StorageEngineConfiguration;
import org.opencb.opencga.storage.core.variant.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.io.json.VariantSourceEntryJsonMixin;
import org.opencb.opencga.storage.core.variant.io.json.VariantSourceJsonMixin;
import org.opencb.opencga.storage.core.variant.io.json.VariantStatsJsonMixin;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;

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

        VariantDBAdaptor variantDBAdaptor = variantStorageManager.getDBAdaptor(queryVariantsCommandOptions.dbName);

        Query query = new Query();
        QueryOptions options = new QueryOptions(new HashMap<>(queryVariantsCommandOptions.params));

        /**
         * Parse Regions
         */
        if (queryVariantsCommandOptions.region != null && !queryVariantsCommandOptions.region.isEmpty()) {
            query.put(VariantDBAdaptor.VariantQueryParams.REGION.key(), queryVariantsCommandOptions.region);
        } else if (queryVariantsCommandOptions.regionFile != null && !queryVariantsCommandOptions.regionFile.isEmpty()) {
            Path gffPath = Paths.get(queryVariantsCommandOptions.regionFile);
            FileUtils.checkFile(gffPath);
            String regionsFromFile = Files.readAllLines(gffPath).stream().map(line -> {
                String[] array = line.split("\t");
                return new String(array[0].replace("chr", "")+":"+array[3]+"-"+array[4]);
            }).collect(Collectors.joining(","));
            query.put(VariantDBAdaptor.VariantQueryParams.REGION.key(), regionsFromFile);
        }

        if (queryVariantsCommandOptions.gene != null && !queryVariantsCommandOptions.gene.isEmpty()) {   //csv
            query.put(VariantDBAdaptor.VariantQueryParams.GENE.key(), queryVariantsCommandOptions.gene);
        }

        if (queryVariantsCommandOptions.id != null && !queryVariantsCommandOptions.id.isEmpty()) {   //csv
            query.put(VariantDBAdaptor.VariantQueryParams.ID.key(), queryVariantsCommandOptions.id);
        }



        if (queryVariantsCommandOptions.study != null && !queryVariantsCommandOptions.study.isEmpty()) {
            query.put(VariantDBAdaptor.VariantQueryParams.STUDIES.key(), Arrays.asList(queryVariantsCommandOptions.study.split(",")));
        }

        // If the studies to be returned is empty then we return the studies being queried
        if (queryVariantsCommandOptions.returnStudy == null || queryVariantsCommandOptions.returnStudy.isEmpty()) {

        }

        if (queryVariantsCommandOptions.file != null && !queryVariantsCommandOptions.file.isEmpty()) {
            options.add(VariantDBAdaptor.VariantQueryParams.FILES.key(), Arrays.asList(queryVariantsCommandOptions.file.split(",")));
        }

//        if (queryVariantsCommandOptions.annot != null && !queryVariantsCommandOptions.annot.isEmpty()) {
//            options.add(, Arrays.asList(queryVariantsCommandOptions.annot.split(",")));
//        }

        if (queryVariantsCommandOptions.stats != null && !queryVariantsCommandOptions.stats.isEmpty()) {
            HashSet<String> acceptedStatKeys = new HashSet<>(Arrays.asList("maf", "mgf", "missingAlleles", "missingGenotypes"));
            for (String stat : queryVariantsCommandOptions.stats.split(",")) {
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

                if (acceptedStatKeys.contains(name)) {
                    query.put(name, cond);
                } else {
                    throw new UnsupportedOperationException("Unknown stat filter name: " + name);
                }
                logger.info("Parsed stat filter: {} {}", name, cond);
            }
        }

        if (queryVariantsCommandOptions.type != null && !queryVariantsCommandOptions.type.isEmpty()) {   //csv
            query.put(VariantDBAdaptor.VariantQueryParams.TYPE.key(), queryVariantsCommandOptions.type);
        }

        /*
         * Output parameters
         */
        String outputFormat = "vcf";
        boolean gzip = true;
        if(queryVariantsCommandOptions.outputFormat != null && !queryVariantsCommandOptions.outputFormat.isEmpty()) {
            switch (queryVariantsCommandOptions.outputFormat) {
                case "vcf":
                    gzip = false;
                case "vcf.gz":
                    outputFormat = "vcf";
                    break;
                case "json":
                    gzip = false;
                case "json.gz":
                    outputFormat = "json";
                    break;
                default:
                    logger.error("Format '{}' not supported", queryVariantsCommandOptions.outputFormat);
                    throw new ParameterException("Format '"+queryVariantsCommandOptions.outputFormat+"' not supported");
            }
        }

        if (outputFormat.equalsIgnoreCase("vcf")) {
            if (queryVariantsCommandOptions.returnStudy == null || queryVariantsCommandOptions.returnStudy.split(",").length > 1) {
                logger.error("Only one study is allowed when returning VCF, please use '--return-study' to select the returned study");
                System.exit(1);
            }
        }

        final PrintWriter printWriter;
        if(queryVariantsCommandOptions.output == null || queryVariantsCommandOptions.output.isEmpty()) {
            if (gzip) {
                printWriter = new PrintWriter(new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(System.out))));
            } else {
                printWriter = new PrintWriter(System.out);
            }
        } else {
            if (gzip && !queryVariantsCommandOptions.output.endsWith(".gz")) {
                queryVariantsCommandOptions.output += ".gz";
            }
            Path outputPath = Paths.get(queryVariantsCommandOptions.output);
            printWriter = new PrintWriter(FileUtils.newBufferedWriter(outputPath));
        }


        /*
         * Setting QueryOptions parameters
         */
        if (queryVariantsCommandOptions.include != null) {
            options.add("include", queryVariantsCommandOptions.include);
        }

        if (queryVariantsCommandOptions.exclude != null) {
            options.add("exclude", queryVariantsCommandOptions.exclude);
        }

        if (queryVariantsCommandOptions.skip > 0) {
            options.add("skip", queryVariantsCommandOptions.skip);
        }

        if (queryVariantsCommandOptions.limit > 0) {
            options.add("limit", queryVariantsCommandOptions.limit);
        }

        if (queryVariantsCommandOptions.count) {
            options.add("count", queryVariantsCommandOptions.count);
        }



        ObjectMapper objectMapper = new ObjectMapper();
        if (queryVariantsCommandOptions.rank != null && !queryVariantsCommandOptions.rank.isEmpty()) {
            executeRank(query, variantDBAdaptor);
        } else {
            if (queryVariantsCommandOptions.groupBy != null && !queryVariantsCommandOptions.groupBy.isEmpty()) {
                QueryResult groupBy = variantDBAdaptor.groupBy(query, queryVariantsCommandOptions.groupBy, options);
                System.out.println("groupBy = " + objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(groupBy));
            } else {
                VariantDBIterator iterator = variantDBAdaptor.iterator(query, options);
                if (outputFormat.equalsIgnoreCase("vcf")) {
                    StudyConfigurationManager studyConfigurationManager = variantDBAdaptor.getStudyConfigurationManager();
                    printVcfResult(iterator, studyConfigurationManager, printWriter);
                } else {
                    // we know that it is JSON, otherwise we have not reached this point
                    printJsonResult(iterator, printWriter);
                }
            }
        }
        printWriter.close();

        /**
         * Run query
         */
//        int subListSize = 20;
//        logger.info("options = " + options.toJson());
//        if (regions != null && !regions.isEmpty()) {
//            for (int i = 0; i < (regions.size() + subListSize - 1) / subListSize; i++) {
//                List<Region> subRegions = regions.subList(
//                        i * subListSize,
//                        Math.min((i + 1) * subListSize, regions.size()));
//
//                logger.info("subRegions = " + subRegions);
////                    List<QueryResult<Variant>> queryResults = dbAdaptor.getAllVariants(subRegions, options);
////                List<QueryResult<Variant>> queryResults = dbAdaptor.getAllVariantsByRegionList(subRegions, options);
//                QueryResult<Variant> queryResults = variantDBAdaptor.get(query, options);
//                StringBuilder sb = new StringBuilder();
////                for (QueryResult<Variant> queryResult : queryResults) {
////                    printQueryResult(queryResult, sb);
////                }
//                printQueryResult(queryResults, sb);
//                System.out.println(sb);
//            }
//        } else if (gffReader != null) {
//            List<Gff> gffList;
//            List<Region> subRegions;
//            while ((gffList = gffReader.read(subListSize)) != null) {
//                subRegions = new ArrayList<>(subListSize);
//                for (Gff gff : gffList) {
//                    subRegions.add(new Region(gff.getSequenceName(), gff.getStart(), gff.getEnd()));
//                }
//                logger.info("subRegions = " + subRegions);
//                List<QueryResult<Variant>> queryResults = variantDBAdaptor.getAllVariantsByRegionList(subRegions, options);
//                StringBuilder sb = new StringBuilder();
//                for (QueryResult<Variant> queryResult : queryResults) {
//                    printQueryResult(queryResult, sb);
//                }
//                System.out.println(sb);
//            }
//        } else {
//            System.out.println(printQueryResult(variantDBAdaptor.getAllVariants(options), null));
//        }
    }

    private void executeRank(Query query, VariantDBAdaptor variantDBAdaptor) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        String field = queryVariantsCommandOptions.rank;
        boolean asc = false;
        if(queryVariantsCommandOptions.rank.contains(":")) {  //  eg. gene:-1
            String[] arr= queryVariantsCommandOptions.rank.split(":");
            field = arr[0];
            if (arr[1].endsWith("-1")) {
                asc = true;
            }
        }
        int limit = 10;
        if (queryVariantsCommandOptions.limit > 0) {
            limit = queryVariantsCommandOptions.limit;
        }
        QueryResult rank = variantDBAdaptor.rank(query, field, limit, asc);
        System.out.println("rank = " + objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(rank));
    }

    private void printJsonResult(VariantDBIterator variantDBIterator, PrintWriter printWriter) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectWriter objectWriter = objectMapper.writer();
        while (variantDBIterator.hasNext()) {
            Variant variant = variantDBIterator.next();
            printWriter.println(objectWriter.writeValueAsString(variant));
        }
    }

    private void printVcfResult(VariantDBIterator variantDBIterator, StudyConfigurationManager studyConfigurationManager, PrintWriter printWriter) {

        Map<String, StudyConfiguration> studyConfigurationMap = null;
        while (variantDBIterator.hasNext()) {
            Variant variant = variantDBIterator.next();

//            if(studyConfigurationMap == null) {
//                studyConfigurationMap = new HashMap<>();
//                Iterator<String> vcfIterator = variant.getSourceEntries().keySet().iterator();
//                while (vcfIterator.hasNext()) {
//                    String studyName = vcfIterator.next();
//                    logger.debug(studyName);
//                    studyConfigurationMap.put(studyName, studyConfigurationManager
//                            .getStudyConfiguration(studyName, new QueryOptions("sessionId", queryVariantsCommandOptions.params.get("sessionId")))
//                            .getResult().get(0));
//                }
//            }

            StringBuilder vcfRecord = new StringBuilder();
            vcfRecord.append(variant.getChromosome()).append("\t")
                    .append(variant.getStart()).append("\t")
                    .append(variant.getIds().stream().map(i -> i.toString()).collect(Collectors.joining(","))).append("\t")
                    .append(variant.getReference()).append("\t")
                    .append(variant.getAlternate()).append("\t");


            // If there is only one study returned
            if(variant.getSourceEntries().keySet().size() == 1) {

            } else {

            }

            Iterator<String> vcfIterator = variant.getSourceEntries().keySet().iterator();
            while (vcfIterator.hasNext()) {
                String file = vcfIterator.next();
                vcfRecord.append(variant.getSourceEntries().get(file).getAttribute("QUAL")).append("\t")
                        .append(variant.getSourceEntries().get(file).getAttribute("FILTER")).append("\t")
                        .append("File=" + file).append(",")
                        .append(variant.getSourceEntries().get(file).getAttribute("INFO")).append("\t")
                        .append(variant.getSourceEntries().get(file).getFormat()).append("\t")
                        .append(variant.getSourceEntries().get(file).getSamplesData());
            }
            printWriter.println(vcfRecord.toString());
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
