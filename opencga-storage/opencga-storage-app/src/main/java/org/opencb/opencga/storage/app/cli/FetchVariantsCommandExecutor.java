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
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.commons.utils.FileUtils;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.Query;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.config.StorageEngineConfiguration;
import org.opencb.opencga.storage.core.variant.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor.VariantQueryParams;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.io.VariantVcfExporter;
import org.opencb.opencga.storage.core.variant.io.json.VariantSourceEntryJsonMixin;
import org.opencb.opencga.storage.core.variant.io.json.VariantSourceJsonMixin;
import org.opencb.opencga.storage.core.variant.io.json.VariantStatsJsonMixin;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor.VariantQueryParams.*;

/**
 * Created by imedina on 25/05/15.
 */
public class FetchVariantsCommandExecutor extends CommandExecutor {

    private CliOptionsParser.QueryVariantsCommandOptions queryVariantsCommandOptions;


    public FetchVariantsCommandExecutor(CliOptionsParser.QueryVariantsCommandOptions queryVariantsCommandOptions) {
        super(queryVariantsCommandOptions.logLevel, queryVariantsCommandOptions.verbose,
                queryVariantsCommandOptions.configFile);

        this.logFile = queryVariantsCommandOptions.logFile;
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
        List<String> studyNames = variantDBAdaptor.getStudyConfigurationManager().getStudyNames(new QueryOptions());

        Query query = new Query();
        QueryOptions options = new QueryOptions(new HashMap<>(queryVariantsCommandOptions.params));


        /**
         * Parse Variant parameters
         */
        if (queryVariantsCommandOptions.region != null && !queryVariantsCommandOptions.region.isEmpty()) {
            query.put(REGION.key(), queryVariantsCommandOptions.region);
        } else if (queryVariantsCommandOptions.regionFile != null && !queryVariantsCommandOptions.regionFile.isEmpty()) {
            Path gffPath = Paths.get(queryVariantsCommandOptions.regionFile);
            FileUtils.checkFile(gffPath);
            String regionsFromFile = Files.readAllLines(gffPath).stream().map(line -> {
                String[] array = line.split("\t");
                return new String(array[0].replace("chr", "")+":"+array[3]+"-"+array[4]);
            }).collect(Collectors.joining(","));
            query.put(REGION.key(), regionsFromFile);
        }

        addParam(query, ID, queryVariantsCommandOptions.id);
        addParam(query, GENE, queryVariantsCommandOptions.gene);
        addParam(query, TYPE, queryVariantsCommandOptions.type);


        List<String> studies = new LinkedList<>();
        if (queryVariantsCommandOptions.study != null && !queryVariantsCommandOptions.study.isEmpty()) {
            query.put(STUDIES.key(), queryVariantsCommandOptions.study);
            for (String study : queryVariantsCommandOptions.study.split(",|;")) {
                if (!study.startsWith("!")) {
                    studies.add(study);
                }
            }
        }

        // If the studies to be returned is empty then we return the studies being queried
        if (queryVariantsCommandOptions.returnStudy != null && !queryVariantsCommandOptions.returnStudy.isEmpty()) {
            query.put(RETURNED_STUDIES.key(), Arrays.asList(queryVariantsCommandOptions.returnStudy.split(",")));
        } else {
            if (!studies.isEmpty()) {
                query.put(RETURNED_STUDIES.key(), studies);
            }
        }

        addParam(query, FILES, queryVariantsCommandOptions.file);
        addParam(query, GENOTYPE, queryVariantsCommandOptions.sampleGenotype);
        addParam(query, RETURNED_SAMPLES, queryVariantsCommandOptions.returnSample);
        addParam(query, UNKNOWN_GENOTYPE, queryVariantsCommandOptions.unknownGenotype);

        /**
         * Annotation parameters
         */
        addParam(query, ANNOT_CONSEQUENCE_TYPE, queryVariantsCommandOptions.consequenceType);
        addParam(query, ANNOT_BIOTYPE, queryVariantsCommandOptions.biotype);
        addParam(query, ALTERNATE_FREQUENCY, queryVariantsCommandOptions.populationFreqs);
        addParam(query, POPULATION_MINOR_ALLELE_FREQUENCY, queryVariantsCommandOptions.populationMaf);
        addParam(query, CONSERVATION, queryVariantsCommandOptions.conservation);

        if (queryVariantsCommandOptions.proteinSubstitution != null && !queryVariantsCommandOptions.proteinSubstitution.isEmpty()) {
            String[] fields = queryVariantsCommandOptions.proteinSubstitution.split(",");
            for (String field : fields) {
                String[] arr = field.replaceAll("==", " ").replaceAll(">=", " ").replaceAll("<=", " ").replaceAll("=", " ").replaceAll("<", " ").replaceAll(">", " ").split(" ");
                if(arr != null && arr.length > 1) {
                    switch (arr[0]) {
                        case "sift":
                            query.put(SIFT.key(), field.replaceAll("sift", ""));
                            break;
                        case "polyphen":
                            query.put(POLYPHEN.key(), field.replaceAll("polyphen", ""));
                            break;
                        default:
                            query.put(PROTEIN_SUBSTITUTION.key(), field.replaceAll(arr[0], ""));
                            break;
                    }
                }
            }
        }


        /**
         * Stats parameters
         */
        if (queryVariantsCommandOptions.stats != null && !queryVariantsCommandOptions.stats.isEmpty()) {
            Set<String> acceptedStatKeys = new HashSet<>(Arrays.asList(STATS_MAF.key(),
                    STATS_MGF.key(),
                    MISSING_ALLELES.key(),
                    MISSING_GENOTYPES.key()));

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

        addParam(query, STATS_MAF, queryVariantsCommandOptions.maf);
        addParam(query, STATS_MGF, queryVariantsCommandOptions.mgf);
        addParam(query, MISSING_ALLELES, queryVariantsCommandOptions.missingAlleleCount);
        addParam(query, MISSING_GENOTYPES, queryVariantsCommandOptions.missingGenotypeCount);



        /**
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
                    throw new ParameterException("Format '" + queryVariantsCommandOptions.outputFormat + "' not supported");
            }
        }

        boolean returnVariants = !queryVariantsCommandOptions.count
                && StringUtils.isEmpty(queryVariantsCommandOptions.groupBy)
                && StringUtils.isEmpty(queryVariantsCommandOptions.rank);

        if (returnVariants && outputFormat.equalsIgnoreCase("vcf")) {
            int returnedStudiesSize = query.getAsStringList(RETURNED_STUDIES.key()).size();
            if (returnedStudiesSize == 0 && studies.size() == 1) {
                query.put(RETURNED_STUDIES.key(), studies.get(0));
            } else if (returnedStudiesSize == 0 && studyNames.size() != 1 //If there are no returned studies, and there are more than one study
                    || returnedStudiesSize > 1) {     // Or is required more than one returned study
                throw new Exception("Only one study is allowed when returning VCF, please use '--return-study' to select the returned study. " +
                        "Available studies: [ " + String.join(", ", studyNames) + " ]");
            } else {
                if (returnedStudiesSize == 0) {    //If there were no returned studies, set the study existing one
                    query.put(RETURNED_STUDIES.key(), studyNames.get(0));
                }
            }
        }

        // output format has priority over output name  
        OutputStream outputStream;
        if(queryVariantsCommandOptions.output == null || queryVariantsCommandOptions.output.isEmpty()) {
            outputStream = System.out;
        } else {
            if (gzip && !queryVariantsCommandOptions.output.endsWith(".gz")) {
                queryVariantsCommandOptions.output += ".gz";
            }
            logger.debug("writing to %s", queryVariantsCommandOptions.output);
            outputStream = new FileOutputStream(queryVariantsCommandOptions.output);
        }
        if (gzip) {
            outputStream = new GZIPOutputStream(outputStream);
        }

        logger.debug("using %s output stream", gzip? "gzipped": "plain");

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


        if (queryVariantsCommandOptions.count) {
            QueryResult<Long> result = variantDBAdaptor.count(query);
            System.out.println("Num. results\t" + result.getResult().get(0));
            return;
        }

        ObjectMapper objectMapper = new ObjectMapper();
        if (StringUtils.isNotEmpty(queryVariantsCommandOptions.rank)) {
            executeRank(query, variantDBAdaptor);
        } else {
            if (StringUtils.isNotEmpty(queryVariantsCommandOptions.groupBy)) {
                QueryResult groupBy = variantDBAdaptor.groupBy(query, queryVariantsCommandOptions.groupBy, options);
                System.out.println("groupBy = " + objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(groupBy));
            } else {
                VariantDBIterator iterator = variantDBAdaptor.iterator(query, options);
                if (outputFormat.equalsIgnoreCase("vcf")) {
                    StudyConfigurationManager studyConfigurationManager = variantDBAdaptor.getStudyConfigurationManager();
                    QueryResult<StudyConfiguration> studyConfigurationResult = studyConfigurationManager.getStudyConfiguration(
                            query.getAsStringList(RETURNED_STUDIES.key()).get(0), null);
                    if (studyConfigurationResult.getResult().size() >= 1) {
                        // Samples to be returned
                        if (query.containsKey(RETURNED_SAMPLES.key())) {
                            options.put(RETURNED_SAMPLES.key(), query.get(RETURNED_SAMPLES.key()));
                        }

//                        options.add("includeAnnotations", queryVariantsCommandOptions.includeAnnotations);
                        if(queryVariantsCommandOptions.annotations != null) {
                            options.add("annotations", queryVariantsCommandOptions.annotations);
                        }
                        VariantVcfExporter variantVcfExporter = new VariantVcfExporter();
                        variantVcfExporter.export(iterator, studyConfigurationResult.first(), outputStream, options);
                    } else {
                        logger.warn("no study found named " + query.getAsStringList(RETURNED_STUDIES.key()).get(0));
                    }
//                    printVcfResult(iterator, studyConfigurationManager, printWriter);
                } else {
                    // we know that it is JSON, otherwise we have not reached this point
                    printJsonResult(iterator, outputStream);
                }
            }
        }
        outputStream.close();
    }

    private void addParam(ObjectMap objectMap, VariantQueryParams key, String value) {
        if (value != null && !value.isEmpty()) {
            objectMap.put(key.key(), value);
        }
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

    private void printJsonResult(VariantDBIterator variantDBIterator, OutputStream outputStream) throws IOException {
        while (variantDBIterator.hasNext()) {
            Variant variant = variantDBIterator.next();
            outputStream.write(variant.toJson().getBytes());
        }
    }

    @Deprecated
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

            if (variant.getIds() == null) {
                variant.setIds(new LinkedList<>());
            }

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
                        .append(variant.getSourceEntries().get(file).getFormatAsString()).append("\t")
                        .append(variant.getSourceEntries().get(file).getSamplesDataAsMap());
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

        jsonObjectMapper.addMixIn(StudyEntry.class, VariantSourceEntryJsonMixin.class);
        jsonObjectMapper.addMixIn(VariantSource.class, VariantSourceJsonMixin.class);
        jsonObjectMapper.addMixIn(VariantStats.class, VariantStatsJsonMixin.class);

        jsonObjectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
//                objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        jsonObjectMapper.setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);

        sb.append(jsonObjectMapper.writeValueAsString(queryResult)).append("\n");
        return sb;
    }
}
