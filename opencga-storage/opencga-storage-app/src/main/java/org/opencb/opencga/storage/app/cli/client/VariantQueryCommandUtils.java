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

import com.beust.jcommander.ParameterException;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.utils.FileUtils;
import org.opencb.datastore.core.Query;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor.VariantQueryParams.*;

/**
 * Created by imedina on 30/12/15.
 */
public class VariantQueryCommandUtils {

    private static Logger logger = LoggerFactory.getLogger("org.opencb.opencga.storage.app.cli.client.VariantQueryCommandUtils");

    public static Query parseQuery(CliOptionsParser.QueryVariantsCommandOptions queryVariantsOptions, List<String> studyNames)
            throws Exception {
        Query query = new Query();

        /*
         * Parse Variant parameters
         */
        if (queryVariantsOptions.region != null && !queryVariantsOptions.region.isEmpty()) {
            query.put(REGION.key(), queryVariantsOptions.region);
        } else if (queryVariantsOptions.regionFile != null && !queryVariantsOptions.regionFile.isEmpty()) {
            Path gffPath = Paths.get(queryVariantsOptions.regionFile);
            FileUtils.checkFile(gffPath);
            String regionsFromFile = Files.readAllLines(gffPath).stream().map(line -> {
                String[] array = line.split("\t");
                return new String(array[0].replace("chr", "") + ":" + array[3] + "-" + array[4]);
            }).collect(Collectors.joining(","));
            query.put(REGION.key(), regionsFromFile);
        }

        addParam(query, ID, queryVariantsOptions.id);
        addParam(query, GENE, queryVariantsOptions.gene);
        addParam(query, TYPE, queryVariantsOptions.type);


        List<String> studies = new LinkedList<>();
        if (queryVariantsOptions.study != null && !queryVariantsOptions.study.isEmpty()) {
            query.put(STUDIES.key(), queryVariantsOptions.study);
            for (String study : queryVariantsOptions.study.split(",|;")) {
                if (!study.startsWith("!")) {
                    studies.add(study);
                }
            }
        }

        // If the studies to be returned is empty then we return the studies being queried
        if (queryVariantsOptions.returnStudy != null && !queryVariantsOptions.returnStudy.isEmpty()) {
//            query.put(RETURNED_STUDIES.key(), Arrays.asList(queryVariantsOptions.returnStudy.split(",")));
            List<String> list = new ArrayList<>();
            Collections.addAll(list, queryVariantsOptions.returnStudy.split(","));
            query.put(RETURNED_STUDIES.key(), list);
        } else {
            if (!studies.isEmpty()) {
                query.put(RETURNED_STUDIES.key(), studies);
            }
        }

        addParam(query, FILES, queryVariantsOptions.file);
        addParam(query, GENOTYPE, queryVariantsOptions.sampleGenotype);
        addParam(query, RETURNED_SAMPLES, queryVariantsOptions.returnSample);
        addParam(query, UNKNOWN_GENOTYPE, queryVariantsOptions.unknownGenotype);


        /**
         * Annotation parameters
         */
        addParam(query, ANNOT_CONSEQUENCE_TYPE, queryVariantsOptions.consequenceType);
        addParam(query, ANNOT_BIOTYPE, queryVariantsOptions.biotype);
        addParam(query, ALTERNATE_FREQUENCY, queryVariantsOptions.populationFreqs);
        addParam(query, POPULATION_MINOR_ALLELE_FREQUENCY, queryVariantsOptions.populationMaf);
        addParam(query, CONSERVATION, queryVariantsOptions.conservation);

        if (queryVariantsOptions.proteinSubstitution != null && !queryVariantsOptions.proteinSubstitution.isEmpty()) {
            String[] fields = queryVariantsOptions.proteinSubstitution.split(",");
            for (String field : fields) {
                String[] arr = field
                        .replaceAll("==", " ")
                        .replaceAll(">=", " ")
                        .replaceAll("<=", " ")
                        .replaceAll("=", " ")
                        .replaceAll("<", " ")
                        .replaceAll(">", " ")
                        .split(" ");

                if (arr != null && arr.length > 1) {
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


        /*
         * Stats parameters
         */
        if (queryVariantsOptions.stats != null && !queryVariantsOptions.stats.isEmpty()) {
            Set<String> acceptedStatKeys = new HashSet<>(Arrays.asList(STATS_MAF.key(),
                    STATS_MGF.key(),
                    MISSING_ALLELES.key(),
                    MISSING_GENOTYPES.key()));

            for (String stat : queryVariantsOptions.stats.split(",")) {
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

        addParam(query, STATS_MAF, queryVariantsOptions.maf);
        addParam(query, STATS_MGF, queryVariantsOptions.mgf);
        addParam(query, MISSING_ALLELES, queryVariantsOptions.missingAlleleCount);
        addParam(query, MISSING_GENOTYPES, queryVariantsOptions.missingGenotypeCount);


        boolean returnVariants = !queryVariantsOptions.count && StringUtils.isEmpty(queryVariantsOptions.groupBy)
                && StringUtils.isEmpty(queryVariantsOptions.rank);


        String outputFormat = "vcf";
        if (StringUtils.isNotEmpty(queryVariantsOptions.outputFormat)) {
            if (queryVariantsOptions.outputFormat.equals("json") || queryVariantsOptions.outputFormat.equals("json.gz")) {
                outputFormat = "json";
            }
        }

        if (returnVariants && outputFormat.equalsIgnoreCase("vcf")) {
            int returnedStudiesSize = query.getAsStringList(RETURNED_STUDIES.key()).size();
            if (returnedStudiesSize == 0 && studies.size() == 1) {
                query.put(RETURNED_STUDIES.key(), studies.get(0));
            } else if (returnedStudiesSize == 0 && studyNames.size() != 1 //If there are no returned studies, and there are more than one
                    // study
                    || returnedStudiesSize > 1) {     // Or is required more than one returned study
                throw new Exception("Only one study is allowed when returning VCF, please use '--return-study' to select the returned "
                        + "study. Available studies: [ " + String.join(", ", studyNames) + " ]");
            } else {
                if (returnedStudiesSize == 0) {    //If there were no returned studies, set the study existing one
                    query.put(RETURNED_STUDIES.key(), studyNames.get(0));
                }
            }
        }

        return query;
    }

    public static QueryOptions parseQueryOptions(CliOptionsParser.QueryVariantsCommandOptions queryVariantsOptions) {
        QueryOptions queryOptions = new QueryOptions(new HashMap<>(queryVariantsOptions.commonOptions.params));

        if (StringUtils.isNotEmpty(queryVariantsOptions.include)) {
            queryOptions.add("include", queryVariantsOptions.include);
        }

        if (StringUtils.isNotEmpty(queryVariantsOptions.exclude)) {
            queryOptions.add("exclude", queryVariantsOptions.exclude + ",_id");
        }
//        else {
//            queryOptions.put("exclude", "_id");
//        }

        if (queryVariantsOptions.skip > 0) {
            queryOptions.add("skip", queryVariantsOptions.skip);
        }

        if (queryVariantsOptions.limit > 0) {
            queryOptions.add("limit", queryVariantsOptions.limit);
        }

        if (queryVariantsOptions.count) {
            queryOptions.add("count", true);
        }

        return queryOptions;
    }

    public static OutputStream getOutputStream(CliOptionsParser.QueryVariantsCommandOptions queryVariantsOptions) throws IOException {
        /*
         * Output parameters
         */
        boolean gzip = true;
        if (queryVariantsOptions.outputFormat != null && !queryVariantsOptions.outputFormat.isEmpty()) {
            switch (queryVariantsOptions.outputFormat) {
                case "vcf":
                    gzip = false;
                case "vcf.gz":
                    break;
                case "json":
                    gzip = false;
                case "json.gz":
                    break;
                default:
                    logger.error("Format '{}' not supported", queryVariantsOptions.outputFormat);
                    throw new ParameterException("Format '" + queryVariantsOptions.outputFormat + "' not supported");
            }
        }

        // output format has priority over output name
        OutputStream outputStream;
        if (queryVariantsOptions.output == null || queryVariantsOptions.output.isEmpty()) {
            outputStream = System.out;
        } else {
            if (gzip && !queryVariantsOptions.output.endsWith(".gz")) {
                queryVariantsOptions.output += ".gz";
            }
            outputStream = new FileOutputStream(queryVariantsOptions.output);
            logger.debug("writing to %s", queryVariantsOptions.output);
        }

        // If compressed a GZip output stream is used
        if (gzip) {
            outputStream = new GZIPOutputStream(outputStream);
        }

        logger.debug("using %s output stream", gzip ? "gzipped" : "plain");

        return outputStream;
    }

    private static void addParam(Query query, VariantDBAdaptor.VariantQueryParams key, String value) {
        if (value != null && !value.isEmpty()) {
            query.put(key.key(), value);
        }
    }

}
