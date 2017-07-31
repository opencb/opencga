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

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;

/**
 * Created by imedina on 30/12/15.
 */
public class VariantQueryCommandUtils {

    private static Logger logger = LoggerFactory.getLogger("org.opencb.opencga.storage.app.cli.client.VariantQueryCommandUtils");

    public static Query parseBasicVariantQuery(StorageVariantCommandOptions.BasicVariantQueryOptions options,
                                               Query query) throws Exception {

        /*
         * Parse Variant parameters
         */
        if (options.region != null && !options.region.isEmpty()) {
            query.put(REGION.key(), options.region);
        } else if (options.regionFile != null && !options.regionFile.isEmpty()) {
            Path gffPath = Paths.get(options.regionFile);
            FileUtils.checkFile(gffPath);
            String regionsFromFile = Files.readAllLines(gffPath).stream().map(line -> {
                String[] array = line.split("\t");
                return new String(array[0].replace("chr", "") + ":" + array[3] + "-" + array[4]);
            }).collect(Collectors.joining(","));
            query.put(REGION.key(), regionsFromFile);
        }

        addParam(query, ID, options.id);
        addParam(query, GENE, options.gene);
        addParam(query, TYPE, options.type);

        /**
         * Annotation parameters
         */
        addParam(query, ANNOT_CONSEQUENCE_TYPE, options.consequenceType);
        addParam(query, ANNOT_POPULATION_ALTERNATE_FREQUENCY, options.populationFreqs);
        addParam(query, ANNOT_CONSERVATION, options.conservation);
        addParam(query, ANNOT_FUNCTIONAL_SCORE, options.functionalScore);
        addParam(query, ANNOT_PROTEIN_SUBSTITUTION, options.proteinSubstitution);

        /*
         * Stats parameters
         */
//        if (options.stats != null && !options.stats.isEmpty()) {
//            Set<String> acceptedStatKeys = new HashSet<>(Arrays.asList(STATS_MAF.key(),
//                    STATS_MGF.key(),
//                    MISSING_ALLELES.key(),
//                    MISSING_GENOTYPES.key()));
//
//            for (String stat : options.stats.split(",")) {
//                int index = stat.indexOf("<");
//                index = index >= 0 ? index : stat.indexOf("!");
//                index = index >= 0 ? index : stat.indexOf("~");
//                index = index >= 0 ? index : stat.indexOf("<");
//                index = index >= 0 ? index : stat.indexOf(">");
//                index = index >= 0 ? index : stat.indexOf("=");
//                if (index < 0) {
//                    throw new UnsupportedOperationException("Unknown stat filter operation: " + stat);
//                }
//                String name = stat.substring(0, index);
//                String cond = stat.substring(index);
//
//                if (acceptedStatKeys.contains(name)) {
//                    query.put(name, cond);
//                } else {
//                    throw new UnsupportedOperationException("Unknown stat filter name: " + name);
//                }
//                logger.info("Parsed stat filter: {} {}", name, cond);
//            }
//        }

        addParam(query, STATS_MAF, options.maf);

        return query;
    }

    public static Query parseQuery(StorageVariantCommandOptions.GenericVariantSearchOptions options, Query query)
            throws Exception {
        query = parseBasicVariantQuery(options, query);
        addParam(query, ANNOT_CLINVAR, options.clinvar);
        addParam(query, ANNOT_COSMIC, options.cosmic);
        return query;
    }

    public static Query parseQuery(StorageVariantCommandOptions.VariantQueryCommandOptions queryVariantsOptions, List<String> studyNames)
            throws Exception {
        VariantWriterFactory.VariantOutputFormat of = VariantWriterFactory.toOutputFormat(queryVariantsOptions.outputFormat, null);
        return parseGenericVariantQuery(queryVariantsOptions, queryVariantsOptions.study, studyNames, queryVariantsOptions.commonQueryOptions.count, of);
    }

    protected static Query parseGenericVariantQuery(StorageVariantCommandOptions.GenericVariantQueryOptions queryVariantsOptions,
                                                    String studiesFilter, Collection<String> allStudyNames, boolean count,
                                                    VariantWriterFactory.VariantOutputFormat of)
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
        if (StringUtils.isNotEmpty(studiesFilter)) {
            query.put(STUDIES.key(), studiesFilter);
            for (String study : studiesFilter.split(",|;")) {
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
        addParam(query, RETURNED_FILES, queryVariantsOptions.returnFile);
        addParam(query, FILTER, queryVariantsOptions.filter);
        addParam(query, GENOTYPE, queryVariantsOptions.sampleGenotype);
        addParam(query, SAMPLES, queryVariantsOptions.samples);
        addParam(query, RETURNED_SAMPLES, queryVariantsOptions.returnSample);
        addParam(query, INCLUDE_FORMAT, queryVariantsOptions.includeFormat);
        addParam(query, INCLUDE_GENOTYPE, queryVariantsOptions.includeGenotype);
        addParam(query, UNKNOWN_GENOTYPE, queryVariantsOptions.unknownGenotype);


        /**
         * Annotation parameters
         */
        addParam(query, ANNOT_CONSEQUENCE_TYPE, queryVariantsOptions.consequenceType);
        addParam(query, ANNOT_BIOTYPE, queryVariantsOptions.geneBiotype);
        addParam(query, ANNOT_POPULATION_ALTERNATE_FREQUENCY, queryVariantsOptions.populationFreqs);
        addParam(query, ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY, queryVariantsOptions.populationMaf);
        addParam(query, ANNOT_CONSERVATION, queryVariantsOptions.conservation);

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
                            query.put(ANNOT_SIFT.key(), field.replaceAll("sift", ""));
                            break;
                        case "polyphen":
                            query.put(ANNOT_POLYPHEN.key(), field.replaceAll("polyphen", ""));
                            break;
                        default:
                            query.put(ANNOT_PROTEIN_SUBSTITUTION.key(), field.replaceAll(arr[0], ""));
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


        boolean returnVariants = !count && StringUtils.isEmpty(queryVariantsOptions.groupBy)
                && StringUtils.isEmpty(queryVariantsOptions.rank);

        if (returnVariants && !of.isMultiStudyOutput()) {
            int returnedStudiesSize = query.getAsStringList(RETURNED_STUDIES.key()).size();
            if (returnedStudiesSize == 0 && studies.size() == 1) {
                query.put(RETURNED_STUDIES.key(), studies.get(0));
            } else if (returnedStudiesSize == 0 && allStudyNames.size() != 1 //If there are no returned studies, and there are more than one
                    // study
                    || returnedStudiesSize > 1) {     // Or is required more than one returned study

                String availableStudies = allStudyNames == null || allStudyNames.isEmpty()
                        ? ""
                        : " Available studies: [ " + String.join(", ", allStudyNames) + " ]";
                throw new Exception("Only one study is allowed when returning " + of + ", please use '--return-study' to select the returned "
                        + "study." + availableStudies);
            } else {
                if (returnedStudiesSize == 0) {    //If there were no returned studies, set the study existing one
                    query.put(RETURNED_STUDIES.key(), allStudyNames.iterator().next());
                }
            }
        }

        return query;
    }

    public static QueryOptions parseQueryOptions(StorageVariantCommandOptions.VariantQueryCommandOptions queryVariantsOptions) {
        QueryOptions queryOptions = new QueryOptions(new HashMap<>(queryVariantsOptions.commonOptions.params));

        if (StringUtils.isNotEmpty(queryVariantsOptions.commonQueryOptions.include)) {
            queryOptions.add("include", queryVariantsOptions.commonQueryOptions.include);
        }

        if (StringUtils.isNotEmpty(queryVariantsOptions.commonQueryOptions.exclude)) {
            queryOptions.add("exclude", queryVariantsOptions.commonQueryOptions.exclude);
        }
//        else {
//            queryOptions.put("exclude", "_id");
//        }

        if (queryVariantsOptions.commonQueryOptions.skip > 0) {
            queryOptions.add("skip", queryVariantsOptions.commonQueryOptions.skip);
        }

        if (queryVariantsOptions.commonQueryOptions.limit > 0) {
            queryOptions.add("limit", queryVariantsOptions.commonQueryOptions.limit);
        }

        if (queryVariantsOptions.commonQueryOptions.count) {
            queryOptions.add("count", true);
        }

        return queryOptions;
    }

    protected static void addParam(Query query, QueryParam key, Collection value) {
        if (CollectionUtils.isNotEmpty(value)) {
            query.put(key.key(), value);
        }
    }

    protected static void addParam(Query query, QueryParam key, String value) {
        if (StringUtils.isNotEmpty(value)) {
            query.put(key.key(), value);
        }
    }

}
