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

package org.opencb.opencga.storage.app.cli.executors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.core.Region;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.storage.app.cli.options.VariantStorageCommandOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;

/**
 * Created by imedina on 30/12/15.
 */
public class VariantCliQueryParserUtils {

    private static Logger logger = LoggerFactory.getLogger(VariantCliQueryParserUtils.class);

    public static Query parseBasicVariantQuery(VariantStorageCommandOptions.BasicVariantQueryOptions options,
                                               Query query) throws IOException {

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
                return Region.normalizeChromosome(array[0]) + ":" + array[3] + "-" + array[4];
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
        addParam(query, ANNOT_HPO, options.hpo);

        /*
         * Stats parameters
         */
        addParam(query, STATS_MAF, options.maf);

        return query;
    }

    public static Query parseQuery(VariantStorageCommandOptions.GenericVariantSearchOptions options, Query query) throws Exception {
        query = parseBasicVariantQuery(options, query);
        addParam(query, ANNOT_CLINVAR, options.clinvar);
        addParam(query, ANNOT_COSMIC, options.cosmic);
        return query;
    }

    public static Query parseQuery(VariantStorageCommandOptions.VariantQueryCommandOptions queryVariantsOptions, List<String> studyNames)
            throws Exception {
        VariantWriterFactory.VariantOutputFormat of = VariantWriterFactory.toOutputFormat(queryVariantsOptions.outputFormat, null);
        return parseGenericVariantQuery(queryVariantsOptions, queryVariantsOptions.study, studyNames, queryVariantsOptions.commonQueryOptions.count, of);
    }

    protected static Query parseGenericVariantQuery(VariantStorageCommandOptions.GenericVariantQueryOptions queryVariantsOptions,
                                                    String studiesFilter, Collection<String> allStudyNames, boolean count,
                                                    VariantWriterFactory.VariantOutputFormat of)
            throws IOException {

        Query query = new Query();
        parseBasicVariantQuery(queryVariantsOptions, query);


        addParam(query, STUDY, studiesFilter);
        addParam(query, INCLUDE_STUDY, queryVariantsOptions.returnStudy);
        addParam(query, FILE, queryVariantsOptions.file);
        addParam(query, INCLUDE_FILE, queryVariantsOptions.returnFile);
        addParam(query, FILTER, queryVariantsOptions.filter);
        addParam(query, GENOTYPE, queryVariantsOptions.sampleGenotype);
        addParam(query, SAMPLE, queryVariantsOptions.samples);
        addParam(query, INCLUDE_SAMPLE, queryVariantsOptions.returnSample);
        addParam(query, INCLUDE_FORMAT, queryVariantsOptions.includeFormat);
        addParam(query, INCLUDE_GENOTYPE, queryVariantsOptions.includeGenotype);
        addParam(query, UNKNOWN_GENOTYPE, queryVariantsOptions.unknownGenotype);
        addParam(query, SAMPLE_METADATA, queryVariantsOptions.samplesMetadata);

        /**
         * Annotation parameters
         */
        addParam(query, ANNOT_BIOTYPE, queryVariantsOptions.geneBiotype);
        addParam(query, ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY, queryVariantsOptions.populationMaf);
        addParam(query, ANNOT_TRANSCRIPTION_FLAG, queryVariantsOptions.flags);
//        addParam(query, ANNOT_GENE_TRAITS, queryVariantsOptions.geneTrait);
        addParam(query, ANNOT_GENE_TRAIT_ID, queryVariantsOptions.geneTraitId);
        addParam(query, ANNOT_GENE_TRAIT_NAME, queryVariantsOptions.geneTraitName);
        addParam(query, ANNOT_GO, queryVariantsOptions.go);
        addParam(query, ANNOT_PROTEIN_KEYWORD, queryVariantsOptions.proteinKeywords);
        addParam(query, ANNOT_DRUG, queryVariantsOptions.drugs);
        addParam(query, ANNOT_COSMIC, queryVariantsOptions.cosmic);
        addParam(query, ANNOT_CLINVAR, queryVariantsOptions.clinvar);
        addParam(query, ANNOT_XREF, queryVariantsOptions.annotXref);

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
            if (VariantQueryUtils.isOutputMultiStudy(query, null, allStudyNames)) {
                String availableStudies = allStudyNames == null || allStudyNames.isEmpty()
                        ? ""
                        : " Available studies: [ " + String.join(", ", allStudyNames) + " ]";
                throw new VariantQueryException("Only one study is allowed when returning " + of + ", " +
                        "please use '--output-study' to select the returned study. " + availableStudies);
            }
        }

        return query;
    }

    public static QueryOptions parseQueryOptions(VariantStorageCommandOptions.VariantQueryCommandOptions queryVariantsOptions) {
        QueryOptions queryOptions = new QueryOptions();

        if (StringUtils.isNotEmpty(queryVariantsOptions.commonQueryOptions.include)) {
            queryOptions.put(QueryOptions.INCLUDE, queryVariantsOptions.commonQueryOptions.include);
        }

        if (StringUtils.isNotEmpty(queryVariantsOptions.commonQueryOptions.exclude)) {
            queryOptions.put(QueryOptions.EXCLUDE, queryVariantsOptions.commonQueryOptions.exclude);
        }

        if (queryVariantsOptions.commonQueryOptions.skip > 0) {
            queryOptions.put(QueryOptions.SKIP, queryVariantsOptions.commonQueryOptions.skip);
        }

        if (queryVariantsOptions.commonQueryOptions.limit > 0) {
            queryOptions.put(QueryOptions.LIMIT, queryVariantsOptions.commonQueryOptions.limit);
        }

        if (queryVariantsOptions.commonQueryOptions.count) {
            queryOptions.put(QueryOptions.COUNT, true);
        }

        if (queryVariantsOptions.sort) {
            queryOptions.put(QueryOptions.SORT, true);
        }

        if (queryVariantsOptions.summary) {
            queryOptions.put(VariantField.SUMMARY, true);
        }

        queryOptions.putAll(queryVariantsOptions.commonOptions.params);

        return queryOptions;
    }

    protected static void addParam(ObjectMap query, QueryParam key, Collection value) {
        if (CollectionUtils.isNotEmpty(value)) {
            query.put(key.key(), value);
        }
    }

    protected static void addParam(ObjectMap query, QueryParam key, String value) {
        query.putIfNotEmpty(key.key(), value);

    }

    protected static void addParam(ObjectMap query, QueryParam key, boolean value) {
        if (value) {
            query.put(key.key(), true);
        }

    }

}
