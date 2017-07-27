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

import com.beust.jcommander.ParameterException;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.app.cli.analysis.options.VariantCommandOptions;
import org.opencb.opencga.storage.core.manager.variant.VariantCatalogQueryUtils;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory.VariantOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;
import static org.opencb.opencga.storage.core.variant.io.VariantWriterFactory.VariantOutputFormat.VCF;

/**
 * Created by imedina on 30/12/15.
 */
public class VariantQueryCommandUtils extends org.opencb.opencga.storage.app.cli.client.executors.VariantQueryCommandUtils {

    private static Logger logger = LoggerFactory.getLogger("org.opencb.opencga.storage.app.cli.client.VariantQueryCommandUtils");

    public static Query parseQuery(VariantCommandOptions.VariantQueryCommandOptions queryVariantsOptions, Map<Long, String> studyIds)
            throws Exception {
        VariantOutputFormat of = VariantWriterFactory.toOutputFormat(queryVariantsOptions.commonOptions.outputFormat, queryVariantsOptions.output);
        Query query = parseGenericVariantQuery(
                queryVariantsOptions.genericVariantQueryOptions, queryVariantsOptions.study, studyIds.values(),
                queryVariantsOptions.numericOptions.count, of);

        addParam(query, VariantCatalogQueryUtils.SAMPLE_FILTER, queryVariantsOptions.sampleFilter);

        return query;
    }

    @Deprecated
    public static Query oldParseQuery(VariantCommandOptions.VariantQueryCommandOptions queryVariantsOptions, Map<Long, String> studyIds)
            throws Exception {
        Query query = new Query();

        /*
         * Parse Variant parameters
         */
        if (queryVariantsOptions.genericVariantQueryOptions.region != null && !queryVariantsOptions.genericVariantQueryOptions.region.isEmpty()) {
            query.put(REGION.key(), queryVariantsOptions.genericVariantQueryOptions.region);
        } else if (queryVariantsOptions.genericVariantQueryOptions.regionFile != null && !queryVariantsOptions.genericVariantQueryOptions.regionFile.isEmpty()) {
            Path gffPath = Paths.get(queryVariantsOptions.genericVariantQueryOptions.regionFile);
            FileUtils.checkFile(gffPath);
            String regionsFromFile = Files.readAllLines(gffPath).stream().map(line -> {
                String[] array = line.split("\t");
                return new String(array[0].replace("chr", "") + ":" + array[3] + "-" + array[4]);
            }).collect(Collectors.joining(","));
            query.put(REGION.key(), regionsFromFile);
        }

        addParam(query, ID, queryVariantsOptions.genericVariantQueryOptions.id);
        addParam(query, GENE, queryVariantsOptions.genericVariantQueryOptions.gene);
        addParam(query, TYPE, queryVariantsOptions.genericVariantQueryOptions.type);


        List studies = new LinkedList<>();
        if (StringUtils.isNotEmpty(queryVariantsOptions.study)) {
            query.put(STUDIES.key(), queryVariantsOptions.study);
            for (String study : queryVariantsOptions.study.split(",|;")) {
                if (!study.startsWith("!")) {
                    studies.add(study);
                }
            }
        } else {
            studies = new ArrayList<>(studyIds.keySet());
        }

        // If the studies to be returned is empty then we return the studies being queried
        if (StringUtils.isNotEmpty(queryVariantsOptions.genericVariantQueryOptions.returnStudy)) {
//            query.put(RETURNED_STUDIES.key(), Arrays.asList(queryVariantCommandOptions.returnStudy.split(",")));
            List<String> list = new ArrayList<>();
            Collections.addAll(list, queryVariantsOptions.genericVariantQueryOptions.returnStudy.split(","));
            query.put(RETURNED_STUDIES.key(), list);
        } else {
            if (!studies.isEmpty()) {
                query.put(RETURNED_STUDIES.key(), studies);
            }
        }

        addParam(query, FILES, queryVariantsOptions.genericVariantQueryOptions.file);
        addParam(query, RETURNED_FILES, queryVariantsOptions.genericVariantQueryOptions.returnFile);
        addParam(query, FILTER, queryVariantsOptions.genericVariantQueryOptions.filter);
        addParam(query, GENOTYPE, queryVariantsOptions.genericVariantQueryOptions.sampleGenotype);
        addParam(query, SAMPLES, queryVariantsOptions.genericVariantQueryOptions.samples);
        addParam(query, VariantCatalogQueryUtils.SAMPLE_FILTER, queryVariantsOptions.sampleFilter);
        if (queryVariantsOptions.genericVariantQueryOptions.returnSample != null) {
            if (queryVariantsOptions.genericVariantQueryOptions.returnSample.isEmpty() || queryVariantsOptions.genericVariantQueryOptions.returnSample.equals(".")) {
                query.put(RETURNED_SAMPLES.key(), Collections.emptyList());
            } else {
                query.put(RETURNED_SAMPLES.key(), queryVariantsOptions.genericVariantQueryOptions.returnSample);
            }
        }
        addParam(query, INCLUDE_FORMAT, queryVariantsOptions.genericVariantQueryOptions.includeFormat);
        addParam(query, INCLUDE_GENOTYPE, queryVariantsOptions.genericVariantQueryOptions.includeGenotype);
        addParam(query, UNKNOWN_GENOTYPE, queryVariantsOptions.genericVariantQueryOptions.unknownGenotype);


        /**
         * Annotation parameters
         */
        addParam(query, ANNOT_CONSEQUENCE_TYPE, queryVariantsOptions.genericVariantQueryOptions.consequenceType);
        addParam(query, ANNOT_BIOTYPE, queryVariantsOptions.genericVariantQueryOptions.geneBiotype);
        addParam(query, ANNOT_POPULATION_ALTERNATE_FREQUENCY, queryVariantsOptions.genericVariantQueryOptions.populationFreqs);
        addParam(query, ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY, queryVariantsOptions.genericVariantQueryOptions.populationMaf);
        addParam(query, ANNOT_CONSERVATION, queryVariantsOptions.genericVariantQueryOptions.conservation);
        addParam(query, ANNOT_TRANSCRIPTION_FLAGS, queryVariantsOptions.genericVariantQueryOptions.flags);
        addParam(query, ANNOT_GENE_TRAITS_ID, queryVariantsOptions.genericVariantQueryOptions.geneTraitId);
        addParam(query, ANNOT_GENE_TRAITS_NAME, queryVariantsOptions.genericVariantQueryOptions.geneTraitName);
        addParam(query, ANNOT_HPO, queryVariantsOptions.genericVariantQueryOptions.hpo);
        addParam(query, ANNOT_GO, queryVariantsOptions.genericVariantQueryOptions.go);
//        addParam(query, ANNOT_EXPRESSION, queryVariantsOptions.genericVariantQueryOptions.expression);
        addParam(query, ANNOT_PROTEIN_KEYWORDS, queryVariantsOptions.genericVariantQueryOptions.proteinKeywords);
        addParam(query, ANNOT_DRUG, queryVariantsOptions.genericVariantQueryOptions.drugs);

        if (StringUtils.isNoneEmpty(queryVariantsOptions.genericVariantQueryOptions.proteinSubstitution)) {
            query.put(ANNOT_PROTEIN_SUBSTITUTION.key(), queryVariantsOptions.genericVariantQueryOptions.proteinSubstitution);
        }


        /*
         * Stats parameters
         */
        if (queryVariantsOptions.genericVariantQueryOptions.stats != null && !queryVariantsOptions.genericVariantQueryOptions.stats.isEmpty()) {
            Set<String> acceptedStatKeys = new HashSet<>(Arrays.asList(STATS_MAF.key(),
                    STATS_MGF.key(),
                    MISSING_ALLELES.key(),
                    MISSING_GENOTYPES.key()));

            for (String stat : queryVariantsOptions.genericVariantQueryOptions.stats.split(",")) {
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

        addParam(query, STATS_MAF, queryVariantsOptions.genericVariantQueryOptions.maf);
        addParam(query, STATS_MGF, queryVariantsOptions.genericVariantQueryOptions.mgf);
        addParam(query, MISSING_ALLELES, queryVariantsOptions.genericVariantQueryOptions.missingAlleleCount);
        addParam(query, MISSING_GENOTYPES, queryVariantsOptions.genericVariantQueryOptions.missingGenotypeCount);


        boolean returnVariants = !queryVariantsOptions.numericOptions.count && StringUtils.isEmpty(queryVariantsOptions.genericVariantQueryOptions.groupBy)
                && StringUtils.isEmpty(queryVariantsOptions.genericVariantQueryOptions.rank);


        VariantOutputFormat of = VCF;
        if (StringUtils.isNotEmpty(queryVariantsOptions.commonOptions.outputFormat)) {
            of = VariantWriterFactory.toOutputFormat(queryVariantsOptions.commonOptions.outputFormat, null);
            if (of == null) {
                throw variantFormatNotSupported(queryVariantsOptions.commonOptions.outputFormat);
            }
        }

        if (returnVariants && !of.isMultiStudyOutput()) {
            int returnedStudiesSize = query.getAsStringList(RETURNED_STUDIES.key()).size();
            if (returnedStudiesSize == 0 && studies.size() == 1) {
                query.put(RETURNED_STUDIES.key(), studies.get(0));
            } else if (returnedStudiesSize == 0 && studyIds.size() != 1 //If there are no returned studies, and there are more than one study
                    || returnedStudiesSize > 1) {     // Or is required more than one returned study
                throw new Exception("Only one study is allowed when returning " + of + ", please use '--return-study' to select the returned "
                        + "study. Available studies: " + studyIds);
            } else {
                if (returnedStudiesSize == 0) {    //If there were no returned studies, set the study existing one
                    query.put(RETURNED_STUDIES.key(), studyIds.get(0));
                }
            }
        }

        return query;
    }

    public static QueryOptions parseQueryOptions(VariantCommandOptions.VariantQueryCommandOptions queryVariantsOptions) {
        QueryOptions queryOptions = new QueryOptions(new HashMap<>(queryVariantsOptions.commonOptions.params));

        if (StringUtils.isNotEmpty(queryVariantsOptions.dataModelOptions.include)) {
            queryOptions.add(QueryOptions.INCLUDE, queryVariantsOptions.dataModelOptions.include);
        }

        if (StringUtils.isNotEmpty(queryVariantsOptions.dataModelOptions.exclude)) {
            queryOptions.add(QueryOptions.EXCLUDE, queryVariantsOptions.dataModelOptions.exclude);
        }
//        else {
//            queryOptions.put("exclude", "_id");
//        }

        if (queryVariantsOptions.numericOptions.skip > 0) {
            queryOptions.add(QueryOptions.SKIP, queryVariantsOptions.numericOptions.skip);
        }


        if (queryVariantsOptions.numericOptions.limit > 0) {
            queryOptions.add(QueryOptions.LIMIT, queryVariantsOptions.numericOptions.limit);
        }



        if (queryVariantsOptions.numericOptions.count) {
            queryOptions.add("count", true);
        }

//        if (queryVariantsOptions.numericOptions.sort) {
//            queryOptions.add(QueryOptions.SORT, true);
//        }

        return queryOptions;
    }

    public static ParameterException variantFormatNotSupported(String outputFormat) {
        logger.error("Format '{}' not supported", outputFormat);
        return new ParameterException("Format '" + outputFormat + "' not supported");
    }

}
