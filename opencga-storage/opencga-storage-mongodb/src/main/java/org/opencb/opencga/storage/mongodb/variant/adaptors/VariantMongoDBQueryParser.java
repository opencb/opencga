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

package org.opencb.opencga.storage.mongodb.variant.adaptors;

import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.bson.Document;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.mongodb.variant.converters.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.*;
import static org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageEngine.MongoDBVariantOptions.DEFAULT_GENOTYPE;

/**
 * Created on 31/03/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantMongoDBQueryParser {

    public static final VariantStringIdConverter STRING_ID_CONVERTER = new VariantStringIdConverter();
    protected static Logger logger = LoggerFactory.getLogger(VariantMongoDBQueryParser.class);
    private final StudyConfigurationManager studyConfigurationManager;
    private final ObjectMapper queryMapper;
    //    private final CellBaseUtils cellBaseUtils;

    interface VariantMixin {
        // Serialize variants with "toString". Used to serialize queries.
        @JsonValue
        String toString();
    }

    public VariantMongoDBQueryParser(StudyConfigurationManager studyConfigurationManager) {
        this.studyConfigurationManager = studyConfigurationManager;

        queryMapper = new ObjectMapper();
        queryMapper.addMixIn(Variant.class, VariantMixin.class);
    }

    protected Document parseQuery(final Query originalQuery) {
        QueryBuilder builder = new QueryBuilder();
        if (originalQuery != null) {
            // Copy given query. It may be modified
            Query query = new Query(originalQuery);
            boolean nonGeneRegionFilter = false;
            /* VARIANT PARAMS */
            List<Region> regions = new ArrayList<>();
            if (isValidParam(query, CHROMOSOME)) {
                nonGeneRegionFilter = true;
                regions.addAll(Region.parseRegions(query.getString(CHROMOSOME.key()), true));
            }

            if (isValidParam(query, REGION)) {
                nonGeneRegionFilter = true;
                regions.addAll(Region.parseRegions(query.getString(REGION.key()), true));
            }
            if (!regions.isEmpty()) {
                getRegionFilter(regions, builder);
            }

            // List with all MongoIds from ID and XREF filters
            List<String> mongoIds = new ArrayList<>();

            if (isValidParam(query, ID)) {
                nonGeneRegionFilter = true;
                List<String> idsList = query.getAsStringList(ID.key());
                List<String> otherIds = new ArrayList<>(idsList.size());

                for (String value : idsList) {
                    Variant variant = toVariant(value);
                    if (variant != null) {
                        mongoIds.add(STRING_ID_CONVERTER.buildId(variant));
                    } else {
                        otherIds.add(value);
                    }
                }

                if (!otherIds.isEmpty()) {
                    String ids = otherIds.stream().collect(Collectors.joining(","));
                    addQueryStringFilter(DocumentToVariantConverter.ANNOTATION_FIELD
                            + "." + DocumentToVariantAnnotationConverter.XREFS_FIELD
                            + "." + DocumentToVariantAnnotationConverter.XREF_ID_FIELD, ids, builder, QueryOperation.OR);
                    addQueryStringFilter(DocumentToVariantConverter.IDS_FIELD, ids, builder, QueryOperation.OR);
                }
            }

            List<String> genes = new ArrayList<>(query.getAsStringList(GENE.key()));

            if (isValidParam(query, ANNOT_XREF)) {
                List<String> xrefs = query.getAsStringList(ANNOT_XREF.key());
                List<String> otherXrefs = new ArrayList<>();
                for (String value : xrefs) {
                    Variant variant = toVariant(value);
                    if (variant != null) {
                        mongoIds.add(STRING_ID_CONVERTER.buildId(variant));
                    } else {
                        if (isVariantAccession(value) || isClinicalAccession(value) || isGeneAccession(value)) {
                            otherXrefs.add(value);
                        } else {
                            genes.add(value);
                        }
                    }
                }

                if (!otherXrefs.isEmpty()) {
                    nonGeneRegionFilter = true;
                    addQueryStringFilter(DocumentToVariantConverter.ANNOTATION_FIELD
                                    + '.' + DocumentToVariantAnnotationConverter.XREFS_FIELD
                                    + '.' + DocumentToVariantAnnotationConverter.XREF_ID_FIELD,
                            String.join(",", otherXrefs), builder, QueryOperation.OR);
                }
            }

            if (!genes.isEmpty()) {
                if (isValidParam(query, ANNOT_CONSEQUENCE_TYPE)) {
                    List<String> soList = query.getAsStringList(ANNOT_CONSEQUENCE_TYPE.key());
                    Set<String> gnSo = new HashSet<>(genes.size() * soList.size());
                    for (String gene : genes) {
                        for (String so : soList) {
                            int soNumber = parseConsequenceType(so);
                            gnSo.add(DocumentToVariantAnnotationConverter.buildGeneSO(gene, soNumber));
                        }
                    }
                    builder.or(new BasicDBObject(DocumentToVariantConverter.ANNOTATION_FIELD
                            + '.' + DocumentToVariantAnnotationConverter.GENE_SO_FIELD, new BasicDBObject("$in", gnSo)));
                    if (!nonGeneRegionFilter) {
                        // Filter already present in the GENE_SO_FIELD
                        query.remove(ANNOT_CONSEQUENCE_TYPE.key());
                    }
                } else {
                    addQueryStringFilter(DocumentToVariantConverter.ANNOTATION_FIELD
                                    + '.' + DocumentToVariantAnnotationConverter.XREFS_FIELD
                                    + '.' + DocumentToVariantAnnotationConverter.XREF_ID_FIELD,
                            String.join(",", genes), builder, QueryOperation.OR);
                }
            }

            if (!mongoIds.isEmpty()) {
                if (mongoIds.size() == 1) {
                    builder.or(new QueryBuilder().and("_id").is(mongoIds.get(0)).get());
                } else {
                    builder.or(new QueryBuilder().and("_id").in(mongoIds).get());
                }
            }


            if (isValidParam(query, REFERENCE)) {
                addQueryStringFilter(DocumentToVariantConverter.REFERENCE_FIELD, query.getString(REFERENCE.key()),
                        builder, QueryOperation.AND);
            }

            if (isValidParam(query, ALTERNATE)) {
                addQueryStringFilter(DocumentToVariantConverter.ALTERNATE_FIELD, query.getString(ALTERNATE.key()),
                        builder, QueryOperation.AND);
            }

            if (isValidParam(query, TYPE)) {
                addQueryFilter(DocumentToVariantConverter.TYPE_FIELD, query.getString(TYPE.key()), builder,
                        QueryOperation.AND, s -> {
                            Set<VariantType> subTypes = Variant.subTypes(VariantType.valueOf(s));
                            List<String> types = new ArrayList<>(subTypes.size() + 1);
                            types.add(s);
                            subTypes.forEach(subType -> types.add(subType.toString()));
                            return types;
                        }); //addQueryStringFilter(DBObjectToVariantConverter.TYPE_FIELD,
//                query.getString(VariantQueryParams.TYPE.key()), builder, QueryOperation.AND);
            }

            /* ANNOTATION PARAMS */
            parseAnnotationQueryParams(query, builder);

            /* STUDIES */
            final StudyConfiguration defaultStudyConfiguration = parseStudyQueryParams(query, builder);

            /* STATS PARAMS */
            parseStatsQueryParams(query, builder, defaultStudyConfiguration);
        }

        try {
            logger.debug("Query         = {}", originalQuery == null ? "{}" : queryMapper.writeValueAsString(originalQuery));
        } catch (IOException e) {
            logger.debug("Query         = {}", originalQuery.toString());
        }
        Document mongoQuery = new Document(builder.get().toMap());
        logger.debug("MongoDB Query = {}", mongoQuery.toJson(new JsonWriterSettings(JsonMode.SHELL, false)));
        return mongoQuery;
    }

    private void parseAnnotationQueryParams(Query query, QueryBuilder builder) {
        if (query != null) {
            if (isValidParam(query, ANNOTATION_EXISTS)) {
                boolean exists = query.getBoolean(ANNOTATION_EXISTS.key());
                builder.and(DocumentToVariantConverter.ANNOTATION_FIELD + "." + DocumentToVariantAnnotationConverter.ANNOT_ID_FIELD);
                builder.exists(exists);
                if (!exists) {
                    builder.and(DocumentToVariantConverter.ANNOTATION_FIELD
                            + '.' + DocumentToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD
                            + '.' + DocumentToVariantAnnotationConverter.CT_SO_ACCESSION_FIELD)
                            .exists(false);
                }
                // else , should be combined with an or, and it would not speed up the filtering. This scenario is not so common
            }

            if (isValidParam(query, ANNOT_CONSEQUENCE_TYPE)) {
                String value = query.getString(ANNOT_CONSEQUENCE_TYPE.key());
                addQueryFilter(DocumentToVariantConverter.ANNOTATION_FIELD
                                + '.' + DocumentToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD
                                + '.' + DocumentToVariantAnnotationConverter.CT_SO_ACCESSION_FIELD, value, builder, QueryOperation.AND,
                        VariantQueryUtils::parseConsequenceType);
            }

            if (isValidParam(query, ANNOT_BIOTYPE)) {
                String biotypes = query.getString(ANNOT_BIOTYPE.key());
                addQueryStringFilter(DocumentToVariantConverter.ANNOTATION_FIELD
                        + "." + DocumentToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD
                        + "." + DocumentToVariantAnnotationConverter.CT_BIOTYPE_FIELD, biotypes, builder, QueryOperation.AND);
            }

            if (isValidParam(query, ANNOT_POLYPHEN)) {
                String value = query.getString(ANNOT_POLYPHEN.key());
//                addCompListQueryFilter(DocumentToVariantConverter.ANNOTATION_FIELD
//                                + "." + DocumentToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD
//                                + "." + DocumentToVariantAnnotationConverter.CT_PROTEIN_POLYPHEN_FIELD
//                                + "." + DocumentToVariantAnnotationConverter.SCORE_SCORE_FIELD,
//                        value, builder);
                addScoreFilter(value, builder, ANNOT_POLYPHEN, DocumentToVariantAnnotationConverter.POLYPHEN, true);
            }

            if (isValidParam(query, ANNOT_SIFT)) {
                String value = query.getString(ANNOT_SIFT.key());
//                addCompListQueryFilter(DocumentToVariantConverter.ANNOTATION_FIELD
//                        + "." + DocumentToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD
//                        + "." + DocumentToVariantAnnotationConverter.CT_PROTEIN_SIFT_FIELD + "."
//                        + DocumentToVariantAnnotationConverter.SCORE_SCORE_FIELD, value, builder);
                addScoreFilter(value, builder, ANNOT_SIFT, DocumentToVariantAnnotationConverter.SIFT, true);
            }

            if (isValidParam(query, ANNOT_PROTEIN_SUBSTITUTION)) {
                String value = query.getString(ANNOT_PROTEIN_SUBSTITUTION.key());
                addScoreFilter(value, builder, ANNOT_PROTEIN_SUBSTITUTION, true);
            }

            if (isValidParam(query, ANNOT_CONSERVATION)) {
                String value = query.getString(ANNOT_CONSERVATION.key());
                addScoreFilter(value, builder, ANNOT_CONSERVATION, false);
            }

            if (isValidParam(query, ANNOT_TRANSCRIPTION_FLAGS)) {
                String value = query.getString(ANNOT_TRANSCRIPTION_FLAGS.key());
                addQueryStringFilter(DocumentToVariantConverter.ANNOTATION_FIELD
                        + "." + DocumentToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD
                        + "." + DocumentToVariantAnnotationConverter.CT_TRANSCRIPT_ANNOT_FLAGS, value, builder, QueryOperation.AND);
            }

//            QueryBuilder geneTraitBuilder = QueryBuilder.start();
            if (isValidParam(query, ANNOT_GENE_TRAITS_ID)) {
                String value = query.getString(ANNOT_GENE_TRAITS_ID.key());
                addQueryStringFilter(DocumentToVariantConverter.ANNOTATION_FIELD
                        + '.' + DocumentToVariantAnnotationConverter.GENE_TRAIT_FIELD
                        + '.' + DocumentToVariantAnnotationConverter.GENE_TRAIT_ID_FIELD, value, builder, QueryOperation.AND);
            }

            if (isValidParam(query, ANNOT_GENE_TRAITS_NAME)) {
                String value = query.getString(ANNOT_GENE_TRAITS_NAME.key());
                addCompQueryFilter(DocumentToVariantConverter.ANNOTATION_FIELD
                        + '.' + DocumentToVariantAnnotationConverter.GENE_TRAIT_FIELD
                        + '.' + DocumentToVariantAnnotationConverter.GENE_TRAIT_NAME_FIELD, value, builder, false);
            }

            if (isValidParam(query, ANNOT_HPO)) {
                String value = query.getString(ANNOT_HPO.key());
//                addQueryStringFilter(DocumentToVariantAnnotationConverter.GENE_TRAIT_HPO_FIELD, value, geneTraitBuilder,
//                        QueryOperation.AND);
                addQueryStringFilter(DocumentToVariantConverter.ANNOTATION_FIELD
                                + '.' + DocumentToVariantAnnotationConverter.XREFS_FIELD
                                + '.' + DocumentToVariantAnnotationConverter.XREF_ID_FIELD, value, builder,
                        QueryOperation.AND);
            }

//            DBObject geneTraitQuery = geneTraitBuilder.get();
//            if (geneTraitQuery.keySet().size() != 0) {
//                builder.and(DocumentToVariantConverter.ANNOTATION_FIELD
//                        + "." + DocumentToVariantAnnotationConverter.GENE_TRAIT_FIELD).elemMatch(geneTraitQuery);
//            }

//            if (isValidParam(query, ANNOT_GO)) {
//                String value = query.getString(ANNOT_GO.key());
//
//                // Check if comma separated of semi colon separated (AND or OR)
//                QueryOperation queryOperation = checkOperator(value);
//                // Split by comma or semi colon
//                List<String> goValues = splitValue(value, queryOperation);
//
//                if (queryOperation == QueryOperation.AND) {
//                    throw VariantQueryException.malformedParam(ANNOT_GO, value, "Unimplemented AND operator");
//                }
//
//                Set<String> genes = cellBaseUtils.getGenesByGo(goValues);
//
//                builder.and(DocumentToVariantConverter.ANNOTATION_FIELD
//                        + "." + DocumentToVariantAnnotationConverter.XREFS_FIELD
//                        + "." + DocumentToVariantAnnotationConverter.XREF_ID_FIELD).in(genes);
//
//            }
//
//            if (isValidParam(query, ANNOT_EXPRESSION)) {
//                String value = query.getString(ANNOT_EXPRESSION.key());
//
//                // Check if comma separated of semi colon separated (AND or OR)
//                QueryOperation queryOperation = checkOperator(value);
//                // Split by comma or semi colon
//                List<String> expressionValues = splitValue(value, queryOperation);
//
//                if (queryOperation == QueryOperation.AND) {
//                    throw VariantQueryException.malformedParam(ANNOT_EXPRESSION, value, "Unimplemented AND operator");
//                }
//
//                Set<String> genes = cellBaseUtils.getGenesByExpression(expressionValues);
//
//                builder.and(DocumentToVariantConverter.ANNOTATION_FIELD
//                        + "." + DocumentToVariantAnnotationConverter.XREFS_FIELD
//                        + "." + DocumentToVariantAnnotationConverter.XREF_ID_FIELD).in(genes);
//
//            }


            if (isValidParam(query, ANNOT_PROTEIN_KEYWORDS)) {
                String value = query.getString(ANNOT_PROTEIN_KEYWORDS.key());
                addQueryStringFilter(DocumentToVariantConverter.ANNOTATION_FIELD
                        + "." + DocumentToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD
                        + "." + DocumentToVariantAnnotationConverter.CT_PROTEIN_KEYWORDS, value, builder, QueryOperation.AND);
            }

            if (isValidParam(query, ANNOT_DRUG)) {
                String value = query.getString(ANNOT_DRUG.key());
                addQueryStringFilter(DocumentToVariantConverter.ANNOTATION_FIELD
                        + "." + DocumentToVariantAnnotationConverter.DRUG_FIELD
                        + "." + DocumentToVariantAnnotationConverter.DRUG_NAME_FIELD, value, builder, QueryOperation.AND);
            }

            if (isValidParam(query, ANNOT_FUNCTIONAL_SCORE)) {
                String value = query.getString(ANNOT_FUNCTIONAL_SCORE.key());
                addScoreFilter(value, builder, ANNOT_FUNCTIONAL_SCORE, false);
            }

            if (isValidParam(query, ANNOT_CUSTOM)) {
                String value = query.getString(ANNOT_CUSTOM.key());
                addCompListQueryFilter(DocumentToVariantConverter.CUSTOM_ANNOTATION_FIELD, value, builder, true);
            }

            if (isValidParam(query, ANNOT_POPULATION_ALTERNATE_FREQUENCY)) {
                String value = query.getString(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key());
                addFrequencyFilter(DocumentToVariantConverter.ANNOTATION_FIELD
                                + "." + DocumentToVariantAnnotationConverter.POPULATION_FREQUENCIES_FIELD,
                        DocumentToVariantAnnotationConverter.POPULATION_FREQUENCY_ALTERNATE_FREQUENCY_FIELD, value, builder,
                        ANNOT_POPULATION_ALTERNATE_FREQUENCY); // Same
                // method addFrequencyFilter is used for reference and allele frequencies. Need to provide the field
                // (reference/alternate) where to check the frequency
            }

            if (isValidParam(query, ANNOT_POPULATION_REFERENCE_FREQUENCY)) {
                String value = query.getString(ANNOT_POPULATION_REFERENCE_FREQUENCY.key());
                addFrequencyFilter(DocumentToVariantConverter.ANNOTATION_FIELD
                                + "." + DocumentToVariantAnnotationConverter.POPULATION_FREQUENCIES_FIELD,
                        DocumentToVariantAnnotationConverter.POPULATION_FREQUENCY_REFERENCE_FREQUENCY_FIELD, value, builder,
                        ANNOT_POPULATION_REFERENCE_FREQUENCY); // Same
                // method addFrequencyFilter is used for reference and allele frequencies. Need to provide the field
                // (reference/alternate) where to check the frequency
            }

            if (isValidParam(query, ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY)) {
                String value = query.getString(ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY.key());
                addFrequencyFilter(DocumentToVariantConverter.ANNOTATION_FIELD + "."
                                + DocumentToVariantAnnotationConverter.POPULATION_FREQUENCIES_FIELD,
                        value, builder, ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY,
                        (v, queryBuilder) -> {
                            String[] split = splitOperator(v);
                            String op = split[1];
                            String obj = split[2];

                            double aDouble = Double.parseDouble(obj);
                            switch (op) {
                                case "<":
                                    queryBuilder.or(QueryBuilder.start(DocumentToVariantAnnotationConverter.
                                                    POPULATION_FREQUENCY_REFERENCE_FREQUENCY_FIELD).lessThan(aDouble).get(),
                                            QueryBuilder.start(DocumentToVariantAnnotationConverter.
                                                    POPULATION_FREQUENCY_ALTERNATE_FREQUENCY_FIELD).lessThan(aDouble).get()
                                    );
                                    break;
                                case "<=":
                                    queryBuilder.or(QueryBuilder.start(DocumentToVariantAnnotationConverter.
                                                    POPULATION_FREQUENCY_REFERENCE_FREQUENCY_FIELD).lessThanEquals(aDouble).get(),
                                            QueryBuilder.start(DocumentToVariantAnnotationConverter.
                                                    POPULATION_FREQUENCY_ALTERNATE_FREQUENCY_FIELD).lessThanEquals(aDouble).get()
                                    );
                                    break;
                                case ">":
                                    queryBuilder.and(DocumentToVariantAnnotationConverter.
                                            POPULATION_FREQUENCY_REFERENCE_FREQUENCY_FIELD).greaterThan(aDouble)
                                            .and(DocumentToVariantAnnotationConverter.
                                                    POPULATION_FREQUENCY_ALTERNATE_FREQUENCY_FIELD).greaterThan(aDouble);
                                    break;
                                case ">=":
                                    queryBuilder.and(DocumentToVariantAnnotationConverter.
                                            POPULATION_FREQUENCY_REFERENCE_FREQUENCY_FIELD).greaterThanEquals(aDouble)
                                            .and(DocumentToVariantAnnotationConverter.
                                                    POPULATION_FREQUENCY_ALTERNATE_FREQUENCY_FIELD).greaterThanEquals(aDouble);
                                    break;
                                default:
                                    throw new IllegalArgumentException("Unsupported operator '" + op + "'");
                            }
                        });
            }
        }
    }

    private StudyConfiguration parseStudyQueryParams(Query query, QueryBuilder builder) {

        if (query != null) {
            Map<String, Integer> studies = studyConfigurationManager.getStudies(null);

            boolean singleStudy = studies.size() == 1;
            boolean validStudiesFilter = isValidParam(query, STUDIES);
            // SAMPLES filter will add a FILES filter if absent
            boolean validFilesFilter = isValidParam(query, FILES) || isValidParam(query, SAMPLES);
            boolean otherFilters =
                    isValidParam(query, FILES)
                            || isValidParam(query, GENOTYPE)
                            || isValidParam(query, SAMPLES)
                            || isValidParam(query, FILTER);

            // Use an elemMatch with all the study filters if there is more than one study registered,
            // or FILES and STUDIES filters are being used.
            // If filters STUDIES+FILES is used, elemMatch is required to use the index correctly. See #493
            boolean studyElemMatch = (!singleStudy || (validFilesFilter && validStudiesFilter));

            // If only studyId filter is being used, elemMatch is not needed
            if (validStudiesFilter && !otherFilters) {
                studyElemMatch = false;
            }

            // If using an elemMatch for the study, keys don't need to start with "studies"
            String studyQueryPrefix = studyElemMatch ? "" : DocumentToVariantConverter.STUDIES_FIELD + '.';
            QueryBuilder studyBuilder = QueryBuilder.start();
            final StudyConfiguration defaultStudyConfiguration = getDefaultStudyConfiguration(query, null, studyConfigurationManager);

            if (isValidParam(query, STUDIES)) {
                String sidKey = DocumentToVariantConverter.STUDIES_FIELD + '.' + DocumentToStudyVariantEntryConverter.STUDYID_FIELD;
                String value = query.getString(STUDIES.key());

                // Check that the study exists
                QueryOperation studiesOperation = checkOperator(value);
                List<String> studiesNames = splitValue(value, studiesOperation);
                List<Integer> studyIds = studyConfigurationManager.getStudyIds(studiesNames, studies); // Non negated studyIds

                // If the Studies query has an AND operator or includes negated fields, it can not be represented only
                // in the "elemMatch". It needs to be in the root
                boolean anyNegated = studiesNames.stream().anyMatch(VariantQueryUtils::isNegated);
                boolean studyFilterAtRoot = studiesOperation == QueryOperation.AND || anyNegated;
                if (studyFilterAtRoot) {
                    addQueryFilter(sidKey, value, builder, QueryOperation.AND, study ->
                            studyConfigurationManager.getStudyId(study, false, studies));
                }

                // Add all non negated studies to the elemMatch builder if it is being used,
                // or it is not and it has not been added to the root
                if (studyElemMatch || !studyFilterAtRoot) {
                    if (!studyIds.isEmpty()) {
                        if (!singleStudy || anyNegated || validFilesFilter) {
                            String studyIdsCsv = studyIds.stream().map(Object::toString).collect(Collectors.joining(","));
                            addQueryIntegerFilter(studyQueryPrefix + DocumentToStudyVariantEntryConverter.STUDYID_FIELD, studyIdsCsv,
                                    studyBuilder, QueryOperation.AND);
                        } // There is only one study! We can skip this filter
                    }
                }
            }

            if (isValidParam(query, FILES)) {
                addQueryFilter(studyQueryPrefix + DocumentToStudyVariantEntryConverter.FILES_FIELD
                                + '.' + DocumentToStudyVariantEntryConverter.FILEID_FIELD,
                        query.getString(FILES.key()), studyBuilder, QueryOperation.AND,
                        f -> studyConfigurationManager.getFileId(f, false, defaultStudyConfiguration));
            }

            if (isValidParam(query, FILTER)) {
                String filesValue = query.getString(FILES.key());
                QueryOperation filesOperation = checkOperator(filesValue);
                List<String> fileNames = splitValue(filesValue, filesOperation);
                List<Integer> fileIds = studyConfigurationManager.getFileIds(fileNames, true, defaultStudyConfiguration);

                String fileQueryPrefix;
                if (fileIds.isEmpty()) {
                    fileQueryPrefix = studyQueryPrefix + DocumentToStudyVariantEntryConverter.FILES_FIELD + '.';
                    addQueryStringFilter(fileQueryPrefix + DocumentToStudyVariantEntryConverter.ATTRIBUTES_FIELD + '.' + StudyEntry.FILTER,
                            query.getString(FILTER.key()), studyBuilder, QueryOperation.AND);
                } else {
                    QueryBuilder fileBuilder = QueryBuilder.start();
                    addQueryStringFilter(DocumentToStudyVariantEntryConverter.ATTRIBUTES_FIELD + '.' + StudyEntry.FILTER,
                            query.getString(FILTER.key()), fileBuilder, QueryOperation.AND);
                    fileBuilder.and(DocumentToStudyVariantEntryConverter.FILEID_FIELD).in(fileIds);
                    studyBuilder.and(studyQueryPrefix + DocumentToStudyVariantEntryConverter.FILES_FIELD).elemMatch(fileBuilder.get());
                }
            }

            Map<Object, List<String>> genotypesFilter = new HashMap<>();
            if (isValidParam(query, GENOTYPE)) {
                String sampleGenotypes = query.getString(GENOTYPE.key());
                parseGenotypeFilter(sampleGenotypes, genotypesFilter);
            }

            if (isValidParam(query, SAMPLES)) {
                Set<Integer> files = new HashSet<>();
                String samples = query.getString(SAMPLES.key());

                boolean filesFilterBySamples = !isValidParam(query, FILES) && defaultStudyConfiguration != null;
                for (String sample : samples.split(",")) {
                    int sampleId = studyConfigurationManager.getSampleId(sample, defaultStudyConfiguration);
                    genotypesFilter.put(sampleId, Arrays.asList(
                            "1",
                            "0/1", "0|1", "1|0",
                            "1/1", "1|1",
                            "1/2", "1|2", "2|1"
                    ));
                    if (filesFilterBySamples) {
                        int filesFromSample = 0;
                        for (Integer file : defaultStudyConfiguration.getIndexedFiles()) {
                            if (defaultStudyConfiguration.getSamplesInFiles().get(file).contains(sampleId)) {
                                files.add(file);
                                filesFromSample++;
                                if (filesFromSample > 1) {
                                    // If there are more than one indexed file per sample, do not use filesFilterBySamples
                                    filesFilterBySamples = false;
                                    break;
                                }
                            }
                        }
                    }
                }

                // If there is no valid files filter, add files filter to speed up this query
                if (filesFilterBySamples && !files.isEmpty()
                        && !defaultStudyConfiguration.getIndexedFiles().containsAll(files)) {
                    addQueryFilter(studyQueryPrefix + DocumentToStudyVariantEntryConverter.FILES_FIELD
                                    + '.' + DocumentToStudyVariantEntryConverter.FILEID_FIELD, files, studyBuilder, QueryOperation.AND,
                            f -> studyConfigurationManager.getFileId(f, false, defaultStudyConfiguration));
                }
            }

            if (!genotypesFilter.isEmpty()) {
                for (Map.Entry<Object, List<String>> entry : genotypesFilter.entrySet()) {
                    Object sample = entry.getKey();
                    List<String> genotypes = entry.getValue();

                    int sampleId = studyConfigurationManager.getSampleId(sample, defaultStudyConfiguration);

                    QueryBuilder genotypesBuilder = QueryBuilder.start();

                    List<String> defaultGenotypes;
                    if (defaultStudyConfiguration != null) {
                        defaultGenotypes = defaultStudyConfiguration.getAttributes().getAsStringList(DEFAULT_GENOTYPE.key());
                    } else {
                        defaultGenotypes = Arrays.asList("0/0", "0|0");
                    }
                    for (String genotype : genotypes) {
                        boolean negated = isNegated(genotype);
                        if (negated) {
                            genotype = removeNegation(genotype);
                        }
                        if (defaultGenotypes.contains(genotype)) {
                            List<String> otherGenotypes = Arrays.asList(
                                    "0/0", "0|0",
                                    "0/1", "1/0", "1/1", "-1/-1",
                                    "0|1", "1|0", "1|1", "-1|-1",
                                    "0|2", "2|0", "2|1", "1|2", "2|2",
                                    "0/2", "2/0", "2/1", "1/2", "2/2",
                                    DocumentToSamplesConverter.UNKNOWN_GENOTYPE);
                            if (negated) {
                                for (String otherGenotype : otherGenotypes) {
                                    if (defaultGenotypes.contains(otherGenotype)) {
                                        continue;
                                    }
                                    String key = studyQueryPrefix
                                            + DocumentToStudyVariantEntryConverter.GENOTYPES_FIELD
                                            + '.' + otherGenotype;
                                    genotypesBuilder.or(new BasicDBObject(key, sampleId));
                                }
                            } else {
                                QueryBuilder andBuilder = QueryBuilder.start();
                                for (String otherGenotype : otherGenotypes) {
                                    if (defaultGenotypes.contains(otherGenotype)) {
                                        continue;
                                    }
                                    String key = studyQueryPrefix
                                            + DocumentToStudyVariantEntryConverter.GENOTYPES_FIELD
                                            + '.' + otherGenotype;
                                    andBuilder.and(new BasicDBObject(key,
                                            new Document("$ne", sampleId)));
                                }
                                genotypesBuilder.or(andBuilder.get());
                            }
                        } else {
                            String s = studyQueryPrefix
                                    + DocumentToStudyVariantEntryConverter.GENOTYPES_FIELD
                                    + '.' + DocumentToSamplesConverter.genotypeToStorageType(genotype);
                            if (negated) {
                                //and [ {"gt.0|1" : { $ne : <sampleId> } } ]
                                genotypesBuilder.and(new BasicDBObject(s, new BasicDBObject("$ne", sampleId)));

                            } else {
                                //or [ {"gt.0|1" : <sampleId> } ]
                                genotypesBuilder.or(new BasicDBObject(s, sampleId));
                            }
                        }
                    }
                    studyBuilder.and(genotypesBuilder.get());
                }
            }

            // If Study Query is used then we add a elemMatch query
            DBObject studyQuery = studyBuilder.get();
            if (!studyQuery.keySet().isEmpty()) {
                if (studyElemMatch) {
                    builder.and(DocumentToVariantConverter.STUDIES_FIELD).elemMatch(studyQuery);
                } else {
                    builder.and(studyQuery);
                }
            }
            return defaultStudyConfiguration;
        } else {
            return null;
        }
    }

    private void parseStatsQueryParams(Query query, QueryBuilder builder, StudyConfiguration defaultStudyConfiguration) {
        if (query != null) {
            if (query.get(COHORTS.key()) != null && !query.getString(COHORTS.key()).isEmpty()) {
                addQueryFilter(DocumentToVariantConverter.STATS_FIELD
                                + "." + DocumentToVariantStatsConverter.COHORT_ID,
                        query.getString(COHORTS.key()), builder, QueryOperation.AND,
                        s -> {
                            try {
                                return Integer.parseInt(s);
                            } catch (NumberFormatException ignore) {
                                int indexOf = s.lastIndexOf(":");
                                if (defaultStudyConfiguration == null && indexOf < 0) {
                                    throw VariantQueryException.malformedParam(COHORTS, s, "Expected {study}:{cohort}");
                                } else {
                                    String study;
                                    String cohort;
                                    Integer cohortId;
                                    if (defaultStudyConfiguration != null && indexOf < 0) {
                                        cohort = s;
                                        cohortId = studyConfigurationManager.getCohortId(cohort, defaultStudyConfiguration);
                                    } else {
                                        study = s.substring(0, indexOf);
                                        cohort = s.substring(indexOf + 1);
                                        StudyConfiguration studyConfiguration =
                                                studyConfigurationManager.getStudyConfiguration(study, defaultStudyConfiguration);
                                        cohortId = studyConfigurationManager.getCohortId(cohort, studyConfiguration);
                                    }
                                    return cohortId;
                                }
                            }
                        });
            }

            if (query.get(STATS_MAF.key()) != null && !query.getString(STATS_MAF.key()).isEmpty()) {
                addStatsFilterList(DocumentToVariantStatsConverter.MAF_FIELD, query.getString(STATS_MAF.key()),
                        builder, defaultStudyConfiguration);
            }

            if (query.get(STATS_MGF.key()) != null && !query.getString(STATS_MGF.key()).isEmpty()) {
                addStatsFilterList(DocumentToVariantStatsConverter.MGF_FIELD, query.getString(STATS_MGF.key()),
                        builder, defaultStudyConfiguration);
            }

            if (query.get(MISSING_ALLELES.key()) != null && !query.getString(MISSING_ALLELES.key())
                    .isEmpty()) {
                addStatsFilterList(DocumentToVariantStatsConverter.MISSALLELE_FIELD, query.getString(MISSING_ALLELES
                        .key()), builder, defaultStudyConfiguration);
            }

            if (query.get(MISSING_GENOTYPES.key()) != null && !query.getString(MISSING_GENOTYPES
                    .key()).isEmpty()) {
                addStatsFilterList(DocumentToVariantStatsConverter.MISSGENOTYPE_FIELD, query.getString(
                        MISSING_GENOTYPES.key()), builder, defaultStudyConfiguration);
            }

            if (query.get("numgt") != null && !query.getString("numgt").isEmpty()) {
                for (String numgt : query.getAsStringList("numgt")) {
                    String[] split = numgt.split(":");
                    addCompQueryFilter(
                            DocumentToVariantConverter.STATS_FIELD + "." + DocumentToVariantStatsConverter.NUMGT_FIELD + "." + split[0],
                            split[1], builder, false);
                }
            }
        }
    }

    protected Document createProjection(Query query, QueryOptions options) {
        Document projection = new Document();

        if (options == null) {
            options = new QueryOptions();
        }

        if (options.containsKey(QueryOptions.SORT) && !options.getString(QueryOptions.SORT).equals("_id")) {
            if (options.getBoolean(QueryOptions.SORT)) {
                options.put(QueryOptions.SORT, "_id");
                options.putIfAbsent(QueryOptions.ORDER, QueryOptions.ASCENDING);
            } else {
                options.remove(QueryOptions.SORT);
            }
        }

        Set<VariantField> returnedFields = VariantField.getReturnedFields(options);
        // Add all required fields
        returnedFields.addAll(DocumentToVariantConverter.REQUIRED_FIELDS_SET);
        // StudyID is mandatory if returning any STUDY element
        if (returnedFields.contains(VariantField.STUDIES) && !returnedFields.contains(VariantField.STUDIES_STUDY_ID)) {
            returnedFields.add(VariantField.STUDIES_STUDY_ID);
        }

        // Top level $elemMatch MUST be at the very beginning in the projection document, so all the fields apply correctly.
        //
        // This two queries return different values:
        //
        // > db.variants.find({}, {studies:{$elemMatch:{sid:1}}, "studies.files":1})
        // {  studies : [ { sid : 1, files : [ ... ] } ]  }
        //
        // > db.variants.find({}, {"studies.files":1, studies:{$elemMatch:{sid:1}}})
        // {  studies : [ { sid : 1, files : [ ... ] , gt : { ... } } ]  }
        List<Integer> studiesIds = VariantQueryUtils.getReturnedStudies(query, options, studyConfigurationManager);
        // Use elemMatch only if there is one study to return.
        if (studiesIds.size() == 1) {
            projection.put(
                    DocumentToVariantConverter.STUDIES_FIELD,
                    new Document(
                            "$elemMatch",
                            new Document(
                                    DocumentToStudyVariantEntryConverter.STUDYID_FIELD,
                                    new Document(
                                            "$in",
                                            studiesIds
                                    )
                            )
                    )
            );
        }

        if (returnedFields.contains(VariantField.STUDIES_SAMPLES_DATA)) {
            List<String> formats = VariantQueryUtils.getIncludeFormats(query);
            if (formats != null) { // If null, undefined. Return all
                // Special conversion
                returnedFields.remove(VariantField.STUDIES_SAMPLES_DATA);

                for (String format : formats) {
                    if (format.equals(GT)) {
                        projection.put(DocumentToVariantConverter.STUDIES_FIELD + '.'
                                + DocumentToStudyVariantEntryConverter.GENOTYPES_FIELD, 1);
                    } else {
                        projection.put(DocumentToVariantConverter.STUDIES_FIELD + '.'
                                + DocumentToStudyVariantEntryConverter.FILES_FIELD + '.'
                                + DocumentToStudyVariantEntryConverter.SAMPLE_DATA_FIELD + '.' + format.toLowerCase(), 1);
                        projection.put(DocumentToVariantConverter.STUDIES_FIELD + '.'
                                + DocumentToStudyVariantEntryConverter.FILES_FIELD + '.'
                                + DocumentToStudyVariantEntryConverter.FILEID_FIELD, 1);
                    }
                }
            }
        }

        returnedFields = VariantField.prune(returnedFields);

        if (!returnedFields.isEmpty()) { //Include some
            for (VariantField s : returnedFields) {
                List<String> keys = DocumentToVariantConverter.toShortFieldName(s);
                if (keys != null) {
                    for (String key : keys) {
                        // Put if absent. Do not overwrite $elemMatch, if any
                        projection.putIfAbsent(key, 1);
                    }
                } else {
                    logger.warn("Unknown include field: {}", s);
                }
            }
        }

        logger.debug("QueryOptions: = {}", options.toJson());
        logger.debug("Projection:   = {}", projection.toJson(new JsonWriterSettings(JsonMode.SHELL, false)));
        return projection;
    }

    private QueryBuilder addQueryStringFilter(String key, String value, final QueryBuilder builder, VariantQueryUtils.QueryOperation op) {
        return this.addQueryFilter(key, value, builder, op, Function.identity());
    }

    private QueryBuilder addQueryIntegerFilter(String key, String value, final QueryBuilder builder, VariantQueryUtils.QueryOperation op) {
        return this.<Integer>addQueryFilter(key, value, builder, op, elem -> {
            try {
                return Integer.parseInt(elem);
            } catch (NumberFormatException e) {
                throw new VariantQueryException("Unable to parse int " + elem, e);
            }
        });
    }

    private <T> QueryBuilder addQueryFilter(String key, Collection<?> value, final QueryBuilder builder, QueryOperation op,
                                            Function<String, T> map) {
        return addQueryFilter(key, value.stream().map(Object::toString).collect(Collectors.joining(AND)), builder, op, map);
    }

    private <T> QueryBuilder addQueryFilter(String key, String value, final QueryBuilder builder, QueryOperation op,
                                            Function<String, T> map) {
        VariantQueryUtils.QueryOperation operation = checkOperator(value);
        QueryBuilder auxBuilder;
        if (op == VariantQueryUtils.QueryOperation.OR) {
            auxBuilder = QueryBuilder.start();
        } else {
            auxBuilder = builder;
        }

        if (operation == null) {
            if (isNegated(value)) {
                T mapped = map.apply(removeNegation(value));
                if (mapped instanceof Collection) {
                    auxBuilder.and(key).notIn(mapped);
                } else {
                    auxBuilder.and(key).notEquals(mapped);
                }
            } else {
                T mapped = map.apply(value);
                if (mapped instanceof Collection) {
                    auxBuilder.and(key).in(mapped);
                } else {
                    auxBuilder.and(key).is(mapped);
                }
            }
        } else if (operation == VariantQueryUtils.QueryOperation.OR) {
            String[] array = value.split(OR);
            List list = new ArrayList(array.length);
            for (String elem : array) {
                if (isNegated(elem)) {
                    throw new VariantQueryException("Unable to use negate (!) operator in OR sequences (<it_1>(,<it_n>)*)");
                } else {
                    T mapped = map.apply(elem);
                    if (mapped instanceof Collection) {
                        list.addAll(((Collection) mapped));
                    } else {
                        list.add(mapped);
                    }
                }
            }
            auxBuilder.and(key).in(list);
        } else {
            //Split in two lists: positive and negative
            String[] array = value.split(AND);
            List listIs = new ArrayList(array.length);
            List listNotIs = new ArrayList(array.length);

            for (String elem : array) {
                if (isNegated(elem)) {
                    T mapped = map.apply(removeNegation(elem));
                    if (mapped instanceof Collection) {
                        listNotIs.addAll(((Collection) mapped));
                    } else {
                        listNotIs.add(mapped);
                    }
                } else {
                    T mapped = map.apply(elem);
                    if (mapped instanceof Collection) {
                        listIs.addAll(((Collection) mapped));
                    } else {
                        listIs.add(mapped);
                    }
                }
            }

            if (!listIs.isEmpty()) {    //Can not use method "is" because it will be overwritten with the "notEquals" or "notIn" method
                auxBuilder.and(key).all(listIs);
            }
            if (listNotIs.size() == 1) {
                auxBuilder.and(key).notEquals(listNotIs.get(0));
            } else if (listNotIs.size() > 1) {
                auxBuilder.and(key).notIn(listNotIs);
            }

        }

        if (op == VariantQueryUtils.QueryOperation.OR) {
            builder.or(auxBuilder.get());
        }
        return builder;
    }

    /**
     * Accept a list of comparative filters separated with "," or ";" with the expression:
     * {OPERATION}{VALUE}, where the accepted operations are: <, <=, >, >=, =, ==, !=, ~=.
     *
     * @param key
     * @param value
     * @param builder
     * @param extendKey
     * @return
     */
    private QueryBuilder addCompListQueryFilter(String key, String value, QueryBuilder builder, boolean extendKey) {
        VariantQueryUtils.QueryOperation op = checkOperator(value);
        List<String> values = splitValue(value, op);

        QueryBuilder compBuilder;
        if (op == QueryOperation.OR) {
            compBuilder = QueryBuilder.start();
        } else {
            compBuilder = builder;
        }

        for (String elem : values) {
            addCompQueryFilter(key, elem, compBuilder, extendKey);
        }

        if (op == QueryOperation.OR) {
            builder.or(compBuilder.get());
        }
        return builder;
    }

    private QueryBuilder addCompQueryFilter(String key, String value, QueryBuilder builder, boolean extendKey) {
        String[] strings = splitOperator(value);
        String op = "";
        if (strings.length == 3) {
            if (extendKey && !strings[0].isEmpty()) {
                key = key + "." + strings[0];
            }
            value = strings[2];
            op = strings[1];
        }
        return addCompQueryFilter(key, value, builder, op);
    }

    private QueryBuilder addCompQueryFilter(String key, String obj, QueryBuilder builder, String op) {

        switch (op) {
            case "<":
                builder.and(key).lessThan(Double.parseDouble(obj));
                break;
            case "<=":
                builder.and(key).lessThanEquals(Double.parseDouble(obj));
                break;
            case ">":
                builder.and(key).greaterThan(Double.parseDouble(obj));
                break;
            case ">=":
                builder.and(key).greaterThanEquals(Double.parseDouble(obj));
                break;
            case "=":
            case "==":
                try {
                    builder.and(key).is(Double.parseDouble(obj));
                } catch (NumberFormatException e) {
                    builder.and(key).is(obj);
                }
                break;
            case "!=":
                builder.and(key).notEquals(Double.parseDouble(obj));
                break;
            case "~=":
            case "~":
                builder.and(key).regex(Pattern.compile(obj));
                break;
            default:
                break;
        }
        return builder;
    }

    private QueryBuilder addStringCompQueryFilter(String key, String value, QueryBuilder builder) {
        String[] split = splitOperator(value);
        String op = split[1];
        String obj = split[2];

        switch (op) {
            case "!=":
            case "!":
                builder.and(key).notEquals(obj);
                break;
            case "~=":
            case "~":
                builder.and(key).regex(Pattern.compile(obj));
                break;
            case "":
            case "=":
            case "==":
            default:
                builder.and(key).is(obj);
                break;
        }
        return builder;
    }

    /**
     * Accepts a list of filters separated with "," or ";" with the expression: {SCORE}{OPERATION}{VALUE}.
     *
     * @param value        Value to parse
     * @param builder      QueryBuilder
     * @param scoreParam Score query param
     * @param allowDescriptionFilter Use string values as filters for the score description
     * @return QueryBuilder
     */
    private QueryBuilder addScoreFilter(String value, QueryBuilder builder, VariantQueryParam scoreParam,
                                        boolean allowDescriptionFilter) {
        return addScoreFilter(value, builder, scoreParam, null, allowDescriptionFilter);
    }

    /**
     * Accepts a list of filters separated with "," or ";" with the expression: {SOURCE}{OPERATION}{VALUE}.
     *
     * @param value         Value to parse
     * @param builder       QueryBuilder
     * @param scoreParam    Score VariantQueryParam
     * @param defaultSource Default source value. If null, must be present in the filter. If not, must not be present.
     * @param allowDescriptionFilter Use string values as filters for the score description
     * @return QueryBuilder
     */
    private QueryBuilder addScoreFilter(String value, QueryBuilder builder, VariantQueryParam scoreParam, final String defaultSource,
                                        boolean allowDescriptionFilter) {
        final List<String> list;
        QueryOperation operation = checkOperator(value);
        list = splitValue(value, operation);
        List<DBObject> dbObjects = new ArrayList<>();
        for (String elem : list) {
            String[] score = VariantQueryUtils.splitOperator(elem);
            String source;
            String op;
            String scoreValue;
            // No given score
            if (StringUtils.isEmpty(score[0])) {
                if (defaultSource == null) {
                    logger.error("Bad score filter: " + elem);
                    throw VariantQueryException.malformedParam(scoreParam, value);
                }
                source = defaultSource;
                op = score[1];
                scoreValue = score[2];
            } else {
                if (defaultSource != null) {
                    logger.error("Bad score filter: " + elem);
                    throw VariantQueryException.malformedParam(scoreParam, value);
                }
                source = score[0];
                op = score[1];
                scoreValue = score[2];
            }

            String key = DocumentToVariantAnnotationConverter.SCORE_FIELD_MAP.get(source);
            if (key == null) {
                // Unknown score
                throw VariantQueryException.malformedParam(scoreParam, value);
            }

            QueryBuilder scoreBuilder = new QueryBuilder();
            if (NumberUtils.isParsable(scoreValue)) {
                // Query by score
                key += '.' + DocumentToVariantAnnotationConverter.SCORE_SCORE_FIELD;
                addCompQueryFilter(key, scoreValue, scoreBuilder, op);
            } else if (allowDescriptionFilter) {
                // Query by description
                key += '.' + DocumentToVariantAnnotationConverter.SCORE_DESCRIPTION_FIELD;
                addStringCompQueryFilter(key, scoreValue, scoreBuilder);
            } else {
                throw VariantQueryException.malformedParam(scoreParam, value);
            }
            dbObjects.add(scoreBuilder.get());
        }

        if (!dbObjects.isEmpty()) {
            if (operation == null || operation == QueryOperation.AND) {
                builder.and(dbObjects.toArray(new DBObject[dbObjects.size()]));
            } else {
                builder.and(new BasicDBObject("$or", dbObjects));
            }
        }
        return builder;
    }

    /**
     * Accepts a list of filters separated with "," or ";" with the expression:
     * {STUDY}:{POPULATION}{OPERATION}{VALUE}.
     *
     * @param key                  PopulationFrequency schema field
     * @param alleleFrequencyField Allele frequency schema field
     * @param value                Value to parse
     * @param builder              QueryBuilder
     * @param queryParam           QueryParam filter
     * @return QueryBuilder
     */
    private QueryBuilder addFrequencyFilter(String key, String alleleFrequencyField, String value, QueryBuilder builder,
                                            VariantQueryParam queryParam) {
        return addFrequencyFilter(key, value, builder, queryParam, (v, qb) -> addCompQueryFilter(alleleFrequencyField, v, qb, false));
    }

    /**
     * Accepts a list of filters separated with "," or ";" with the expression:
     * {STUDY}:{POPULATION}{OPERATION}{VALUE}.
     *
     * @param key       PopulationFrequency schema field
     * @param value     Value to parse
     * @param builder   QueryBuilder
     * @param addFilter For complex filter
     * @return QueryBuilder
     */
    private QueryBuilder addFrequencyFilter(String key, String value, QueryBuilder builder, VariantQueryParam queryParam,
                                            BiConsumer<String, QueryBuilder> addFilter) {
        final List<String> list;
        QueryOperation operation = checkOperator(value);
        list = splitValue(value, operation);

        List<BasicDBObject> dbObjects = new ArrayList<>();
        for (String elem : list) {
            String[] split = elem.split(IS);
            if (split.length != 2) {
                logger.error("Bad population frequency filter: " + elem);
                throw VariantQueryException.malformedParam(queryParam, value);
                //new IllegalArgumentException("Bad population frequency filter: " + elem);
            }
            String study = split[0];
            String population = split[1];
            String[] populationFrequency = splitOperator(population);
            logger.debug("populationFrequency = " + Arrays.toString(populationFrequency));

            QueryBuilder frequencyBuilder = new QueryBuilder();
            frequencyBuilder.and(DocumentToVariantAnnotationConverter.POPULATION_FREQUENCY_STUDY_FIELD).is(study);
            frequencyBuilder.and(DocumentToVariantAnnotationConverter.POPULATION_FREQUENCY_POP_FIELD).is(populationFrequency[0]);
            Document studyPopFilter = new Document(frequencyBuilder.get().toMap());
            addFilter.accept(populationFrequency[1] + populationFrequency[2], frequencyBuilder);
            BasicDBObject elemMatch = new BasicDBObject(key, new BasicDBObject("$elemMatch", frequencyBuilder.get()));
            if (populationFrequency[1].startsWith("<")) {
                BasicDBObject orNotExistsAnyPopulation = new BasicDBObject(key, new BasicDBObject("$exists", false));
                BasicDBObject orNotExistsPopulation =
                        new BasicDBObject(key, new BasicDBObject("$not", new BasicDBObject("$elemMatch", studyPopFilter)));
                dbObjects.add(new BasicDBObject("$or", Arrays.asList(orNotExistsAnyPopulation, orNotExistsPopulation, elemMatch)));
            } else {
                dbObjects.add(elemMatch);
            }
        }
        if (!dbObjects.isEmpty()) {
            if (operation == null || operation == QueryOperation.AND) {
                builder.and(dbObjects.toArray(new BasicDBObject[dbObjects.size()]));
            } else {
                builder.and(new BasicDBObject("$or", dbObjects));
            }
        }
        return builder;
    }

    /**
     * Accept filters separated with "," or ";" with the expression:
     * [{STUDY}:]{COHORT}{OPERATION}{VALUE}.
     * Where STUDY is optional if defaultStudyConfiguration is provided
     *
     * @param key                       Stats field to filter
     * @param values                    Values to parse
     * @param builder                   QueryBuilder
     * @param defaultStudyConfiguration
     */
    private void addStatsFilterList(String key, String values, QueryBuilder builder, StudyConfiguration defaultStudyConfiguration) {
        QueryOperation op = checkOperator(values);
        List<String> valuesList = splitValue(values, op);
        List<DBObject> statsQueries = new LinkedList<>();
        for (String value : valuesList) {
            statsQueries.add(addStatsFilter(key, value, new QueryBuilder(), defaultStudyConfiguration).get());
        }

        if (!statsQueries.isEmpty()) {
            if (op == QueryOperation.OR) {
                builder.or(statsQueries.toArray(new DBObject[statsQueries.size()]));
            } else {
                builder.and(statsQueries.toArray(new DBObject[statsQueries.size()]));
            }
        }
    }

    /**
     * Accepts filters with the expresion: [{STUDY}:]{COHORT}{OPERATION}{VALUE}.
     * Where STUDY is optional if defaultStudyConfiguration is provided
     *
     * @param key                       Stats field to filter
     * @param filter                    Filter to parse
     * @param builder                   QueryBuilder
     * @param defaultStudyConfiguration
     */
    private QueryBuilder addStatsFilter(String key, String filter, QueryBuilder builder, StudyConfiguration defaultStudyConfiguration) {
        if (filter.contains(":") || defaultStudyConfiguration != null) {
            Integer studyId;
            Integer cohortId;
            String operator;
            String valueStr;
            if (filter.contains(":")) {
                String[] studyValue = filter.split(":");
                String[] cohortOpValue = VariantQueryUtils.splitOperator(studyValue[1]);
                String study = studyValue[0];
                String cohort = cohortOpValue[0];
                operator = cohortOpValue[1];
                valueStr = cohortOpValue[2];

                StudyConfiguration studyConfiguration = studyConfigurationManager.getStudyConfiguration(study, defaultStudyConfiguration);
                cohortId = studyConfigurationManager.getCohortId(cohort, studyConfiguration);
                studyId = studyConfiguration.getStudyId();
            } else {
//                String study = defaultStudyConfiguration.getStudyName();
                studyId = defaultStudyConfiguration.getStudyId();
                String[] cohortOpValue = VariantQueryUtils.splitOperator(filter);
                String cohort = cohortOpValue[0];
                cohortId = studyConfigurationManager.getCohortId(cohort, defaultStudyConfiguration);
                operator = cohortOpValue[1];
                valueStr = cohortOpValue[2];
            }

            QueryBuilder statsBuilder = new QueryBuilder();
            statsBuilder.and(DocumentToVariantStatsConverter.STUDY_ID).is(studyId);
            if (cohortId != null) {
                statsBuilder.and(DocumentToVariantStatsConverter.COHORT_ID).is(cohortId);
            }
            addCompQueryFilter(key, valueStr, statsBuilder, operator);
            builder.and(DocumentToVariantConverter.STATS_FIELD).elemMatch(statsBuilder.get());
        } else {
            addCompQueryFilter(DocumentToVariantConverter.STATS_FIELD + "." + key, filter, builder, false);
        }
        return builder;
    }

    private QueryBuilder getRegionFilter(Region region, QueryBuilder builder) {
        List<String> chunkIds = getChunkIds(region);
        builder.and(DocumentToVariantConverter.AT_FIELD + '.' + DocumentToVariantConverter.CHUNK_IDS_FIELD).in(chunkIds);
        builder.and(DocumentToVariantConverter.END_FIELD).greaterThanEquals(region.getStart());
        builder.and(DocumentToVariantConverter.START_FIELD).lessThanEquals(region.getEnd());
        return builder;
    }

    private QueryBuilder getRegionFilter(List<Region> regions, QueryBuilder builder) {
        if (regions != null && !regions.isEmpty()) {
            DBObject[] objects = new DBObject[regions.size()];
            int i = 0;
            for (Region region : regions) {
                DBObject regionObject = new BasicDBObject();
//                if (region.getEnd() - region.getStart() < 1000000) {
//                    List<String> chunkIds = getChunkIds(region);
//                    regionObject.put(DocumentToVariantConverter.AT_FIELD + '.' + DocumentToVariantConverter
//                            .CHUNK_IDS_FIELD,
//                            new Document("$in", chunkIds));
//                } else {
//                    regionObject.put(DocumentToVariantConverter.CHROMOSOME_FIELD, region.getChromosome());
//                }

                int end = region.getEnd();
                if (end < Integer.MAX_VALUE) { // Avoid overflow
                    end++;
                }
                regionObject.put("_id", new Document()
                        .append("$gte", VariantStringIdConverter.buildId(region.getChromosome(), region.getStart()))
                        .append("$lt", VariantStringIdConverter.buildId(region.getChromosome(), end)));

                objects[i] = regionObject;
                i++;
            }
            builder.or(objects);
        }
        return builder;
    }

    /* *******************
     * Auxiliary methods *
     * *******************/
    private List<String> getChunkIds(Region region) {
        List<String> chunkIds = new LinkedList<>();

        int chunkSize = (region.getEnd() - region.getStart() > VariantMongoDBAdaptor.CHUNK_SIZE_BIG)
                ? VariantMongoDBAdaptor.CHUNK_SIZE_BIG
                : VariantMongoDBAdaptor.CHUNK_SIZE_SMALL;
        int ks = chunkSize / 1000;
        int chunkStart = region.getStart() / chunkSize;
        int chunkEnd = region.getEnd() / chunkSize;

        for (int i = chunkStart; i <= chunkEnd; i++) {
            String chunkId = region.getChromosome() + "_" + i + "_" + ks + "k";
            chunkIds.add(chunkId);
        }
        return chunkIds;
    }

    protected int getChunkId(int position, int chunksize) {
        return position / chunksize;
    }

    protected int getChunkStart(int id, int chunksize) {
        return (id == 0) ? 1 : id * chunksize;
    }

    protected int getChunkEnd(int id, int chunksize) {
        return (id * chunksize) + chunksize - 1;
    }

}
