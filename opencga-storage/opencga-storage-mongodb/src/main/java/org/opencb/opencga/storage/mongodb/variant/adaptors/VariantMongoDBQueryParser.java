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

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;
import htsjdk.variant.vcf.VCFConstants;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.bson.Document;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.ClinicalSignificance;
import org.opencb.cellbase.core.variant.annotation.VariantAnnotationUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.FileMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.*;
import org.opencb.opencga.storage.core.variant.query.VariantQueryParser;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageEngine;
import org.opencb.opencga.storage.mongodb.variant.converters.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.VariantStorageEngine.Options.LOADED_GENOTYPES;
import static org.opencb.opencga.storage.core.variant.VariantStorageEngine.Options.SEARCH_INDEX_LAST_TIMESTAMP;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.*;
import static org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageEngine.MongoDBVariantOptions.DEFAULT_GENOTYPE;
import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToVariantConverter.INDEX_FIELD;

/**
 * Created on 31/03/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantMongoDBQueryParser {

    public static final String OVERLAPPED_FILES_ONLY = "overlappedFilesOnly";
    public static final VariantStringIdConverter STRING_ID_CONVERTER = new VariantStringIdConverter();
    protected static Logger logger = LoggerFactory.getLogger(VariantMongoDBQueryParser.class);
    private final VariantStorageMetadataManager metadataManager;
    //    private final CellBaseUtils cellBaseUtils;

    public VariantMongoDBQueryParser(VariantStorageMetadataManager variantStorageMetadataManager) {
        this.metadataManager = variantStorageMetadataManager;

    }

    protected Document parseQuery(final Query originalQuery) {
        QueryBuilder builder = new QueryBuilder();
        if (originalQuery != null) {
            // Copy given query. It may be modified
            Query query = new Query(originalQuery);
            boolean nonGeneRegionFilter = false;
            /* VARIANT PARAMS */

            if (isValidParam(query, REGION)) {
                nonGeneRegionFilter = true;
                List<Region> regions = Region.parseRegions(query.getString(REGION.key()), true);
                if (!regions.isEmpty()) {
                    getRegionFilter(regions, builder);
                }
            }

            // Object with all VariantIds, ids, genes and xrefs from ID, XREF, GENES, ... filters
            VariantQueryParser.VariantQueryXref variantQueryXref = VariantQueryParser.parseXrefs(query);

            if (!variantQueryXref.getIds().isEmpty()) {
                addQueryStringFilter(DocumentToVariantConverter.ANNOTATION_FIELD
                        + '.' + DocumentToVariantAnnotationConverter.XREFS_FIELD
                        + '.' + DocumentToVariantAnnotationConverter.XREF_ID_FIELD,
                        variantQueryXref.getIds(), builder, QueryOperation.OR);
                addQueryStringFilter(DocumentToVariantConverter.IDS_FIELD, variantQueryXref.getIds(), builder, QueryOperation.OR);
            }

            if (!variantQueryXref.getOtherXrefs().isEmpty()) {
                nonGeneRegionFilter = true;
                addQueryStringFilter(DocumentToVariantConverter.ANNOTATION_FIELD
                                + '.' + DocumentToVariantAnnotationConverter.XREFS_FIELD
                                + '.' + DocumentToVariantAnnotationConverter.XREF_ID_FIELD,
                        variantQueryXref.getOtherXrefs(), builder, QueryOperation.OR);
            }

            if (!variantQueryXref.getVariants().isEmpty()) {
                nonGeneRegionFilter = true;
                List<String> mongoIds = new ArrayList<>(variantQueryXref.getVariants().size());
                for (Variant variant : variantQueryXref.getVariants()) {
                    mongoIds.add(STRING_ID_CONVERTER.buildId(variant));
                }
                if (mongoIds.size() == 1) {
                    builder.or(new QueryBuilder().and("_id").is(mongoIds.get(0)).get());
                } else {
                    builder.or(new QueryBuilder().and("_id").in(mongoIds).get());
                }
            }

            if (!variantQueryXref.getGenes().isEmpty()) {
                if (isValidParam(query, ANNOT_CONSEQUENCE_TYPE)) {
                    List<String> soList = query.getAsStringList(ANNOT_CONSEQUENCE_TYPE.key());
                    Set<String> gnSo = new HashSet<>(variantQueryXref.getGenes().size() * soList.size());
                    for (String gene : variantQueryXref.getGenes()) {
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
                            variantQueryXref.getGenes(), builder, QueryOperation.OR);
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
                addQueryFilter(DocumentToVariantConverter.TYPE_FIELD, query.getAsStringList(TYPE.key()), builder,
                        QueryOperation.AND); //addQueryStringFilter(DBObjectToVariantConverter.TYPE_FIELD,
//                query.getString(VariantQueryParams.TYPE.key()), builder, QueryOperation.AND);
            }

            if (isValidParam(query, RELEASE)) {
                int release = query.getInt(RELEASE.key(), -1);
                if (release <= 0) {
                    throw VariantQueryException.malformedParam(RELEASE, query.getString(RELEASE.key()));
                }

                builder.and(DocumentToVariantConverter.RELEASE_FIELD).lessThanEquals(release);
            }

            /* ANNOTATION PARAMS */
            QueryBuilder annotationQueryBuilder = new QueryBuilder();
            parseAnnotationQueryParams(query, annotationQueryBuilder);
            DBObject annotationQuery = annotationQueryBuilder.get();
            if (!annotationQuery.keySet().isEmpty()) {
                builder.and(annotationQuery);
            }

            /* STUDIES */
            QueryBuilder studyQueryBuilder = new QueryBuilder();
            final StudyMetadata defaultStudy = parseStudyQueryParams(query, studyQueryBuilder);

            /* STATS PARAMS */
            parseStatsQueryParams(query, studyQueryBuilder, defaultStudy);
            if (builder.get().keySet().isEmpty()) {
                builder = studyQueryBuilder;
            } else {
                builder.and(studyQueryBuilder.get());
            }

        }

        logger.debug("----------------------");
        logger.debug("Query         = {}", VariantQueryUtils.printQuery(originalQuery));
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

            if (isValidParam(query, ANNOT_TRANSCRIPT_FLAG)) {
                String value = query.getString(ANNOT_TRANSCRIPT_FLAG.key());
                addQueryStringFilter(DocumentToVariantConverter.ANNOTATION_FIELD
                        + "." + DocumentToVariantAnnotationConverter.CONSEQUENCE_TYPE_FIELD
                        + "." + DocumentToVariantAnnotationConverter.CT_TRANSCRIPT_ANNOT_FLAGS, value, builder, QueryOperation.AND);
            }

            if (isValidParam(query, ANNOT_GENE_TRAIT_ID)) {
                String value = query.getString(ANNOT_GENE_TRAIT_ID.key());
                QueryOperation internalOp = checkOperator(value);
                List<String> values = splitValue(value, internalOp);
                QueryBuilder geneTraitBuilder;
                if (internalOp == QueryOperation.OR) {
                    geneTraitBuilder = QueryBuilder.start();
                } else {
                    geneTraitBuilder = builder;
                }


                List<String> geneTraitId = new LinkedList<>();
                List<String> hpo = new LinkedList<>();
                for (String v : values) {
                    if (isHpo(v)) {
                        hpo.add(v);
                    } else {
                        geneTraitId.add(v);
                    }
                }

                if (!geneTraitId.isEmpty()) {
                    addQueryFilter(DocumentToVariantConverter.ANNOTATION_FIELD
                                    + '.' + DocumentToVariantAnnotationConverter.GENE_TRAIT_FIELD
                                    + '.' + DocumentToVariantAnnotationConverter.GENE_TRAIT_ID_FIELD, geneTraitId, geneTraitBuilder,
                            internalOp, internalOp, Object::toString);
                }

                if (!hpo.isEmpty()) {
                    addQueryFilter(DocumentToVariantConverter.ANNOTATION_FIELD
                                    + '.' + DocumentToVariantAnnotationConverter.XREFS_FIELD
                                    + '.' + DocumentToVariantAnnotationConverter.XREF_ID_FIELD, hpo, geneTraitBuilder,
                            internalOp, internalOp, Object::toString);
                }

                if (internalOp == QueryOperation.OR) {
                    builder.and(geneTraitBuilder.get());
                }
            }

            if (isValidParam(query, ANNOT_GENE_TRAIT_NAME)) {
                String value = query.getString(ANNOT_GENE_TRAIT_NAME.key());
//                addCompQueryFilter(DocumentToVariantConverter.ANNOTATION_FIELD
//                        + '.' + DocumentToVariantAnnotationConverter.GENE_TRAIT_FIELD
//                        + '.' + DocumentToVariantAnnotationConverter.GENE_TRAIT_NAME_FIELD, value, builder, false);
                addQueryStringFilter(DocumentToVariantConverter.ANNOTATION_FIELD
                        + '.' + DocumentToVariantAnnotationConverter.GENE_TRAIT_FIELD
                        + '.' + DocumentToVariantAnnotationConverter.GENE_TRAIT_NAME_FIELD, value, builder, QueryOperation.AND);
            }

            if (isValidParam(query, ANNOT_CLINICAL_SIGNIFICANCE)) {
                String value = query.getString(ANNOT_CLINICAL_SIGNIFICANCE.key());
                String key = DocumentToVariantConverter.ANNOTATION_FIELD
                        + '.' + DocumentToVariantAnnotationConverter.CLINICAL_DATA_FIELD
                        + '.' + DocumentToVariantAnnotationConverter.CLINICAL_CLINVAR_FIELD
                        + '.' + "clinicalSignificance";
                Pair<QueryOperation, List<String>> pair = splitValue(value);
                List<DBObject> list = new ArrayList<>(pair.getValue().size());
                for (String clinicalSignificance : pair.getValue()) {
                    ClinicalSignificance enumValue = EnumUtils.getEnum(ClinicalSignificance.class, clinicalSignificance);
                    if (enumValue != null) {
                        for (Map.Entry<String, ClinicalSignificance> entry : VariantAnnotationUtils.CLINVAR_CLINSIG_TO_ACMG.entrySet()) {
                            if (entry.getValue() == enumValue) {
                                clinicalSignificance = entry.getKey();
                                break;
                            }
                        }
                    }
                    list.add(new QueryBuilder().and(key).regex(Pattern.compile("^" + clinicalSignificance, Pattern.CASE_INSENSITIVE)).get());
                }
                if (QueryOperation.OR.equals(pair.getKey())) {
                    builder.or(list.toArray(new DBObject[0]));
                } else {
                    builder.and(list.toArray(new DBObject[0]));
                }

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

            if (isValidParam(query, ANNOT_GO_GENES)) {
                String value = query.getString(ANNOT_GO_GENES.key());

                // Check if comma separated of semi colon separated (AND or OR)
                QueryOperation queryOperation = checkOperator(value);
                // Split by comma or semi colon
                List<String> goGenes = splitValue(value, queryOperation);

                builder.and(DocumentToVariantConverter.ANNOTATION_FIELD
                        + '.' + DocumentToVariantAnnotationConverter.XREFS_FIELD
                        + '.' + DocumentToVariantAnnotationConverter.XREF_ID_FIELD).in(goGenes);

            }

            if (isValidParam(query, ANNOT_EXPRESSION_GENES)) {
                String value = query.getString(ANNOT_EXPRESSION_GENES.key());

                // Check if comma separated of semi colon separated (AND or OR)
                QueryOperation queryOperation = checkOperator(value);
                // Split by comma or semi colon
                List<String> expressionGenes = splitValue(value, queryOperation);

                builder.and(DocumentToVariantConverter.ANNOTATION_FIELD
                        + '.' + DocumentToVariantAnnotationConverter.XREFS_FIELD
                        + '.' + DocumentToVariantAnnotationConverter.XREF_ID_FIELD).in(expressionGenes);

            }


            if (isValidParam(query, ANNOT_PROTEIN_KEYWORD)) {
                String value = query.getString(ANNOT_PROTEIN_KEYWORD.key());
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

            if (isValidParam(query, CUSTOM_ANNOTATION)) {
                String value = query.getString(CUSTOM_ANNOTATION.key());
                addCompListQueryFilter(DocumentToVariantConverter.CUSTOM_ANNOTATION_FIELD, value, builder, true);
            }

            if (isValidParam(query, ANNOT_POPULATION_ALTERNATE_FREQUENCY)) {
                String value = query.getString(ANNOT_POPULATION_ALTERNATE_FREQUENCY.key());
                addFrequencyFilter(DocumentToVariantConverter.ANNOTATION_FIELD
                                + "." + DocumentToVariantAnnotationConverter.POPULATION_FREQUENCIES_FIELD,
                        DocumentToVariantAnnotationConverter.POPULATION_FREQUENCY_ALTERNATE_FREQUENCY_FIELD, value, builder,
                        ANNOT_POPULATION_ALTERNATE_FREQUENCY, true); // Same
                // method addFrequencyFilter is used for reference and allele frequencies. Need to provide the field
                // (reference/alternate) where to check the frequency
            }

            if (isValidParam(query, ANNOT_POPULATION_REFERENCE_FREQUENCY)) {
                String value = query.getString(ANNOT_POPULATION_REFERENCE_FREQUENCY.key());
                addFrequencyFilter(DocumentToVariantConverter.ANNOTATION_FIELD
                                + "." + DocumentToVariantAnnotationConverter.POPULATION_FREQUENCIES_FIELD,
                        DocumentToVariantAnnotationConverter.POPULATION_FREQUENCY_REFERENCE_FREQUENCY_FIELD, value, builder,
                        ANNOT_POPULATION_REFERENCE_FREQUENCY, false); // Same
                // method addFrequencyFilter is used for reference and allele frequencies. Need to provide the field
                // (reference/alternate) where to check the frequency
            }

            if (isValidParam(query, ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY)) {
                String value = query.getString(ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY.key());
                addFrequencyFilter(DocumentToVariantConverter.ANNOTATION_FIELD + "."
                                + DocumentToVariantAnnotationConverter.POPULATION_FREQUENCIES_FIELD,
                        value, builder, ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY, true,
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

            if (isValidParam(query, VARIANTS_TO_INDEX)) {
                long ts = metadataManager.getProjectMetadata().getAttributes()
                        .getLong(SEARCH_INDEX_LAST_TIMESTAMP.key());
                if (ts > 0) {
                    String key = INDEX_FIELD + '.' + DocumentToVariantConverter.INDEX_TIMESTAMP_FIELD;
                    builder.or(
                            QueryBuilder.start(key).greaterThan(ts).get(),
                            QueryBuilder.start(key).exists(false).get()); // It may not exist from versions <1.4.x
                } // Otherwise, get all variants
            }
        }
    }

    private StudyMetadata parseStudyQueryParams(Query query, QueryBuilder builder) {

        if (query != null) {
            Map<String, Integer> studies = metadataManager.getStudies(null);

            String studyQueryPrefix = DocumentToVariantConverter.STUDIES_FIELD + '.';
            final StudyMetadata defaultStudy = getDefaultStudy(query, null, metadataManager);

            if (isValidParam(query, STUDY)) {
                String value = query.getString(STUDY.key());

                addQueryFilter(studyQueryPrefix + DocumentToStudyVariantEntryConverter.STUDYID_FIELD, value,
                        builder, QueryOperation.AND, study -> metadataManager.getStudyId(study, false, studies));
            }

            boolean overlappedFilesFiles = query.getBoolean(OVERLAPPED_FILES_ONLY);
            List<Integer> fileIds = Collections.emptyList();
            List<String> fileNames = Collections.emptyList();
            QueryOperation filesOperation = QueryOperation.OR;
            if (isValidParam(query, FILE)) {
                String filesValue = query.getString(FILE.key());
                filesOperation = checkOperator(filesValue);
                fileNames = splitValue(filesValue, filesOperation);

                fileIds = fileNames
                        .stream()
                        .filter(value -> !isNegated(value))
                        .map(value -> {
                            Integer fileId = metadataManager.getFileIdPair(value, false, defaultStudy).getValue();
                            if (fileId == null) {
                                throw VariantQueryException.fileNotFound(value, defaultStudy.getName());
                            }
                            if (overlappedFilesFiles) {
                                fileId = -fileId;
                            }
                            return fileId;
                        })
                        .collect(Collectors.toList());
            } else if (isValidParam(query, INCLUDE_FILE)) {
                List<String> files = getIncludeFilesList(query);
                if (files != null) {
                    fileIds = new ArrayList<>(files.size());
                    for (String file : files) {
                        fileIds.add(metadataManager.getFileIdPair(file, false, defaultStudy).getValue());
                    }
                }
            }

            if (isValidParam(query, FILTER) || isValidParam(query, QUAL) || isValidParam(query, INFO)) {
                String filterValue = query.getString(FILTER.key());
                QueryOperation filterOperation = checkOperator(filterValue);
                List<String> filterValues = splitValue(filterValue, filterOperation);
                Pair<QueryOperation, Map<String, String>> infoParamPair = parseInfo(query);
                QueryOperation infoOperator = infoParamPair.getKey();
                Map<String, String> infoMap = infoParamPair.getValue();


                boolean useFileElemMatch = !fileIds.isEmpty();
                boolean infoInFileElemMatch = useFileElemMatch && (infoOperator == null || filesOperation == infoOperator);

//                values = query.getString(QUAL.key());
//                QueryOperation qualOperation = checkOperator(values);
//                List<String> qualValues = splitValue(values, qualOperation);
                if (!useFileElemMatch) {
                    String key = studyQueryPrefix
                            + DocumentToStudyVariantEntryConverter.FILES_FIELD + '.'
                            + DocumentToStudyVariantEntryConverter.ATTRIBUTES_FIELD + '.';

                    if (isValidParam(query, FILTER)) {
                        DBObject[] regexList = getFileFilterDBObjects(key + StudyEntry.FILTER, filterValues);
                        if (filterOperation == QueryOperation.OR) {
                            builder.or(regexList);
                        } else {
                            builder.and(regexList);
                        }
                    }
                    if (isValidParam(query, QUAL)) {
                        addCompListQueryFilter(key + StudyEntry.QUAL, query.getString(QUAL.key()), builder, false);
                    }
                } else {
                    DBObject[] fileElemMatch = new DBObject[fileIds.size()];
                    String key = DocumentToStudyVariantEntryConverter.ATTRIBUTES_FIELD + '.';
                    DBObject[] regexList = getFileFilterDBObjects(key + StudyEntry.FILTER, filterValues);

                    int i = 0;
                    for (Integer fileId : fileIds) {
                        QueryBuilder fileBuilder = QueryBuilder.start();

                        fileBuilder.and(DocumentToStudyVariantEntryConverter.FILEID_FIELD).is(fileId);
                        if (isValidParam(query, FILTER)) {
                            if (filterOperation == QueryOperation.OR) {
                                fileBuilder.or(regexList);
                            } else {
                                fileBuilder.and(regexList);
                            }
                        }
                        if (isValidParam(query, QUAL)) {
                            addCompListQueryFilter(key + StudyEntry.QUAL, query.getString(QUAL.key()), fileBuilder, false);
                        }

                        if (infoInFileElemMatch && !infoMap.isEmpty()) {
                            if (defaultStudy == null) {
                                throw VariantQueryException.missingStudyForFile(fileId.toString(),
                                        metadataManager.getStudyNames());
                            }
                            String fileName = metadataManager.getFileName(defaultStudy.getId(), fileId);
                            String infoValue = infoMap.get(fileName);
                            if (infoValue != null) {
                                addCompListQueryFilter(DocumentToStudyVariantEntryConverter.ATTRIBUTES_FIELD, infoValue, fileBuilder, true);
                            }
                        }

                        fileElemMatch[i++] = new BasicDBObject(studyQueryPrefix + DocumentToStudyVariantEntryConverter.FILES_FIELD,
                                new BasicDBObject("$elemMatch", fileBuilder.get()));
                    }
                    if (filesOperation == QueryOperation.OR) {
                        builder.or(fileElemMatch);
                    } else {
                        builder.and(fileElemMatch);
                    }

                }

                if (!infoInFileElemMatch && !infoMap.isEmpty()) {
                    DBObject[] infoElemMatch = new DBObject[infoMap.size()];
                    int i = 0;
                    for (Map.Entry<String, String> entry : infoMap.entrySet()) {
                        if (defaultStudy == null) {
                            throw VariantQueryException.missingStudyForFile(entry.getKey(), metadataManager.getStudyNames());
                        }
                        QueryBuilder infoBuilder = new QueryBuilder();
                        Integer fileId = metadataManager.getFileId(defaultStudy.getId(), entry.getKey(), true);
                        infoBuilder.and(DocumentToStudyVariantEntryConverter.FILEID_FIELD).is(fileId);
                        String infoValue = entry.getValue();
                        if (infoValue != null) {
                            addCompListQueryFilter(DocumentToStudyVariantEntryConverter.ATTRIBUTES_FIELD, infoValue, infoBuilder, true);
                        }
                        infoElemMatch[i++] = new BasicDBObject(studyQueryPrefix + DocumentToStudyVariantEntryConverter.FILES_FIELD,
                                new BasicDBObject("$elemMatch", infoBuilder.get()));
                    }
                    if (infoOperator == QueryOperation.OR) {
                        builder.or(infoElemMatch);
                    } else {
                        builder.and(infoElemMatch);
                    }
                }
            }

            if (isValidParam(query, FORMAT)) {
                throw VariantQueryException.unsupportedVariantQueryFilter(FORMAT, MongoDBVariantStorageEngine.STORAGE_ENGINE_ID);
            }

            Map<Object, List<String>> genotypesFilter = new HashMap<>();
            QueryOperation gtQueryOperation = null;
            if (isValidParam(query, GENOTYPE)) {
                String sampleGenotypes = query.getString(GENOTYPE.key());
                gtQueryOperation = parseGenotypeFilter(sampleGenotypes, genotypesFilter);
            } else if (isValidParam(query, SAMPLE)) {
                String samples = query.getString(SAMPLE.key());

                List<String> genotypes;
                if (defaultStudy != null) {
                    genotypes = defaultStudy.getAttributes().getAsStringList(LOADED_GENOTYPES.key()).stream()
                            .filter(gt -> DocumentToSamplesConverter.genotypeToDataModelType(gt).contains("1"))
                            .collect(Collectors.toList());
                } else {
                    genotypes = Arrays.asList(
                            "1",
                            "0/1", "0|1", "1|0",
                            "1/1", "1|1",
                            "1/2", "1|2", "2|1"
                    );
                }
                QueryOperation sampleQueryOperation = VariantQueryUtils.checkOperator(samples);
                if (gtQueryOperation != null && sampleQueryOperation != null && !gtQueryOperation.equals(sampleQueryOperation)) {
                    throw VariantQueryException.incompatibleSampleAndGenotypeOperators();
                }
                if (gtQueryOperation == null) {
                    gtQueryOperation = sampleQueryOperation;
                }

                for (String sample : VariantQueryUtils.splitValue(samples, gtQueryOperation)) {
                    if (isNegated(sample)) {
                        throw VariantQueryException.malformedParam(SAMPLE, samples, "Unsupported negated samples");
                    }
                    int sampleId = metadataManager.getSampleId(defaultStudy.getId(), sample, true);
                    genotypesFilter.put(sampleId, genotypes);
                }
            }

            // Only will contain values if the genotypesOperator is AND
            Set<List<Integer>> fileIdGroupsFromSamples = Collections.emptySet();
            Set<Integer> fileIdsFromSamples = Collections.emptySet();
            if (!genotypesFilter.isEmpty()) {
                fileIdGroupsFromSamples = new HashSet<>();
                fileIdsFromSamples = new HashSet<>();

                List<String> defaultGenotypes;
                List<String> loadedGenotypes;
                if (defaultStudy != null) {
                    defaultGenotypes = defaultStudy.getAttributes().getAsStringList(DEFAULT_GENOTYPE.key());
                    loadedGenotypes = defaultStudy.getAttributes().getAsStringList(LOADED_GENOTYPES.key());
                    loadedGenotypes.replaceAll(DocumentToSamplesConverter::genotypeToDataModelType);
                } else {
                    defaultGenotypes = DEFAULT_GENOTYPE.defaultValue();
                    loadedGenotypes = Arrays.asList(
                            "0/0", "0|0",
                            "0/1", "1/0", "1/1", "./.",
                            "0|1", "1|0", "1|1", ".|.",
                            "0|2", "2|0", "2|1", "1|2", "2|2",
                            "0/2", "2/0", "2/1", "1/2", "2/2",
                            GenotypeClass.UNKNOWN_GENOTYPE);
                }

                List<DBObject> genotypeQueries = new ArrayList<>(genotypesFilter.size());

                for (Map.Entry<Object, List<String>> entry : genotypesFilter.entrySet()) {
                    Object sample = entry.getKey();
                    List<String> genotypes = GenotypeClass.filter(entry.getValue(), loadedGenotypes, defaultGenotypes);

                    // If empty, should find none. Add non-existing genotype
                    // TODO: Fast empty result
                    if (!entry.getValue().isEmpty() && genotypes.isEmpty()) {
                        genotypes.add(GenotypeClass.NONE_GT_VALUE);
                    }

                    int sampleId = metadataManager.getSampleId(defaultStudy.getId(), sample, true);

                    // We can not filter sample by file if one of the requested genotypes is the unknown genotype
                    boolean canFilterSampleByFile = !genotypes.contains(GenotypeClass.UNKNOWN_GENOTYPE);
                    boolean defaultGenotypeNegated = false;
                    if (canFilterSampleByFile) {
                        for (String genotype : genotypes) {
                            // Do not filter sample by file if the genotypes are negated, unless is a defaultGenotype
                            if (isNegated(genotype)) {
                                if (defaultGenotypes.contains(removeNegation(genotype))) {
                                    canFilterSampleByFile = true;
                                    defaultGenotypeNegated = true;
                                    break;
                                } else {
                                    canFilterSampleByFile = false;
                                }
                            }
                        }
                    }
                    QueryBuilder genotypesBuilder = QueryBuilder.start();
                    if (canFilterSampleByFile) {
                        List<Integer> fileIdsFromSample = new ArrayList<>();
                        for (Integer file : metadataManager.getIndexedFiles(defaultStudy.getId())) {
                            FileMetadata fileMetadata = metadataManager.getFileMetadata(defaultStudy.getId(), file);
                            if (fileMetadata.getSamples().contains(sampleId)) {
                                fileIdsFromSample.add(file);
                            }
                        }

                        if (defaultGenotypeNegated) {
                            QueryBuilder negatedFileBuilder = new QueryBuilder()
                                    .and(studyQueryPrefix + DocumentToStudyVariantEntryConverter.FILES_FIELD
                                            + '.' + DocumentToStudyVariantEntryConverter.FILEID_FIELD);
                            if (fileIdsFromSample.size() == 1) {
                                negatedFileBuilder.notEquals(fileIdsFromSample.get(0));
                            } else {
                                negatedFileBuilder.notIn(fileIdsFromSample);
                            }
                            genotypesBuilder.or(negatedFileBuilder.get());
                        } else if (gtQueryOperation == QueryOperation.OR) {
                            genotypesBuilder.and(studyQueryPrefix + DocumentToStudyVariantEntryConverter.FILES_FIELD
                                    + '.' + DocumentToStudyVariantEntryConverter.FILEID_FIELD);
                            if (fileIdsFromSample.size() == 1) {
                                genotypesBuilder.is(fileIdsFromSample.get(0));
                            } else {
                                genotypesBuilder.in(fileIdsFromSample);
                            }
                        } else {
                            // FILE ID filter can be added at the end, together with the main FILE filter
                            fileIdGroupsFromSamples.add(fileIdsFromSample);
                            fileIdsFromSamples.addAll(fileIdsFromSample);
                        }
                    }

                    for (String genotype : genotypes) {
                        if (genotype.equals(GenotypeClass.NA_GT_VALUE)) {
                            continue;
                        }

                        boolean negated = isNegated(genotype);
                        if (negated) {
                            genotype = removeNegation(genotype);
                        }
                        if (defaultGenotypes.contains(genotype)) {

                            if (negated) {
                                for (String otherGenotype : loadedGenotypes) {
                                    if (defaultGenotypes.contains(otherGenotype)) {
                                        continue;
                                    }
                                    String key = studyQueryPrefix
                                            + DocumentToStudyVariantEntryConverter.GENOTYPES_FIELD
                                            + '.' + DocumentToSamplesConverter.genotypeToStorageType(otherGenotype);
                                    genotypesBuilder.or(new BasicDBObject(key, sampleId));
                                }
                            } else {
                                QueryBuilder andBuilder = QueryBuilder.start();
                                for (String otherGenotype : loadedGenotypes) {
                                    if (defaultGenotypes.contains(otherGenotype)) {
                                        continue;
                                    }
                                    String key = studyQueryPrefix
                                            + DocumentToStudyVariantEntryConverter.GENOTYPES_FIELD
                                            + '.' + DocumentToSamplesConverter.genotypeToStorageType(otherGenotype);
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
                    genotypeQueries.add(genotypesBuilder.get());
                }

                if (gtQueryOperation == QueryOperation.OR) {
                    builder.or(genotypeQueries.toArray(new DBObject[genotypeQueries.size()])).get();
                } else {
                    builder.and(genotypeQueries.toArray(new DBObject[genotypeQueries.size()]));
                }

            }

            if (fileNames.isEmpty()) {
                // If there is no valid files filter, add files filter to speed up this query
                if (!fileIdGroupsFromSamples.isEmpty()) {
                    if ((gtQueryOperation != QueryOperation.AND || fileIdGroupsFromSamples.size() == 1)
                            && fileIdsFromSamples.containsAll(metadataManager.getIndexedFiles(defaultStudy.getId()))) {
                        // Do not add files filter if operator is OR and must select all the files.
                        // i.e. ANY file among ALL indexed files
                        logger.debug("Skip filter by all files");
                    } else {
                        addFileGroupsFilter(builder, studyQueryPrefix, gtQueryOperation, fileIdGroupsFromSamples, null);
                    }
                }
            } else {
                if (fileIdGroupsFromSamples.isEmpty()) {
                    addQueryFilter(studyQueryPrefix + DocumentToStudyVariantEntryConverter.FILES_FIELD
                                    + '.' + DocumentToStudyVariantEntryConverter.FILEID_FIELD,
                            fileNames, builder, QueryOperation.AND, filesOperation,
                            value -> {
                                Integer fileId = metadataManager.getFileIdPair(value, false, defaultStudy).getValue();
                                if (fileId == null) {
                                    throw VariantQueryException.fileNotFound(value, defaultStudy.getName());
                                }
                                if (overlappedFilesFiles) {
                                    fileId = -fileId;
                                }
                                return fileId;
                            });
                } else {
                    // fileIdGroupsFromSamples is not empty. gtQueryOperation is always AND at this point
                    // assert gtQueryOperation == Operation.AND || gtQueryOperation == null
                    if (filesOperation == QueryOperation.AND || filesOperation == null) {
                        // sample = AND, files = AND
                        // Simple mix

                        // Some files may be negated. Get them, and put them appart
                        List<Integer> negatedFiles = null;
                        if (fileNames.stream().anyMatch(VariantQueryUtils::isNegated)) {
                            negatedFiles = fileNames
                                    .stream()
                                    .filter(VariantQueryUtils::isNegated)
                                    .map(value -> {
                                        Integer fileId = metadataManager.getFileIdPair(value, false, defaultStudy)
                                                .getValue();
                                        if (fileId == null) {
                                            throw VariantQueryException.fileNotFound(value, defaultStudy.getName());
                                        }
                                        if (overlappedFilesFiles) {
                                            fileId = -fileId;
                                        }
                                        return fileId;
                                    })
                                    .collect(Collectors.toList());
                        }
                        for (Integer fileId : fileIds) {
                            fileIdGroupsFromSamples.add(Collections.singletonList(fileId));
                        }
                        addFileGroupsFilter(builder, studyQueryPrefix, QueryOperation.AND, fileIdGroupsFromSamples, negatedFiles);

                    } else if (filesOperation == QueryOperation.OR) {
                        // samples = AND, files = OR

                        // Put all files in a group
                        // the filesOperation==OR will will be expressed with an "$in" of the new group
                        fileIdGroupsFromSamples.add(fileIds);
                        addFileGroupsFilter(builder, studyQueryPrefix, QueryOperation.AND, fileIdGroupsFromSamples, null);
                    }
                }
            }

            return defaultStudy;
        } else {
            return null;
        }
    }

    private void addFileGroupsFilter(QueryBuilder builder, String studyQueryPrefix, QueryOperation operation,
                                     Set<List<Integer>> fileIdGroups, List<Integer> negatedFiles) {
        if (operation == QueryOperation.OR) {
            // Merge into one single group
            HashSet<Integer> fileIds = new HashSet<>();
            for (List<Integer> files : fileIdGroups) {
                fileIds.addAll(files);
            }
            fileIdGroups = Collections.singleton(new ArrayList<>(fileIds));
        }

        String fileIdField = studyQueryPrefix + DocumentToStudyVariantEntryConverter.FILES_FIELD
                + '.' + DocumentToStudyVariantEntryConverter.FILEID_FIELD;
        List<DBObject> fileQueries = new ArrayList<>(fileIdGroups.size());
        List<Integer> singleElementGroups = new ArrayList<>();
        for (List<Integer> group : fileIdGroups) {
            if (group.size() == 1) {
                singleElementGroups.add(group.get(0));
            } else {
                fileQueries.add(new BasicDBObject(fileIdField, new BasicDBObject("$in", group)));
            }
        }
        if (!singleElementGroups.isEmpty()) {
            if (singleElementGroups.size() == 1) {
                fileQueries.add(new BasicDBObject(fileIdField, singleElementGroups.get(0)));
            } else if (operation == QueryOperation.AND) {
                fileQueries.add(new BasicDBObject(fileIdField, new BasicDBObject("$all", singleElementGroups)));
            } else {
                fileQueries.add(new BasicDBObject(fileIdField, new BasicDBObject("$in", singleElementGroups)));
            }
        }

        if (CollectionUtils.isNotEmpty(negatedFiles)) {
            DBObject negatedFilter;
            if (negatedFiles.size() == 1) {
                negatedFilter = new BasicDBObject(fileIdField, new BasicDBObject("$ne", negatedFiles.get(0)));
            } else {
                negatedFilter = new BasicDBObject(fileIdField, new BasicDBObject("$nin", negatedFiles));
            }
            if (operation != QueryOperation.OR) {
                fileQueries.add(negatedFilter);
            } else {
                // This should never happen
                throw VariantQueryException.internalException(new IllegalStateException("Unsupported negated files with operator OR"));
            }
        }

        if (fileQueries.size() == 1) {
            builder.get().putAll(fileQueries.get(0));
        } else if (operation == QueryOperation.OR) {
//            builder.and(new BasicDBObject("$or", fileQueries));
            Object or = builder.get().removeField("$or");
            if (or == null) {
                builder.or(fileQueries.toArray(new DBObject[0]));
            } else {
                builder.and(new BasicDBObject("$or", or), new BasicDBObject("$or", fileQueries));
            }
        } else {
            builder.and(fileQueries.toArray(new DBObject[0]));
        }
    }

    private DBObject[] getFileFilterDBObjects(String key, List<String> filterValues) {
        DBObject[] regexList = new DBObject[filterValues.size()];
        for (int i = 0; i < filterValues.size(); i++) {
            String filter = filterValues.get(i);
            boolean negated = isNegated(filter);
            if (negated) {
                filter = removeNegation(filter);
            }
            if (filter.contains(VCFConstants.FILTER_CODE_SEPARATOR) || filter.equals(VCFConstants.PASSES_FILTERS_v4)) {
                if (!negated) {
                    regexList[i] = new BasicDBObject(key, filter);
                } else {
                    regexList[i] = new BasicDBObject(key, new BasicDBObject("$ne", filter));
                }
            } else {
                if (!negated) {
                    regexList[i] = new BasicDBObject(key, new BasicDBObject("$regex", filter));
                } else {
                    regexList[i] = new BasicDBObject(key, new BasicDBObject("$not", Pattern.compile(filter)));
                }
            }
        }
        return regexList;
    }

    private void parseStatsQueryParams(Query query, QueryBuilder builder, StudyMetadata defaultStudy) {
        if (query != null) {
            if (query.get(COHORT.key()) != null && !query.getString(COHORT.key()).isEmpty()) {
                addQueryFilter(DocumentToVariantConverter.STATS_FIELD
                                + '.' + DocumentToVariantStatsConverter.COHORT_ID,
                        query.getString(COHORT.key()), builder, QueryOperation.AND,
                        s -> {
                            try {
                                return Integer.parseInt(s);
                            } catch (NumberFormatException ignore) {
                                String[] split = VariantQueryUtils.splitStudyResource(s);
                                if (defaultStudy == null && split.length == 1) {
                                    throw VariantQueryException.malformedParam(COHORT, s, "Expected {study}:{cohort}");
                                } else {
                                    String study;
                                    String cohort;
                                    Integer cohortId;
                                    if (defaultStudy != null && split.length == 1) {
                                        cohort = s;
                                        cohortId = metadataManager.getCohortId(defaultStudy.getId(), cohort);
                                        if (cohortId == null) {
                                            List<String> availableCohorts = new LinkedList<>();
                                            metadataManager.cohortIterator(defaultStudy.getId())
                                                    .forEachRemaining(c -> availableCohorts.add(c.getName()));
                                            throw VariantQueryException.cohortNotFound(cohort, defaultStudy.getId(), availableCohorts);
                                        }
                                    } else {
                                        study = split[0];
                                        cohort = split[1];
                                        int studyId = metadataManager.getStudyId(study);
                                        cohortId = metadataManager.getCohortId(studyId, cohort);
                                    }
                                    return cohortId;
                                }
                            }
                        });
            }

            if (query.get(STATS_REF.key()) != null && !query.getString(STATS_REF.key()).isEmpty()) {
                addStatsFilterList(DocumentToVariantStatsConverter.REF_FREQ_FIELD, query.getString(STATS_REF.key()),
                        builder, defaultStudy);
            }

            if (query.get(STATS_ALT.key()) != null && !query.getString(STATS_ALT.key()).isEmpty()) {
                addStatsFilterList(DocumentToVariantStatsConverter.ALT_FREQ_FIELD, query.getString(STATS_ALT.key()),
                        builder, defaultStudy);
            }

            if (query.get(STATS_MAF.key()) != null && !query.getString(STATS_MAF.key()).isEmpty()) {
                addStatsFilterList(DocumentToVariantStatsConverter.MAF_FIELD, query.getString(STATS_MAF.key()),
                        builder, defaultStudy);
            }

            if (query.get(STATS_MGF.key()) != null && !query.getString(STATS_MGF.key()).isEmpty()) {
                addStatsFilterList(DocumentToVariantStatsConverter.MGF_FIELD, query.getString(STATS_MGF.key()),
                        builder, defaultStudy);
            }

            if (query.get(MISSING_ALLELES.key()) != null && !query.getString(MISSING_ALLELES.key())
                    .isEmpty()) {
                addStatsFilterList(DocumentToVariantStatsConverter.MISSALLELE_FIELD, query.getString(MISSING_ALLELES
                        .key()), builder, defaultStudy);
            }

            if (query.get(MISSING_GENOTYPES.key()) != null && !query.getString(MISSING_GENOTYPES
                    .key()).isEmpty()) {
                addStatsFilterList(DocumentToVariantStatsConverter.MISSGENOTYPE_FIELD, query.getString(
                        MISSING_GENOTYPES.key()), builder, defaultStudy);
            }

            if (query.get("numgt") != null && !query.getString("numgt").isEmpty()) {
                for (String numgt : query.getAsStringList("numgt")) {
                    String[] split = numgt.split(":");
                    addCompQueryFilter(
                            DocumentToVariantConverter.STATS_FIELD + '.' + DocumentToVariantStatsConverter.NUMGT_FIELD + '.' + split[0],
                            split[1], builder, false);
                }
            }
        }
    }

    protected Document createProjection(Query query, QueryOptions options) {
        return createProjection(query, options, VariantQueryUtils.parseVariantQueryFields(query, options, metadataManager));
    }

    protected Document createProjection(Query query, QueryOptions options, VariantQueryFields selectVariantElements) {
        if (options == null) {
            options = new QueryOptions();
        }

        Document projection = new Document();

        if (options.containsKey(QueryOptions.SORT) && !("_id").equals(options.getString(QueryOptions.SORT))) {
            if (options.getBoolean(QueryOptions.SORT)) {
                options.put(QueryOptions.SORT, "_id");
                options.putIfAbsent(QueryOptions.ORDER, QueryOptions.ASCENDING);
            } else {
                options.remove(QueryOptions.SORT);
            }
        }

        Set<VariantField> returnedFields = new HashSet<>(selectVariantElements.getFields());
        // Add all required fields
        returnedFields.addAll(DocumentToVariantConverter.REQUIRED_FIELDS_SET);
        // StudyID is mandatory if returning any STUDY element
        if (returnedFields.contains(VariantField.STUDIES)) {
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
        List<Integer> studiesIds = selectVariantElements.getStudies();
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
                    }
                    projection.put(DocumentToVariantConverter.STUDIES_FIELD + '.'
                            + DocumentToStudyVariantEntryConverter.FILES_FIELD + '.'
                            + DocumentToStudyVariantEntryConverter.FILEID_FIELD, 1);
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

        if (query.getBoolean(VARIANTS_TO_INDEX.key(), false)) {
            projection.putIfAbsent(INDEX_FIELD, 1);
        }

        logger.debug("QueryOptions: = {}", options.toJson());
        logger.debug("Projection:   = {}", projection.toJson(new JsonWriterSettings(JsonMode.SHELL, false)));
        return projection;
    }

    private QueryBuilder addQueryStringFilter(String key, String value, final QueryBuilder builder, VariantQueryUtils.QueryOperation op) {
        return this.addQueryFilter(key, value, builder, op, Function.identity());
    }

    private QueryBuilder addQueryStringFilter(String key, List<String> value, final QueryBuilder builder,
                                              VariantQueryUtils.QueryOperation op) {
        return this.addQueryFilter(key, value, builder, op, Function.identity());
    }

    private QueryBuilder addQueryIntegerFilter(String key, String value, final QueryBuilder builder, VariantQueryUtils.QueryOperation op) {
        return this.addQueryFilter(key, value, builder, op, elem -> {
            try {
                return Integer.parseInt(elem);
            } catch (NumberFormatException e) {
                throw new VariantQueryException("Unable to parse int " + elem, e);
            }
        });
    }

    private QueryBuilder addQueryIntegerFilter(String key, Collection<Integer> value, final QueryBuilder builder,
                                               VariantQueryUtils.QueryOperation op) {
        return this.addQueryFilter(key, value, builder, op);
    }

    private <T> QueryBuilder addQueryFilter(String key, String value, final QueryBuilder builder, QueryOperation op,
                                            Function<String, T> map) {
        VariantQueryUtils.QueryOperation intraOp = checkOperator(value);
        return addQueryFilter(key, splitValue(value, intraOp), builder, op, intraOp, map);
    }

    private <T> QueryBuilder addQueryFilter(String key, Collection<T> value, final QueryBuilder builder, QueryOperation op) {
        return addQueryFilter(key, value, builder, op, t -> t);
    }

    private <S, T> QueryBuilder addQueryFilter(String key, Collection<S> value, final QueryBuilder builder, QueryOperation op,
                                               Function<S, T> map) {
        return addQueryFilter(key, value, builder, op, QueryOperation.OR, map);
    }

    private <S, T> QueryBuilder addQueryFilter(String key, Collection<S> values, QueryBuilder builder, QueryOperation op,
                                               QueryOperation intraOp, Function<S, T> map) {
        QueryBuilder auxBuilder;
        if (op == VariantQueryUtils.QueryOperation.OR) {
            auxBuilder = QueryBuilder.start();
        } else {
            auxBuilder = builder;
        }

        if (values.size() == 1) {
            S elem = values.iterator().next();
            if (elem instanceof String && isNegated((String) elem)) {
                T mapped = map.apply((S) removeNegation((String) elem));
                if (mapped instanceof Collection) {
                    auxBuilder.and(key).notIn(mapped);
                } else {
                    auxBuilder.and(key).notEquals(mapped);
                }
            } else {
                T mapped = map.apply(elem);
                if (mapped instanceof Collection) {
                    auxBuilder.and(key).in(mapped);
                } else {
                    auxBuilder.and(key).is(mapped);
                }
            }
        } else if (intraOp == QueryOperation.OR) {
            List<Object> list = new ArrayList<>(values.size());
            for (S elem : values) {
                if (elem instanceof String && isNegated((String) elem)) {
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
            if (list.size() == 1) {
                auxBuilder.and(key).is(list);
            } else {
                auxBuilder.and(key).in(list);
            }
        } else {
            //Split in two lists: positive and negative
            List<Object> listIs = new ArrayList<>(values.size());
            List<Object> listNotIs = new ArrayList<>(values.size());

            for (S elem : values) {
                if (elem instanceof String && isNegated((String) elem)) {
                    T mapped = map.apply((S) removeNegation((String) elem));
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
            case "<<":
                builder.and(new BasicDBObject("$or", Arrays.asList(
                        QueryBuilder.start(key).lessThan(Double.parseDouble(obj)).get(),
                        QueryBuilder.start(key).exists(false).get()
                )));
                break;
            case "<=":
                builder.and(key).lessThanEquals(Double.parseDouble(obj));
                break;
            case "<<=":
                builder.and(new BasicDBObject("$or", Arrays.asList(
                        QueryBuilder.start(key).lessThanEquals(Double.parseDouble(obj)).get(),
                        QueryBuilder.start(key).exists(false).get()
                )));
                break;
            case ">":
                builder.and(key).greaterThan(Double.parseDouble(obj));
                break;
            case ">>":
                builder.and(new BasicDBObject("$or", Arrays.asList(
                        QueryBuilder.start(key).greaterThan(Double.parseDouble(obj)).get(),
                        QueryBuilder.start(key).exists(false).get()
                )));
                break;
            case ">=":
                builder.and(key).greaterThanEquals(Double.parseDouble(obj));
                break;
            case ">>=":
                builder.and(new BasicDBObject("$or", Arrays.asList(
                        QueryBuilder.start(key).greaterThanEquals(Double.parseDouble(obj)).get(),
                        QueryBuilder.start(key).exists(false).get()
                )));
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
                try {
                    builder.and(key).notEquals(Double.parseDouble(obj));
                } catch (NumberFormatException e) {
                    builder.and(key).notEquals(obj);
                }
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

    private QueryBuilder addStringCompQueryFilter(VariantQueryParam param, String key, String value, QueryBuilder builder) {
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
                builder.and(key).is(obj);
                break;
            default:
                throw VariantQueryException.malformedParam(param, value, "Unsupported operator " + op);
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
                addStringCompQueryFilter(scoreParam, key, scoreValue, scoreBuilder);
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
                                            VariantQueryParam queryParam, boolean alternate) {
        return addFrequencyFilter(key, value, builder, queryParam, alternate,
                (v, qb) -> addCompQueryFilter(alleleFrequencyField, v, qb, false));
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
                                            boolean alternate, BiConsumer<String, QueryBuilder> addFilter) {
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
            String populationFrequency = split[1];
            String[] populationFrequencySplit = splitOperator(populationFrequency);
            String population = populationFrequencySplit[0];
            String operator = populationFrequencySplit[1];
            String numValue = populationFrequencySplit[2];
            if (operator.startsWith(">>") || operator.startsWith("<<")) {
                // Remove first char
                operator = operator.substring(1);
            }

            logger.debug("populationFrequency = " + Arrays.toString(populationFrequencySplit));

            QueryBuilder frequencyBuilder = new QueryBuilder();
            frequencyBuilder.and(DocumentToVariantAnnotationConverter.POPULATION_FREQUENCY_STUDY_FIELD).is(study);
            frequencyBuilder.and(DocumentToVariantAnnotationConverter.POPULATION_FREQUENCY_POP_FIELD).is(population);
            Document studyPopFilter = new Document(frequencyBuilder.get().toMap());
            addFilter.accept(operator + numValue, frequencyBuilder);
            BasicDBObject elemMatch = new BasicDBObject(key, new BasicDBObject("$elemMatch", frequencyBuilder.get()));
            if (alternate && operator.startsWith("<") || !alternate && operator.startsWith(">")) {
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
     * Where STUDY is optional if defaultStudyMetadata is provided
     *
     * @param key                       Stats field to filter
     * @param values                    Values to parse
     * @param builder                   QueryBuilder
     * @param defaultStudyMetadata
     */
    private void addStatsFilterList(String key, String values, QueryBuilder builder, StudyMetadata defaultStudyMetadata) {
        QueryOperation op = checkOperator(values);
        List<String> valuesList = splitValue(values, op);
        List<DBObject> statsQueries = new LinkedList<>();
        for (String value : valuesList) {
            statsQueries.add(addStatsFilter(key, value, new QueryBuilder(), defaultStudyMetadata).get());
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
     * Where STUDY is optional if defaultStudyMetadata is provided
     *
     * @param key                       Stats field to filter
     * @param filter                    Filter to parse
     * @param builder                   QueryBuilder
     * @param defaultStudyMetadata
     */
    private QueryBuilder addStatsFilter(String key, String filter, QueryBuilder builder, StudyMetadata defaultStudyMetadata) {
        String[] studyValue = VariantQueryUtils.splitStudyResource(filter);
        if (studyValue.length == 2 || defaultStudyMetadata != null) {
            int studyId;
            Integer cohortId;
            String operator;
            String valueStr;
            if (studyValue.length == 2) {
                String[] cohortOpValue = VariantQueryUtils.splitOperator(studyValue[1]);
                String study = studyValue[0];
                String cohort = cohortOpValue[0];
                operator = cohortOpValue[1];
                valueStr = cohortOpValue[2];

                studyId = metadataManager.getStudyId(study);
                cohortId = metadataManager.getCohortId(studyId, cohort);
            } else {
//                String study = defaultStudyMetadata.getStudyName();
                studyId = defaultStudyMetadata.getId();
                String[] cohortOpValue = VariantQueryUtils.splitOperator(filter);
                String cohort = cohortOpValue[0];
                cohortId = metadataManager.getCohortId(studyId, cohort);
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
