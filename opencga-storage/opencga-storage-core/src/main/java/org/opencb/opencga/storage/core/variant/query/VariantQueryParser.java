package org.opencb.opencga.storage.core.variant.query;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.ClinicalSignificance;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.models.variant.metadata.VariantFileHeaderComplexLine;
import org.opencb.opencga.core.models.variant.VariantAnnotationConstants;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.metadata.models.VariantScoreMetadata;
import org.opencb.opencga.storage.core.utils.CellBaseUtils;
import org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.query.projection.VariantQueryProjection;
import org.opencb.opencga.storage.core.variant.query.projection.VariantQueryProjectionParser;

import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.VariantStorageOptions.EXCLUDE_GENOTYPES;
import static org.opencb.opencga.storage.core.variant.VariantStorageOptions.LOADED_GENOTYPES;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;
import static org.opencb.opencga.storage.core.variant.query.VariantQueryUtils.*;

/**
 * Created on 01/04/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantQueryParser {

    protected final CellBaseUtils cellBaseUtils;
    protected final VariantStorageMetadataManager metadataManager;
    protected final VariantQueryProjectionParser projectionParser;

    private static final Map<VariantType, VariantType> DEPRECATED_VARIANT_TYPES = new HashMap<>();
    static {
        DEPRECATED_VARIANT_TYPES.put(VariantType.SNP, VariantType.SNV);
        DEPRECATED_VARIANT_TYPES.put(VariantType.MNP, VariantType.MNV);
        DEPRECATED_VARIANT_TYPES.put(VariantType.CNV, VariantType.COPY_NUMBER);
    }


    public VariantQueryParser(CellBaseUtils cellBaseUtils, VariantStorageMetadataManager metadataManager) {
        this.cellBaseUtils = cellBaseUtils;
        this.metadataManager = metadataManager;
        this.projectionParser = new VariantQueryProjectionParser(metadataManager);
    }

    public ParsedVariantQuery parseQuery(Query query, QueryOptions options) {
        return parseQuery(query, options, false);
    }

    public ParsedVariantQuery parseQuery(Query query, QueryOptions options, boolean skipPreProcess) {
        if (query == null) {
            query = new Query();
        }
        if (options == null) {
            options = new QueryOptions();
        }

        ParsedVariantQuery variantQuery = new ParsedVariantQuery(new Query(query), new QueryOptions(options));

        if (!skipPreProcess) {
            query = preProcessQuery(query, options);
        }
        variantQuery.setQuery(query);
        variantQuery.setProjection(projectionParser.parseVariantQueryProjection(query, options));

        ParsedVariantQuery.VariantStudyQuery studyQuery = variantQuery.getStudyQuery();

        StudyMetadata defaultStudy = getDefaultStudy(query);
        studyQuery.setDefaultStudy(defaultStudy);
        if (isValidParam(query, STUDY)) {
            studyQuery.setStudies(VariantQueryUtils.splitValue(query, STUDY));
        }
        if (isValidParam(query, GENOTYPE)) {
            HashMap<Object, List<String>> map = new HashMap<>();
            QueryOperation op = VariantQueryUtils.parseGenotypeFilter(query.getString(GENOTYPE.key()), map);

            if (defaultStudy == null) {
                List<String> studyNames = metadataManager.getStudyNames();
                throw VariantQueryException.missingStudyForSamples(map.keySet()
                        .stream().map(Object::toString).collect(Collectors.toSet()), studyNames);
            }

            List<KeyOpValue<SampleMetadata, List<String>>> values = new ArrayList<>();
            for (Map.Entry<Object, List<String>> entry : map.entrySet()) {
                Integer sampleId = metadataManager.getSampleId(defaultStudy.getId(), entry.getKey());
                if (sampleId == null) {
                    throw VariantQueryException.sampleNotFound(entry.getKey(), defaultStudy.getName());
                }
                values.add(new KeyOpValue<>(metadataManager.getSampleMetadata(defaultStudy.getId(), sampleId), "=", entry.getValue()));
            }

            studyQuery.setGenotypes(new ParsedQuery<>(GENOTYPE, op, values));
        }
        ParsedQuery<KeyValues<String, KeyOpValue<String, String>>> sampleDataQuery = parseSampleData(query);
        if (sampleDataQuery.isNotEmpty()) {
            ParsedQuery<KeyValues<SampleMetadata, KeyOpValue<String, String>>> sampleDataQueryWithMetadata
                    = new ParsedQuery<>(sampleDataQuery.getKey(), sampleDataQuery.getOperation(), new ArrayList<>(sampleDataQuery.size()));
            for (KeyValues<String, KeyOpValue<String, String>> keyValues : sampleDataQuery) {
                sampleDataQueryWithMetadata.getValues().add(
                        keyValues.mapKey(sample -> {
                            int sampleId = metadataManager.getSampleIdOrFail(defaultStudy.getId(), sample);
                            return metadataManager.getSampleMetadata(defaultStudy.getId(), sampleId);
                        }));
            }
            studyQuery.setSampleDataQuery(sampleDataQueryWithMetadata);
        }

        return variantQuery;
    }

    public Query preProcessQuery(Query originalQuery, QueryOptions options) {
        // Copy input query! Do not modify original query!
        Query query = VariantQueryUtils.copy(originalQuery);

        preProcessAnnotationParams(query);

        preProcessStudyParams(query, options);

        if (options != null && options.getLong(QueryOptions.LIMIT) < 0) {
            throw VariantQueryException.malformedParam(QueryOptions.LIMIT, options.getString(QueryOptions.LIMIT),
                    "Invalid negative limit");
        }

        if (options != null && options.getLong(QueryOptions.SKIP) < 0) {
            throw VariantQueryException.malformedParam(QueryOptions.SKIP, options.getString(QueryOptions.SKIP),
                    "Invalid negative skip");
        }

        return query;
    }

    protected void preProcessAnnotationParams(Query query) {
        convertGoToGeneQuery(query, cellBaseUtils);
        convertExpressionToGeneQuery(query, cellBaseUtils);

        ParsedVariantQuery.VariantQueryXref xrefs = parseXrefs(query);
        List<String> allIds = new ArrayList<>(xrefs.getIds().size() + xrefs.getVariants().size());
        allIds.addAll(xrefs.getIds());
        for (Variant variant : xrefs.getVariants()) {
            allIds.add(variant.toString());
        }
        query.put(ID.key(), allIds);
        query.put(GENE.key(), xrefs.getGenes());
        query.put(ANNOT_XREF.key(), xrefs.getOtherXrefs());
        query.remove(ANNOT_CLINVAR.key());
        query.remove(ANNOT_COSMIC.key());

        if (VariantQueryUtils.isValidParam(query, TYPE)) {
            Set<VariantType> types = new HashSet<>();
            List<String> typesFromQuery = query.getAsStringList(TYPE.key());
            if (typesFromQuery.contains(VariantType.SNP.name()) && !typesFromQuery.contains(VariantType.SNV.name())) {
                throw VariantQueryException.malformedParam(TYPE, "Unable to filter by SNP");
            }
            if (typesFromQuery.contains(VariantType.MNP.name()) && !typesFromQuery.contains(VariantType.MNV.name())) {
                throw VariantQueryException.malformedParam(TYPE, "Unable to filter by MNP");
            }
            if (query.getString(TYPE.key()).contains(NOT)) {
                // Invert negations
                types.addAll(Arrays.asList(VariantType.values()));
                for (String type : typesFromQuery) {
                    if (isNegated(type)) {
                        type = removeNegation(type);
                    } else {
                        throw VariantQueryException.malformedParam(TYPE, "Can not mix negated and no negated values");
                    }
                    // Expand types to subtypes
                    VariantType variantType = parseVariantType(type);
                    Set<VariantType> subTypes = Variant.subTypes(variantType);
                    types.remove(variantType);
                    types.removeAll(subTypes);
                }
            } else {
                // Expand types to subtypes
                for (String type : typesFromQuery) {
                    VariantType variantType = parseVariantType(type);
                    Set<VariantType> subTypes = Variant.subTypes(variantType);
                    types.add(variantType);
                    types.addAll(subTypes);
                }
            }
            query.put(TYPE.key(), new ArrayList<>(types));
        }

        if (VariantQueryUtils.isValidParam(query, ANNOT_CLINICAL_SIGNIFICANCE)) {
            String v = query.getString(ANNOT_CLINICAL_SIGNIFICANCE.key());
            QueryOperation operator = VariantQueryUtils.checkOperator(v);
            List<String> values = VariantQueryUtils.splitValue(v, operator);
            List<String> clinicalSignificanceList = new ArrayList<>(values.size());
            for (String clinicalSignificance : values) {
                ClinicalSignificance enumValue = EnumUtils.getEnum(ClinicalSignificance.class, clinicalSignificance);
                if (enumValue == null) {
                    String key = clinicalSignificance.toLowerCase().replace(' ', '_');
                    enumValue = EnumUtils.getEnum(ClinicalSignificance.class, key);
                }
                if (enumValue == null) {
                    String key = clinicalSignificance.toLowerCase();
                    if (VariantAnnotationConstants.CLINVAR_CLINSIG_TO_ACMG.containsKey(key)) {
                        // No value set
                        enumValue = VariantAnnotationConstants.CLINVAR_CLINSIG_TO_ACMG.get(key);
                    }
                }
                if (enumValue != null) {
                    clinicalSignificance = enumValue.toString();
                } // else should throw exception?

                clinicalSignificanceList.add(clinicalSignificance);
            }
            query.put(ANNOT_CLINICAL_SIGNIFICANCE.key(),
                    String.join(operator == null ? "" : operator.separator(), clinicalSignificanceList));
        }

        if (isValidParam(query, ANNOT_SIFT)) {
            String sift = query.getString(ANNOT_SIFT.key());
            String[] split = splitOperator(sift);
            if (StringUtils.isNotEmpty(split[0])) {
                throw VariantQueryException.malformedParam(ANNOT_SIFT, sift);
            }
            if (isValidParam(query, ANNOT_PROTEIN_SUBSTITUTION)) {
                String proteinSubstitution = query.getString(ANNOT_PROTEIN_SUBSTITUTION.key());
                if (proteinSubstitution.contains("sift")) {
                    throw VariantQueryException.malformedParam(ANNOT_SIFT,
                            "Conflict with parameter \"" + ANNOT_PROTEIN_SUBSTITUTION.key() + "\"");
                }
                query.put(ANNOT_PROTEIN_SUBSTITUTION.key(), proteinSubstitution + AND + "sift" + split[1] + split[2]);
            } else {
                query.put(ANNOT_PROTEIN_SUBSTITUTION.key(), "sift" + split[1] + split[2]);
            }
            query.remove(ANNOT_SIFT.key());
        }

        if (isValidParam(query, ANNOT_POLYPHEN)) {
            String polyphen = query.getString(ANNOT_POLYPHEN.key());
            String[] split = splitOperator(polyphen);
            if (StringUtils.isNotEmpty(split[0])) {
                throw VariantQueryException.malformedParam(ANNOT_POLYPHEN, polyphen);
            }
            if (isValidParam(query, ANNOT_PROTEIN_SUBSTITUTION)) {
                String proteinSubstitution = query.getString(ANNOT_PROTEIN_SUBSTITUTION.key());
                if (proteinSubstitution.contains("sift")) {
                    throw VariantQueryException.malformedParam(ANNOT_SIFT,
                            "Conflict with parameter \"" + ANNOT_PROTEIN_SUBSTITUTION.key() + "\"");
                }
                query.put(ANNOT_PROTEIN_SUBSTITUTION.key(), proteinSubstitution + AND + "polyphen" + split[1] + split[2]);
            } else {
                query.put(ANNOT_PROTEIN_SUBSTITUTION.key(), "polyphen" + split[1] + split[2]);
            }
            query.remove(ANNOT_POLYPHEN.key());
        }

        if (isValidParam(query, ANNOT_CONSEQUENCE_TYPE)) {
            Values<String> values = VariantQueryUtils.splitValues(query.getString(ANNOT_CONSEQUENCE_TYPE.key()));
            List<String> parsedCts = parseConsequenceTypes(values.getValues());
            query.put(ANNOT_CONSEQUENCE_TYPE.key(), values.operation == null
                    ? parsedCts
                    : String.join(values.operation.separator(), parsedCts));
        }
    }

    private VariantType parseVariantType(String type) {
        try {
            VariantType variantType = VariantType.valueOf(type.toUpperCase());
            return DEPRECATED_VARIANT_TYPES.getOrDefault(variantType, variantType);
        } catch (IllegalArgumentException e) {
            throw VariantQueryException.malformedParam(TYPE, "Unknown variant type " + type);
        }
    }

    protected void preProcessStudyParams(Query query, QueryOptions options) {
        StudyMetadata defaultStudy = getDefaultStudy(query);
        QueryOperation formatOperator = null;
        if (isValidParam(query, SAMPLE_DATA)) {
            extractGenotypeFromFormatFilter(query);

            ParsedQuery<KeyValues<String, KeyOpValue<String, String>>> sampleData = parseSampleData(query);
            formatOperator = sampleData.getOperation();

            for (KeyValues<String, KeyOpValue<String, String>> sampleDataFilter : sampleData.getValues()) {
                String sampleName = sampleDataFilter.getKey();
                if (defaultStudy == null) {
                    throw VariantQueryException.missingStudyForSample(sampleName, metadataManager.getStudyNames());
                }
                Integer sampleId = metadataManager.getSampleId(defaultStudy.getId(), sampleName, true);
                if (sampleId == null) {
                    throw VariantQueryException.sampleNotFound(sampleName, defaultStudy.getName());
                }
                for (KeyOpValue<String, String> formatFilter : sampleDataFilter) {
                    VariantFileHeaderComplexLine line = defaultStudy.getVariantHeaderLine("FORMAT", formatFilter.getKey());
                    if (line == null) {
                        throw VariantQueryException.malformedParam(SAMPLE_DATA, query.getString(SAMPLE_DATA.key()),
                                "FORMAT field \"" + formatFilter.getKey() + "\" not found. Available keys in study: "
                                        + defaultStudy.getVariantHeaderLines("FORMAT").keySet());
                    }
                }
            }
        }

        if (isValidParam(query, FILE_DATA)) {
            ParsedQuery<KeyValues<String, KeyOpValue<String, String>>> parsedQuery = parseFileData(query);
            if (isValidParam(query, FILE) && parsedQuery.getOperation() != null) {
                QueryOperation fileOperator = checkOperator(query.getString(FILE.key()));
                if (fileOperator != null && parsedQuery.getOperation() != fileOperator) {
                    throw VariantQueryException.mixedAndOrOperators(FILE, FILE_DATA);
                }
            }
            for (KeyValues<String, KeyOpValue<String, String>> fileDataFilters : parsedQuery.getValues()) {
                String fileName = fileDataFilters.getKey();
                if (defaultStudy == null) {
                    throw VariantQueryException.missingStudyForFile(fileName, metadataManager.getStudyNames());
                }
                Integer fileId = metadataManager.getFileId(defaultStudy.getId(), fileName, true);
                if (fileId == null) {
                    throw VariantQueryException.fileNotFound(fileName, defaultStudy.getName());
                }
                for (KeyOpValue<String, String> fileDataFilter : fileDataFilters.getValues()) {
                    String fileDataKey = fileDataFilter.getKey();
                    if (fileDataKey.equals(StudyEntry.FILTER)) {
                        if (isValidParam(query, FILTER)) {
                            throw VariantQueryException.unsupportedParamsCombination(
                                    FILE_DATA, query.getString(FILE_DATA.key()),
                                    FILTER, query.getString(FILTER.key()));
                        }
                    } else if (fileDataKey.equals(StudyEntry.QUAL)) {
                        if (isValidParam(query, QUAL)) {
                            throw VariantQueryException.unsupportedParamsCombination(
                                    FILE_DATA, query.getString(FILE_DATA.key()),
                                    QUAL, query.getString(QUAL.key()));
                        }
                    } else {
                        VariantFileHeaderComplexLine line = defaultStudy.getVariantHeaderLine("INFO", fileDataKey);
                        if (line == null) {
                            throw VariantQueryException.malformedParam(FILE_DATA, query.getString(FILE_DATA.key()),
                                    "INFO field \"" + fileDataKey + "\" not found. Available keys in study: "
                                            + defaultStudy.getVariantHeaderLines("INFO").keySet());
                        }
                    }
                }
            }
        }

        QueryOperation genotypeOperator = null;
        VariantQueryParam genotypeParam = null;

        List<QueryParam> sampleParamsList = new LinkedList<>();

        if (isValidParam(query, SAMPLE)) {
            sampleParamsList.add(SAMPLE);
        }
        if (isValidParam(query, GENOTYPE)) {
            sampleParamsList.add(GENOTYPE);
        }
        if (isValidParam(query, SAMPLE_DE_NOVO)) {
            sampleParamsList.add(SAMPLE_DE_NOVO);
        }
        if (isValidParam(query, SAMPLE_MENDELIAN_ERROR)) {
            sampleParamsList.add(SAMPLE_MENDELIAN_ERROR);
        }
        if (isValidParam(query, SAMPLE_COMPOUND_HETEROZYGOUS)) {
            sampleParamsList.add(SAMPLE_COMPOUND_HETEROZYGOUS);
        }
        if (sampleParamsList.size() > 1) {
            throw VariantQueryException.unsupportedParamsCombination(sampleParamsList);
        }

        if (isValidParam(query, SAMPLE)) {
            String sampleValue = query.getString(SAMPLE.key());
            if (sampleValue.contains(IS)) {
                QueryParam newSampleParam;
                String expectedValue = null;

                if (sampleValue.toLowerCase().contains(IS + "denovo")) {
                    newSampleParam = SAMPLE_DE_NOVO;
                    expectedValue = "denovo";
                } else if (sampleValue.toLowerCase().contains(IS + "mendelianerror")) {
                    newSampleParam = SAMPLE_MENDELIAN_ERROR;
                    expectedValue = "mendelianerror";
                } else if (sampleValue.toLowerCase().contains(IS + "compoundheterozygous")) {
                    newSampleParam = SAMPLE_COMPOUND_HETEROZYGOUS;
                    expectedValue = "compoundheterozygous";
                } else {
                    newSampleParam = GENOTYPE;
                    query.remove(SAMPLE.key());
                    query.put(newSampleParam.key(), sampleValue);
                }

                if (newSampleParam != GENOTYPE) {
                    ParsedQuery<String> parsedQuery = splitValue(query, SAMPLE);
                    if (QueryOperation.AND.equals(parsedQuery.getOperation())) {
                        throw VariantQueryException.malformedParam(SAMPLE, sampleValue, "Unsupported AND operator");
                    }
                    List<String> samples = new ArrayList<>(parsedQuery.getValues().size());
                    for (String value : parsedQuery.getValues()) {
                        if (!value.contains(IS)) {
                            throw VariantQueryException.malformedParam(SAMPLE, value);
                        }
                        String[] split = value.split(IS, 2);
                        if (!split[1].equalsIgnoreCase(expectedValue)) {
                            throw VariantQueryException.malformedParam(SAMPLE, sampleValue,
                                    "Unable to mix " + expectedValue + " and " + split[1] + " filters.");
                        }
                        samples.add(split[0]);
                    }
                    query.remove(SAMPLE.key());
                    query.put(newSampleParam.key(), samples);
                }
            }
        }

        if (isValidParam(query, SAMPLE)) {
            genotypeParam = SAMPLE;

            if (defaultStudy == null) {
                throw VariantQueryException.missingStudyForSamples(query.getAsStringList(SAMPLE.key()),
                        metadataManager.getStudyNames());
            }
            List<String> loadedGenotypes = defaultStudy.getAttributes().getAsStringList(LOADED_GENOTYPES.key());
            if (CollectionUtils.isEmpty(loadedGenotypes)) {
                loadedGenotypes = Arrays.asList(
                        "0/0", "0|0",
                        "0/1", "1/0", "1/1", "./.",
                        "0|1", "1|0", "1|1", ".|.",
                        "0|2", "2|0", "2|1", "1|2", "2|2",
                        "0/2", "2/0", "2/1", "1/2", "2/2",
                        GenotypeClass.UNKNOWN_GENOTYPE);
            }
            String genotypes;
            List<String> mainGts = GenotypeClass.MAIN_ALT.filter(loadedGenotypes);
            if (loadedGenotypes.contains(GenotypeClass.NA_GT_VALUE)
                    || defaultStudy.getAttributes().getBoolean(EXCLUDE_GENOTYPES.key(), EXCLUDE_GENOTYPES.defaultValue())) {
                mainGts.add(GenotypeClass.NA_GT_VALUE);
            }
            genotypes = String.join(",", mainGts);

            Values<String> samples = VariantQueryUtils.splitValues(query.getString(SAMPLE.key()));
            genotypeOperator = samples.getOperation();

            StringBuilder sb = new StringBuilder();
            for (String sample : samples) {
                if (sb.length() > 0) {
                    sb.append(genotypeOperator.separator());
                }
                sb.append(sample).append(IS).append(genotypes);
            }
            query.remove(SAMPLE.key());
            query.put(GENOTYPE.key(), sb.toString());
        }

        if (isValidParam(query, GENOTYPE)) {
            genotypeParam = GENOTYPE;

            List<String> loadedGenotypes = defaultStudy.getAttributes().getAsStringList(LOADED_GENOTYPES.key());
            if (CollectionUtils.isEmpty(loadedGenotypes)) {
                loadedGenotypes = Arrays.asList(
                        "0/0", "0|0",
                        "0/1", "1/0", "1/1", "./.",
                        "0|1", "1|0", "1|1", ".|.",
                        "0|2", "2|0", "2|1", "1|2", "2|2",
                        "0/2", "2/0", "2/1", "1/2", "2/2",
                        GenotypeClass.UNKNOWN_GENOTYPE);
            }

            Map<Object, List<String>> map = new LinkedHashMap<>();
            genotypeOperator = VariantQueryUtils.parseGenotypeFilter(query.getString(GENOTYPE.key()), map);

            String filter = preProcessGenotypesFilter(map, genotypeOperator, loadedGenotypes);
            query.put(GENOTYPE.key(), filter);
        }

        if (formatOperator != null && genotypeOperator != null && formatOperator != genotypeOperator) {
            throw VariantQueryException.mixedAndOrOperators(SAMPLE_DATA, genotypeParam);
        }

        if (isValidParam(query, SAMPLE_MENDELIAN_ERROR) || isValidParam(query, SAMPLE_DE_NOVO)) {
            QueryParam param;
            if (isValidParam(query, SAMPLE_MENDELIAN_ERROR) && isValidParam(query, SAMPLE_DE_NOVO)) {
                throw VariantQueryException.unsupportedParamsCombination(
                        SAMPLE_MENDELIAN_ERROR, query.getString(SAMPLE_MENDELIAN_ERROR.key()),
                        SAMPLE_DE_NOVO, query.getString(SAMPLE_DE_NOVO.key()));
            } else if (isValidParam(query, SAMPLE_MENDELIAN_ERROR)) {
                param = SAMPLE_MENDELIAN_ERROR;
            } else {
                param = SAMPLE_DE_NOVO;
            }
            if (defaultStudy == null) {
                throw VariantQueryException.missingStudyForSamples(query.getAsStringList(param.key()),
                        metadataManager.getStudyNames());
            }
            // Check no other samples filter is being used, and all samples are precomputed
            if (genotypeParam != null) {
                throw VariantQueryException.unsupportedParamsCombination(
                        param, query.getString(param.key()),
                        genotypeParam, query.getString(genotypeParam.key())
                );
            }
            List<String> samples = query.getAsStringList(param.key());
            Set<String> samplesAndParents = new LinkedHashSet<>(samples);
            for (String sample : samples) {
                Integer sampleId = metadataManager.getSampleId(defaultStudy.getId(), sample);
                if (sampleId == null) {
                    throw VariantQueryException.sampleNotFound(sample, defaultStudy.getName());
                }
                SampleMetadata sampleMetadata = metadataManager.getSampleMetadata(defaultStudy.getId(), sampleId);
                if (!TaskMetadata.Status.READY.equals(sampleMetadata.getMendelianErrorStatus())) {
                    throw VariantQueryException.malformedParam(param, "Sample \"" + sampleMetadata.getName()
                            + "\" does not have the Mendelian Errors precomputed yet");
                }
                if (sampleMetadata.getFather() != null) {
                    samplesAndParents.add(metadataManager.getSampleName(defaultStudy.getId(), sampleMetadata.getFather()));
                }
                if (sampleMetadata.getMother() != null) {
                    samplesAndParents.add(metadataManager.getSampleName(defaultStudy.getId(), sampleMetadata.getMother()));
                }
            }
            if (VariantQueryUtils.isValidParam(query, INCLUDE_SAMPLE)) {
                List<String> includeSamples = query.getAsStringList(INCLUDE_SAMPLE.key());
                boolean includeAll = isAllOrNull(includeSamples);
                if (!includeAll && !includeSamples.containsAll(samplesAndParents)) {
                    throw new VariantQueryException("Invalid list of '" + INCLUDE_SAMPLE.key() + "'. "
                            + "It must include, at least, all parents.");
                }
            } else {
                query.put(INCLUDE_SAMPLE.key(), new ArrayList<>(samplesAndParents));
            }
        }

        if (isValidParam(query, SCORE)) {
            Values<String> scoreValues = splitValues(query.getString(SCORE.key()));
            for (String scoreFilter : scoreValues) {
                String variantScore = parseKeyOpValue(scoreFilter).getKey();
                VariantScoreMetadata variantScoreMetadata;
                String[] studyScore = splitStudyResource(variantScore);
                if (studyScore.length == 2) {
                    int studyId = metadataManager.getStudyId(studyScore[0]);
                    variantScoreMetadata = metadataManager.getVariantScoreMetadata(studyId, studyScore[1]);
                } else {
                    if (defaultStudy == null) {
                        throw VariantQueryException.missingStudyFor("score", variantScore, metadataManager.getStudyNames());
                    } else {
                        variantScoreMetadata = metadataManager.getVariantScoreMetadata(defaultStudy, variantScore);
                    }
                }
                if (variantScoreMetadata == null) {
                    throw VariantQueryException.scoreNotFound(variantScore, defaultStudy.getName());
                }
            }
        }

        if (!isValidParam(query, INCLUDE_STUDY)
                || !isValidParam(query, INCLUDE_SAMPLE)
                || !isValidParam(query, INCLUDE_FILE)
                || !isValidParam(query, SAMPLE_SKIP)
                || !isValidParam(query, SAMPLE_LIMIT)
        ) {
            VariantQueryProjection selectVariantElements =
                    VariantQueryProjectionParser.parseVariantQueryFields(query, options, metadataManager);
            // Apply the sample pagination.
            // Remove the sampleLimit and sampleSkip to avoid applying the pagination twice
            query.remove(SAMPLE_SKIP.key());
            query.remove(SAMPLE_LIMIT.key());
            query.put(NUM_TOTAL_SAMPLES.key(), selectVariantElements.getNumTotalSamples());
            query.put(NUM_SAMPLES.key(), selectVariantElements.getNumSamples());

            if (!isValidParam(query, INCLUDE_STUDY)) {
                List<String> includeStudy = new ArrayList<>();
                for (Integer studyId : selectVariantElements.getStudyIds()) {
                    includeStudy.add(selectVariantElements.getStudy(studyId).getStudyMetadata().getName());
                }
                if (includeStudy.isEmpty()) {
                    query.put(INCLUDE_STUDY.key(), NONE);
                } else {
                    query.put(INCLUDE_STUDY.key(), includeStudy);
                }
            }
            if (!isValidParam(query, INCLUDE_SAMPLE) || selectVariantElements.getSamplePagination()) {
                List<String> includeSample = selectVariantElements.getSamples()
                        .entrySet()
                        .stream()
                        .flatMap(e -> e.getValue()
                                .stream()
                                .map(s -> metadataManager.getSampleName(e.getKey(), s)))
                        .collect(Collectors.toList());
                if (includeSample.isEmpty()) {
                    query.put(INCLUDE_SAMPLE.key(), NONE);
                } else {
                    query.put(INCLUDE_SAMPLE.key(), includeSample);
                }
            }
            if (!isValidParam(query, INCLUDE_FILE) || selectVariantElements.getSamplePagination()) {
                List<String> includeFile = selectVariantElements.getFiles()
                        .entrySet()
                        .stream()
                        .flatMap(e -> e.getValue()
                                .stream()
                                .map(f -> metadataManager.getFileName(e.getKey(), f)))
                        .collect(Collectors.toList());
                if (includeFile.isEmpty()) {
                    query.put(INCLUDE_FILE.key(), NONE);
                } else {
                    query.put(INCLUDE_FILE.key(), includeFile);
                }
            }
        }

        List<String> formats = getIncludeSampleData(query);
        if (formats == null) {
            formats = Collections.singletonList(ALL);
        } else if (formats.isEmpty()) {
            formats = Collections.singletonList(NONE);
        }

        query.put(INCLUDE_SAMPLE_DATA.key(), formats);
        query.remove(INCLUDE_GENOTYPE.key(), formats);
    }

    public static String preProcessGenotypesFilter(Map<Object, List<String>> map, QueryOperation op, List<String> loadedGenotypes) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<Object, List<String>> entry : map.entrySet()) {

            List<String> genotypes = preProcessGenotypesFilter(entry.getValue(), loadedGenotypes);

            if (sb.length() > 0) {
                sb.append(op.separator());
            }
            sb.append(entry.getKey()).append(IS);
            for (int i = 0; i < genotypes.size(); i++) {
                if (i > 0) {
                    sb.append(OR);
                }
                sb.append(genotypes.get(i));
            }
        }
        return sb.toString();
    }

    public static List<String> preProcessGenotypesFilter(List<String> genotypesInput, List<String> loadedGenotypes) {
        List<String> genotypes = new ArrayList<>(genotypesInput);

        // Loop for multi-allelic values genotypes
        // Iterate on genotypesInput, as this loop may add new values to List<String> genotypes
        for (String genotypeStr : genotypesInput) {
            boolean negated = isNegated(genotypeStr);
            if (negated) {
                genotypeStr = removeNegation(genotypeStr);
            }
            for (String multiAllelicGenotype : GenotypeClass.expandMultiAllelicGenotype(genotypeStr, loadedGenotypes)) {
                genotypes.add((negated ? NOT : "") + multiAllelicGenotype);
            }
        }
        genotypes = GenotypeClass.filter(genotypes, loadedGenotypes);

        if (genotypes.stream().anyMatch(VariantQueryUtils::isNegated) && !genotypes.stream().allMatch(VariantQueryUtils::isNegated)) {
            throw VariantQueryException.malformedParam(GENOTYPE, genotypesInput.toString(),
                    "Can not mix negated and not negated genotypes");
        }

        // If empty, should find none. Add non-existing genotype
        if (genotypes.isEmpty()) {
            // TODO: Do fast fail, NO RESULTS!
            genotypes = Collections.singletonList(GenotypeClass.NONE_GT_VALUE);
        }
        return genotypes;
    }

    /**
     * Parses XREFS related filters, and sorts in different lists.
     *
     * - {@link VariantQueryParam#ID}
     * - {@link VariantQueryParam#GENE}
     * - {@link VariantQueryParam#ANNOT_XREF}
     * - {@link VariantQueryParam#ANNOT_CLINVAR}
     * - {@link VariantQueryParam#ANNOT_COSMIC}
     *
     * @param query Query to parse
     * @return VariantQueryXref with all VariantIds, ids, genes and xrefs
     */
    public static ParsedVariantQuery.VariantQueryXref parseXrefs(Query query) {
        ParsedVariantQuery.VariantQueryXref xrefs = new ParsedVariantQuery.VariantQueryXref();
        if (query == null) {
            return xrefs;
        }
        xrefs.getGenes().addAll(query.getAsStringList(GENE.key(), OR));

        if (isValidParam(query, ID)) {
            List<String> idsList = query.getAsStringList(ID.key(), OR);

            for (String value : idsList) {
                Variant variant = toVariant(value);
                if (variant != null) {
                    xrefs.getVariants().add(variant);
                } else {
                    xrefs.getIds().add(value);
                }
            }
        }

        if (isValidParam(query, ANNOT_XREF)) {
            List<String> xrefsList = query.getAsStringList(ANNOT_XREF.key(), OR);
            for (String value : xrefsList) {
                Variant variant = toVariant(value);
                if (variant != null) {
                    xrefs.getVariants().add(variant);
                } else {
                    if (isVariantAccession(value) || isClinicalAccession(value) || isGeneAccession(value)) {
                        xrefs.getOtherXrefs().add(value);
                    } else {
                        xrefs.getGenes().add(value);
                    }
                }
            }

        }
//        xrefs.getOtherXrefs().addAll(query.getAsStringList(ANNOT_HPO.key(), OR));
        xrefs.getOtherXrefs().addAll(query.getAsStringList(ANNOT_COSMIC.key(), OR));
        xrefs.getOtherXrefs().addAll(query.getAsStringList(ANNOT_CLINVAR.key(), OR));

        return xrefs;
    }

    @Deprecated
    public static StudyMetadata getDefaultStudy(Query query, VariantStorageMetadataManager metadataManager) {
        return new VariantQueryParser(null, metadataManager).getDefaultStudy(query);
    }

    public StudyMetadata getDefaultStudy(Query query) {
        final StudyMetadata defaultStudy;
        if (isValidParam(query, STUDY)) {
            String value = query.getString(STUDY.key());

            // Check that the study exists
            QueryOperation studiesOperation = checkOperator(value);
            List<String> studiesNames = splitValue(value, studiesOperation);
            List<Integer> studyIds = metadataManager.getStudyIds(studiesNames); // Non negated studyIds
            if (studyIds.size() == 1) {
                defaultStudy = metadataManager.getStudyMetadata(studyIds.get(0));
            } else {
                defaultStudy = null;
            }
        } else {
            List<String> studyNames = metadataManager.getStudyNames();
            if (studyNames != null && studyNames.size() == 1) {
                defaultStudy = metadataManager.getStudyMetadata(studyNames.get(0));
            } else {
                defaultStudy = null;
            }
        }
        return defaultStudy;
    }

}
