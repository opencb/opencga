package org.opencb.opencga.storage.core.variant.query;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.ClinicalSignificance;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.models.variant.metadata.VariantFileHeaderComplexLine;
import org.opencb.cellbase.core.variant.annotation.VariantAnnotationUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.metadata.models.VariantScoreMetadata;
import org.opencb.opencga.storage.core.utils.CellBaseUtils;
import org.opencb.opencga.storage.core.variant.adaptors.*;

import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.VariantStorageOptions.EXCLUDE_GENOTYPES;
import static org.opencb.opencga.storage.core.variant.VariantStorageOptions.LOADED_GENOTYPES;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.*;

/**
 * Created on 01/04/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantQueryParser {

    public static final String SAMPLE_ID = "SAMPLE_ID";
    public static final String FILE_IDX = "FILE_IDX";
    public static final String FILE_ID = "FILE_ID";

    protected final CellBaseUtils cellBaseUtils;
    protected final VariantStorageMetadataManager metadataManager;

    public VariantQueryParser(CellBaseUtils cellBaseUtils, VariantStorageMetadataManager metadataManager) {
        this.cellBaseUtils = cellBaseUtils;
        this.metadataManager = metadataManager;
    }

    //    public VariantQuery parseQuery(Query query, QueryOptions options) throws StorageEngineException {};

    public Query preProcessQuery(Query originalQuery, QueryOptions options) {
        // Copy input query! Do not modify original query!
        Query query = originalQuery == null ? new Query() : new Query(originalQuery);

        preProcessAnnotationParams(query);

        preProcessStudyParams(options, query);

        return query;
    }

    protected void preProcessAnnotationParams(Query query) {
        convertGoToGeneQuery(query, cellBaseUtils);
        convertExpressionToGeneQuery(query, cellBaseUtils);

        VariantQueryXref xrefs = parseXrefs(query);
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
            Set<String> types = new HashSet<>();
            if (query.getString(TYPE.key()).contains(NOT)) {
                // Invert negations
                for (VariantType value : VariantType.values()) {
                    types.add(value.name());
                }
                for (String type : query.getAsStringList(TYPE.key())) {
                    if (isNegated(type)) {
                        type = removeNegation(type);
                    } else {
                        throw VariantQueryException.malformedParam(TYPE, "Can not mix negated and no negated values");
                    }
                    // Expand types to subtypes
                    type = type.toUpperCase();
                    Set<VariantType> subTypes = Variant.subTypes(VariantType.valueOf(type));
                    types.remove(type);
                    subTypes.forEach(subType -> types.remove(subType.toString()));
                }
            } else {
                // Expand types to subtypes
                for (String type : query.getAsStringList(TYPE.key())) {
                    type = type.toUpperCase();
                    Set<VariantType> subTypes = Variant.subTypes(VariantType.valueOf(type));
                    types.add(type);
                    subTypes.forEach(subType -> types.add(subType.toString()));
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
                    if (VariantAnnotationUtils.CLINVAR_CLINSIG_TO_ACMG.containsKey(key)) {
                        // No value set
                        enumValue = VariantAnnotationUtils.CLINVAR_CLINSIG_TO_ACMG.get(key);
                    }
                }
                if (enumValue != null) {
                    clinicalSignificance = enumValue.toString();
                } // else should throw exception?

                clinicalSignificanceList.add(clinicalSignificance);
            }
            query.put(ANNOT_CLINICAL_SIGNIFICANCE.key(), clinicalSignificanceList);
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
            Pair<QueryOperation, List<String>> pair = VariantQueryUtils.splitValue(query.getString(ANNOT_CONSEQUENCE_TYPE.key()));
            QueryOperation op = pair.getLeft();
            List<String> cts = pair.getRight();
            List<String> parsedCts = parseConsequenceTypes(cts);
            query.put(ANNOT_CONSEQUENCE_TYPE.key(), op == null ? parsedCts : String.join(op.separator(), parsedCts));
        }
    }

    protected void preProcessStudyParams(QueryOptions options, Query query) {
        StudyMetadata defaultStudy = getDefaultStudy(query, options, metadataManager);
        QueryOperation formatOperator = null;
        if (isValidParam(query, FORMAT)) {
            extractGenotypeFromFormatFilter(query);

            Pair<QueryOperation, Map<String, String>> pair = parseFormat(query);
            formatOperator = pair.getKey();

            for (Map.Entry<String, String> entry : pair.getValue().entrySet()) {
                String sampleName = entry.getKey();
                if (defaultStudy == null) {
                    throw VariantQueryException.missingStudyForSample(sampleName, metadataManager.getStudyNames());
                }
                Integer sampleId = metadataManager.getSampleId(defaultStudy.getId(), sampleName, true);
                if (sampleId == null) {
                    throw VariantQueryException.sampleNotFound(sampleName, defaultStudy.getName());
                }
                List<String> formats = splitValue(entry.getValue()).getValue();
                for (String format : formats) {
                    String[] split = splitOperator(format);
                    VariantFileHeaderComplexLine line = defaultStudy.getVariantHeaderLine("FORMAT", split[0]);
                    if (line == null) {
                        throw VariantQueryException.malformedParam(FORMAT, query.getString(FORMAT.key()),
                                "FORMAT field \"" + split[0] + "\" not found. Available keys in study: "
                                        + defaultStudy.getVariantHeaderLines("FORMAT").keySet());
                    }
                }
            }
        }

        if (isValidParam(query, INFO)) {
            Pair<QueryOperation, Map<String, String>> pair = parseInfo(query);
            if (isValidParam(query, FILE) && pair.getKey() != null) {
                QueryOperation fileOperator = checkOperator(query.getString(FILE.key()));
                if (fileOperator != null && pair.getKey() != fileOperator) {
                    throw VariantQueryException.mixedAndOrOperators(FILE, INFO);
                }
            }
            for (Map.Entry<String, String> entry : pair.getValue().entrySet()) {
                String fileName = entry.getKey();
                if (defaultStudy == null) {
                    throw VariantQueryException.missingStudyForFile(fileName, metadataManager.getStudyNames());
                }
                Integer fileId = metadataManager.getFileId(defaultStudy.getId(), fileName, true);
                if (fileId == null) {
                    throw VariantQueryException.fileNotFound(fileName, defaultStudy.getName());
                }
                List<String> infos = splitValue(entry.getValue()).getValue();
                for (String info : infos) {
                    String[] split = splitOperator(info);
                    VariantFileHeaderComplexLine line = defaultStudy.getVariantHeaderLine("INFO", split[0]);
                    if (line == null) {
                        throw VariantQueryException.malformedParam(INFO, query.getString(INFO.key()),
                                "INFO field \"" + split[0] + "\" not found. Available keys in study: "
                                        + defaultStudy.getVariantHeaderLines("INFO").keySet());
                    }
                }
            }
        }

        QueryOperation genotypeOperator = null;
        VariantQueryParam genotypeParam = null;
        if (isValidParam(query, SAMPLE)) {
            if (isValidParam(query, GENOTYPE)) {
                throw VariantQueryException.malformedParam(SAMPLE, query.getString(SAMPLE.key()),
                        "Can not be used along with filter \"" + GENOTYPE.key() + '"');
            }
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
            if (loadedGenotypes.contains(GenotypeClass.NA_GT_VALUE)
                    || defaultStudy.getAttributes().getBoolean(EXCLUDE_GENOTYPES.key(), EXCLUDE_GENOTYPES.defaultValue())) {
                genotypes = GenotypeClass.NA_GT_VALUE;
            } else {
                genotypes = String.join(",", GenotypeClass.MAIN_ALT.filter(loadedGenotypes));
            }

            Pair<QueryOperation, List<String>> pair = VariantQueryUtils.splitValue(query.getString(SAMPLE.key()));
            genotypeOperator = pair.getLeft();

            StringBuilder sb = new StringBuilder();
            for (String sample : pair.getValue()) {
                if (sb.length() > 0) {
                    sb.append(genotypeOperator.separator());
                }
                sb.append(sample).append(IS).append(genotypes);
            }
            query.remove(SAMPLE.key());
            query.put(GENOTYPE.key(), sb.toString());
        } else if (isValidParam(query, GENOTYPE)) {
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
            throw VariantQueryException.mixedAndOrOperators(FORMAT, genotypeParam);
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
            for (String sample : query.getAsStringList(param.key())) {
                Integer sampleId = metadataManager.getSampleId(defaultStudy.getId(), sample);
                if (sampleId == null) {
                    throw VariantQueryException.sampleNotFound(sample, defaultStudy.getName());
                }
                SampleMetadata sampleMetadata = metadataManager.getSampleMetadata(defaultStudy.getId(), sampleId);
                if (!TaskMetadata.Status.READY.equals(sampleMetadata.getMendelianErrorStatus())) {
                    throw VariantQueryException.malformedParam(param, "Sample \"" + sampleMetadata.getName()
                            + "\" does not have the Mendelian Errors precomputed yet");
                }
            }
        }

        if (isValidParam(query, SCORE)) {
            String value = query.getString(SCORE.key());
            List<String> values = splitValue(value).getValue();
            for (String scoreFilter : values) {
                String variantScore = splitOperator(scoreFilter)[0];
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
            VariantQueryFields selectVariantElements =
                    parseVariantQueryFields(query, options, metadataManager);
            // Apply the sample pagination.
            // Remove the sampleLimit and sampleSkip to avoid applying the pagination twice
            query.remove(SAMPLE_SKIP.key());
            query.remove(SAMPLE_LIMIT.key());
            query.put(NUM_TOTAL_SAMPLES.key(), selectVariantElements.getNumTotalSamples());
            query.put(NUM_SAMPLES.key(), selectVariantElements.getNumSamples());

            if (!isValidParam(query, INCLUDE_STUDY)) {
                List<String> includeStudy = new ArrayList<>();
                for (Integer studyId : selectVariantElements.getStudies()) {
                    includeStudy.add(selectVariantElements.getStudyMetadatas().get(studyId).getName());
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

        List<String> formats = getIncludeFormats(query);
        if (formats == null) {
            formats = Collections.singletonList(ALL);
        } else if (formats.isEmpty()) {
            formats = Collections.singletonList(NONE);
        }

        query.put(INCLUDE_FORMAT.key(), formats);
        query.remove(INCLUDE_GENOTYPE.key(), formats);
    }

    protected String preProcessGenotypesFilter(Map<Object, List<String>> map, QueryOperation op, List<String> loadedGenotypes) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<Object, List<String>> entry : map.entrySet()) {
//                List<String> genotypes = GenotypeClass.filter(entry.getValue(), loadedGenotypes, defaultGenotypes);
            List<String> genotypes = new ArrayList<>(entry.getValue());
            for (String genotypeStr : entry.getValue()) {
                boolean negated = isNegated(genotypeStr);
                if (negated) {
                    removeNegation(genotypeStr);
                }
                Genotype genotype = new Genotype(genotypeStr);
                int[] allelesIdx = genotype.getAllelesIdx();
                boolean multiallelic = false;
                for (int i = 0; i < allelesIdx.length; i++) {
                    if (allelesIdx[i] > 1) {
                        allelesIdx[i] = 2;
                        multiallelic = true;
                    }
                }
                if (multiallelic) {
                    String regex = genotype.toString()
                            .replace(".", "\\.")
                            .replace("2", "([2-9]|[0-9][0-9])");// Replace allele "2" with "any number >= 2")
                    Pattern pattern = Pattern.compile(regex);
                    for (String loadedGenotype : loadedGenotypes) {
                        if (pattern.matcher(loadedGenotype).matches()) {
                            genotypes.add((negated ? NOT : "") + loadedGenotype);
                        }
                    }
                }
            }
            genotypes = GenotypeClass.filter(genotypes, loadedGenotypes);

            if (genotypes.isEmpty()) {
                // TODO: Do fast fail, NO RESULTS!
                genotypes = Collections.singletonList(GenotypeClass.NONE_GT_VALUE);
            }

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
    public static VariantQueryXref parseXrefs(Query query) {
        VariantQueryXref xrefs = new VariantQueryXref();
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

    public static class VariantQueryXref {
        private final List<String> genes = new LinkedList<>();
        private final List<Variant> variants = new LinkedList<>();
        private final List<String> ids = new LinkedList<>();
        private final List<String> otherXrefs = new LinkedList<>();

        /**
         * @return List of genes found at {@link VariantQueryParam#GENE} and {@link VariantQueryParam#ANNOT_XREF}
         */
        public List<String> getGenes() {
            return genes;
        }

        /**
         * @return List of variants found at {@link VariantQueryParam#ANNOT_XREF} and {@link VariantQueryParam#ID}
         */
        public List<Variant> getVariants() {
            return variants;
        }

        /**
         * @return List of ids found at {@link VariantQueryParam#ID}
         */
        public List<String> getIds() {
            return ids;
        }

        /**
         * @return List of other xrefs found at
         * {@link VariantQueryParam#ANNOT_XREF},
         * {@link VariantQueryParam#ID},
         * {@link VariantQueryParam#ANNOT_CLINVAR},
         * {@link VariantQueryParam#ANNOT_COSMIC}
         */
        public List<String> getOtherXrefs() {
            return otherXrefs;
        }
    }
}
