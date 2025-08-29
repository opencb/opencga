package org.opencb.opencga.storage.core.variant.search.solr;

import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.CohortMetadata;
import org.opencb.opencga.storage.core.metadata.models.project.SearchIndexMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;

import java.util.List;

import static org.opencb.opencga.storage.core.variant.search.VariantSearchToVariantConverter.*;
import static org.opencb.opencga.storage.core.variant.search.VariantSearchUtils.FIELD_SEPARATOR;

public class FunctionalStatsVariantStorageSolrQueryParser extends VariantStorageSolrQueryParser {
    // Experimental! No performance improvement seen on small datasets. Disabled by default.
    private final boolean auxQueryStats = false;
    private final boolean functionQueryStats;


    protected FunctionalStatsVariantStorageSolrQueryParser(VariantStorageMetadataManager variantStorageMetadataManager,
                                                        SearchIndexMetadata searchIndexMetadata) {
        super(variantStorageMetadataManager, searchIndexMetadata);
        functionQueryStats = VariantSearchManager.isStatsFunctionalQueryEnabled(searchIndexMetadata);
    }


    @Override
    protected void parseVariantStatsFilter(VariantQueryParam param, String value, FreqField field, FreqType type,
                                           String study, String cohort, String op, String numValue,
                                           boolean addOr, List<String> filters, List<String> auxFilters) {
        if (!functionQueryStats) {
            // Backwards compatibility for altStats and passStats
            super.parseVariantStatsFilter(param, value, field, type, study, cohort, op, numValue, addOr, filters, auxFilters);
        } else {
            int studyId = variantStorageMetadataManager.getStudyId(study);
            CohortMetadata cohortMetadata = variantStorageMetadataManager.getCohortMetadata(studyId, cohort);
            if (cohortMetadata == null) {
                throw VariantQueryException.malformedParam(param, value, "Missing cohort " + cohort + " in study " + study);
            }

            if (field == FreqField.ALT_STATS) {
                StringBuilder sb = new StringBuilder();

                // See https://solr.apache.org/guide/solr/latest/query-guide/local-params.html#specifying-the-parameter-value-with-the-v-key
                // The v key is used to specify the value of the parameter. This is required so we can use AND/OR
                // e.g. {!frange l=0 u=0.1 v='div(altCount, sub(cohortSize * 2 , alleleCountDiff))'}

                String functionQuery;

                switch (type) {
                    case ALT:
                        functionQuery = altFreqF(study, cohortMetadata);
                        if (auxQueryStats) {
                            // Add extra FQ query for optimization
                            // (missingAllelesOrGap == 0 AND alternateAlleles < $expectedAlleles) OR missingAllelesOrGap > 0
                            float expectedAlleles = Float.parseFloat(numValue) * cohortMetadata.getSamples().size() * 2;
                            String range; // "[X TO Y]";
                            switch (op) {
                                case "<":
                                case "<<":
                                    range = "[* TO " + expectedAlleles + "}";
                                    break;
                                case "<=":
                                case "<<=":
                                    range = "[* TO " + expectedAlleles + "]";
                                    break;
                                case ">":
                                case ">>":
                                    range = "{" + expectedAlleles + " TO *]";
                                    break;
                                case ">=":
                                case ">>=":
                                    range = "[" + expectedAlleles + " TO *]";
                                    break;
                                default:
                                    throw new IllegalArgumentException("Invalid operator '" + op + "' for field " + field
                                            + ". Valid values are: <, <=, >, >=.");
                            }
                            String cohortName = cohortMetadata.getName();
                            String altAlleleCount = buildStatsAltAlleleCountField(studyIdToSearchModel(study), cohortName);
                            String missingAllelesOrGap = buildStatsAlleleMissGapCountField(studyIdToSearchModel(study), cohortName);
                            auxFilters.add("(" + missingAllelesOrGap + ":0 AND " + altAlleleCount + ":" + range + ")"
                                    + " OR " + missingAllelesOrGap + ":[1 TO *]");
                        }
                        break;
                    case REF:
                        functionQuery = refFreqF(study, cohortMetadata);
                        break;
                    case MAF:
                        functionQuery = mafFreqF(study, cohortMetadata);
                        break;
                    default:
                        throw new IllegalArgumentException("Invalid type '" + type + "' for field " + field
                                + ". Valid values are: MAF, REF, ALT.");
                }

                sb.append("{!frange ")
                        .append(getFrangeQuery(op, Float.parseFloat(numValue)))
                        .append(" v='").append(functionQuery).append("'}");

                filters.add(sb.toString());
            } else if (field == FreqField.PASS_STATS) {
                // concat expression, e.g.: value:[0 TO 12]
                filters.add(getRange(field + FIELD_SEPARATOR + studyIdToSearchModel(study) + FIELD_SEPARATOR,
                        cohort, op, numValue, addOr));
            } else {
                throw new IllegalArgumentException("Unknown field: " + field);
            }
        }
    }


    /**
     * Obtain a FunctionQuery to calculate the AlleleCount.
     *
     * AlleleCount ~ count of non-missing alleles
     *             ~ expected alleles of diploid GTs - (missing alleles + gaps from non-diploid GTs)
     *
     * AlleleCount = expectedNumAlleles - (missingAlleles + gapAlleles)
     * AlleleCount = numSamples * 2 - (missingAlleles + gapAlleles)
     *
     * @param study          Study Name
     * @param cohortMetadata Cohort Metadata
     * @return function query to calculate the AlleleCount
     */
    private static String alleleCountF(String study, CohortMetadata cohortMetadata) {
        String studySearchModel = studyIdToSearchModel(study);
        double expectedNumAlleles = cohortMetadata.getSamples().size() * 2;
        String alleleMissOrGap = buildStatsAlleleMissGapCountField(studySearchModel, cohortMetadata.getName());

        return "sub(" + expectedNumAlleles + ", " + alleleMissOrGap + ")";
    }

    /**
     * Obtain a FunctionQuery to calculate the AlternateAlleleFrequency.
     *
     * AlternateAlleleFrequency = alternateAlleles / alelleCount
     *
     * @param study          Study Name
     * @param cohortMetadata Cohort Metadata
     * @return function query to calculate the AlternateAlleleFrequency
     */
    private static String altFreqF(String study, CohortMetadata cohortMetadata) {
        String studySearchModel = studyIdToSearchModel(study);
        String altAlleleCount = buildStatsAltAlleleCountField(studySearchModel, cohortMetadata.getName());

        return "div(" + altAlleleCount + ", " + alleleCountF(study, cohortMetadata) + ")";
    }

    /**
     * Obtain a FunctionQuery to calculate the ReferenceAlleleFrequency.
     *
     * ReferenceAlleleFrequency = referenceAlleles / alleleCount
     * referenceAlleles = alleleCount - (alternateAlleles + otherAlleles)
     * nonRefAlleles = alternateAlleles + otherAlleles == alleleCount - referenceAlleles
     * ReferenceAlleleFrequency = (alleleCount - nonRefAlleles) / alelleCount
     * ReferenceAlleleFrequency = 1 - (nonRefAlleles / alleleCount)
     *
     * @param study          Study Name
     * @param cohortMetadata Cohort Metadata
     * @return function query to calculate the ReferenceAlleleFrequency
     */
    private static String refFreqF(String study, CohortMetadata cohortMetadata) {
        String studySearchModel = studyIdToSearchModel(study);
        String nonRefCount = buildStatsAlleleNonRefCountField(studySearchModel, cohortMetadata.getName());

        return "sub(1, div(" + nonRefCount + ", " + alleleCountF(study, cohortMetadata) + "))";
    }

    /**
     * Obtain a FunctionQuery to calculate the Minor Allele Frequency.
     *
     * MinorAlleleFrequency = min(AlternateAlleleFrequency, ReferenceAlleleFrequency)
     *
     * @param study          Study Name
     * @param cohortMetadata Cohort Metadata
     * @return
     */
    private static String mafFreqF(String study, CohortMetadata cohortMetadata) {
        String studySearchModel = studyIdToSearchModel(study);
        String altFreq = altFreqF(studySearchModel, cohortMetadata);
        String refFreq = refFreqF(studySearchModel, cohortMetadata);
        return "min(" + altFreq + ", " + refFreq + ")";
    }

    /**
     * Translate num operator into a FunctionRangeQuery (frange).
     *
     * @see <a href="https://solr.apache.org/guide/solr/latest/query-guide/other-parsers.html#function-range-query-parser"></a>
     *
     * @param op       Operator, e.g.: <, <=, <<, <<=, >, >=, >>, >>=
     * @param value    Num value
     * @return         String with the frange query parameters
     */
    protected static String getFrangeQuery(String op, float value) {
        StringBuilder frangeVars = new StringBuilder();
        switch (op) {
            case "<":
            case "<<":
                // Exclude the upper bound
                frangeVars.append("u=").append(value).append(" incu=false");
                break;
            case "<=":
            case "<<=":
                // Include the upper bound
                frangeVars.append("u=").append(value).append(" incu=true");
                break;
            case ">":
            case ">>":
                // Exclude the lower bound
                frangeVars.append("l=").append(value).append(" incl=false");
                break;
            case ">=":
            case ">>=":
                // Include the lower bound
                frangeVars.append("l=").append(value).append(" incl=true");
                break;
            default:
                throw new IllegalArgumentException("Invalid operator: " + op);
        }
        return frangeVars.toString();
    }

}
