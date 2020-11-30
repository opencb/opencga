package org.opencb.opencga.storage.core.clinical.clinical;

import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.commons.datastore.mongodb.MongoDBQueryUtils;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.storage.core.clinical.ReportedVariantQueryParam;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.variant.search.solr.SolrQueryParser;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ClinicalQueryParser {

    private SolrQueryParser solrQueryParser;

    private static final Pattern OPERATION_DATE_PATTERN = Pattern.compile("^(<=?|>=?|!=|!?=?~|=?=?)([0-9]+)(-?)([0-9]*)");

    public ClinicalQueryParser(VariantStorageMetadataManager variantStorageMetadataManager) {
        solrQueryParser = new SolrQueryParser(variantStorageMetadataManager);
    }

    public SolrQuery parse(Query query, QueryOptions queryOptions) {
        // First, call SolrQueryParser.parse
        SolrQuery solrQuery = solrQueryParser.parse(query, queryOptions);

        String key;

        // ---------- ClinicalAnalysis ----------
        //
        // ID, description, disorder, files, proband ID, family ID, family phenotype name, family member ID

        // ClinicalAnalysis ID
        key = ReportedVariantQueryParam.CA_ID.key();
        if (StringUtils.isNotEmpty(key)) {
            solrQuery.addField(solrQueryParser.parseCategoryTermValue(key, query.getString(key)));
        }

        // ClinicalAnalysis description
        key = ReportedVariantQueryParam.CA_DESCRIPTION.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            solrQuery.addField(parseInfo(ClinicalVariantUtils.DESCRIPTION_PREFIX, query.getString(key),
                    ReportedVariantQueryParam.CA_INFO.key()));
        }

        // ClinicalAnalysis disorder
        key = ReportedVariantQueryParam.CA_DISORDER.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            solrQuery.addField(solrQueryParser.parseCategoryTermValue(key, query.getString(key)));
        }

        // ClinicalAnalysis files
        key = ReportedVariantQueryParam.CA_FILE.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            solrQuery.addField(solrQueryParser.parseCategoryTermValue(key, query.getString(key)));
        }

        // ClinicalAnalysis proband ID
        key = ReportedVariantQueryParam.CA_PROBAND_ID.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            solrQuery.addField(solrQueryParser.parseCategoryTermValue(key, query.getString(key)));
        }

        // ClinicalAnalysis proband disorders
        key = ReportedVariantQueryParam.CA_PROBAND_DISORDERS.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            solrQuery.addField(solrQueryParser.parseCategoryTermValue(key, query.getString(key)));
        }

        // ClinicalAnalysis proband phenotypes
        key = ReportedVariantQueryParam.CA_PROBAND_PHENOTYPES.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            solrQuery.addField(solrQueryParser.parseCategoryTermValue(key, query.getString(key)));
        }

        // ClinicalAnalysis family ID
        key = ReportedVariantQueryParam.CA_FAMILY_ID.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            solrQuery.addField(solrQueryParser.parseCategoryTermValue(key, query.getString(key)));
        }

        // ClinicalAnalysis family member IDs
        key = ReportedVariantQueryParam.CA_FAMILY_MEMBER_IDS.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            solrQuery.addField(solrQueryParser.parseCategoryTermValue(key, query.getString(key)));
        }

        key = ReportedVariantQueryParam.CA_COMMENTS.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            solrQuery.addField(parseInfo(ClinicalVariantUtils.COMMENT_PREFIX, query.getString(key),
                    ReportedVariantQueryParam.CA_INFO.key()));
        }

        // ---------- Interpretation ----------
        //
        //    ID, software name, software version, analyst name, panel name, creation date, more info

        // Interpretation ID
        key = ReportedVariantQueryParam.INT_ID.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            solrQuery.addField(solrQueryParser.parseCategoryTermValue(key, query.getString(key)));
        }

        // Interpretation software name
        key = ReportedVariantQueryParam.INT_SOFTWARE_NAME.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            solrQuery.addField(solrQueryParser.parseCategoryTermValue(key, query.getString(key)));
        }

        // Interpretation software version
        key = ReportedVariantQueryParam.INT_SOFTWARE_VERSION.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            solrQuery.addField(solrQueryParser.parseCategoryTermValue(key, query.getString(key)));
        }

        // Interpretation analysit name
        key = ReportedVariantQueryParam.INT_ANALYST_NAME.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            solrQuery.addField(solrQueryParser.parseCategoryTermValue(key, query.getString(key)));
        }

        // Interpretation panel names
        key = ReportedVariantQueryParam.INT_PANELS.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            solrQuery.addField(solrQueryParser.parseCategoryTermValue(key, query.getString(key)));
        }

        // Interpretation info: description, dependency, filters, comments
        key = ReportedVariantQueryParam.INT_DESCRIPTION.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            solrQuery.addField(parseInfo(ClinicalVariantUtils.DESCRIPTION_PREFIX, query.getString(key),
                    ReportedVariantQueryParam.INT_INFO.key()));
        }

        key = ReportedVariantQueryParam.INT_DEPENDENCY.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            solrQuery.addField(parseInfo(ClinicalVariantUtils.DEPENDENCY_PREFIX, query.getString(key),
                    ReportedVariantQueryParam.INT_INFO.key()));
        }

        key = ReportedVariantQueryParam.INT_FILTERS.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            solrQuery.addField(parseInfo(ClinicalVariantUtils.FILTER_PREFIX, query.getString(key),
                    ReportedVariantQueryParam.INT_INFO.key()));
        }

        key = ReportedVariantQueryParam.INT_COMMENTS.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            solrQuery.addField(parseInfo(ClinicalVariantUtils.COMMENT_PREFIX, query.getString(key),
                    ReportedVariantQueryParam.INT_INFO.key()));
        }

        // Interpretation creation date
        key = ReportedVariantQueryParam.INT_CREATION_DATE.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            solrQuery.addField(parseInterpretationCreationDate(query.getString(key)));
        }


        // ---------- Catalog ----------
        //
        //    project ID, assembly, study ID

        // Project
        key = ReportedVariantQueryParam.PROJECT_ID.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            solrQuery.addField(solrQueryParser.parseCategoryTermValue(key, query.getString(key)));
        }

        // Assembly
        key = ReportedVariantQueryParam.ASSEMBLY.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            solrQuery.addField(solrQueryParser.parseCategoryTermValue(key, query.getString(key)));
        }

        // Study
        key = ReportedVariantQueryParam.STUDY_ID.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            solrQuery.addField(solrQueryParser.parseCategoryTermValue(key, query.getString(key)));
        }

        // ---------- ReportedVariant ----------
        //
        //   deNovo quality score, comments

        key = ReportedVariantQueryParam.RV_DE_NOVO_QUALITY_SCORE.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            solrQuery.addField(solrQueryParser.parseNumericValue(key, query.getString(key)));
        }

        key = ReportedVariantQueryParam.RV_COMMENTS.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            solrQuery.addField(solrQueryParser.parseCategoryTermValue(key, query.getString(key)));
        }


        // ---------- ReportedEvent ----------
        //
        parseReportedEvent(query, solrQuery);

        return solrQuery;
    }

    private String parseInterpretationCreationDate(String creationDateStr) {
        StringBuilder filter = new StringBuilder();

        String condition;
        boolean firstCondition = true;

        String logicalComparator = " OR ";
        MongoDBQueryUtils.ComparisonOperator comparator;

        String[] creationDates = creationDateStr.split("[,;]");
        for (String creationDate: creationDates) {
            Matcher matcher = OPERATION_DATE_PATTERN.matcher(creationDate);
            String op = "";
            String queryValueString = creationDate;
            if (matcher.find()) {
                op = matcher.group(1);
                queryValueString = matcher.group(2);
            }
            comparator = MongoDBQueryUtils.getComparisonOperator(op, QueryParam.Type.DATE);
            List<String> dateList = new ArrayList<>();
            dateList.add(queryValueString);
            if (!matcher.group(3).isEmpty()) {
                dateList.add(matcher.group(4));
                comparator = MongoDBQueryUtils.ComparisonOperator.BETWEEN;
            }
            // dateList is a list of 1 or 2 strings (dates). Only one will be expected when something like the following is passed:
            // =20171210, 20171210, >=20171210, >20171210, <20171210, <=20171210
            // When 2 strings are passed, we will expect it to be a range such as: 20171201-20171210
            Date date = convertStringToDate(dateList.get(0));

            condition = null;
            switch (comparator) {
                case BETWEEN:
                    if (dateList.size() == 2) {
                        Date to = convertStringToDate(dateList.get(1));
                        condition = ReportedVariantQueryParam.INT_CREATION_DATE.key() + ":{" + date.getTime() + " TO " + to.getTime() + "}";
                    }
                    break;
                case EQUALS:
                    condition = ReportedVariantQueryParam.INT_CREATION_DATE.key() + ":" + date.getTime();
                    break;
                case GREATER_THAN:
                    condition = ReportedVariantQueryParam.INT_CREATION_DATE.key() + ":{" + date.getTime() + " TO *]";
                    break;
                case GREATER_THAN_EQUAL:
                    condition = ReportedVariantQueryParam.INT_CREATION_DATE.key() + ":[" + date.getTime() + " TO *]";
                    break;
                case LESS_THAN:
                    condition = ReportedVariantQueryParam.INT_CREATION_DATE.key() + ":[* TO " + date.getTime() + "}";
                    break;
                case LESS_THAN_EQUAL:
                    condition = ReportedVariantQueryParam.INT_CREATION_DATE.key() + ":[* TO " + date.getTime() + "]";
                    break;
                default:
                    break;
            }
            if (condition != null) {
                if (!firstCondition) {
                    filter.append(logicalComparator);
                }
                firstCondition = false;
                filter.append("(").append(condition).append(")");
            }
        }

        return filter.toString();
    }

    private Date convertStringToDate(String stringDate) {
        if (stringDate.length() == 4) {
            stringDate = stringDate + "0101";
        } else if (stringDate.length() == 6) {
            stringDate = stringDate + "01";
        }
        String myDate = String.format("%-14s", stringDate).replace(" ", "0");
        LocalDateTime localDateTime = LocalDateTime.parse(myDate, DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
        // We convert it to date because it is the type used by mongo
        return Date.from(localDateTime.atZone(ZoneId.systemDefault()).toInstant());
    }


    private String parseInfo(String prefix, String value, String solrFieldName) {
        if (prefix.equals(ClinicalVariantUtils.DEPENDENCY_PREFIX)) {
            return parseInterpretationDependency(value);
        }

        String val = value.replace("\"", "");
        String wildcard = "*";
        String logicalComparator = " OR ";
        String[] values = val.split("[,;]");

        StringBuilder filter = new StringBuilder();
        for (int i = 0; i < values.length; ++i) {
            if (filter.length() > 0) {
                filter.append(logicalComparator);
            }
            filter.append(solrFieldName).append(":\"").append(prefix).append(wildcard).append(values[i]).append(wildcard).append("\"");
        }

        return filter.toString();
    }

    private String parseInterpretationDependency(String dependency) {
        String wildcard = "*";
        String logicalComparator = " OR ";
        String[] values = dependency.split("[,;]");
        StringBuilder filter = new StringBuilder();
        for (int i = 0; i < values.length; i++) {
            String[] dependencySplit = values[i].split(":");
            if (filter.length() > 0) {
                filter.append(logicalComparator);
            }
            if (dependencySplit.length == 1) {
                filter.append(ReportedVariantQueryParam.INT_INFO.key()).append(":\"").append(ClinicalVariantUtils.DEPENDENCY_PREFIX)
                        .append(wildcard).append(dependencySplit[0]).append(wildcard).append("\"");
            } else if (dependencySplit.length == 2) {
                filter.append(ReportedVariantQueryParam.INT_INFO.key()).append(":\"").append(ClinicalVariantUtils.DEPENDENCY_PREFIX)
                        .append(wildcard).append(dependencySplit[0]).append(ClinicalVariantUtils.FIELD_SEPARATOR)
                        .append(dependencySplit[1]).append(wildcard).append("\"");
            }
        }
        return filter.toString();
    }

    private void parseReportedEvent(Query query, SolrQuery solrQuery) {
        List<List<String>> combinations = new ArrayList<>();

        String key = ReportedVariantQueryParam.RE_PHENOTYPE_NAMES.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            updateCombinations(query.getAsStringList(key), combinations);
        }

        key = ReportedVariantQueryParam.RE_CONSEQUENCE_TYPE_IDS.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            updateCombinations(query.getAsStringList(key), combinations);
        }

        Set<String> xrefs = new HashSet<>();
        key = ReportedVariantQueryParam.RE_GENE_NAMES.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            xrefs.addAll(query.getAsStringList(key));
        }
        key = ReportedVariantQueryParam.RE_XREFS.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            xrefs.addAll(query.getAsStringList(key));
        }
        if (xrefs.size() > 0) {
            updateCombinations(query.getAsStringList(key), combinations);
        }

        key = ReportedVariantQueryParam.RE_PANEL_IDS.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            updateCombinations(query.getAsStringList(key), combinations);
        }

        key = ReportedVariantQueryParam.RE_ACMG.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            updateCombinations(query.getAsStringList(key), combinations);
        }

        key = ReportedVariantQueryParam.RE_CLINICAL_SIGNIFICANCE.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            updateCombinations(query.getAsStringList(key), combinations);
        }

        key = ReportedVariantQueryParam.RE_DRUG_RESPONSE.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            updateCombinations(query.getAsStringList(key), combinations);
        }

        key = ReportedVariantQueryParam.RE_TRAIT_ASSOCIATION.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            updateCombinations(query.getAsStringList(key), combinations);
        }

        key = ReportedVariantQueryParam.RE_FUNCTIONAL_EFFECT.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            updateCombinations(query.getAsStringList(key), combinations);
        }

        key = ReportedVariantQueryParam.RE_TUMORIGENESIS.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            updateCombinations(query.getAsStringList(key), combinations);
        }

        key = ReportedVariantQueryParam.RE_OTHER_CLASSIFICATION.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            updateCombinations(query.getAsStringList(key), combinations);
        }

        key = ReportedVariantQueryParam.RE_ROLES_IN_CANCER.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            updateCombinations(query.getAsStringList(key), combinations);
        }

        key = ReportedVariantQueryParam.RE_TIER.key();
        if (StringUtils.isNotEmpty(query.getString(key))) {
            updateCombinations(query.getAsStringList(key), combinations);
        }

        StringBuilder sb = new StringBuilder();


        key = ReportedVariantQueryParam.RE_JUSTIFICATION.key();
        List<String> justifications = query.getAsStringList(key);

        boolean firstOR;
        boolean firstAND = true;
        for (int k = 0; k < combinations.size(); k++) {
            firstOR = true;
            if (!firstAND) {
                sb.append(" AND ");
            }
            sb.append("(");
            for (int i = 0; i < combinations.get(k).size() - 1; i++) {
                for (int j = i + 1; j < combinations.get(k).size(); j++) {
                    key = combinations.get(k).get(i) + ClinicalVariantUtils.FIELD_SEPARATOR + combinations.get(k).get(j);
                    if (ListUtils.isEmpty(justifications)) {
                        if (!firstOR) {
                            sb.append(" OR ");
                        }
                        sb.append(ReportedVariantQueryParam.RE_AUX.key()).append(":\"").append(key).append("\"");
                        firstOR = false;
                    } else {
                        for (String justification: justifications) {
                            if (!firstOR) {
                                sb.append(" OR ");
                            }
                            sb.append(ReportedVariantQueryParam.RE_JUSTIFICATION.key()).append("_").append(key).append(":\"*")
                                    .append(justification).append("*\"");
                            firstOR = false;
                        }
                    }
                }
            }
            sb.append(")");
        }

        solrQuery.addField(sb.toString());
    }

    private void updateCombinations(List<String> values, List<List<String>> combinations) {
        if (ListUtils.isEmpty(combinations)) {
            for (String value: values) {
                combinations.add(Collections.singletonList(value));
            }
        } else {
            int size = combinations.size();
            for (int i = 0; i < size; i++) {
                List<String> list = combinations.get(i);
                for (int j = 1; j < values.size(); j++) {
                    List<String> updatedList = new ArrayList<>(list);
                    updatedList.add(values.get(j));
                    combinations.add(updatedList);
                }
                list.add(values.get(0));
            }
        }
    }
}
