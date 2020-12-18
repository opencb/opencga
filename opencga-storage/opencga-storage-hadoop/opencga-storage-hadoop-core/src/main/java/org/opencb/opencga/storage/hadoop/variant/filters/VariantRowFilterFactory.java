package org.opencb.opencga.storage.hadoop.variant.filters;

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.query.KeyOpValue;
import org.opencb.opencga.storage.core.variant.query.Values;
import org.opencb.opencga.storage.core.variant.query.VariantQueryParser;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantSqlQueryParser;
import org.opencb.opencga.storage.hadoop.variant.converters.VariantRow;
import org.opencb.opencga.storage.hadoop.variant.converters.study.HBaseToStudyEntryConverter;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

public class VariantRowFilterFactory {

    private final List<String> fileAttributes;
    private final List<String> fixedFormat;


    public VariantRowFilterFactory(List<String> fileAttributes, List<String> fixedFormat) {
        this.fileAttributes = fileAttributes;
        this.fixedFormat = fixedFormat;
    }

    public Predicate<VariantRow.FileColumn> buildFileDataFilter(String fileDataQuery) {
        if (StringUtils.isEmpty(fileDataQuery)) {
            return f -> true;
        }
        Values<KeyOpValue<String, String>> filters
                = VariantQueryUtils.parseMultiKeyValueFilterComparators(VariantQueryParam.FILE_DATA, fileDataQuery);
//        System.out.println("filters = " + filters);
        List<Predicate<VariantRow.FileColumn>> predicates = new ArrayList<>(filters.size());
        for (KeyOpValue<String, String> filter : filters.getValues()) {
            final Predicate<VariantRow.FileColumn> predicate;
            if (filter.getKey().equals(StudyEntry.FILTER)) {
                Values<String> filterValues = VariantQueryUtils.splitValues(filter.getValue());
                List<Predicate<VariantRow.FileColumn>> filterPredicates = new ArrayList<>(filterValues.size());
                for (String filterValue : filterValues.getValues()) {
                    if (filter.getOp().equals(VariantQueryUtils.OP_EQ)) {
                        filterPredicates.add(fileColumn -> fileColumn.getFilter().contains(filterValue));
                    } else if (filter.getOp().equals(VariantQueryUtils.OP_NEQ)) {
                        filterPredicates.add(fileColumn -> !fileColumn.getFilter().contains(filterValue));
                    } else {
                        throw new IllegalArgumentException("Unsupported operator '" + filter.getOp() + "' for fileData FILTER");
                    }
                }
                predicate = mergePredicates(filterPredicates, filterValues.getOperation());
            } else {
                int idx;
                if (filter.getKey().equals(StudyEntry.FILTER)) {
                    idx = HBaseToStudyEntryConverter.FILE_FILTER_IDX;
                } else if (filter.getKey().equals(StudyEntry.QUAL)) {
                    idx = HBaseToStudyEntryConverter.FILE_QUAL_IDX;
                } else {
                    idx = HBaseToStudyEntryConverter.FILE_INFO_START_IDX + fileAttributes.indexOf(filter.getKey());
                }
                String filterValue = filter.getValue();
                if (StringUtils.isNumeric(filterValue)) {
                    // Numeric value
                    float filterNumericValue = Float.parseFloat(filterValue);
                    switch (filter.getOp()) {
                        case VariantQueryUtils.OP_LT:
                            predicate = fileColumn -> {
                                Float value = fileColumn.getFloatValue(idx);
                                return value != null && value < filterNumericValue;
                            };
                            break;
                        case VariantQueryUtils.OP_LE:
                            predicate = fileColumn -> {
                                Float value = fileColumn.getFloatValue(idx);
                                return value != null && value <= filterNumericValue;
                            };
                            break;
                        case VariantQueryUtils.OP_GT:
                            predicate = fileColumn -> {
                                Float value = fileColumn.getFloatValue(idx);
                                return value != null && value > filterNumericValue;
                            };
                            break;
                        case VariantQueryUtils.OP_GE:
                            predicate = fileColumn -> {
                                Float value = fileColumn.getFloatValue(idx);
                                return value != null && value >= filterNumericValue;
                            };
                            break;
                        case VariantQueryUtils.OP_EQ:
                            predicate = fileColumn -> {
                                Float value = fileColumn.getFloatValue(idx);
                                return value != null && value == filterNumericValue;
                            };
                            break;
                        case VariantQueryUtils.OP_NEQ:
                            predicate = fileColumn -> {
                                Float value = fileColumn.getFloatValue(idx);
                                return value != null && value != filterNumericValue;
                            };
                            break;
                        default:
                            throw new IllegalArgumentException("Unknown operator '" + filter.getOp() + "'");
                    }
                } else {
                    switch (filter.getOp()) {
                        case VariantQueryUtils.OP_LT:
                            predicate = fileColumn -> fileColumn.getString(idx).compareTo(filterValue) < 0;
                            break;
                        case VariantQueryUtils.OP_LE:
                            predicate = fileColumn -> fileColumn.getString(idx).compareTo(filterValue) <= 0;
                            break;
                        case VariantQueryUtils.OP_GT:
                            predicate = fileColumn -> fileColumn.getString(idx).compareTo(filterValue) > 0;
                            break;
                        case VariantQueryUtils.OP_GE:
                            predicate = fileColumn -> fileColumn.getString(idx).compareTo(filterValue) >= 0;
                            break;
                        case VariantQueryUtils.OP_EQ:
                            predicate = fileColumn -> fileColumn.getString(idx).equals(filterValue);
                            break;
                        case VariantQueryUtils.OP_NEQ:
                            predicate = fileColumn -> !fileColumn.getString(idx).equals(filterValue);
                            break;
                        default:
                            throw new IllegalArgumentException("Unknown operator '" + filter.getOp() + "'");
                    }
                }
            }
            predicates.add(predicate);
        }
        return mergePredicates(predicates, filters.getOperation());
    }

    public Predicate<VariantRow.SampleColumn> buildSampleDataFilter(String sampleDataQuery) {

        if (StringUtils.isEmpty(sampleDataQuery)) {
            return c -> true;
        } else {
            Values<KeyOpValue<String, String>> filters
                    = VariantQueryUtils.parseMultiKeyValueFilterComparators(VariantQueryParam.SAMPLE_DATA, sampleDataQuery);

            List<Predicate<VariantRow.SampleColumn>> predicates = new ArrayList<>(filters.size());
            for (KeyOpValue<String, String> filter : filters.getValues()) {
                final Predicate<VariantRow.SampleColumn> predicate;

                int idx = fixedFormat.indexOf(filter.getKey());
                String filterValue = filter.getValue();
                if (StringUtils.isNumeric(filterValue)) {
                    // Numeric value
                    float filterNumericValue = Float.parseFloat(filterValue);
                    switch (filter.getOp()) {
                        case VariantQueryUtils.OP_LT:
                            predicate = sampleColumn -> {
                                Float value = sampleColumn.getSampleDataFloat(idx);
                                return value != null && value < filterNumericValue;
                            };
                            break;
                        case VariantQueryUtils.OP_LE:
                            predicate = sampleColumn -> {
                                Float value = sampleColumn.getSampleDataFloat(idx);
                                return value != null && value <= filterNumericValue;
                            };
                            break;
                        case VariantQueryUtils.OP_GT:
                            predicate = sampleColumn -> {
                                Float value = sampleColumn.getSampleDataFloat(idx);
                                return value != null && value > filterNumericValue;
                            };
                            break;
                        case VariantQueryUtils.OP_GE:
                            predicate = sampleColumn -> {
                                Float value = sampleColumn.getSampleDataFloat(idx);
                                return value != null && value >= filterNumericValue;
                            };
                            break;
                        case VariantQueryUtils.OP_EQ:
                            predicate = sampleColumn -> {
                                Float value = sampleColumn.getSampleDataFloat(idx);
                                return value != null && value == filterNumericValue;
                            };
                            break;
                        case VariantQueryUtils.OP_NEQ:
                            predicate = sampleColumn -> {
                                Float value = sampleColumn.getSampleDataFloat(idx);
                                return value != null && value != filterNumericValue;
                            };
                            break;
                        default:
                            throw new IllegalArgumentException("Unknown operator '" + filter.getOp() + "'");
                    }
                } else {
                    switch (filter.getOp()) {
                        case VariantQueryUtils.OP_LT:
                            predicate = fileColumn -> fileColumn.getSampleData(idx).compareTo(filterValue) < 0;
                            break;
                        case VariantQueryUtils.OP_LE:
                            predicate = fileColumn -> fileColumn.getSampleData(idx).compareTo(filterValue) <= 0;
                            break;
                        case VariantQueryUtils.OP_GT:
                            predicate = fileColumn -> fileColumn.getSampleData(idx).compareTo(filterValue) > 0;
                            break;
                        case VariantQueryUtils.OP_GE:
                            predicate = fileColumn -> fileColumn.getSampleData(idx).compareTo(filterValue) >= 0;
                            break;
                        case VariantQueryUtils.OP_EQ:
                        case VariantQueryUtils.OP_NEQ:
                            Values<String> filterValues = VariantQueryUtils.splitValues(filterValue);
                            if (filter.getKey().equals(VariantQueryUtils.GT)) {
                                filterValues = new Values<>(filter.getOp().equals(VariantQueryUtils.OP_EQ)
                                        ? VariantQueryUtils.QueryOperation.OR
                                        : VariantQueryUtils.QueryOperation.AND,
                                        VariantQueryParser.preProcessGenotypesFilter(filterValues.getValues(),
                                                VariantSqlQueryParser.DEFAULT_LOADED_GENOTYPES));
                            }
                            List<Predicate<VariantRow.SampleColumn>> filterPredicates = new ArrayList<>(filterValues.size());
                            for (String filterSubValue : filterValues.getValues()) {
                                if (filter.getOp().equals(VariantQueryUtils.OP_EQ)) {
                                    filterPredicates.add(fileColumn -> fileColumn.getSampleData(idx).equals(filterSubValue));
                                } else if (filter.getOp().equals(VariantQueryUtils.OP_NEQ)) {
                                    filterPredicates.add(fileColumn -> !fileColumn.getSampleData(idx).equals(filterSubValue));
                                } else {
                                    throw new IllegalArgumentException("Unsupported operator '" + filter.getOp() + "' for sampleData GT");
                                }
                            }
                            predicate = mergePredicates(filterPredicates, filterValues.getOperation());
                            break;
                        default:
                            throw new IllegalArgumentException("Unknown operator '" + filter.getOp() + "'");
                    }
                }
                predicates.add(predicate);
            }
            return mergePredicates(predicates, filters.getOperation());
        }
    }

    private <T> Predicate<T> mergePredicates(List<Predicate<T>> predicates,
                                                             VariantQueryUtils.QueryOperation operation) {
        if (predicates.size() == 1) {
            return predicates.get(0);
        } else {
            Predicate<T> p = predicates.get(0);
            for (int i = 1; i < predicates.size(); i++) {
                if (operation.equals(VariantQueryUtils.QueryOperation.OR)) {
                    p = p.or(predicates.get(i));
                } else {
                    p = p.and(predicates.get(i));
                }
            }
            return p;
        }
    }
}
