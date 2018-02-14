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

package org.opencb.opencga.storage.hadoop.variant.adaptors;

import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.core.Region;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveRowKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixKeyFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.*;
import static org.opencb.opencga.storage.hadoop.variant.index.VariantTableStudyRow.*;
import static org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper.VariantColumn.*;
import static org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper.buildFileColumnKey;
import static org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper.buildSampleColumnKey;

/**
 * Created on 07/07/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantHBaseQueryParser {
    private static Logger logger = LoggerFactory.getLogger(VariantHBaseQueryParser.class);

    private final GenomeHelper genomeHelper;
    private final StudyConfigurationManager studyConfigurationManager;

    public static final Set<VariantQueryParam> SUPPORTED_QUERY_PARAMS = Collections.unmodifiableSet(Sets.newHashSet(
//            STUDIES, // Not fully supported
//            FILES,   // Not supported at all
//            SAMPLES, // May be supported
//            REGION,  // Only one region supported
            INCLUDE_FILE,
            INCLUDE_STUDY,
            INCLUDE_SAMPLE,
            UNKNOWN_GENOTYPE));

    public VariantHBaseQueryParser(GenomeHelper genomeHelper, StudyConfigurationManager studyConfigurationManager) {
        this.genomeHelper = genomeHelper;
        this.studyConfigurationManager = studyConfigurationManager;
    }

    public static boolean isSupportedQuery(Query query) {
        Set<VariantQueryParam> otherParams = validParams(query);
        otherParams.removeAll(SUPPORTED_QUERY_PARAMS);



//        if (otherParams.contains(ID)) {
//            List<String> ids = query.getAsStringList(ID.key());
//            for (String id : ids) {
//                if (!VariantQueryUtils.isVariantId(id)) {
//                    // If there is any ID that is not a variantId, the query is not fully supported
//                    return false;
//                }
//            }
//            otherParams.remove(ID);
//        }

        return otherParams.isEmpty();
    }

    public Scan parseQuery(Query query, QueryOptions options) {
        VariantQueryUtils.SelectVariantElements selectElements =
                VariantQueryUtils.parseSelectElements(query, options, studyConfigurationManager);
        return parseQuery(selectElements, query, options);
    }

    public Scan parseQuery(VariantQueryUtils.SelectVariantElements selectElements, Query query, QueryOptions options) {

        Scan scan = new Scan();
        scan.addFamily(genomeHelper.getColumnFamily());
        FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
//        FilterList regionFilters = new FilterList(FilterList.Operator.MUST_PASS_ONE);
//        filters.addFilter(regionFilters);
        List<byte[]> columnPrefixes = new LinkedList<>();

        List<Region> regions = getRegions(query);

        if (regions != null && !regions.isEmpty()) {
            if (regions.size() > 1) {
                throw VariantQueryException.malformedParam(REGION, regions.toString(), "Unsupported multiple region filter");
            }
            Region region = regions.get(0);
            logger.debug("region = " + region);
            // TODO: Use MultiRowRangeFilter
            addRegionFilter(scan, region);
        } else {
            addDefaultRegionFilter(scan);
        }


//        if (isValidParam(query, ID)) {
//            List<String> ids = query.getAsStringList(ID.key());
//            List<byte[]> rowKeys = new ArrayList<>(ids.size());
//            for (String id : ids) {
//                Variant variant = VariantQueryUtils.toVariant(id);
//                if (variant != null) {
//                    byte[] rowKey = VariantPhoenixKeyFactory.generateVariantRowKey(variant);
//                    rowKeys.add(rowKey);
////                    regionFilters.addFilter(new RowFilter(CompareFilter.CompareOp.EQUAL, new ByteArrayComparable));
//                    regionFilters.addFilter(new PrefixFilter(CompareFilter.CompareOp.EQUAL, new ByteArrayComparable));
//                }
//            }
//        }


        if (!StringUtils.isEmpty(query.getString(GENE.key()))) {
            addValueFilter(filters, GENES.bytes(), query.getAsStringList(GENE.key()));
        }
        if (!StringUtils.isEmpty(query.getString(ANNOT_BIOTYPE.key()))) {
            addValueFilter(filters, BIOTYPE.bytes(), query.getAsStringList(ANNOT_BIOTYPE.key()));
        }

        if (isValidParam(query, ANNOTATION_EXISTS)) {
            if (!query.getBoolean(ANNOTATION_EXISTS.key())) {
                // Use a column different from FULL_ANNOTATION to read few elements from disk
//                byte[] annotationColumn = VariantPhoenixHelper.VariantColumn.FULL_ANNOTATION.bytes();
                byte[] annotationColumn = VariantPhoenixHelper.VariantColumn.SO.bytes();

                filters.addFilter(missingColumnFilter(annotationColumn));
                if (!selectElements.getFields().contains(VariantField.ANNOTATION)) {
                    scan.addColumn(genomeHelper.getColumnFamily(), annotationColumn);
                }
            } else {
                logger.warn("Filter " + ANNOTATION_EXISTS.key() + "=true not implemented in native mode");
            }
        }

        if (selectElements.getFields().contains(VariantField.STUDIES)) {
            for (Integer studyId : selectElements.getStudies()) {
                VariantStorageEngine.MergeMode mergeMode = VariantStorageEngine.MergeMode.from(
                        selectElements.getStudyConfigurations().get(studyId).getAttributes());
                List<String> studyColumns;
                if (selectElements.getFields().contains(VariantField.STUDIES_SAMPLES_DATA)
                        && mergeMode.equals(VariantStorageEngine.MergeMode.ADVANCED)) {
                    studyColumns = STUDY_COLUMNS;
                } else {
                    // If samples are not required, do not fetch all the fields
                    studyColumns = Collections.singletonList(HOM_REF);
                }
                for (String studyColumn : studyColumns) {
                    scan.addColumn(genomeHelper.getColumnFamily(), Bytes.toBytes(buildColumnKey(studyId, studyColumn)));
                }
            }

            if (selectElements.getFields().contains(VariantField.STUDIES_STATS)) {
                for (StudyConfiguration sc : selectElements.getStudyConfigurations().values()) {
                    for (Integer cohortId : sc.getCalculatedStats()) {
                        scan.addColumn(genomeHelper.getColumnFamily(),
                                VariantPhoenixHelper.getStatsColumn(sc.getStudyId(), cohortId).bytes());
                    }
                    for (Integer cohortId : sc.getInvalidStats()) {
                        scan.addColumn(genomeHelper.getColumnFamily(),
                                VariantPhoenixHelper.getStatsColumn(sc.getStudyId(), cohortId).bytes());
                    }
                }
            }

            selectElements.getSamples().forEach((studyId, sampleIds) -> {
                scan.addColumn(genomeHelper.getColumnFamily(), VariantPhoenixHelper.getStudyColumn(studyId).bytes());
                for (Integer sampleId : sampleIds) {
                    scan.addColumn(genomeHelper.getColumnFamily(), buildSampleColumnKey(studyId, sampleId));
                }
            });

            selectElements.getFiles().forEach((studyId, fileIds) -> {
                scan.addColumn(genomeHelper.getColumnFamily(), VariantPhoenixHelper.getStudyColumn(studyId).bytes());
                for (Integer fileId : fileIds) {
                    scan.addColumn(genomeHelper.getColumnFamily(), VariantPhoenixHelper.buildFileColumnKey(studyId, fileId));
                }
            });
        }

        StudyConfiguration defaultStudyConfiguration;
        if (isValidParam(query, STUDY)) {
            String value = query.getString(STUDY.key());
            VariantQueryUtils.QueryOperation operation = checkOperator(value);
            List<String> values = splitValue(value, operation);

            FilterList subFilters;
            if (operation == QueryOperation.OR) {
                subFilters = new FilterList(FilterList.Operator.MUST_PASS_ONE);
                filters.addFilter(subFilters);
            } else {
                subFilters = filters;
            }
            List<Integer> nonNegatedStudies = new ArrayList<>();
            for (String studyStr : values) {
                Integer studyId = studyConfigurationManager.getStudyId(studyStr, null);
                byte[] column = Bytes.toBytes(buildColumnKey(studyId, HOM_REF));
                if (isNegated(studyStr)) {
                    subFilters.addFilter(missingColumnFilter(column));
                } else {
                    nonNegatedStudies.add(studyId);
                    subFilters.addFilter(existingColumnFilter(column));
                }
            }
            if (nonNegatedStudies.size() == 1) {
                defaultStudyConfiguration = studyConfigurationManager.getStudyConfiguration(nonNegatedStudies.get(0), null).first();
            } else {
                defaultStudyConfiguration = null;
            }
        } else {
            List<Integer> studyIds = studyConfigurationManager.getStudyIds(options);
            if (studyIds.size() == 1) {
                defaultStudyConfiguration = studyConfigurationManager.getStudyConfiguration(studyIds.get(0), options).first();
            } else {
                defaultStudyConfiguration = null;
            }
        }

        if (isValidParam(query, FILE)) {
            String value = query.getString(FILE.key());
            VariantQueryUtils.QueryOperation operation = checkOperator(value);
            List<String> values = splitValue(value, operation);
            FilterList subFilters;
            if (operation == QueryOperation.OR) {
                subFilters = new FilterList(FilterList.Operator.MUST_PASS_ONE);
                filters.addFilter(subFilters);
            } else {
                subFilters = filters;
            }
            for (String file : values) {
                Pair<Integer, Integer> fileIdPair = studyConfigurationManager.getFileIdPair(file, false, null);
                byte[] column = buildFileColumnKey(fileIdPair.getKey(), fileIdPair.getValue());
                if (isNegated(file)) {
                    subFilters.addFilter(missingColumnFilter(column));
                } else {
                    subFilters.addFilter(existingColumnFilter(column));
                }
            }
        }

        if (isValidParam(query, SAMPLE)) {
            String value = query.getString(SAMPLE.key());
            VariantQueryUtils.QueryOperation operation = checkOperator(value);
            List<String> values = splitValue(value, operation);
            FilterList subFilters;
            if (operation == QueryOperation.OR) {
                subFilters = new FilterList(FilterList.Operator.MUST_PASS_ONE);
                filters.addFilter(subFilters);
            } else {
                subFilters = filters;
            }
            for (String sample : values) {
                if (defaultStudyConfiguration == null) {
                    List<String> studyNames = studyConfigurationManager.getStudyNames(null);
                    throw VariantQueryException.missingStudyForSample(sample, studyNames);
                }
                Integer sampleId = StudyConfigurationManager.getSampleIdFromStudy(sample, defaultStudyConfiguration, true);
                if (sampleId == null) {
                    List<String> studyNames = studyConfigurationManager.getStudyNames(null);
                    throw VariantQueryException.missingStudyForSample(sample, studyNames);
                }
                byte[] column = buildSampleColumnKey(defaultStudyConfiguration.getStudyId(), sampleId);
                if (isNegated(sample)) {
                    subFilters.addFilter(missingColumnFilter(column));
                } else {
                    subFilters.addFilter(existingColumnFilter(column));
                }
            }
        }

        if (selectElements.getFields().contains(VariantField.ANNOTATION)) {
            scan.addColumn(genomeHelper.getColumnFamily(), FULL_ANNOTATION.bytes());
        }

//        if (!returnedFields.contains(VariantField.ANNOTATION) && !returnedFields.contains(VariantField.STUDIES)) {
////            KeyOnlyFilter keyOnlyFilter = new KeyOnlyFilter();
////            filters.addFilter(keyOnlyFilter);
//            scan.addColumn(genomeHelper.getColumnFamily(), VariantPhoenixHelper.VariantColumn.TYPE.bytes());
//        }
        scan.addColumn(genomeHelper.getColumnFamily(), VariantPhoenixHelper.VariantColumn.TYPE.bytes());

        if (!columnPrefixes.isEmpty()) {
            MultipleColumnPrefixFilter columnPrefixFilter = new MultipleColumnPrefixFilter(
                    columnPrefixes.toArray(new byte[columnPrefixes.size()][]));
            filters.addFilter(columnPrefixFilter);
        }

        if (!filters.getFilters().isEmpty()) {
            scan.setFilter(filters);
        }
        scan.setMaxResultSize(options.getInt(QueryOptions.LIMIT));

        logger.info("StartRow = " + Bytes.toStringBinary(scan.getStartRow()));
        logger.info("StopRow = " + Bytes.toStringBinary(scan.getStopRow()));
        logger.info("columns = " + scan.getFamilyMap().get(
                genomeHelper.getColumnFamily()).stream().map(Bytes::toString).collect(Collectors.joining(",")));
        logger.info("MaxResultSize = " + scan.getMaxResultSize());
        logger.info("Filters = " + scan.getFilter());
        logger.info("Batch = " + scan.getBatch());
        return scan;
    }

    /**
     * Filter : SKIP QualifierFilter(!=, {COLUMN}).
     * Skip rows where NOT ALL cells ( has QualifierName != {COLUMN} )
     * == Get rows where ALL cells (has QualifierName != {COLUMN})
     * == Get rows where {COLUMN} is missing
     * @param column Column
     * @return Filter
     */
    public Filter missingColumnFilter(byte[] column) {

      //filters.addFilter(new SkipFilter(new SingleColumnValueFilter(
      //        genomeHelper.getColumnFamily(), annotationColumn,
      //        CompareFilter.CompareOp.EQUAL, new BinaryComparator(new byte[]{}))));

        return new SkipFilter(new QualifierFilter(
                CompareFilter.CompareOp.NOT_EQUAL, new BinaryComparator(column)));
    }

    /**
     * Filter : SingleColumnValueFilter('0', {COLUMM}, !=, null, true, true).
     * Get rows that contain column {COLUMN} and {COLUMN} is not null
     * @param column Column
     * @return Filter
     */
    public Filter existingColumnFilter(byte[] column) {
        SingleColumnValueFilter filter = new SingleColumnValueFilter(genomeHelper.getColumnFamily(), column,
                CompareFilter.CompareOp.NOT_EQUAL, new NullComparator());
        filter.setFilterIfMissing(true);
        filter.setLatestVersionOnly(true);
        return filter;
    }

    private List<Region> getRegions(Query query) {
        List<Region> regions;
        if (isValidParam(query, REGION)) {
            regions = Region.parseRegions(query.getString(REGION.key()));
        } else {
            regions = Collections.emptyList();
        }
        return regions;
    }

    private void addValueFilter(FilterList filters, byte[] column, List<String> values) {
        List<Filter> valueFilters = new ArrayList<>(values.size());
        for (String value : values) {
            SingleColumnValueFilter valueFilter = new SingleColumnValueFilter(genomeHelper.getColumnFamily(),
                    column, CompareFilter.CompareOp.EQUAL, new SubstringComparator(value));
            valueFilter.setFilterIfMissing(true);
            valueFilters.add(valueFilter);
        }
        filters.addFilter(new FilterList(FilterList.Operator.MUST_PASS_ONE, valueFilters));
    }

    public static void addArchiveRegionFilter(Scan scan, Region region, ArchiveRowKeyFactory keyFactory) {
        if (region == null) {
            addDefaultRegionFilter(scan);
        } else {
            scan.setStartRow(keyFactory.generateBlockIdAsBytes(region.getChromosome(), region.getStart()));
            long endSlice = keyFactory.getSliceId((long) region.getEnd()) + 1;
            // +1 because the stop row is exclusive
            scan.setStopRow(Bytes.toBytes(keyFactory.generateBlockIdFromSlice(region.getChromosome(), endSlice)));
        }
    }

    public static void addRegionFilter(Scan scan, Region region) {
        if (region == null) {
            addDefaultRegionFilter(scan);
        } else {
            scan.setStartRow(VariantPhoenixKeyFactory.generateVariantRowKey(region.getChromosome(), region.getStart()));
            scan.setStopRow(VariantPhoenixKeyFactory.generateVariantRowKey(region.getChromosome(), region.getEnd()));
        }
    }

    public static Scan addDefaultRegionFilter(Scan scan) {
//        return scan.setStopRow(Bytes.toBytes(String.valueOf(GenomeHelper.METADATA_PREFIX)));
        return scan.setFilter(new ColumnPrefixFilter(GenomeHelper.VARIANT_COLUMN_B_PREFIX));
    }

}
