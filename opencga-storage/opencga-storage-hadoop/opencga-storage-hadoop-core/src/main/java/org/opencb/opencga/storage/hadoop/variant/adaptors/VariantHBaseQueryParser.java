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
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveRowKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveTableHelper;
import org.opencb.opencga.storage.hadoop.variant.gaps.FillGapsTask;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixKeyFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.*;
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
            REGION,
            INCLUDE_FILE,
            INCLUDE_STUDY,
            INCLUDE_SAMPLE,
            UNKNOWN_GENOTYPE));

    public VariantHBaseQueryParser(GenomeHelper genomeHelper, StudyConfigurationManager studyConfigurationManager) {
        this.genomeHelper = genomeHelper;
        this.studyConfigurationManager = studyConfigurationManager;
    }

    public static boolean isSupportedQueryParam(Query query, QueryParam param) {
        return isSupportedQuery(new Query(param.key(), query.get(param.key())));
    }

    /**
     * Check if the given query can be fully executed directly with hbase.
     * @param query Query to test
     * @return      If the query can be fully executed with hbase
     */
    public static boolean isSupportedQuery(Query query) {
        return unsupportedParamsFromQuery(query).isEmpty();
    }

    public static Set<String> unsupportedParamsFromQuery(Query query) {
        Set<VariantQueryParam> otherParams = validParams(query);
        otherParams.removeAll(SUPPORTED_QUERY_PARAMS);
        Set<String> messages = new HashSet<>();
        if (otherParams.contains(ID)) {
            List<String> ids = query.getAsStringList(ID.key());
            for (String id : ids) {
                if (!VariantQueryUtils.isVariantId(id)) {
                    messages.add("If there is any ID that is not a variantId, the query is not fully supported");
                }
            }
            otherParams.remove(ID);
        }
//        if (otherParams.contains(REGION)) {
//            if (query.getAsStringList(REGION.key()).size() != 1) {
//                messages.add("Only one region is supported at a time");
//            }
//            otherParams.remove(REGION);
//        }
        if (otherParams.contains(STUDY)) {
            String value = query.getString(STUDY.key());
            if (splitValue(value).getValue().stream().anyMatch(VariantQueryUtils::isNegated)) {
                messages.add("Negated studies not supported");
            }
            otherParams.remove(STUDY);
        }
        if (otherParams.contains(STUDY)) {
            String value = query.getString(STUDY.key());
            if (splitValue(value).getValue().stream().anyMatch(VariantQueryUtils::isNegated)) {
                messages.add("Negated studies not supported");
            }
            otherParams.remove(STUDY);
        }
        if (otherParams.contains(FILE)) {
            String value = query.getString(FILE.key());
            if (splitValue(value).getValue().stream().anyMatch(VariantQueryUtils::isNegated)) {
                messages.add("Negated files not supported");
            }
            otherParams.remove(FILE);
        }
        if (otherParams.contains(SAMPLE)) {
            String value = query.getString(SAMPLE.key());
            if (splitValue(value).getValue().stream().anyMatch(VariantQueryUtils::isNegated)) {
                messages.add("Negated samples not supported");
            }
            otherParams.remove(SAMPLE);
        }
        if (otherParams.contains(GENOTYPE)) {
            HashMap<Object, List<String>> map = new HashMap<>();
            parseGenotypeFilter(query.getString(GENOTYPE.key()), map);

            for (List<String> gts : map.values()) {
                if (gts.stream().anyMatch(VariantQueryUtils::isNegated)) {
                    messages.add("Negated genotypes not supported");
                }
//                if (gts.stream().anyMatch(gt -> gt.equals("0/0") || gt.equals("0|0"))) {
//                    messages.add("Reference genotype [0/0] not supported");
//                }
            }
            otherParams.remove(GENOTYPE);
        }

        if (messages.isEmpty() && otherParams.isEmpty()) {
            return Collections.emptySet();
        } else {
            if (!otherParams.isEmpty()) {
                for (VariantQueryParam otherParam : otherParams) {
                    messages.add("Unsupported param " + otherParam);
                }
            }
            return messages;
        }
    }

    public List<Scan> parseQueryMultiRegion(Query query, QueryOptions options) {
        return parseQueryMultiRegion(VariantQueryUtils.parseSelectElements(query, options, studyConfigurationManager), query, options);
    }
    public List<Scan> parseQueryMultiRegion(SelectVariantElements selectElements, Query query, QueryOptions options) {
        VariantQueryXref xrefs = VariantQueryUtils.parseXrefs(query);
        if (!xrefs.getOtherXrefs().isEmpty()) {
            throw VariantQueryException.unsupportedVariantQueryFilter(VariantQueryParam.ANNOT_XREF,
                    HadoopVariantStorageEngine.STORAGE_ENGINE_ID, "Only variant ids are supported with HBase native query");
        } else if (!xrefs.getIds().isEmpty()) {
            throw VariantQueryException.unsupportedVariantQueryFilter(VariantQueryParam.ID,
                    HadoopVariantStorageEngine.STORAGE_ENGINE_ID, "Only variant ids are supported with HBase native query");
        }

        List<Region> regions = getRegions(query);
        List<Variant> variants = xrefs.getVariants();

        regions = mergeRegions(regions);
        if (!regions.isEmpty()) {
            for (Iterator<Variant> iterator = variants.iterator(); iterator.hasNext();) {
                Variant variant = iterator.next();
                if (regions.stream().anyMatch(r -> r.overlaps(variant.getChromosome(), variant.getStart(), variant.getEnd()))) {
                    iterator.remove();
                }
            }
        }

        List<Scan> scans;
        if (regions.isEmpty() && variants.isEmpty()) {
            scans = Collections.singletonList(parseQuery(selectElements, query, options));
        } else {
            scans = new ArrayList<>(regions.size() + variants.size());
            Query subQuery = new Query(query);
            subQuery.remove(REGION.key());
            subQuery.remove(ANNOT_GENE_REGIONS.key());
            subQuery.remove(ANNOT_XREF.key());
            subQuery.remove(ID.key());

            subQuery.put(REGION.key(), "MULTI_REGION");
            Scan templateScan = parseQuery(selectElements, subQuery, options);

            for (Region region : regions) {
                subQuery.put(REGION.key(), region);
                try {
                    Scan scan = new Scan(templateScan);
                    addRegionFilter(scan, region);
                    scans.add(scan);
                } catch (IOException e) {
                    throw VariantQueryException.internalException(e);
                }
            }
            subQuery.remove(REGION.key());
            for (Variant variant : variants) {

                subQuery.put(ID.key(), variant);
                try {
                    Scan scan = new Scan(templateScan);
                    addVariantIdFilter(scan, variant);
                    scans.add(scan);
                } catch (IOException e) {
                    throw VariantQueryException.internalException(e);
                }
            }
        }

        return scans;
    }

    public Scan parseQuery(Query query, QueryOptions options) {
        VariantQueryUtils.SelectVariantElements selectElements =
                VariantQueryUtils.parseSelectElements(query, options, studyConfigurationManager);
        return parseQuery(selectElements, query, options);
    }

    public Scan parseQuery(VariantQueryUtils.SelectVariantElements selectElements, Query query, QueryOptions options) {

        Scan scan = new Scan();
        byte[] family = genomeHelper.getColumnFamily();
        FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
//        FilterList regionFilters = new FilterList(FilterList.Operator.MUST_PASS_ONE);
//        filters.addFilter(regionFilters);
//        List<byte[]> columnPrefixes = new LinkedList<>();

        List<Region> regions = getRegions(query);

        if (regions != null && !regions.isEmpty()) {
            if (regions.size() > 1) {
                throw VariantQueryException.malformedParam(REGION, regions.toString(), "Unsupported multiple region filter");
            }
            Region region = regions.get(0);
            logger.debug("region = " + region);
            addRegionFilter(scan, region);
        } else if (isValidParam(query, ID)) {
            List<String> ids = query.getAsStringList(ID.key());
            if (ids.size() != 1) {
                throw VariantQueryException.malformedParam(ID, ids.toString(), "Unsupported multiple variant ids filter");
            }
            Variant variant = VariantQueryUtils.toVariant(ids.get(0));
            addVariantIdFilter(scan, variant);
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
                    scan.addColumn(family, annotationColumn);
                }
            } else {
                logger.warn("Filter " + ANNOTATION_EXISTS.key() + "=true not implemented in native mode");
            }
        }

        if (selectElements.getFields().contains(VariantField.STUDIES)) {
            for (Integer studyId : selectElements.getStudies()) {
                scan.addColumn(family, VariantPhoenixHelper.getStudyColumn(studyId).bytes());
                scan.addColumn(family, VariantPhoenixHelper.getFillMissingColumn(studyId).bytes());
            }

            if (selectElements.getFields().contains(VariantField.STUDIES_STATS)) {
                for (StudyConfiguration sc : selectElements.getStudyConfigurations().values()) {
                    for (Integer cohortId : sc.getCalculatedStats()) {
                        scan.addColumn(family,
                                VariantPhoenixHelper.getStatsColumn(sc.getStudyId(), cohortId).bytes());
                    }
                    for (Integer cohortId : sc.getInvalidStats()) {
                        scan.addColumn(family,
                                VariantPhoenixHelper.getStatsColumn(sc.getStudyId(), cohortId).bytes());
                    }
                }
            }

            selectElements.getSamples().forEach((studyId, sampleIds) -> {
                scan.addColumn(family, VariantPhoenixHelper.getStudyColumn(studyId).bytes());
                for (Integer sampleId : sampleIds) {
                    scan.addColumn(family, buildSampleColumnKey(studyId, sampleId));
                }
            });

            selectElements.getFiles().forEach((studyId, fileIds) -> {
                scan.addColumn(family, VariantPhoenixHelper.getStudyColumn(studyId).bytes());
                for (Integer fileId : fileIds) {
                    scan.addColumn(family, VariantPhoenixHelper.buildFileColumnKey(studyId, fileId));
                }
            });
        }

        // If we already add a filter that requires a sample from a certain study, we can skip latter the filter for that study
        Set<Integer> filteredStudies = new HashSet<>();
        final StudyConfiguration defaultStudyConfiguration = getDefaultStudyConfiguration(query, options, studyConfigurationManager);
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
                Pair<Integer, Integer> fileIdPair = studyConfigurationManager.getFileIdPair(file, false, defaultStudyConfiguration);
                byte[] column = buildFileColumnKey(fileIdPair.getKey(), fileIdPair.getValue());
                if (isNegated(file)) {
                    subFilters.addFilter(missingColumnFilter(column));
                } else {
                    filteredStudies.add(fileIdPair.getKey());
                    subFilters.addFilter(existingColumnFilter(column));
                }
                scan.addColumn(family, column);
            }
        }

        if (isValidParam(query, GENOTYPE)) {
            HashMap<Object, List<String>> genotypesMap = new HashMap<>();
            QueryOperation operation = VariantQueryUtils.parseGenotypeFilter(query.getString(GENOTYPE.key()), genotypesMap);

            FilterList subFilters;
            if (operation == QueryOperation.OR) {
                subFilters = new FilterList(FilterList.Operator.MUST_PASS_ONE);
                filters.addFilter(subFilters);
            } else {
                subFilters = filters;
            }
            for (Map.Entry<Object, List<String>> entry : genotypesMap.entrySet()) {
                if (defaultStudyConfiguration == null) {
                    List<String> studyNames = studyConfigurationManager.getStudyNames(null);
                    throw VariantQueryException.missingStudyForSample(entry.getKey().toString(), studyNames);
                }
                int studyId = defaultStudyConfiguration.getStudyId();
                int sampleId = studyConfigurationManager.getSampleId(entry.getKey(), defaultStudyConfiguration);
                List<String> genotypes = entry.getValue();

                if (genotypes.stream().allMatch(VariantQueryUtils::isNegated)) {
                    throw VariantQueryException.unsupportedVariantQueryFilter(GENOTYPE, query.getString(GENOTYPE.key()),
                            "Unable to negate genotypes.");
                } else if (genotypes.stream().anyMatch(VariantQueryUtils::isNegated)) {
                    throw VariantQueryException.malformedParam(GENOTYPE, query.getString(GENOTYPE.key()),
                            "Can not mix negated and not negated genotypes");
                } else {
                    filteredStudies.add(studyId);
                }

                byte[] column = buildSampleColumnKey(studyId, sampleId);
                List<Filter> gtSubFilters = genotypes.stream()
                        .map(genotype -> {
                            SingleColumnValueFilter filter = new SingleColumnValueFilter(family, column, CompareFilter.CompareOp.EQUAL,
                                    new BinaryPrefixComparator(Bytes.toBytes(genotype)));
                            filter.setFilterIfMissing(true);
                            filter.setLatestVersionOnly(true);
                            if (FillGapsTask.isHomRefDiploid(genotype)) {
                                return new FilterList(FilterList.Operator.MUST_PASS_ONE, filter, missingColumnFilter(column));
                            } else {
                                return filter;
                            }
                        })
                        .collect(Collectors.toList());
                if (gtSubFilters.size() == 1) {
                    subFilters.addFilter(gtSubFilters.get(0));
                } else {
                    subFilters.addFilter(new FilterList(FilterList.Operator.MUST_PASS_ONE, gtSubFilters));
                }
                scan.addColumn(family, column);
            }
        }

        if (isValidParam(query, SAMPLE)) {
            String value = query.getString(SAMPLE.key());
            VariantQueryUtils.QueryOperation operation = checkOperator(value);
            List<String> values = splitValue(value, operation);
            operation = QueryOperation.AND; // SAMPLES filter is always an AND
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
                    // Check on only if the column exists. Also check if the GT starts contains a 1 ( 1/? or ?/1 )
                    filteredStudies.add(defaultStudyConfiguration.getStudyId());

                    // 1/?
                    SingleColumnValueFilter filter1 = new SingleColumnValueFilter(family, column, CompareFilter.CompareOp.EQUAL,
                            new BinaryPrefixComparator(Bytes.toBytes("1")));
                    filter1.setFilterIfMissing(true);
                    filter1.setLatestVersionOnly(true);

                    // ?/1
                    SingleColumnValueFilter filter2 = new SingleColumnValueFilter(family, column, CompareFilter.CompareOp.EQUAL,
                            new RegexStringComparator("^[\\.0-9][/\\|]1")
                    );
                    filter2.setFilterIfMissing(true);
                    filter2.setLatestVersionOnly(true);

                    subFilters.addFilter(new FilterList(FilterList.Operator.MUST_PASS_ONE, filter1, filter2));
                }
                scan.addColumn(family, column);
            }
        }

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
            for (String studyStr : values) {
                Integer studyId = studyConfigurationManager.getStudyId(studyStr, null);
                byte[] column = VariantPhoenixHelper.getStudyColumn(studyId).bytes();
                if (isNegated(studyStr)) {
                    subFilters.addFilter(missingColumnFilter(column));
                    scan.addColumn(family, column);
                } else {
                    if (!filteredStudies.contains(studyId)) {
                        subFilters.addFilter(existingColumnFilter(column));
                        scan.addColumn(family, column);
                    }
                }
            }
        }

        if (selectElements.getFields().contains(VariantField.ANNOTATION)) {
            if (isValidParam(query, VariantHadoopDBAdaptor.ANNOT_NAME)) {
                int id = query.getInt(VariantHadoopDBAdaptor.ANNOT_NAME.key());
                scan.addColumn(family, Bytes.toBytes(VariantPhoenixHelper.getAnnotationSnapshotColumn(id)));
            } else {
                scan.addColumn(family, FULL_ANNOTATION.bytes());
            }
            int release = studyConfigurationManager.getProjectMetadata().first().getRelease();
            for (int i = 1; i <= release; i++) {
                scan.addColumn(family, VariantPhoenixHelper.buildReleaseColumnKey(i));
            }
        }

//        if (!returnedFields.contains(VariantField.ANNOTATION) && !returnedFields.contains(VariantField.STUDIES)) {
////            KeyOnlyFilter keyOnlyFilter = new KeyOnlyFilter();
////            filters.addFilter(keyOnlyFilter);
//            scan.addColumn(genomeHelper.getColumnFamily(), VariantPhoenixHelper.VariantColumn.TYPE.bytes());
//        }
        if (selectElements.getFields().contains(VariantField.TYPE) || !scan.hasFamilies()) {
            scan.addColumn(family, VariantPhoenixHelper.VariantColumn.TYPE.bytes());
        }

//        if (!columnPrefixes.isEmpty()) {
//            MultipleColumnPrefixFilter columnPrefixFilter = new MultipleColumnPrefixFilter(
//                    columnPrefixes.toArray(new byte[columnPrefixes.size()][]));
//            filters.addFilter(columnPrefixFilter);
//        }
        int limit = options.getInt(QueryOptions.LIMIT);
        int skip = options.getInt(QueryOptions.SKIP);
        if (limit > 0) {
            if (skip > 0) {
                limit += skip;
            }
            filters.addFilter(new PageFilter(limit));
        }
        if (!filters.getFilters().isEmpty()) {
            scan.setFilter(filters);
        }
//        scan.setMaxResultSize(limit);
        scan.setReversed(options.getString(QueryOptions.ORDER, QueryOptions.ASCENDING).equals(QueryOptions.DESCENDING));

        logger.info("StartRow = " + Bytes.toStringBinary(scan.getStartRow()));
        logger.info("StopRow = " + Bytes.toStringBinary(scan.getStopRow()));
        logger.info("columns = " + scan.getFamilyMap().getOrDefault(family, Collections.emptyNavigableSet())
                .stream().map(Bytes::toString).collect(Collectors.joining(",")));
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
        List<Region> regions = new ArrayList<>();
        if (isValidParam(query, REGION)) {
            regions.addAll(Region.parseRegions(query.getString(REGION.key())));
        }

        if (isValidParam(query, ANNOT_GENE_REGIONS)) {
            regions.addAll(Region.parseRegions(query.getString(ANNOT_GENE_REGIONS.key())));
        }

        regions = mergeRegions(regions);

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

    public static void addArchiveRegionFilter(Scan scan, Region region, ArchiveTableHelper helper) {
        addArchiveRegionFilter(scan, region, helper.getFileId(), helper.getKeyFactory());
    }

    public static void addArchiveRegionFilter(Scan scan, Region region, int fileId, ArchiveRowKeyFactory keyFactory) {
        if (region != null) {
            scan.setStartRow(keyFactory.generateBlockIdAsBytes(fileId, region.getChromosome(), region.getStart()));
            long endSlice = keyFactory.getSliceId((long) region.getEnd()) + 1;
            // +1 because the stop row is exclusive
            scan.setStopRow(Bytes.toBytes(keyFactory.generateBlockIdFromSlice(
                    fileId, region.getChromosome(), endSlice)));
        }
    }

    public static void addVariantIdFilter(Scan scan, Variant variant) {
        byte[] rowKey = VariantPhoenixKeyFactory.generateVariantRowKey(variant);
        scan.setStartRow(rowKey);
        scan.setStopRow(rowKey);
    }

    public static void addRegionFilter(Scan scan, Region region) {
        if (region != null) {
            scan.setStartRow(VariantPhoenixKeyFactory.generateVariantRowKey(region.getChromosome(), region.getStart()));
            int end = region.getEnd();
            if (end != Integer.MAX_VALUE) {
                end++;
            }
            scan.setStopRow(VariantPhoenixKeyFactory.generateVariantRowKey(region.getChromosome(), end));
        }
    }

}
