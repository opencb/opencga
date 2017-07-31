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
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.core.Region;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveRowKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveTableHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixKeyFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.isValidParam;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.validParams;
import static org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper.VariantColumn.*;

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
            RETURNED_FILES,
            RETURNED_STUDIES,
            RETURNED_SAMPLES,
            UNKNOWN_GENOTYPE));

    public VariantHBaseQueryParser(GenomeHelper genomeHelper, StudyConfigurationManager studyConfigurationManager) {
        this.genomeHelper = genomeHelper;
        this.studyConfigurationManager = studyConfigurationManager;
    }

    public static boolean fullySupportedQuery(Query query) {
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

    ////// Util methods:
    public Scan parseQuery(Query query, QueryOptions options) {

        Scan scan = new Scan();
        scan.addFamily(genomeHelper.getColumnFamily());
        FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
//        FilterList regionFilters = new FilterList(FilterList.Operator.MUST_PASS_ONE);
//        filters.addFilter(regionFilters);
        List<byte[]> columnPrefixes = new LinkedList<>();

        if (isValidParam(query, REGION)) {
            // TODO: Use MultiRowRangeFilter
            Region region = Region.parseRegion(query.getString(REGION.key()));
            logger.debug("region = " + region);
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

        Set<VariantField> returnedFields = VariantField.getReturnedFields(options);
        Map<Integer, List<Integer>> returnedSamples
                = VariantQueryUtils.getReturnedSamples(query, options, studyConfigurationManager);

        if (returnedFields.contains(VariantField.STUDIES)) {
            if (isValidParam(query, STUDIES)) {
                //TODO: Handle negations(!), and(;) and or(,)
                List<Integer> studyIdList = query.getAsIntegerList(STUDIES.key());
                for (Integer studyId : studyIdList) {
                    columnPrefixes.add(Bytes.toBytes(studyId.toString() + genomeHelper.getSeparator()));
                }
            }
        }

        returnedSamples.forEach((studyId, sampleIds) -> {
            scan.addColumn(genomeHelper.getColumnFamily(), VariantPhoenixHelper.getStudyColumn(studyId).bytes());
            for (Integer sampleId : sampleIds) {
                scan.addColumn(genomeHelper.getColumnFamily(), VariantPhoenixHelper.buildSampleColumnKey(studyId, sampleId));
            }
        });

        if (returnedFields.contains(VariantField.ANNOTATION)) {
            scan.addColumn(genomeHelper.getColumnFamily(), FULL_ANNOTATION.bytes());
        }

        if (!returnedFields.contains(VariantField.ANNOTATION) && !returnedFields.contains(VariantField.STUDIES)) {
            KeyOnlyFilter keyOnlyFilter = new KeyOnlyFilter();
            filters.addFilter(keyOnlyFilter);
        }

        MultipleColumnPrefixFilter columnPrefixFilter = new MultipleColumnPrefixFilter(
                columnPrefixes.toArray(new byte[columnPrefixes.size()][]));
        filters.addFilter(columnPrefixFilter);

        scan.setFilter(filters);
        scan.setMaxResultSize(options.getInt(QueryOptions.LIMIT));

        logger.debug("StartRow = " + new String(scan.getStartRow()));
        logger.debug("StopRow = " + new String(scan.getStopRow()));
        logger.debug("MaxResultSize = " + scan.getMaxResultSize());
        logger.debug("Filters = " + scan.getFilter().toString());
        return scan;
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

    public static void addArchiveRegionFilter(Scan scan, Region region, ArchiveTableHelper archiveHelper) {
        if (region == null) {
            addDefaultRegionFilter(scan);
        } else {
            ArchiveRowKeyFactory keyFactory = archiveHelper.getKeyFactory();
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
        return scan.setStopRow(Bytes.toBytes(String.valueOf(GenomeHelper.METADATA_PREFIX)));
    }

}
