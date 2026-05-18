/*
 * Copyright 2015-2020 OpenCB
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

package org.opencb.opencga.storage.hadoop.variant.annotation;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.hadoop.utils.CopyHBaseColumnDriver;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchema;

import java.util.List;
import java.util.Map;

/**
 * Specialisation of {@link CopyHBaseColumnDriver} used by {@code saveAnnotation} to materialise a
 * per-id snapshot column on the variant table. Adds a native HBase scan filter on the variant row's
 * {@code A_ID} ({@link VariantPhoenixSchema.VariantColumn#ANNOTATION_ID}) column so only rows whose
 * stored annotationSetId equals the named generation get copied. Rows tagged with another id (or
 * none) are skipped, which means phantom intermediate ids (where no variant was ever stamped under
 * that generation) materialise an empty snapshot column — the correct semantic for "nothing to save
 * under this name."
 *
 * <p>The annotation-specific knowledge lives here, not in the generic
 * {@link CopyHBaseColumnDriver}, so unrelated callers of the column-copy utility don't drag in
 * Phoenix variant schema imports.
 */
public class CopyVariantAnnotationSnapshotDriver extends CopyHBaseColumnDriver {

    /**
     * Required parameter — the annotationSetId of the generation being materialised. Rows whose
     * {@code A_ID} column does not equal this value (or is missing) are filtered out of the copy.
     * Must be a positive integer.
     */
    public static final String FILTER_ANNOTATION_SET_ID = "filterAnnotationSetId";

    private int filterAnnotationSetId;

    @Override
    protected void parseAndValidateParameters() throws java.io.IOException {
        super.parseAndValidateParameters();
        filterAnnotationSetId = getConf().getInt(FILTER_ANNOTATION_SET_ID, 0);
        if (filterAnnotationSetId <= 0) {
            throw new IllegalArgumentException("Missing or invalid '" + FILTER_ANNOTATION_SET_ID
                    + "' parameter (got " + filterAnnotationSetId + "); must be a positive integer.");
        }
    }

    @Override
    protected void customiseScan(Scan scan) {
        // filterIfMissing=true drops rows that lack the A_ID column entirely — variants without
        // recorded annotation generation are not part of any snapshot.
        SingleColumnValueFilter filter = new SingleColumnValueFilter(
                GenomeHelper.COLUMN_FAMILY_BYTES,
                VariantPhoenixSchema.VariantColumn.ANNOTATION_ID.bytes(),
                CompareFilter.CompareOp.EQUAL,
                VariantPhoenixSchema.VariantColumn.ANNOTATION_ID.getPDataType().toBytes(filterAnnotationSetId));
        filter.setFilterIfMissing(true);
        scan.setFilter(filter);
        scan.addColumn(GenomeHelper.COLUMN_FAMILY_BYTES, VariantPhoenixSchema.VariantColumn.ANNOTATION_ID.bytes());
    }

    @Override
    protected String getJobName() {
        return "opencga: copy variant annotation column filtered by annotationSetId == " + filterAnnotationSetId;
    }

    /**
     * Build CLI args for the driver: same as the base driver's {@code buildArgs}, plus the
     * required {@link #FILTER_ANNOTATION_SET_ID} parameter.
     *
     * @param table              variant table name
     * @param columnsToCopyMap   source→target column mapping
     * @param columnsToInclude   additional columns to include in the scan (nullable)
     * @param annotationSetId    annotationSetId to filter rows by
     * @param options            extra options forwarded to the underlying MR job (nullable)
     * @return assembled CLI args
     */
    public static String[] buildArgs(String table, Map<String, String> columnsToCopyMap,
                                     List<String> columnsToInclude, int annotationSetId, ObjectMap options) {
        ObjectMap opts = options == null ? new ObjectMap() : new ObjectMap(options);
        opts.put(FILTER_ANNOTATION_SET_ID, annotationSetId);
        return CopyHBaseColumnDriver.buildArgs(table, columnsToCopyMap, columnsToInclude, opts);
    }

    public static void main(String[] args) {
        main(args, CopyVariantAnnotationSnapshotDriver.class);
    }
}
