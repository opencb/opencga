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

/**
 *
 */
package org.opencb.opencga.storage.hadoop.variant.index;

import com.google.common.collect.BiMap;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Mapper;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixKeyFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static org.opencb.opencga.storage.hadoop.variant.mr.AnalysisTableMapReduceHelper.COUNTER_GROUP_NAME;

/**
 * Removes Sample data for a provided file from the Analysis (Variant) and the
 * file data from the Archive Table.
 *
 * @author Matthias Haimel mh719+git@cam.ac.uk
 *
 */
public class VariantTableRemoveMapper extends AbstractArchiveTableMapper {

    private Logger logger = LoggerFactory.getLogger(VariantTableRemoveMapper.class);
    private boolean removeSampleColumns;
    private VariantStorageEngine.MergeMode mergeMode;

    @Override
    protected void setup(Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Mutation>.Context context) throws IOException,
            InterruptedException {
        super.setup(context);
        this.loadSampleColumns = false;

        removeSampleColumns = context.getConfiguration().getBoolean(HadoopVariantStorageEngine.MERGE_LOAD_SAMPLE_COLUMNS,
                HadoopVariantStorageEngine.DEFAULT_MERGE_LOAD_SAMPLE_COLUMNS);
        mergeMode = VariantStorageEngine.MergeMode.from(getStudyConfiguration().getAttributes());

    }

    @Override
    protected void map(VariantMapReduceContext ctx) throws IOException, InterruptedException {
        Collection<Variant> updateLst = new ArrayList<>();
        Collection<Variant> removeLst = new ArrayList<>();
        BiMap<Integer, String> sampleIds = getStudyConfiguration().getSampleIds().inverse();

        if (mergeMode.equals(VariantStorageEngine.MergeMode.ADVANCED)) {
            List<Variant> analysisVar = parseCurrentVariantsRegion(ctx);
            ctx.getContext().getCounter(COUNTER_GROUP_NAME, "VARIANTS_FROM_ANALYSIS").increment(analysisVar.size());
            logger.info("Loaded {} variants ... ", analysisVar.size());

            endStep("2 Unpack and convert input ANALYSIS variants (" + GenomeHelper.VARIANT_COLUMN_PREFIX + ")");

            if (!analysisVar.isEmpty()) {
                Variant tmpVar = analysisVar.get(0);
                if (logger.isDebugEnabled()) {
                    logger.debug("Loaded variant from analysis table: " + tmpVar.toJson());
                }
                for (Variant var : analysisVar) {
                    // remove values for Sample
                    for (Integer sample : ctx.sampleIds) {
                        String sampleName = sampleIds.get(sample);
                        removeSample(var, sampleName);
                    }
                    // check if there are still variants to be committed
                    boolean hasVariants = containsVariants(var);
                    // save to commit list
                    if (hasVariants) {
                        updateLst.add(var);
                    } else {
                        removeLst.add(var);
                    }
                }
            }

            List<VariantTableStudyRow> rows = new ArrayList<>();
            updateOutputTable(ctx.context, updateLst, rows, null);
            updateArchiveTable(ctx.getCurrRowKey(), ctx.context, rows);
        } else {
            updateLst = new HashSet<>();
            for (Map.Entry<byte[], byte[]> entry : ctx.getValue().getFamilyMap(getHelper().getColumnFamily()).entrySet()) {
                Variant var = GenomeHelper.getVariantFromArchiveVariantColumn(ctx.getChromosome(), entry.getKey());
                if (var != null) {
                    updateLst.add(var);
                }
            }
//            for (Integer fileId : ctx.getFileIds()) {
//                byte[] data = ctx.getValue()
//                         .getValue(getHelper().getColumnFamily(), Bytes.toBytes(ArchiveTableHelper.getColumnName(fileId)));
//                VcfSliceProtos.VcfSlice vcfSlice = VcfSliceProtos.VcfSlice.parseFrom(data);
//                for (VcfSliceProtos.VcfRecord vcfRecord : vcfSlice.getRecordsList()) {
//                    if (VariantMergerTableMapper.TARGET_VARIANT_TYPE_SET.contains(VcfRecordProtoToVariantConverter
//                            .getVariantType(vcfRecord.getType()))) {
//                        int start = VcfRecordProtoToVariantConverter.getStart(vcfRecord, vcfSlice.getPosition());
//                        int end = VcfRecordProtoToVariantConverter.getEnd(vcfRecord, vcfSlice.getPosition());
//                        updateLst.add(
//                                new Variant(vcfSlice.getChromosome(), start, end, vcfRecord.getReference(), vcfRecord.getAlternate()));
//                    }
//                }
//            }
        }

        deleteVariantsFromAnalysisTable(ctx, removeLst);
        deleteSamplesFromAnalysisTable(ctx, updateLst);
        deleteFromArchiveTable(ctx.context, ctx.currRowKey, ctx.getFileIdsInResult());
    }

    private void deleteFromArchiveTable(Context context, byte[] rowKey, Collection<Integer> fileIds)
            throws IOException, InterruptedException {
        if (fileIds.isEmpty()) {
            // Nothing to do!
            context.getCounter(COUNTER_GROUP_NAME, "ARCHIVE_TABLE_ROW-EMPTY").increment(1);
            return;
        }
        byte[] cf = getHelper().getColumnFamily();
        Delete del = new Delete(rowKey); // TODO HBase time stamp specific delete -> more efficient
        for (Integer fid : fileIds) {
            del.addColumn(cf, Bytes.toBytes(fid.toString()));
        }
        context.getCounter(COUNTER_GROUP_NAME, "ARCHIVE_TABLE_ROW-DELETE_cells").increment(fileIds.size());
        context.getCounter(COUNTER_GROUP_NAME, "ARCHIVE_TABLE_ROW-DELETE_commands").increment(1);
//        this.archiveTable.delete(del);
        context.write(new ImmutableBytesWritable(getHelper().getArchiveTable()), del);
    }

    private void deleteVariantsFromAnalysisTable(VariantMapReduceContext variantContext, Collection<Variant> fullRemoveList)
            throws IOException, InterruptedException {
        int studyId = getStudyConfiguration().getStudyId();
        Context context = variantContext.context;
        byte[] columnFamily = getHelper().getColumnFamily();
        BiMap<String, Integer> idMapping = getStudyConfiguration().getSampleIds();
        for (Variant variant : fullRemoveList) {
            VariantTableStudyRow row = new VariantTableStudyRow(variant, studyId, idMapping);
            // Delete ALL files and samples in this row
            Delete delete = row.createDelete(getHelper());
            for (Integer fileId : getStudyConfiguration().getIndexedFiles()) {
                delete.addColumn(columnFamily, VariantPhoenixHelper.buildFileColumnKey(studyId, fileId));
            }
            for (Integer sampleId : getStudyConfiguration().getSampleIds().values()) {
                delete.addColumn(columnFamily, VariantPhoenixHelper.buildSampleColumnKey(studyId, sampleId));
            }
//            this.analysisTable.delete(delete);
            context.write(new ImmutableBytesWritable(getHelper().getAnalysisTable()), delete);
            context.getCounter(COUNTER_GROUP_NAME, "ANALYSIS_TABLE_ROW-DELETE").increment(1);
        }
    }

    private void deleteSamplesFromAnalysisTable(VariantMapReduceContext variantContext, Collection<Variant> partialRemoveList)
            throws IOException, InterruptedException {
        if (!removeSampleColumns) {
            return;
        } else if (variantContext.getSampleIds().isEmpty() && variantContext.getFileIdsInResult().isEmpty()) {
            // Nothing to do!
            variantContext.getContext().getCounter(COUNTER_GROUP_NAME, "ANALYSIS_TABLE_ROW-EMPTY").increment(1);
            return;
        }

        int studyId = getStudyConfiguration().getStudyId();
        Context context = variantContext.context;
        byte[] columnFamily = getHelper().getColumnFamily();
        for (Variant variant : partialRemoveList) {
            byte[] row = VariantPhoenixKeyFactory.generateVariantRowKey(variant);
            Delete delete = new Delete(row);

            for (Integer fileId : variantContext.getFileIdsInResult()) {
                delete.addColumn(columnFamily, VariantPhoenixHelper.buildFileColumnKey(studyId, fileId));
            }
            for (Integer sampleId : variantContext.getSampleIds()) {
                delete.addColumn(columnFamily, VariantPhoenixHelper.buildSampleColumnKey(studyId, sampleId));
            }
//            this.analysisTable.delete(delete);
            context.write(new ImmutableBytesWritable(getHelper().getAnalysisTable()), delete);
            context.getCounter(COUNTER_GROUP_NAME, "ANALYSIS_TABLE_ROW-PARTIAL-DELETE").increment(1);

//            // Mark this variant to be reviewed
//            Put put = new Put(row);
//            put.addColumn(columnFamily, Bytes.toBytes("R"), PDataType.TRUE_BYTES);
//            context.write(new ImmutableBytesWritable(getHelper().getAnalysisTable()), put);
        }
    }

    /**
     * Remove Sample from Variant object.
     * @param var Variant object.
     * @param sampleName Sample name.
     * @throws IllegalStateException If no Study or the Sample name is not found in the first Study of the Variant.
     */
    private void removeSample(Variant var, String sampleName) throws IllegalStateException {
        StudyEntry se = var.getStudies().get(0);
        if (se == null) {
            throw new IllegalStateException("No study found in variant " + var);
        }
        LinkedHashMap<String, Integer> samplesPos = se.getSamplesPosition();
        Integer remPos = samplesPos.get(sampleName);
        if (remPos == null) {
            throw new IllegalStateException("Sample " + sampleName + " not found for variant " + var);
        }
        LinkedHashMap<String, Integer> updSamplesPos = new LinkedHashMap<>(samplesPos.size() - 1);
        samplesPos.forEach((k, v) -> updSamplesPos.put(k, v < remPos ? v : v - 1)); // update positions
        updSamplesPos.remove(sampleName);

        List<List<String>> sd = new LinkedList<>(se.getSamplesData());
        sd.remove(remPos.intValue());
        se.setSamplesData(sd);
        se.setSamplesPosition(updSamplesPos);
    }

    /**
     * Checks if a Variant contains individuals with the ALT variant.
     * @param var Variant object.
     * @return boolean True, if one individual contains one ALT allele. Otherwise False e.g. only nocall, hom_ref or secondary alts.
     */
    private boolean containsVariants(Variant var) {
        StudyEntry se = var.getStudies().get(0);
        Integer gtPos = se.getFormatPositions().get("GT");
        List<List<String>> samplesData = se.getSamplesData();
        for (List<String> data : samplesData) {
            String gts = data.get(gtPos);
            if (gts.contains(",")) {
                for (String gt :  gts.split(",")) {
                    if (hasAlt(gt)) {
                        return true; // Found at least one ALT genotype
                    }
                }
            } else {
                if (hasAlt(gts)) {
                    return true; // Found at least one ALT genotype
                }
            }
        }
        // Only contains secondary alternate, HOM_REF, no-call, etc. -> remove!!!
        return false;
    }

    private boolean hasAlt(String gt) {
        int[] idxArr = new Genotype(gt).getAllelesIdx();
        for (int anIdxArr : idxArr) {
            if (anIdxArr == 1) {
                return true;
            }
        }
        return false;
    }

}
