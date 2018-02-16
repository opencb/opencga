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

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Mapper;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.hadoop.variant.gaps.FillMissingFromArchiveTask;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixKeyFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

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

    @Override
    protected void setup(Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Mutation>.Context context) throws IOException,
            InterruptedException {
        super.setup(context);
    }

    @Override
    protected void map(VariantMapReduceContext ctx) throws IOException, InterruptedException {
        Collection<Variant> updateLst = new HashSet<>();
//        Collection<Variant> removeLst = new ArrayList<>();

        for (Map.Entry<byte[], byte[]> entry : ctx.getValue().getFamilyMap(getHelper().getColumnFamily()).entrySet()) {
            Variant var = FillMissingFromArchiveTask.getVariantFromArchiveVariantColumn(ctx.getChromosome(), entry.getKey());
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


//        deleteVariantsFromAnalysisTable(ctx, removeLst);
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

    private void deleteSamplesFromAnalysisTable(VariantMapReduceContext variantContext, Collection<Variant> partialRemoveList)
            throws IOException, InterruptedException {
        if (variantContext.getSampleIds().isEmpty() && variantContext.getFileIdsInResult().isEmpty()) {
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

}
