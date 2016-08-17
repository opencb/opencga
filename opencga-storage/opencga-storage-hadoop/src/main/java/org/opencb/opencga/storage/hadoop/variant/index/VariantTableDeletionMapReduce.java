/**
 *
 */
package org.opencb.opencga.storage.hadoop.variant.index;

import com.google.common.collect.BiMap;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Mapper;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;

import java.io.IOException;
import java.util.*;

/**
 * Removes Sample data for a provided file from the Analysis (Variant) and the
 * file data from the Archive Table.
 *
 * @author Matthias Haimel mh719+git@cam.ac.uk
 *
 */
public class VariantTableDeletionMapReduce extends AbstractVariantTableMapReduce {

    private Table analysisTable;
    private Table archiveTable;

    @Override
    protected void setup(Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Mutation>.Context context) throws IOException,
            InterruptedException {
        super.setup(context);
        this.analysisTable = getDbConnection().getTable(TableName.valueOf(getHelper().getOutputTable()));
        this.archiveTable = getDbConnection().getTable(TableName.valueOf(getHelper().getIntputTable()));
    }

    @Override
    protected void doMap(VariantMapReduceContext ctx) throws IOException, InterruptedException {
        List<Variant> updateLst = new ArrayList<>();
        List<Variant> removeLst = new ArrayList<>();
        BiMap<Integer, String> sampleIds = getStudyConfiguration().getSampleIds().inverse();

        List<Variant> analysisVar = parseCurrentVariantsRegion(ctx.getValue(), ctx.getChromosome());
        ctx.getContext().getCounter(COUNTER_GROUP_NAME, "VARIANTS_FROM_ANALYSIS").increment(analysisVar.size());
        getLog().info("Loaded {} variants ... ", analysisVar.size());
        if (!analysisVar.isEmpty()) {
            Variant tmpVar = analysisVar.get(0);
            if (getLog().isDebugEnabled()) {
                getLog().debug("Loaded variant from analysis table: " + tmpVar.toJson());
            }
        }

        endTime("2 Unpack and convert input ANALYSIS variants (" + GenomeHelper.VARIANT_COLUMN + ")");


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

        List<VariantTableStudyRow> rows = new ArrayList<>();
        deleteFromAnalysisTable(ctx.context, removeLst);
        updateOutputTable(ctx.context, updateLst, rows, null);
        updateArchiveTable(ctx.getCurrRowKey(), ctx.context, rows);
        deleteFromArchiveTable(ctx.context, ctx.currRowKey, ctx.fileIds);
    }

    private void deleteFromArchiveTable(Context context, byte[] rowKey, Set<Integer> fileIds) throws IOException {
        byte[] cf = getHelper().getColumnFamily();
        Delete del = new Delete(rowKey); // TODO HBase time stamp specific delete -> more efficient
        for (Integer fid : fileIds) {
            del.addColumn(cf, Bytes.toBytes(fid.toString()));
        }
        context.getCounter(COUNTER_GROUP_NAME, "ARCHIVE_TABLE_ROW-DELETE_cells").increment(fileIds.size());
        context.getCounter(COUNTER_GROUP_NAME, "ARCHIVE_TABLE_ROW-DELETE_commands").increment(1);
        this.archiveTable.delete(del);
    }

    private void deleteFromAnalysisTable(Context context, List<Variant> removeLst) throws IOException, InterruptedException {
        int studyId = getStudyConfiguration().getStudyId();
        BiMap<String, Integer> idMapping = getStudyConfiguration().getSampleIds();
        for (Variant variant : removeLst) {
            VariantTableStudyRow row = new VariantTableStudyRow(variant, studyId, idMapping);
            Delete delete = row.createDelete(getHelper());
//            this.analysisTable.delete(delete);
            context.write(new ImmutableBytesWritable(getHelper().getOutputTable()), delete);
            context.getCounter(COUNTER_GROUP_NAME, "ANALYSIS_TABLE_ROW-DELETE").increment(1);
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
            throw new IllegalStateException(String.format("No study found in variant {0}", var));
        }
        LinkedHashMap<String, Integer> samplesPos = se.getSamplesPosition();
        Integer remPos = samplesPos.get(sampleName);
        if (remPos == null) {
            throw new IllegalStateException(String.format("Sample {0} not found for variant {1}", sampleName, var));
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
