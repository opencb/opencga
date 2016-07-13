/**
 *
 */
package org.opencb.opencga.storage.hadoop.variant.index;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Matthias Haimel mh719+git@cam.ac.uk
 */
public class VariantTableMapper extends AbstractVariantTableMapReduce {

    public static final VariantType[] TARGET_VARIANT_TYPE = new VariantType[] {
            VariantType.SNV, VariantType.SNP,
            VariantType.INDEL, VariantType.INSERTION, VariantType.DELETION,
            VariantType.MNV, VariantType.MNP,
    };

    /*
     *
     *             +---------+----------+
     *             | ARCHIVE | ANALYSIS |
     *  +----------+---------+----------+
     *  | 1:10:A:T |   DATA  |   ----   |   <= New variant          (1)
     *  +----------+---------+----------+
     *  | 1:20:C:G |   ----  |   DATA   |   <= Missing variant      (2)
     *  +----------+---------+----------+
     *  | 1:30:G:T |   DATA  |   DATA   |   <= Same variant         (3)
     *  +----------+---------+----------+
     *  | 1:40:T:C |   DATA  |   ----   |   <= Overlapped variant (new)
     *  | 1:40:T:G |   ----  |   DATA   |   <= Overlapped variant (missing)
     *  +----------+---------+----------+
     *
     */
    public enum OpenCGAVariantTableCounters {
        ARCHIVE_TABLE_VARIANTS,
        ANALYSIS_TABLE_VARIANTS,
        NEW_VARIANTS,
        MISSING_VARIANTS,
        SAME_VARIANTS
    }

    @Override
    protected void doMap(VariantMapReduceContext ctx) throws IOException, InterruptedException {

        Cell latestCell = ctx.value.getColumnLatestCell(getHelper().getColumnFamily(), GenomeHelper.VARIANT_COLUMN_B);
        if (latestCell != null) {
            if (latestCell.getTimestamp() == timestamp) {

                List<VariantTableStudyRow> variants = parseVariantStudyRowsFromArchive(ctx.getValue(), ctx.getChromosome());

                ctx.context.getCounter(COUNTER_GROUP_NAME, "ALREADY_LOADED_SLICE").increment(1);
                ctx.context.getCounter(COUNTER_GROUP_NAME, "ALREADY_LOADED_ROWS").increment(variants.size());

                updateOutputTable(ctx.context, variants);
                endTime("X Unpack, convert and write ANALYSIS variants (" + GenomeHelper.VARIANT_COLUMN + ")");
                return;
            }
        }

        List<Variant> analysisVar = parseCurrentVariantsRegion(ctx.getValue(), ctx.getChromosome());
        ctx.getContext().getCounter(COUNTER_GROUP_NAME, "VARIANTS_FROM_ANALYSIS").increment(analysisVar.size());
        endTime("2 Unpack and convert input ANALYSIS variants (" + GenomeHelper.VARIANT_COLUMN + ")");


        // Archive: unpack Archive data (selection only
        List<Variant> archiveVar = getResultConverter().convert(ctx.value, ctx.startPos, ctx.nextStartPos, true);
        ctx.context.getCounter(COUNTER_GROUP_NAME, "VARIANTS_FROM_ARCHIVE").increment(archiveVar.size());

        endTime("3 Unpack and convert input ARCHIVE variants");

        // Variants of target type
        List<Variant> archiveTarget = filterForVariant(archiveVar.stream(), TARGET_VARIANT_TYPE).collect(Collectors.toList());
        if (!archiveTarget.isEmpty()) {
            Variant tmpVar = archiveTarget.get(0);
            if (getLog().isDebugEnabled()) {
                getLog().debug("Loaded variant from archive table: " + tmpVar.toJson());
            }
        }
        ctx.context.getCounter(COUNTER_GROUP_NAME, "VARIANTS_FROM_ARCHIVE_TARGET").increment(archiveTarget.size());

//            Set<Variant> analysisVarSet = new HashSet<>(analysisVar);
        endTime("4 Filter archive variants by target");

        // Check if Archive covers all bases in Analysis
        checkArchiveConsistency(ctx.context, ctx.startPos, ctx.nextStartPos, archiveVar, analysisVar);

        endTime("5 Check consistency");

        /* ******** Update Analysis Variants ************** */
        // (1) NEW variants (only create the position, no filling yet)
        Set<String> analysisVarSet = analysisVar.stream().map(Variant::toString).collect(Collectors.toSet());
        Set<Variant> analysisNew = new HashSet<>();
        Set<String> archiveTargetSet = new HashSet<>();
        for (Variant tar : archiveTarget) {
            // Get all the archive target variants that are not in the analysis variants.
//                Optional<Variant> any = analysisVar.stream().filter(v -> VariantMerger.isSameVariant(v, tar)).findAny();
            // is new Variant?
            String tarString = tar.toString();
            archiveTargetSet.add(tarString);
            if (!analysisVarSet.contains(tarString)) {
                // Empty variant with no Sample information
                // Filled with Sample information later (see 2)
                String studyId = Integer.toString(getStudyConfiguration().getStudyId());
                StudyEntry se = tar.getStudy(studyId);
                if (null == se) {
                    throw new IllegalStateException(String.format(
                            "Study Entry for study %s of target variant is null: %s",  studyId, tar));
                }
                Variant tarNew = this.getVariantMerger().createFromTemplate(tar);
                analysisNew.add(tarNew);
            }
        }
        endTime("6 Create NEW variants");

        int sameVariants = archiveTargetSet.size() - analysisNew.size();
        ctx.context.getCounter(OpenCGAVariantTableCounters.NEW_VARIANTS).increment(analysisNew.size());
        ctx.context.getCounter(OpenCGAVariantTableCounters.SAME_VARIANTS).increment(sameVariants);
        ctx.context.getCounter(OpenCGAVariantTableCounters.MISSING_VARIANTS).increment(analysisVar.size() - sameVariants);
        ctx.context.getCounter(OpenCGAVariantTableCounters.ARCHIVE_TABLE_VARIANTS).increment(archiveTargetSet.size());
        ctx.context.getCounter(OpenCGAVariantTableCounters.ANALYSIS_TABLE_VARIANTS).increment(analysisVar.size());

        // with current files of same region
        for (Variant var : analysisNew) {
            this.getVariantMerger().merge(var, archiveVar);
        }
        endTime("7 Merge NEW variants");

        // with all other gVCF files of same region
        if (!analysisNew.isEmpty()) {
            List<Variant> archiveOther = loadFromArchive(ctx.context, ctx.sliceKey, ctx.fileIds);
            endTime("8 Load archive slice from hbase");
            if (!archiveOther.isEmpty()) {
                ctx.context.getCounter(COUNTER_GROUP_NAME, "OTHER_VARIANTS_FROM_ARCHIVE").increment(archiveOther.size());
                ctx.context.getCounter(COUNTER_GROUP_NAME, "OTHER_VARIANTS_FROM_ARCHIVE_NUM_QUERIES").increment(1);
                for (Variant var : analysisNew) {
                    this.getVariantMerger().merge(var, archiveOther);
                }
                endTime("8 Merge NEW with archive slice");
            }
        }

        // (2) and (3): Same, missing (and overlapping missing) variants
        for (Variant var : analysisVar) {
            this.getVariantMerger().merge(var, archiveVar);
        }
        endTime("9 Merge same and missing");

        // WRITE VALUES
        List<VariantTableStudyRow> rows = new ArrayList<>(analysisNew.size() + analysisVar.size());
        updateOutputTable(ctx.context, analysisNew, rows, null);
        updateOutputTable(ctx.context, analysisVar, rows, ctx.sampleIds);
        endTime("10 Update OUTPUT table");

        updateArchiveTable(ctx.key, ctx.context, rows);
        endTime("11 Update INPUT table");
    }

    /**
     * Load all variants for all files (except in currFileIds) listed in the study configuration for the specified sliceKey.
     * @param context Context
     * @param sliceKey Slice to extract data for
     * @param currFileIds File ids to ignore
     * @return Variants all variants for the slice
     * @throws IOException
     */
    private List<Variant> loadFromArchive(Context context, String sliceKey, Set<Integer> currFileIds) throws IOException {
        // Extract File IDs to search through
        LinkedHashSet<Integer> indexedFiles = getStudyConfiguration().getIndexedFiles();
        Set<String> archiveFileIds = indexedFiles.stream().filter(k -> !currFileIds.contains(k)).map(s -> s.toString())
                .collect(Collectors.toSet());
        if (archiveFileIds.isEmpty()) {
            getLog().info("No files found to search for in archive table");
            return Collections.emptyList();
        }
        if (getLog().isDebugEnabled()) {
            getLog().debug("Add files to search in archive: " + StringUtils.join(archiveFileIds, ','));
        }
        Get get = new Get(Bytes.toBytes(sliceKey));
        byte[] cf = getHelper().getColumnFamily();
        archiveFileIds.forEach(e -> get.addColumn(cf, Bytes.toBytes(e)));
        Result res = getHelper().getHBaseManager().act(getDbConnection(), getHelper().getIntputTable(), table -> {
            return table.get(get);
        });
        if (res.isEmpty()) {
            getLog().warn("No data found in archive table!!!");
            return Collections.emptyList();
        }
        List<Variant> var = getResultConverter().convert(res, true);
        return var;
    }

    /**
     * Check if Archive has Variant objects covering all bases (including no-call objects).
     * Increases HBase counter with the name VCF_VARIANT-error-FIXME to act on.
     * @param context
     * @param startPos
     * @param nextStartPos
     * @param archiveVar
     * @param analysisVar
     */
    private void checkArchiveConsistency(Context context, long startPos,
            long nextStartPos, List<Variant> archiveVar, List<Variant> analysisVar) {
        // Report Missing regions in ARCHIVE table, which are seen in VAR table
        Set<Integer> archPosMissing = generateCoveredPositions(analysisVar.stream(), startPos, nextStartPos);
        archPosMissing.removeAll(generateCoveredPositions(archiveVar.stream(), startPos, nextStartPos));
        if (!archPosMissing.isEmpty()) {
            // should never happen - positions exist in variant table but not in archive table
            context.getCounter(COUNTER_GROUP_NAME, "VCF_VARIANT-error-FIXME").increment(1);
            getLog().error(
                    String.format("Positions found in variant table but not in Archive table: %s",
                            Arrays.toString(archPosMissing.toArray(new Integer[0]))));
        }
    }

    protected Set<Integer> generateCoveredPositions(Stream<Variant> variants, long startPos, long nextStartPos) {
        final int sPos = (int) startPos;
        final int ePos = (int) (nextStartPos - 1);
        // limit to max start position end min end position (only slice region)
        // hope this works
        return variants.map(v -> generateRegion(Math.max(v.getStart(), sPos), Math.min(v.getEnd(), ePos))).flatMap(l -> l.stream())
                .collect(Collectors.toSet());
    }

    private Set<Integer> generateRegion(Integer start, Integer end) {
        if (end < start) {
            throw new IllegalStateException(String.format("End position (%s) is < than Start (%s)!!!", start, end));
        }
        int len = end - start;
        Integer[] array = new Integer[len + 1];
        for (int a = 0; a <= len; a++) { // <= to be inclusive
            array[a] = (start + a);
        }
        return new HashSet<Integer>(Arrays.asList(array));
    }


    protected Stream<Variant> filterForVariant(Stream<Variant> variants, VariantType ... types) {
        Set<VariantType> whileList = new HashSet<>(Arrays.asList(types));
        return variants.filter(v -> whileList.contains(v.getType()));
    }

    /**
     * Fetch already loaded variants in the Variant Table.
     * @param context {@link Context}
     * @param chr Chromosome
     * @param start Start (inclusive) position
     * @param end End (exclusive) position
     * @return L
     * @throws IOException If any IO issue occurs
     * @deprecated Do not read from VariantTable anymore! Use {@link #parseCurrentVariantsRegion} instead
     */
    @Deprecated
    protected List<Variant> loadCurrentVariantsRegion(Context context, String chr, Long start, Long end)
            throws IOException {
        String colPrefix = getHelper().getStudyId() + "_";
        byte[] startKey = getHelper().generateVariantPositionPrefix(chr, start);
        byte[] endKey = getHelper().generateVariantPositionPrefix(chr, end);
        List<Variant> analysisVariants = new ArrayList<Variant>();
//        boolean foundScan = false; // FIXME clean up
        try (Table table = getDbConnection().getTable(TableName.valueOf(getHelper().getOutputTable()));) {
            context.getCounter(COUNTER_GROUP_NAME, "VCF_TABLE_SCAN-query").increment(1);
            if (getLog().isDebugEnabled()) {
                getLog().debug(String.format("Scan chr %s from %s to %s with column prefix %s", chr, start, end, colPrefix));
            }

            Scan scan = new Scan(startKey, endKey);
            scan.setFilter(new ColumnPrefixFilter(Bytes.toBytes(colPrefix))); // Limit to current study
            ResultScanner rs = table.getScanner(scan);
            for (Result r : rs) {
//                foundScan = true;
                Variant var = this.getHbaseToVariantConverter().convert(r);
                if (var.getStudiesMap().isEmpty()) {
                    throw new IllegalStateException("No Studies registered for variant!!! " + var);
                }
                analysisVariants.add(var);
                if (!r.containsColumn(this.getHelper().getColumnFamily(), Bytes.toBytes(colPrefix + "0/0"))) {
                    throw new IllegalStateException("Hom-ref column not found for prefix: " + var);
                }
            }
        }
//        if (!foundScan) {
//            throw new IllegalStateException(String.format("No result returned after scan using prefix %s", colPrefix));
//        }
//        if (analysisVariants.isEmpty()) {
//            throw new IllegalStateException(String.format("No Variants found using prefix %s", colPrefix));
//        }
//        Set<String> maplst = analysisVariants.stream().flatMap(v -> v.getStudiesMap().keySet().stream()).collect(Collectors.toSet());
//        if (maplst.isEmpty()) {
//            throw new IllegalStateException("No study data loaded at all for " + colPrefix + "; ");
//        }
//        List<Variant> noStudy = analysisVariants.stream().filter(v -> v.getStudy(Integer.toString(getHelper().getStudyId())) == null)
//                .collect(Collectors.toList());
//        if (!noStudy.isEmpty()) {
//            throw new IllegalStateException("Loaded variants with no Study id!!! using prefix  " + colPrefix + "; " + noStudy.size() + ";"
//                    + Strings.join(maplst, ","));
//        }
        return analysisVariants;
    }

}
