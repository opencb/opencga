/**
 *
 */
package org.opencb.opencga.storage.hadoop.variant.index;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.AlternateCoordinate;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.tools.variant.merge.VariantMerger;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.models.protobuf.VariantTableStudyRowsProto;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Matthias Haimel mh719+git@cam.ac.uk
 */
public class VariantTableMapper extends AbstractVariantTableMapReduce {

    protected static final VariantType[] TARGET_VARIANT_TYPE = new VariantType[] {
            VariantType.SNV, VariantType.SNP,
            VariantType.INDEL, VariantType.INSERTION, VariantType.DELETION,
            VariantType.MNV, VariantType.MNP,
    };

    public static VariantType[] getTargetVariantType() {
        return Arrays.copyOf(TARGET_VARIANT_TYPE, TARGET_VARIANT_TYPE.length);
    }

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
        ARCHIVE_TABLE_SEC_ALT_VARIANTS,
        ANALYSIS_TABLE_VARIANTS,
        NEW_VARIANTS,
        MISSING_VARIANTS,
        SAME_VARIANTS
    }

    @Override
    protected void doMap(VariantMapReduceContext ctx) throws IOException, InterruptedException {
        List<Cell> list = ctx.getValue().getColumnCells(getHelper().getColumnFamily(), GenomeHelper
                .VARIANT_COLUMN_B);
        Cell latestCell = ctx.value.getColumnLatestCell(getHelper().getColumnFamily(), GenomeHelper.VARIANT_COLUMN_B);
        if (latestCell != null) {
            getLog().info("Column _V: found " + list.size() + " versions.");
            byte[] data = CellUtil.cloneValue(latestCell);
            VariantTableStudyRowsProto proto = VariantTableStudyRowsProto.parseFrom(data);
            if (proto.getTimestamp() == timestamp) {
                List<VariantTableStudyRow> variants = parseVariantStudyRowsFromArchive(ctx.getChromosome(), proto);
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
        completeAlternateCoordinates(archiveVar);
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
        endTime("4 Filter archive variants by target");

        // Check if Archive covers all bases in Analysis
        // TODO switched off at the moment down to removed variant calls from gVCF files (malformated variants)
//        checkArchiveConsistency(ctx.context, ctx.startPos, ctx.nextStartPos, archiveVar, analysisVar);

        endTime("5 Check consistency -- skipped");

        /* ******** Update Analysis Variants ************** */
        Set<Variant> analysisNew = getNewVariantsAsTemplates(ctx, analysisVar, archiveTarget);

        endTime("6 Create NEW variants");

        // with current files of same region
        for (Variant var : analysisNew) {
            Collection<Variant> cleanList = buildOverlappingNonRedundantSet(var, archiveVar);
            this.getVariantMerger().merge(var, cleanList);
        }
        endTime("7 Merge NEW variants");

        // with all other gVCF files of same region
        if (!analysisNew.isEmpty()) {
            List<Variant> archiveOther = loadFromArchive(ctx.context, ctx.getCurrRowKey(), ctx.fileIds);
            endTime("8 Load archive slice from hbase");
            if (!archiveOther.isEmpty()) {
                completeAlternateCoordinates(archiveOther);
                ctx.context.getCounter(COUNTER_GROUP_NAME, "OTHER_VARIANTS_FROM_ARCHIVE").increment(archiveOther.size());
                ctx.context.getCounter(COUNTER_GROUP_NAME, "OTHER_VARIANTS_FROM_ARCHIVE_NUM_QUERIES").increment(1);
                for (Variant var : analysisNew) {
                    Collection<Variant> cleanList = buildOverlappingNonRedundantSet(var, archiveOther);
                    this.getVariantMerger().merge(var, cleanList);
                }
                endTime("8 Merge NEW with archive slice");
            }
        }

        // (2) and (3): Same, missing (and overlapping missing) variants
        for (Variant var : analysisVar) {
            Collection<Variant> cleanList = buildOverlappingNonRedundantSet(var, archiveVar);
            this.getVariantMerger().merge(var, cleanList);
        }
        endTime("9 Merge same and missing");

        // WRITE VALUES
        List<VariantTableStudyRow> rows = new ArrayList<>(analysisNew.size() + analysisVar.size());
        updateOutputTable(ctx.context, analysisNew, rows, null);
        updateOutputTable(ctx.context, analysisVar, rows, ctx.sampleIds);
        endTime("10 Update OUTPUT table");
        endTime("10 Update OUTPUT table");

        updateArchiveTable(ctx.getCurrRowKey(), ctx.context, rows);
        endTime("11 Update INPUT table");
    }

    private void completeAlternateCoordinates(List<Variant> variants) {
        for (Variant variant : variants) {
            for (StudyEntry study : variant.getStudies()) {
                List<AlternateCoordinate> alternates = study.getSecondaryAlternates();
                if (alternates != null) {
                    for (AlternateCoordinate alt : alternates) {
                        alt.setChromosome(alt.getChromosome() == null ? variant.getChromosome() : alt.getChromosome());
                        alt.setStart(alt.getStart() == null ? variant.getStart() : alt.getStart());
                        alt.setEnd(alt.getEnd() == null ? variant.getEnd() : alt.getEnd());
                        alt.setReference(alt.getReference() == null ? variant.getReference() : alt.getReference());
                        alt.setAlternate(alt.getAlternate() == null ? variant.getAlternate() : alt.getAlternate());

                    }
                }
            }
        }
    }

    private Set<Variant> getNewVariantsAsTemplates(VariantMapReduceContext ctx, List<Variant> analysisVar,
                                                   List<Variant> archiveTarget) {
        String studyId = Integer.toString(getStudyConfiguration().getStudyId());
        // (1) NEW variants (only create the position, no filling yet)
        Set<String> analysisVarSet = analysisVar.stream().map(Variant::toString).collect(Collectors.toSet());
        analysisVarSet.addAll(analysisVar.stream().flatMap(v -> v.getStudy(studyId).getSecondaryAlternates().stream())
                .map(a -> toVariantString(a)).collect(Collectors.toSet()));
        Set<Variant> analysisNew = new HashSet<>();
        Set<String> archiveTargetSet = new HashSet<>();
        Set<String> secAltTargetSet = new HashSet<>();

        // For all main variants
        for (Variant tar : archiveTarget) {
            // Get all the archive target variants that are not in the analysis variants.
            // is new Variant?
            String tarString = tar.toString();
            archiveTargetSet.add(tarString);
            if (!analysisVarSet.contains(tarString)) {
                // Empty variant with no Sample information
                // Filled with Sample information later (see 2)
                StudyEntry se = tar.getStudy(studyId);
                if (null == se) {
                    throw new IllegalStateException(String.format(
                            "Study Entry for study %s of target variant is null: %s",  studyId, tar));
                }
                Variant tarNew = this.getVariantMerger().createFromTemplate(tar);
                analysisNew.add(tarNew);
            }
        }

        // For all SecondaryAlternate
        for (Variant tar : archiveTarget) {
            List<AlternateCoordinate> secAlt = tar.getStudy(studyId).getSecondaryAlternates();
            for (AlternateCoordinate coordinate : secAlt) {
                String variantString = toVariantString(coordinate);
                if (!archiveTargetSet.contains(variantString) && !secAltTargetSet.contains(variantString)
                        && !analysisVarSet.contains(variantString)) {
                    secAltTargetSet.add(variantString);
                    // Create new Variant from Secondary Alternate
                    String chromosome = useUnlessNull(coordinate.getChromosome(), tar.getChromosome());
                    Integer start = useUnlessNull(coordinate.getStart(), tar.getStart());
                    Integer end = useUnlessNull(coordinate.getEnd(), tar.getEnd());
                    String reference = useUnlessNull(coordinate.getReference(), tar.getReference());
                    String alternate = coordinate.getAlternate();
                    VariantType type = coordinate.getType();
                    try {
                        Variant tarNew = new Variant(chromosome, start, end, reference, alternate);
                        tarNew.setType(type);
                        for (StudyEntry tse : tar.getStudies()) {
                            StudyEntry se = new StudyEntry(tse.getStudyId());
                            se.setFiles(Collections.singletonList(new FileEntry("", "", new HashMap<>())));
                            se.setFormat(Arrays.asList(VariantMerger.GT_KEY, VariantMerger.GENOTYPE_FILTER_KEY));
                            se.setSamplesPosition(new HashMap<>());
                            se.setSamplesData(new ArrayList<>());
                            tarNew.addStudyEntry(se);
                        }
                        analysisNew.add(tarNew);
                    } catch (NullPointerException e) {
                        throw new IllegalStateException(StringUtils.join(new Object[]{
                                "Chr: ", chromosome, "Start: ", start, "End: ", end, "Ref: ", reference,
                                "ALT: ", alternate, }, ";"), e);
                    }
                }
            }
        }
        Set<String> totalSet = new HashSet<>(archiveTargetSet);
        totalSet.addAll(secAltTargetSet);
        int sameVariants = totalSet.size() - analysisNew.size();
        ctx.context.getCounter(OpenCGAVariantTableCounters.SAME_VARIANTS).increment(sameVariants);
        ctx.context.getCounter(OpenCGAVariantTableCounters.NEW_VARIANTS).increment(analysisNew.size());
        ctx.context.getCounter(OpenCGAVariantTableCounters.MISSING_VARIANTS).increment(analysisVarSet.size() - sameVariants);
        ctx.context.getCounter(OpenCGAVariantTableCounters.ARCHIVE_TABLE_VARIANTS).increment(archiveTargetSet.size());
        ctx.context.getCounter(OpenCGAVariantTableCounters.ARCHIVE_TABLE_SEC_ALT_VARIANTS).increment(secAltTargetSet.size());
        ctx.context.getCounter(OpenCGAVariantTableCounters.ANALYSIS_TABLE_VARIANTS).increment(analysisVar.size());
        return analysisNew;
    }

    private <T> T useUnlessNull(T a, T b) {
        return a != null ? a : b;
    }

    protected String toVariantString(AlternateCoordinate alt) {
        if (alt.getReference() == null) {
            return alt.getChromosome() + ":" + alt.getStart() + ":"
                    + (StringUtils.isEmpty(alt.getAlternate()) ? "-" : alt.getAlternate());
        } else {
            return alt.getChromosome() + ":" + alt.getStart() + ":"
                    + (StringUtils.isEmpty(alt.getReference()) ? "-" : alt.getReference())
                    + ":" + (StringUtils.isEmpty(alt.getAlternate()) ? "-" : alt.getAlternate());
        }
    }

    private Collection<Variant> buildOverlappingNonRedundantSet(Variant var, List<Variant> archiveVar) {
        List<Variant> overlap =
                archiveVar.stream().filter(v -> VariantMerger.hasAnyOverlap(var, v)).collect(Collectors.toList());
        Set<String> origCalls = new HashSet<>();
        List<Variant> uniqueList = new ArrayList<>();
        for (Variant variant : overlap) {
            FileEntry fileEntry = variant.getStudies().get(0).getFiles().get(0);
            String call = fileEntry.getCall();
            if (StringUtils.isBlank(call)) {
                uniqueList.add(variant);
            } else {
                String fileId = fileEntry.getFileId();
                String id = call.substring(0, call.lastIndexOf(':')) + "-" + fileId;
                if (!origCalls.contains(id)) {
                    origCalls.add(id);
                    uniqueList.add(variant);
                }
            }
        }
        return uniqueList;
    }

    /**
     * Load all variants for all files (except in currFileIds) listed in the study configuration for the specified rowKey.
     * @param context Context
     * @param rowKey Slice to extract data for
     * @param currFileIds File ids to ignore
     * @return Variants all variants for the slice
     * @throws IOException
     */
    private List<Variant> loadFromArchive(Context context, byte[] rowKey, Set<Integer> currFileIds) throws IOException {
        // Extract File IDs to search through
        LinkedHashSet<Integer> indexedFiles = getStudyConfiguration().getIndexedFiles();
        Set<String> archiveFileIds = indexedFiles.stream().filter(k -> !currFileIds.contains(k)).map(s -> s.toString())
                .collect(Collectors.toSet());
        if (archiveFileIds.isEmpty()) {
            if (getLog().isDebugEnabled()) {
                getLog().debug("No files found to search for in archive table");
            }
            return Collections.emptyList();
        }
        if (getLog().isDebugEnabled()) {
            getLog().debug("Add files to search in archive: " + StringUtils.join(archiveFileIds, ','));
        }
        Get get = new Get(rowKey);
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

    protected Set<Integer> generateRegion(Integer start, Integer end) {
        if (end < start) {
            end = start;
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

}
