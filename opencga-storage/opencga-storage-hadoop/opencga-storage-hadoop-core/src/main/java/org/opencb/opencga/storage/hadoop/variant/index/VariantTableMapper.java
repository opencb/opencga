/*
 * Copyright 2015-2016 OpenCB
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
import java.util.stream.IntStream;
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
        List<Cell> variantCells = GenomeHelper.getVariantColumns(ctx.getValue().rawCells());
        if (!variantCells.isEmpty()) {
            byte[] data = CellUtil.cloneValue(variantCells.get(0));
            VariantTableStudyRowsProto proto = VariantTableStudyRowsProto.parseFrom(data);
            getLog().info("Column _V: found " + variantCells.size()
                    + " columns - check timestamp " + timestamp + " with " + proto.getTimestamp());
            if (proto.getTimestamp() == timestamp) {
                ctx.context.getCounter(COUNTER_GROUP_NAME, "ALREADY_LOADED_SLICE").increment(1);
                for (Cell cell : variantCells) {
                    VariantTableStudyRowsProto rows = VariantTableStudyRowsProto.parseFrom(CellUtil.cloneValue(cell));
                    List<VariantTableStudyRow> variants = parseVariantStudyRowsFromArchive(ctx.getChromosome(), rows);
                    ctx.context.getCounter(COUNTER_GROUP_NAME, "ALREADY_LOADED_ROWS").increment(variants.size());
                    updateOutputTable(ctx.context, variants);
                }
                endTime("X Unpack, convert and write ANALYSIS variants (" + GenomeHelper.VARIANT_COLUMN_PREFIX + ")");
                return;
            }
        }
        getLog().info("Parse ...");
        List<Variant> analysisVar = parseCurrentVariantsRegion(variantCells, ctx.getChromosome());
        ctx.getContext().getCounter(COUNTER_GROUP_NAME, "VARIANTS_FROM_ANALYSIS").increment(analysisVar.size());
        endTime("2 Unpack and convert input ANALYSIS variants (" + GenomeHelper.VARIANT_COLUMN_PREFIX + ")");

        getLog().info("Archive ...");
        // Archive: unpack Archive data (selection only
        List<Variant> archiveVar = getResultConverter().convert(ctx.value, ctx.startPos, ctx.nextStartPos, true);
        getLog().info("Complete ...");
        completeAlternateCoordinates(archiveVar);
        ctx.context.getCounter(COUNTER_GROUP_NAME, "VARIANTS_FROM_ARCHIVE").increment(archiveVar.size());

        endTime("3 Unpack and convert input ARCHIVE variants");

        getLog().info("Filter ...");
        // Variants of target type
        List<Variant> archiveTarget = filterForVariant(archiveVar.stream(), TARGET_VARIANT_TYPE).collect(Collectors.toList());
        if (!archiveTarget.isEmpty()) {
            Variant tmpVar = archiveTarget.get(0);
            if (getLog().isDebugEnabled()) {
                getLog().debug("Loaded variant from archive table: " + tmpVar.toJson());
            }
        }

        getLog().info("Loaded current: " + analysisVar.size()
                + "; archive: " + archiveVar.size()
                + "; target: " + archiveTarget.size());
        ctx.context.getCounter(COUNTER_GROUP_NAME, "VARIANTS_FROM_ARCHIVE_TARGET").increment(archiveTarget.size());
        endTime("4 Filter archive variants by target");

        // Check if Archive covers all bases in Analysis
        // TODO switched off at the moment down to removed variant calls from gVCF files (malformated variants)
//        checkArchiveConsistency(ctx.context, ctx.startPos, ctx.nextStartPos, archiveVar, analysisVar);

        endTime("5 Check consistency -- skipped");

        /* ******** Update Analysis Variants ************** */
        Set<Variant> analysisNew = getNewVariantsAsTemplates(ctx, analysisVar, archiveTarget, (int) ctx.startPos, (int) ctx.nextStartPos);

        endTime("6 Create NEW variants");

        getLog().info("Index ...");
        NavigableMap<Integer, List<Variant>> varPosRegister = indexAlts(archiveVar, (int) ctx.startPos, (int) ctx.nextStartPos);

        getLog().info("Merge {} new variants ", analysisNew.size());
        long overlap = 0;
        long merge = 0;

        // with current files of same region
        for (Variant var : analysisNew) {
            long start = System.nanoTime();
            Collection<Variant> cleanList = buildOverlappingNonRedundantSet(var, varPosRegister);
            long mid = System.nanoTime();
            this.getVariantMerger().merge(var, cleanList);
            long end = System.nanoTime();
            overlap += mid - start;
            merge += end - mid;
        }
        getLog().info("Merge 1 - overlap {}; merge {}; ns", overlap, merge);

        endTime("7 Merge NEW variants");

        // with all other gVCF files of same region
        if (!analysisNew.isEmpty()) {
            List<Variant> archiveOther = loadFromArchive(ctx.context, ctx.getCurrRowKey(), ctx.fileIds);
            getLog().info("Loaded " + archiveOther.size() + " variants ... ");
            endTime("8 Load archive slice from hbase");
            if (!archiveOther.isEmpty()) {
                overlap = 0;
                merge = 0;
                completeAlternateCoordinates(archiveOther);
                getLog().info("Complete Alternate coordinates ... ");
                NavigableMap<Integer, List<Variant>> varPosSortedOther = indexAlts(archiveOther, (int)ctx.startPos, (int)ctx.nextStartPos);
                getLog().info("Create alts index of size " + varPosSortedOther.size() + " ... ");
                Set<Integer> coveredPositions = analysisNew.stream().flatMap(var -> {
                    int min = toPosition(var, true);
                    int max = toPosition(var, false);
                    return IntStream.range(min, max + 1).boxed();
                }).collect(Collectors.toSet());
                NavigableMap<Integer, List<Variant>> varFiltered = new TreeMap<>(
                        coveredPositions.stream().filter(k -> varPosSortedOther.containsKey(k))
                        .collect(Collectors.toMap(k -> k, k -> varPosSortedOther.get(k))));
                getLog().info("Pre-filter alts index to " + varFiltered.size() + " ... ");
                ctx.context.getCounter(COUNTER_GROUP_NAME, "OTHER_VARIANTS_FROM_ARCHIVE").increment(archiveOther.size());
                ctx.context.getCounter(COUNTER_GROUP_NAME, "OTHER_VARIANTS_FROM_ARCHIVE_NUM_QUERIES").increment(1);
                for (Variant var : analysisNew) {
                    long start = System.nanoTime();
                    Collection<Variant> cleanList = buildOverlappingNonRedundantSet(var, varFiltered);
                    long mid = System.nanoTime();
                    this.getVariantMerger().merge(var, cleanList);
                    overlap += mid - start;
                    merge += System.nanoTime() - mid;
                    getLog().info("Merge snapshot 2 - overlap {}; merge {}; ns", overlap, merge);
                }
                getLog().info("Merge 2 - overlap {}; merge {}; ns", overlap, merge);
                endTime("8 Merge NEW with archive slice");
            }
        }

        // (2) and (3): Same, missing (and overlapping missing) variants
        for (Variant var : analysisVar) {
            Collection<Variant> cleanList = buildOverlappingNonRedundantSet(var, varPosRegister);
            this.getVariantMerger().merge(var, cleanList);
        }
        endTime("9 Merge same and missing");

        // WRITE VALUES
        List<VariantTableStudyRow> rows = new ArrayList<>(analysisNew.size() + analysisVar.size());
        updateOutputTable(ctx.context, analysisNew, rows, null);
        updateOutputTable(ctx.context, analysisVar, rows, ctx.sampleIds);
        endTime("10 Update OUTPUT table");

        updateArchiveTable(ctx.getCurrRowKey(), ctx.context, rows);
        endTime("11 Update INPUT table");
    }

    private NavigableMap<Integer, List<Variant>> indexAlts(List<Variant> variants, int startPos, int nextStartPos) {
        NavigableMap<Integer, List<Variant>> retMap = new TreeMap<>();
        variants.forEach(v -> {
            IntStream.range(Math.max(toPosition(v, true), startPos), Math.min(toPosition(v, false) + 1, nextStartPos))
                    .forEach(p -> {
                        List<Variant> lst = retMap.get(p);
                        if (null == lst) {
                            lst = new ArrayList<Variant>();
                            retMap.put(p, lst);
                        }
                        lst.add(v);
                    });
        });
        return retMap;
    }

    private static Integer toPosition(Variant variant, boolean isStart) {
        Integer pos = getPosition(variant.getStart(), variant.getEnd(), isStart);
        for (StudyEntry study : variant.getStudies()) {
            List<AlternateCoordinate> alternates = study.getSecondaryAlternates();
            if (alternates != null) {
                for (AlternateCoordinate alt : alternates) {
                    pos =  getPosition(pos, getPosition(alt.getStart(), alt.getEnd(), isStart), isStart);
                }
            }
        }
        return pos;
    }

    private static Integer getPosition(Integer start, Integer end, boolean isStart) {
        return isStart ? Math.min(start, end) : Math.max(start, end);
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

    private Set<Variant> getNewVariantsAsTemplates(
            VariantMapReduceContext ctx, List<Variant> analysisVar,
            List<Variant> archiveTarget, int startPos, int nextStartPos) {
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
            int minStart = Math.min(tar.getStart(), tar.getEnd());
            if (minStart < startPos || minStart >= nextStartPos) {
                continue; // Skip variants with start position in previous or next slice
            }
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
                int minStart = Math.min(coordinate.getStart(), coordinate.getEnd());
                if (minStart < startPos || minStart >= nextStartPos) {
                    continue; // Skip variants with start position in previous or next slice
                }
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

    // Find only Objects with the same object ID
    private static class VariantWrapper {
        private Variant var = null;
        VariantWrapper(Variant var) {
            this.var = var;
        }

        @Override
        public int hashCode() {
            return System.identityHashCode(this.var);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof VariantWrapper)) {
                return false;
            }
            VariantWrapper wrapper = (VariantWrapper) o;
            return this.var.equals(wrapper.var);
        }
    }

    private Collection<Variant> buildOverlappingNonRedundantSet(Variant var, NavigableMap<Integer, List<Variant>> archiveVar) {
        int min = toPosition(var, true);
        int max = toPosition(var, false);
        Set<VariantWrapper> vars = new HashSet<>();
        IntStream.range(min, max + 1).boxed().forEach(p -> {
            List<Variant> lst = archiveVar.get(p);
            if (null != lst) {
                for (Variant v : lst) {
                    vars.add(new VariantWrapper(v));
                }
            }
        });
        return vars.stream().map(v -> v.var).collect(Collectors.toList());
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
        getLog().info("Search archive for " + archiveFileIds.size() + " files ... ");
        if (getLog().isDebugEnabled()) {
            getLog().debug("Add files to search in archive: " + StringUtils.join(archiveFileIds, ','));
        }
        Get get = new Get(rowKey);
        byte[] cf = getHelper().getColumnFamily();
        archiveFileIds.forEach(e -> get.addColumn(cf, Bytes.toBytes(e)));
        Result res = getHelper().getHBaseManager().act(getHelper().getIntputTable(), table -> table.get(get));
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
