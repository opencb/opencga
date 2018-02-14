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
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.AlternateCoordinate;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.models.variant.protobuf.VariantProto;
import org.opencb.biodata.tools.variant.converters.proto.VcfRecordProtoToVariantConverter;
import org.opencb.biodata.tools.variant.merge.VariantMerger;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.converters.HBaseToVariantConverter;
import org.opencb.opencga.storage.hadoop.variant.models.protobuf.VariantTableStudyRowsProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.opencb.opencga.storage.hadoop.variant.mr.AnalysisTableMapReduceHelper.COUNTER_GROUP_NAME;
import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine.*;

/**
 * @author Matthias Haimel mh719+git@cam.ac.uk
 */
public class VariantMergerTableMapper extends AbstractArchiveTableMapper {

    private Logger logger = LoggerFactory.getLogger(VariantMergerTableMapper.class);
    private final AtomicBoolean parallel = new AtomicBoolean(false);
    private VariantMerger variantMerger;
    // FIXME: This merger should not be needed
    @Deprecated
    private VariantMerger variantMergerSamplesToIndex;
    private boolean resolveConflict;
    private Integer archiveBatchSize;

    public static final EnumSet<VariantType> TARGET_VARIANT_TYPE_SET = EnumSet.of(
            VariantType.SNV, VariantType.SNP,
            VariantType.INDEL, /* VariantType.INSERTION, VariantType.DELETION,*/
            VariantType.MNV, VariantType.MNP);
    private List<String> fixedFormat;

    private boolean isParallel() {
        return this.parallel.get();
    }

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        int cores = context.getConfiguration().getInt(MRJobConfig.MAP_CPU_VCORES, 1);
        int parallelism = ForkJoinPool.getCommonPoolParallelism();
        this.parallel.set(cores == parallelism); // has to match
        if (isParallel()) {
            logger.info("Using ForkJoinPool of {} ... ", cores);
            this.getResultConverter().setParallel(true);
        }
        this.archiveBatchSize = context.getConfiguration().getInt(MERGE_ARCHIVE_SCAN_BATCH_SIZE, DEFAULT_MERGE_ARCHIVE_SCAN_BATCH_SIZE);

        // TODO: Read from configuration?
        resolveConflict = true;

        boolean collapseDeletions = context.getConfiguration().getBoolean(MERGE_COLLAPSE_DELETIONS, DEFAULT_MERGE_COLLAPSE_DELETIONS);

        Set<Integer> filesToIndex = context.getConfiguration().getStringCollection(VariantStorageEngine.Options.FILE_ID.key())
                .stream()
                .map(Integer::valueOf)
                .collect(Collectors.toSet());
        if (filesToIndex.isEmpty()) {
            throw new IllegalStateException(
                    "File IDs to be indexed not found in configuration: " + VariantStorageEngine.Options.FILE_ID.key());
        }
        Set<String> samplesToIndex = new LinkedHashSet<>();
        BiMap<Integer, String> sampleIdToSampleName = StudyConfiguration.inverseMap(getStudyConfiguration().getSampleIds());
        for (Integer fileId : filesToIndex) {
            for (Integer sampleId : getStudyConfiguration().getSamplesInFiles().get(fileId)) {
                samplesToIndex.add(sampleIdToSampleName.get(sampleId));
            }
        }

        fixedFormat = HBaseToVariantConverter.getFixedFormat(getStudyConfiguration());


        variantMerger = newVariantMerger(collapseDeletions);
        variantMerger.setExpectedSamples(this.getIndexedSamples().keySet());
        // Add all samples which are currently being indexed.
        variantMerger.addExpectedSamples(samplesToIndex);


        variantMergerSamplesToIndex = newVariantMerger(collapseDeletions);
        variantMergerSamplesToIndex.setExpectedSamples(samplesToIndex);

    }

    public VariantMerger newVariantMerger(boolean collapseDeletions) {
        VariantMerger variantMerger = new VariantMerger(collapseDeletions);
        variantMerger.setStudyId(Integer.toString(getStudyConfiguration().getStudyId()));
        // Format is no longer fixed when merging variants.
//        variantMerger.setExpectedFormats(fixedFormat);
        variantMerger.configure(getStudyConfiguration().getVariantHeader());
        return variantMerger;
    }

    public static  ForkJoinPool createForkJoinPool(final String prefix, int vcores) {
        return new ForkJoinPool(vcores, pool -> {
            ForkJoinWorkerThread worker = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
            worker.setName(prefix + "_fjp_" + pool.getPoolSize());
            return worker;
        }, null, false);
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }

    public static EnumSet<VariantType> getTargetVariantType() {
        return TARGET_VARIANT_TYPE_SET;
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

    /**
     * Parse all the read variants from the Archive Table.
     *
     * @param ctx Variant map reduce context
     * @return List of variants from the Archive Table to be loaded
     */
    private List<Variant> parseArchiveVariants(VariantMapReduceContext ctx) {
        // Archive: unpack Archive data (selection only
        logger.info("Read Archive ...");
        AtomicLong protoTime = new AtomicLong();
        AtomicLong resolveConflictTime = new AtomicLong();
        AtomicLong convertTime = new AtomicLong();
        List<Variant> archiveVar = getResultConverter().convert(ctx.value, resolveConflict,
                var -> true,
                var -> {
                    completeAlternateCoordinates(var);
                    int from = toPosition(var, true);
                    int to = toPosition(var, false);
                    return from <= ctx.nextStartPos && to >= ctx.startPos;
                }, protoTime, resolveConflictTime, convertTime);
        ctx.context.getCounter(COUNTER_GROUP_NAME, "VARIANTS_FROM_ARCHIVE").increment(archiveVar.size());
        addStepDuration("1a Unpack and convert input ARCHIVE variants - proto", protoTime.get());
        addStepDuration("1b Unpack and convert input ARCHIVE variants - conflicts", resolveConflictTime.get());
        addStepDuration("1b Unpack and convert input ARCHIVE variants - covnert", convertTime.get());

        return archiveVar;
    }

    private void processAnalysisVariants(VariantMapReduceContext ctx, List<Variant> analysisVar,
                                         final NavigableMap<Integer, List<Variant>> archiveVarPosMap, List<VariantTableStudyRow> rows) {

        /* ******** Update Analysis Variants ************** */

        // Check if Archive covers all bases in Analysis
        // TODO switched off at the moment down to removed variant calls from gVCF files (malformated variants)
        //        checkArchiveConsistency(ctx.context, ctx.startPos, ctx.nextStartPos, archiveVar, analysisVar);
//        endStep("4 Check consistency -- skipped");

        final AtomicLong overlap = new AtomicLong(0);
        final AtomicLong merge = new AtomicLong(0);
        final AtomicLong submit = new AtomicLong(0);

        // (2) and (3): Same, missing (and overlapping missing) variants
        logger.info("Merge ...");
        processVariants(analysisVar, var -> {
            long start = System.nanoTime();
            ctx.getContext().progress(); // Call process to avoid timeouts
            Collection<Variant> cleanList = buildOverlappingNonRedundantSet(var, archiveVarPosMap);
            long mid = System.nanoTime();
            variantMerger.merge(var, cleanList);
            long end = System.nanoTime();
            overlap.getAndAdd(mid - start);
            merge.getAndAdd(end - mid);
        });
        endStep("5 Merge same and distinct");
        addStepDuration("5a Merge same and missing - overlap", overlap.get());
        addStepDuration("5b Merge same and missing - merge", merge.get());

        logger.info("Submit ...");
        Pair<Long, Long> pair = updateOutputTable(ctx.context, analysisVar, rows, ctx.sampleIds);
        addStepDuration("6a Update OUTPUT table - create put", pair.getLeft());
        addStepDuration("6b Update OUTPUT table - write put", pair.getRight());
        endStep("6 Update OUTPUT table");
    }

    private void processVariants(Collection<Variant> variants, Consumer<Variant> variantConsumer) {
        if (isParallel()) {
            variants.parallelStream().forEach(variantConsumer);
        } else {
            variants.forEach(variantConsumer);
        }
    }

    /**
     * Process content of _V column.
     * If the timestamp matches with the current one, there has been some kind of problem.
     * Maybe the MR task died, and it's the second time that this region is processed, or the whole MR job died, and it's being resumed.
     *
     * @param ctx Variant map reduce context
     * @return If the timestamp matches. In this case, the task can be skipped.
     * @throws IOException If there is any problem parsing the proto objects.
     */
    private boolean processVColumn(VariantMapReduceContext ctx) throws IOException {
        List<Cell> variantCells = GenomeHelper.getVariantColumns(ctx.getValue().rawCells());
        if (!variantCells.isEmpty()) {
            byte[] data = CellUtil.cloneValue(variantCells.get(0));
            VariantTableStudyRowsProto proto = VariantTableStudyRowsProto.parseFrom(data);
            logger.info("Column _V: found " + variantCells.size()
                    + " columns - check timestamp " + getTimestamp() + " with " + proto.getTimestamp());
            if (proto.getTimestamp() == getTimestamp()) {
                ctx.context.getCounter(COUNTER_GROUP_NAME, "X_ALREADY_LOADED_SLICE").increment(1);
                for (Cell cell : variantCells) {
                    VariantTableStudyRowsProto rows = VariantTableStudyRowsProto.parseFrom(CellUtil.cloneValue(cell));
                    List<VariantTableStudyRow> variants = parseVariantStudyRowsFromArchive(ctx.getChromosome(), rows);
                    ctx.context.getCounter(COUNTER_GROUP_NAME, "X_ALREADY_LOADED_ROWS").increment(variants.size());
                    updateOutputTable(ctx.context, variants);
                }
                endStep("X Unpack, convert and write ANALYSIS variants (" + GenomeHelper.VARIANT_COLUMN_PREFIX + ")");
                return true;
            }
        }
        return false;
    }

    /**
     * Update and submit Analysis new Variants.
     *
     * For these new variants, fetch data from the Archive Table to get the value of the already loaded samples in that position.
     *
     */
    private void processNewVariants(VariantMapReduceContext ctx, Collection<Variant> analysisNew,
                                    NavigableMap<Integer, List<Variant>> archiveVarPosMap, List<VariantTableStudyRow> rows)
            throws IOException {
        logger.info("Merge {} new variants ", analysisNew.size());
        final AtomicLong overlap = new AtomicLong(0);
        final AtomicLong merge = new AtomicLong(0);
        // with current files of same region
        processVariants(analysisNew, (var) -> {
            ctx.getContext().progress(); // Call process to avoid timeouts
            long start = System.nanoTime();
            Collection<Variant> cleanList = buildOverlappingNonRedundantSet(var, archiveVarPosMap);
            long mid = System.nanoTime();
            variantMergerSamplesToIndex.merge(var, cleanList);
            long end = System.nanoTime();
            overlap.getAndAdd(mid - start);
            merge.getAndAdd(end - mid);
        });
        addStepDuration("8a Merge NEW variants - overlap", overlap.get());
        addStepDuration("8b Merge NEW variants - merge", merge.get());
        logger.info("Merge 1 - overlap {}; merge {}; ns", overlap, merge);

        // with all other gVCF files of same region
        if (!analysisNew.isEmpty()) {
            fillNewWithIndexedSamples(ctx, analysisNew);
        }
        // WRITE VALUES
        startStep();
        updateOutputTable(ctx.context, analysisNew, rows, null);
        endStep("10 Update OUTPUT table");
    }

    private void fillNewWithIndexedSamples(VariantMapReduceContext ctx, Collection<Variant> analysisNew) throws IOException {
        AtomicLong overlap = new AtomicLong(0);
        AtomicLong merge = new AtomicLong(0);
        Set<Integer> coveredPositions = new ConcurrentSkipListSet<>();
        processVariants(analysisNew, var -> {
            int min = toPosition(var, true);
            int max = toPosition(var, false);
            coveredPositions.addAll(IntStream.range(min, max + 1).boxed().collect(Collectors.toList()));
        });

        loadFromArchive(ctx.context, ctx.getCurrRowKey(), ctx.getFileIdsInResult(), (fileIds, res) -> {
            if (null == res || res.isEmpty()) {
                // FIXME: Add missing?
                logger.info("No variants found for {} files", fileIds.size());
                return;
            }
            long startTime = System.nanoTime();
            // Uses ForkJoinPool !!!
            // only load variants which have overlap.
            AtomicLong protoTime = new AtomicLong();
            AtomicLong resolveConflictTime = new AtomicLong();
            AtomicLong convertTime = new AtomicLong();
            AtomicLong discardedVcfRecord = new AtomicLong();
            AtomicLong discardedVariant = new AtomicLong();
            List<Variant> archiveOther = getResultConverter().convert(res, resolveConflict, record -> {
                int start = VcfRecordProtoToVariantConverter.getStart(record, (int) ctx.getStartPos());
                int end = VcfRecordProtoToVariantConverter.getEnd(record, (int) ctx.getStartPos());
                int min = Math.min(start, end);
                int max = Math.max(start, end);
                for (VariantProto.AlternateCoordinate alt : record.getSecondaryAlternatesList()) {
                    int altStart = alt.getStart();
                    if (altStart != 0 && altStart < min) {
                        min = altStart;
                    }
                    int altEnd = alt.getEnd();
                    if (altEnd != 0 && altEnd > max) {
                        max = altEnd;
                    }
                }
                int pad = 3;
                for (int i = min - pad; i <= max + pad; i++) {
                    if (coveredPositions.contains(i)) {
                        return true;
                    }
                }
                discardedVcfRecord.incrementAndGet();
                return false;
            }, var -> {
                // Complete ALTs
                completeAlternateCoordinates(var);
                int min = toPosition(var, true);
                int max = toPosition(var, false);
//                return IntStream.range(min, max + 1).boxed().anyMatch(i -> coveredPositions.contains(i));
                for (int i = min; i <= max; i++) {
                    if (coveredPositions.contains(i)) {
                        return true;
                    }
                }
                discardedVariant.incrementAndGet();
                return false;
            }, protoTime, resolveConflictTime, convertTime);
            ctx.getContext().getCounter(COUNTER_GROUP_NAME, "OTHER_VARIANTS_FROM_ARCHIVE_DISCARDED_VCF_RECORD")
                    .increment(discardedVcfRecord.get());
            ctx.getContext().getCounter(COUNTER_GROUP_NAME, "OTHER_VARIANTS_FROM_ARCHIVE_DISCARDED_VARIANT")
                    .increment(discardedVariant.get());
            addStepDuration("9b.1 Unpack and convert from Archive - proto", protoTime.get());
            addStepDuration("9b.2 Unpack and convert from Archive - conflict", resolveConflictTime.get());
            addStepDuration("9b.3 Unpack and convert from Archive - convert", convertTime.get());
            addStepDuration("9b Unpack and convert from Archive", System.nanoTime() - startTime);
            logger.info("Loaded " + archiveOther.size() + " variants for " + fileIds.size() + " files");

            startTime = System.nanoTime();
            final NavigableMap<Integer, List<Variant>> varPosSortedOther =
                    indexAlts(archiveOther, (int)ctx.startPos, (int)ctx.nextStartPos);
            logger.info("Create alts index of size " + varPosSortedOther.size() + " ... ");
            ctx.context.getCounter(COUNTER_GROUP_NAME, "OTHER_VARIANTS_FROM_ARCHIVE").increment(archiveOther.size());
            ctx.context.getCounter(COUNTER_GROUP_NAME, "OTHER_VARIANTS_FROM_ARCHIVE_NUM_QUERIES").increment(1);
            processVariants(analysisNew, var -> {
                ctx.getContext().progress(); // Call process to avoid timeouts
                long start = System.nanoTime();
                Collection<Variant> cleanList = buildOverlappingNonRedundantSet(var, varPosSortedOther);
                if (logger.isDebugEnabled()) {
                    logger.debug(
                            "Merge 2 - merge {} variants for {} - overlap {}; merge {}; ns ... ",
                            cleanList.size(), var, overlap, merge);
                }
                long mid = System.nanoTime();
                variantMerger.merge(var, cleanList);
                overlap.addAndGet(mid - start);
                merge.addAndGet(System.nanoTime() - mid);
            });
            logger.info("Merge 2 - overlap {}; merge {}; ns", overlap, merge);
            addStepDuration("9c Merge NEW with archive slice - overlap", overlap.get());
            addStepDuration("9d Merge NEW with archive slice - merge", merge.get());
            ctx.getContext().progress(); // Call process to avoid timeouts
        });
    }

    @Override
    protected void map(VariantMapReduceContext ctx) throws IOException, InterruptedException {
        startStep();
        if (processVColumn(ctx)) {
            return; // All stored in V column already.
        }
        // Variant rows to be added or modified in the Variants table.
        List<VariantTableStudyRow> rows = new CopyOnWriteArrayList<>();

        List<Variant> archiveVar = parseArchiveVariants(ctx);
        endStep("1 Unpack and convert input ARCHIVE variants");

        logger.info("Index ...");
        NavigableMap<Integer, List<Variant>> archiveVarPosMap = indexAlts(archiveVar, (int) ctx.startPos, (int) ctx.nextStartPos);
        endStep("2 Index input ARCHIVE variants");

        logger.info("Parse ...");
        List<Variant> analysisVar = parseCurrentVariantsRegion(ctx);
        ctx.getContext().getCounter(COUNTER_GROUP_NAME, "VARIANTS_FROM_ANALYSIS").increment(analysisVar.size());
        endStep("3 Unpack and convert input ANALYSIS variants (" + GenomeHelper.VARIANT_COLUMN_PREFIX + ')');

        logger.info("Filter ...");
        List<Variant> archiveTarget = filterVariantsByType(archiveVar.stream()).collect(Collectors.toList());
        endStep("7a Filter archive variants by target");
        ctx.context.getCounter(COUNTER_GROUP_NAME, "VARIANTS_FROM_ARCHIVE_TARGET").increment(archiveTarget.size());
        logger.debug("Loaded current: {}; archive: {}; target: {}", analysisVar.size(), archiveVar.size(), archiveTarget.size());
        // Variants of target type
        Set<Variant> analysisNew = getNewVariantsAsTemplates(ctx, analysisVar, archiveTarget, (int) ctx.startPos, (int) ctx.nextStartPos);
        endStep("7b Create NEW variants");

        /* Update and submit Analysis missing and same Variants */
        processAnalysisVariants(ctx, analysisVar, archiveVarPosMap, rows);

        /* Update and submit Analysis new Variants */
        processNewVariants(ctx, analysisNew, archiveVarPosMap, rows);

        // Checkpoint -> update archive table!!!
        startStep();
        updateArchiveTable(ctx.getCurrRowKey(), ctx.context, rows);
        endStep("11 Update INPUT table");
        logger.info("Done merging");
    }

    private NavigableMap<Integer, List<Variant>> indexAlts(List<Variant> variants, int startPos, int nextStartPos) {
        // TODO Check if Alternates need indexing as well !!!
        final ConcurrentSkipListMap<Integer, List<Variant>> retMap = new ConcurrentSkipListMap<>();
        Consumer<Variant> variantConsumer = v -> {
            int from = Math.max(toPosition(v, true), startPos);
            int to = Math.min(toPosition(v, false) + 1, nextStartPos);
            IntStream.range(from, to).forEach(p -> retMap.computeIfAbsent(p, (idx) -> new CopyOnWriteArrayList<>()).add(v));
        };
        processVariants(variants, variantConsumer);
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

    private void completeAlternateCoordinates(Variant variant) {
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

    private void completeAlternateCoordinates(List<Variant> variants) {
        Consumer<Variant> variantConsumer = variant -> completeAlternateCoordinates(variant);
        if (isParallel()) {
            variants.parallelStream().forEach(variantConsumer);
        } else {
            variants.stream().forEach(variantConsumer);
        }
    }

    private Set<Variant> getNewVariantsAsTemplates(VariantMapReduceContext ctx, List<Variant> analysisVar, List<Variant> archiveTarget,
                                                   int startPos, int nextStartPos) {
        String studyId = Integer.toString(getStudyConfiguration().getStudyId());
        // (1) NEW variants (only create the position, no filling yet)
        Set<String> analysisVarSet = analysisVar.stream().map(Variant::toString).collect(Collectors.toSet());
        analysisVarSet.addAll(analysisVar.stream().flatMap(v -> v.getStudy(studyId).getSecondaryAlternates().stream())
                .map(a -> toVariantString(a)).collect(Collectors.toSet()));
        Set<Variant> analysisNew = new ConcurrentSkipListSet<>(); // for later parallel processing
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
                Variant var = new Variant(tar.getChromosome(), tar.getStart(), tar.getEnd(), tar.getReference(), tar.getAlternate());
                var.setType(tar.getType());
                for (StudyEntry tse : tar.getStudies()) {
                    StudyEntry studyEntry = new StudyEntry(tse.getStudyId());
                    studyEntry.setFormat(tse.getFormat());
                    studyEntry.setSortedSamplesPosition(new LinkedHashMap<>());
                    studyEntry.setSamplesData(new ArrayList<>());
                    var.addStudyEntry(studyEntry);
                }
                analysisNew.add(var);
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
                            se.setFormat(fixedFormat);
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

    private Collection<Variant> buildOverlappingNonRedundantSet(Variant var, final NavigableMap<Integer, List<Variant>> archiveVar) {
        int min = toPosition(var, true);
        int max = toPosition(var, false);
        Map<Variant, Object> vars = new IdentityHashMap<>();
        IntStream.range(min, max + 1).boxed().forEach(p -> {
            List<Variant> lst = archiveVar.get(p);
            if (null != lst) {
                for (Variant v : lst) {
                    vars.put(v, null);
                }
            }
        });
        return vars.keySet();
    }

    /**
     * Load all variants for all files (except in currFileIds) listed in the study configuration for the specified rowKey.
     * @param context Context
     * @param rowKey Slice to extract data for
     * @param currFileIds File ids to ignore
     * @param merge BiConsumer accepting ID list and List of {@link Variant} to merge (batch mode)
     * @throws IOException
     */
    private void loadFromArchive(Context context, byte[] rowKey, Set<Integer> currFileIds,
                                          BiConsumer<Set<Integer>, Result> merge) throws IOException {
        // Extract File IDs to search through
        LinkedHashSet<Integer> indexedFiles = getStudyConfiguration().getIndexedFiles();
        Set<String> archiveFileIds = indexedFiles.stream().filter(k -> !currFileIds.contains(k)).map(s -> s.toString())
                .collect(Collectors.toSet());
        if (archiveFileIds.isEmpty()) {
            logger.info("No files found to search for in archive table");
            merge.accept(Collections.emptySet(), null);
            return; // done
        }
        logger.info("Search archive for " + archiveFileIds.size() + " files in total in batches of " + this.archiveBatchSize  + " ... ");
        while (!archiveFileIds.isEmpty()) {
            Long startTime = System.nanoTime();
            // create batch
            Set<String> batch = new HashSet<>();
            for (String e : archiveFileIds) {
                if (batch.size() < this.archiveBatchSize) {
                    batch.add(e);
                } else {
                    break;
                }
            }
            archiveFileIds.removeAll(batch); // remove ids
            logger.info("Search archive for " + batch.size() + " files with " + archiveFileIds.size()  + " remaining ... ");
            if (logger.isDebugEnabled()) {
                logger.debug("Add files to search in archive: " + StringUtils.join(batch, ','));
            }
            Get get = new Get(rowKey);
            byte[] cf = getHelper().getColumnFamily();
            batch.forEach(e -> get.addColumn(cf, Bytes.toBytes(e)));
            Set<Integer> batchIds = batch.stream().map(e -> Integer.valueOf(e)).collect(Collectors.toSet());
            Result res = getHBaseManager().act(getHelper().getArchiveTable(), table -> table.get(get));
            addStepDuration("9a Load archive slice from hbase", System.nanoTime() - startTime);
            if (res.isEmpty()) {
                logger.warn("No data found in archive table!!!");
                merge.accept(batchIds, null);
            } else {
                merge.accept(batchIds, res);
            }
        }
        logger.info("Done processing archive data!");
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
            logger.error(
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
        return new HashSet<>(Arrays.asList(array));
    }

    protected Stream<Variant> filterVariantsByType(Stream<Variant> variants) {
        return variants.filter(v -> TARGET_VARIANT_TYPE_SET.contains(v.getType()));
    }

}
