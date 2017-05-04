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

import com.google.common.collect.BiMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.AlternateCoordinate;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.models.variant.protobuf.VariantProto;
import org.opencb.biodata.tools.variant.converters.proto.VcfRecordProtoToVariantConverter;
import org.opencb.biodata.tools.variant.merge.VariantMerger;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
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

import static org.opencb.opencga.storage.hadoop.variant.AnalysisTableMapReduceHelper.COUNTER_GROUP_NAME;

/**
 * @author Matthias Haimel mh719+git@cam.ac.uk
 */
public class VariantMergerTableMapper extends AbstractArchiveTableMapper {

    private Logger logger = LoggerFactory.getLogger(VariantMergerTableMapper.class);
    private final AtomicBoolean parallel = new AtomicBoolean(false);


    private boolean isParallel() {
        return this.parallel.get();
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        int cores = context.getConfiguration().getInt(MRJobConfig.MAP_CPU_VCORES, 1);
        int parallelism = ForkJoinPool.getCommonPoolParallelism();
        this.parallel.set(cores == parallelism); // has to match
        if (isParallel()) {
            logger.info("Using ForkJoinPool of {} ... ", cores);
            this.getResultConverter().setParallel(true);
        }
    }

    public static  ForkJoinPool createForkJoinPool(final String prefix, int vcores) {
        return new ForkJoinPool(vcores, pool -> {
            ForkJoinWorkerThread worker = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
            worker.setName(prefix + "_fjp_" + pool.getPoolSize());
            return worker;
        }, null, false);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }

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

    private List<Variant> loadArchiveVariants(VariantMapReduceContext ctx) {
        // Archive: unpack Archive data (selection only
        logger.info("Read Archive ...");
        List<Variant> archiveVar = getResultConverter().convert(ctx.value, true, var -> {
            completeAlternateCoordinates(var);
            int from = toPosition(var, true);
            int to = toPosition(var, false);
            return from <= ctx.nextStartPos && to >= ctx.startPos;
        });
        ctx.context.getCounter(COUNTER_GROUP_NAME, "VARIANTS_FROM_ARCHIVE").increment(archiveVar.size());
        return archiveVar;
    }

    private Set<Variant> processScanVariants(VariantMapReduceContext ctx, List<VariantTableStudyRow> rows) {
        startStep();
        List<Variant> archiveVar = loadArchiveVariants(ctx);
        endStep("1 Unpack and convert input ARCHIVE variants");
        logger.info("Index ...");
        NavigableMap<Integer, List<Variant>> varPosRegister = indexAlts(archiveVar, (int) ctx.startPos, (int) ctx.nextStartPos);
        endStep("2 Index input ARCHIVE variants");

        /* Update and submit Analysis Variants */
        Set<Variant> analysisNew = processAnalysisVariants(ctx, archiveVar, varPosRegister, rows);
        logger.info("Merge {} new variants ", analysisNew.size());
        final AtomicLong overlap = new AtomicLong(0);
        final AtomicLong merge = new AtomicLong(0);
        this.getVariantMerger().setExpectedSamples(this.currentIndexingSamples); // RESET expected set to current once only
        // with current files of same region
        Consumer<Variant> variantConsumer = (var) -> {
            ctx.getContext().progress(); // Call process to avoid timeouts
            long start = System.nanoTime();
            Collection<Variant> cleanList = buildOverlappingNonRedundantSet(var, varPosRegister);
            long mid = System.nanoTime();
            this.getVariantMerger().merge(var, cleanList);
            long end = System.nanoTime();
            overlap.getAndAdd(mid - start);
            merge.getAndAdd(end - mid);
        };
        processVariants(analysisNew, variantConsumer);
        addStepDuration("8a Merge NEW variants - overlap", overlap.get());
        addStepDuration("8b Merge NEW variants - merge", merge.get());
        logger.info("Merge 1 - overlap {}; merge {}; ns", overlap, merge);
        return analysisNew;
    }

    private Set<Variant> processAnalysisVariants(
            VariantMapReduceContext ctx, List<Variant> archiveVar,
            final NavigableMap<Integer, List<Variant>> varPosRegister, List<VariantTableStudyRow> rows) {
        List<Cell> variantCells = GenomeHelper.getVariantColumns(ctx.getValue().rawCells());
        logger.info("Parse ...");
        List<Variant> analysisVar = parseCurrentVariantsRegion(variantCells, ctx.getChromosome());
        ctx.getContext().getCounter(COUNTER_GROUP_NAME, "VARIANTS_FROM_ANALYSIS").increment(analysisVar.size());
        endStep("3 Unpack and convert input ANALYSIS variants (" + GenomeHelper.VARIANT_COLUMN_PREFIX + ")");

        // Check if Archive covers all bases in Analysis
        // TODO switched off at the moment down to removed variant calls from gVCF files (malformated variants)
        //        checkArchiveConsistency(ctx.context, ctx.startPos, ctx.nextStartPos, archiveVar, analysisVar);
        endStep("4 Check consistency -- skipped");
        final AtomicLong overlap = new AtomicLong(0);
        final AtomicLong merge = new AtomicLong(0);
        final AtomicLong submit = new AtomicLong(0);
        // (2) and (3): Same, missing (and overlapping missing) variants
        Consumer<Variant> variantConsumer = var -> {
            long start = System.nanoTime();
            ctx.getContext().progress(); // Call process to avoid timeouts
            Collection<Variant> cleanList = buildOverlappingNonRedundantSet(var, varPosRegister);
            long mid = System.nanoTime();
            this.getVariantMerger().merge(var, cleanList);
            long end = System.nanoTime();
            overlap.getAndAdd(mid - start);
            merge.getAndAdd(end - mid);
        };
        logger.info("Merge ...");
        processVariants(analysisVar, variantConsumer);
        addStepDuration("5a Merge same and missing - overlap", overlap.get());
        addStepDuration("5b Merge same and missing - merge", merge.get());
        logger.info("Submit ...");
        startStep();
        updateOutputTable(ctx.context, analysisVar, rows, ctx.sampleIds);
        endStep("6 Update OUTPUT table");
        logger.info("Filter ...");
        List<Variant> archiveTarget = filterForVariant(archiveVar.stream(), TARGET_VARIANT_TYPE).collect(Collectors.toList());
        endStep("7a Filter archive variants by target");
        ctx.context.getCounter(COUNTER_GROUP_NAME, "VARIANTS_FROM_ARCHIVE_TARGET").increment(archiveTarget.size());
        logger.info("Loaded current: " + analysisVar.size()
                + "; archive: " + archiveVar.size()
                + "; target: " + archiveTarget.size());

        /* ******** Update Analysis Variants ************** */
        // Variants of target type
        Set<Variant> analysisNew = getNewVariantsAsTemplates(ctx, analysisVar, archiveTarget, (int) ctx.startPos, (int) ctx.nextStartPos);
        endStep("7b Create NEW variants");
        return analysisNew;
    }

    private void processVariants(Collection<Variant> variants, Consumer<Variant> variantConsumer) {
        if (isParallel()) {
            variants.parallelStream().forEach(variantConsumer);
        } else {
            variants.forEach(variantConsumer);
        }
    }


    private boolean processVColumn(VariantMapReduceContext ctx) throws IOException, InterruptedException {
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


    private void processNewVariants(VariantMapReduceContext ctx, Collection<Variant> analysisNew, List<VariantTableStudyRow> rows)
            throws IOException, InterruptedException {
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
        // Reset expected set
        this.getVariantMerger().setExpectedSamples(this.currentIndexingSamples);
        Map<Integer, LinkedHashSet<Integer>> samplesInFiles = this.getStudyConfiguration().getSamplesInFiles();
        BiMap<Integer, String> id2name = StudyConfiguration.getIndexedSamples(this.getStudyConfiguration()).inverse();
        loadFromArchive(ctx.context, ctx.getCurrRowKey(), ctx.fileIds, (fileIds, res) -> {
            Set<String> names = fileIds.stream().flatMap(fid -> samplesInFiles.get(fid).stream())
                    .map(id -> {
                        String name = id2name.get(id);
                        if (name == null) {
                            throw new IllegalStateException("No name for sample id " + id);
                        }
                        return name;
                    }).collect(Collectors.toSet());

            this.getVariantMerger().addExpectedSamples(names); // add loaded names to merger
            if (null == res || res.isEmpty()) {
                logger.info("No variants found for {} files for {} samples...", fileIds.size(), names.size());
                return;
            }
            long startTime = System.nanoTime();
            // Uses ForkJoinPool !!!
            // only load variants which have overlap.
            AtomicLong protoTime = new AtomicLong();
            List<Variant> archiveOther = getResultConverter().convert(res, true, record -> {
                int start = VcfRecordProtoToVariantConverter.getStart(record, (int) ctx.getStartPos());
                int end = VcfRecordProtoToVariantConverter.getEnd(record, (int) ctx.getStartPos());
                for (VariantProto.AlternateCoordinate alt : record.getSecondaryAlternatesList()) {
                    int altStart = alt.getStart();
                    if (altStart != 0 && altStart < start) {
                        start = altStart;
                    }
                    int altEnd = alt.getEnd();
                    if (altEnd != 0 && altEnd > end) {
                        end = altEnd;
                    }
                }
                for (int i = start; i <= end; i++) {
                    if (coveredPositions.contains(i)) {
                        return true;
                    }
                }
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
                return false;
            }, protoTime);
            addStepDuration("9b.1 Parse proto from archive", protoTime.get());
            addStepDuration("9b Convert to Variants", System.nanoTime() - startTime);
            logger.info("Loaded "
                    + archiveOther.size() + " variants for "
                    + fileIds.size() + " files for "
                    + names.size() + " samples... ");

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
                this.getVariantMerger().merge(var, cleanList);
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
    protected void doMap(VariantMapReduceContext ctx) throws IOException, InterruptedException {
        this.getVariantMerger().setExpectedSamples(this.getIndexedSamples().keySet());
        this.getVariantMerger().addExpectedSamples(this.currentIndexingSamples);
        if (processVColumn(ctx)) {
            return; // All stored in V column already.
        }
        final List<VariantTableStudyRow> rows = new CopyOnWriteArrayList<>();
        Set<Variant> analysisNew = processScanVariants(ctx, rows);
        processNewVariants(ctx, analysisNew, rows);

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

    private Set<Variant> getNewVariantsAsTemplates(
            VariantMapReduceContext ctx, List<Variant> analysisVar,
            List<Variant> archiveTarget, int startPos, int nextStartPos) {
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
        private volatile Variant var;
        VariantWrapper(final Variant var) {
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

    private Collection<Variant> buildOverlappingNonRedundantSet(Variant var, final NavigableMap<Integer, List<Variant>> archiveVar) {
        int min = toPosition(var, true);
        int max = toPosition(var, false);
        Set<VariantWrapper> vars = new HashSet<>();
        IntStream.range(min, max + 1).boxed().forEach(p -> {
            List<Variant> lst = archiveVar.get(p);
            if (null != lst) {
                for (Variant v : lst) {
                    vars.add(new VariantWrapper(v)); // Wrap for faster 'HashCode' comparison.
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
        return new HashSet<Integer>(Arrays.asList(array));
    }

    protected Stream<Variant> filterForVariant(Stream<Variant> variants, VariantType ... types) {
        Set<VariantType> whiteList = new HashSet<>(Arrays.asList(types));
        return variants.filter(v -> whiteList.contains(v.getType()));
    }

}
