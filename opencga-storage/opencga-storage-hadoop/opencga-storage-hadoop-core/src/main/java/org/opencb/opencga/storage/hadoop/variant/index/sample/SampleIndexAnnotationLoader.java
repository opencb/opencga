package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Put;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.io.DataReader;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.hadoop.utils.HBaseDataWriter;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.executors.MRExecutor;
import org.opencb.opencga.storage.hadoop.variant.index.annotation.AnnotationIndexDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.index.annotation.AnnotationIndexEntry;
import org.opencb.opencga.storage.hadoop.variant.index.annotation.AnnotationIndexPutBuilder;
import org.opencb.opencga.storage.hadoop.variant.index.annotation.mr.SampleIndexAnnotationLoaderDriver;
import org.opencb.opencga.storage.hadoop.variant.utils.HBaseVariantTableNameGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.opencb.opencga.core.api.ParamConstants.OVERWRITE;
import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageOptions.SAMPLE_INDEX_ANNOTATION_MAX_SAMPLES_PER_MR;

/**
 * Created by jacobo on 04/01/19.
 */
public class SampleIndexAnnotationLoader {

    private final HBaseManager hBaseManager;
    private final HBaseVariantTableNameGenerator tableNameGenerator;
    private final MRExecutor mrExecutor;
    private final AnnotationIndexDBAdaptor annotationIndexDBAdaptor;
    private final SampleIndexDBAdaptor sampleDBAdaptor;
    private final byte[] family;
    private final VariantStorageMetadataManager metadataManager;
    private Logger logger = LoggerFactory.getLogger(SampleIndexAnnotationLoader.class);

    public SampleIndexAnnotationLoader(HBaseManager hBaseManager, HBaseVariantTableNameGenerator tableNameGenerator,
                                       VariantStorageMetadataManager metadataManager, MRExecutor mrExecutor) {
        this.hBaseManager = hBaseManager;
        this.tableNameGenerator = tableNameGenerator;
        this.mrExecutor = mrExecutor;
        this.annotationIndexDBAdaptor = new AnnotationIndexDBAdaptor(hBaseManager, tableNameGenerator.getAnnotationIndexTableName());
        this.metadataManager = metadataManager;
        this.sampleDBAdaptor = new SampleIndexDBAdaptor(hBaseManager, tableNameGenerator, this.metadataManager);
        family = GenomeHelper.COLUMN_FAMILY_BYTES;
    }

    public void updateSampleAnnotation(String study, List<String> samples, ObjectMap options)
            throws StorageEngineException {
        int studyId = metadataManager.getStudyId(study);
        List<Integer> sampleIds;
        if (samples.size() == 1 && samples.get(0).equals(VariantQueryUtils.ALL)) {
            sampleIds = metadataManager.getIndexedSamples(studyId);
        } else {
            sampleIds = new ArrayList<>(samples.size());
            for (String sample : samples) {
                Integer sampleId = metadataManager.getSampleId(studyId, sample, true);
                if (sampleId == null) {
                    throw VariantQueryException.sampleNotFound(sample, study);
                }
                sampleIds.add(sampleId);
            }
        }

        updateSampleAnnotation(studyId, sampleIds, options, options.getBoolean(OVERWRITE, false));
    }

    public void updateSampleAnnotation(int studyId, List<Integer> samples, ObjectMap options)
            throws StorageEngineException {
        updateSampleAnnotation(studyId, samples, options, options.getBoolean(OVERWRITE, false));
    }

    public void updateSampleAnnotation(int studyId, List<Integer> samples, ObjectMap options, boolean overwrite)
            throws StorageEngineException {
        List<Integer> finalSamplesList = new ArrayList<>(samples.size());
        List<String> nonAnnotated = new LinkedList<>();
        List<String> alreadyAnnotated = new LinkedList<>();
        for (Integer sampleId : samples) {
            SampleMetadata sampleMetadata = metadataManager.getSampleMetadata(studyId, sampleId);
            if (sampleMetadata.isAnnotated()) {
                if (SampleIndexDBAdaptor.getSampleIndexAnnotationStatus(sampleMetadata).equals(TaskMetadata.Status.READY) && !overwrite) {
                    // SamplesIndex already annotated
                    alreadyAnnotated.add(sampleMetadata.getName());
                } else {
                    finalSamplesList.add(sampleId);
                }
            } else {
                // Discard non-annotated samples
                nonAnnotated.add(sampleMetadata.getName());
            }
        }
        if (!nonAnnotated.isEmpty()) {
            if (nonAnnotated.size() < 20) {
                logger.warn("Unable to update sample index from samples " + nonAnnotated + ". Samples not fully annotated.");
            } else {
                logger.warn("Unable to update sample index from " + nonAnnotated.size() + " samples. Samples not fully annotated.");
            }
        }
        if (!alreadyAnnotated.isEmpty()) {
            logger.info("Skip sample index annotation for " + alreadyAnnotated.size() + " samples."
                    + " Add " + OVERWRITE + "=true to overwrite existing sample index annotation on all samples");
        }

        if (finalSamplesList.isEmpty()) {
            logger.info("Skip sample index annotation. Nothing to do!");
            return;
        }
        if (finalSamplesList.size() < 20) {
            logger.info("Run sample index annotation on samples " + finalSamplesList);
        } else {
            logger.info("Run sample index annotation on " + finalSamplesList.size() + " samples");
        }

        int batchSize = options.getInt(
                SAMPLE_INDEX_ANNOTATION_MAX_SAMPLES_PER_MR.key(),
                SAMPLE_INDEX_ANNOTATION_MAX_SAMPLES_PER_MR.defaultValue());
//        if (finalSamplesList.size() < 10) {
//            updateSampleAnnotationBatchMultiThread(studyId, finalSamplesList);
//        }
        if (finalSamplesList.size() > batchSize) {
            int batches = (int) Math.round(Math.ceil(finalSamplesList.size() / ((float) batchSize)));
            batchSize = (finalSamplesList.size() / batches) + 1;
            logger.warn("Unable to run sample index annotation in one single MapReduce operation.");
            logger.info("Split in {} jobs of {} samples each.", batches, batchSize);
            for (int i = 0; i < batches; i++) {
                List<Integer> subSet = finalSamplesList.subList(i * batchSize, Math.min((i + 1) * batchSize, finalSamplesList.size()));
                logger.info("Running MapReduce {}/{} over {} samples", i + 1, batches, subSet.size());
                updateSampleAnnotationBatchMapreduce(studyId, subSet, options);
            }
        } else {
            updateSampleAnnotationBatchMapreduce(studyId, finalSamplesList, options);
        }
    }

    private void updateSampleAnnotationBatchMapreduce(int studyId, List<Integer> samples, ObjectMap options)
            throws StorageEngineException {
        mrExecutor.run(SampleIndexAnnotationLoaderDriver.class, SampleIndexAnnotationLoaderDriver.buildArgs(
                tableNameGenerator.getArchiveTableName(studyId),
                tableNameGenerator.getVariantTableName(), studyId, samples, options), options,
                "Annotate sample index for " + (samples.size() < 10 ? "samples " + samples : samples.size() + " samples"));
    }


    private void updateSampleAnnotationBatchMultiThread(int studyId, List<Integer> samples) throws IOException, StorageEngineException {
        logger.info("Update sample index annotation of " + samples.size() + " samples");

        String sampleIndexTableName = tableNameGenerator.getSampleIndexTableName(studyId);

        ProgressLogger progressLogger = new ProgressLogger("Sample index annotation updated variants");

        ParallelTaskRunner<Pair<Variant, AnnotationIndexEntry>, Put> ptr = new ParallelTaskRunner<>(
                new DataReader<Pair<Variant, AnnotationIndexEntry>>() {

                    private Iterator<Pair<Variant, AnnotationIndexEntry>> iterator = annotationIndexDBAdaptor.iterator();
                    private int initialCapacity = 200000;
                    private Pair<Variant, AnnotationIndexEntry> nextPair = null;

                    private String chromosome = "";
                    private int start = -1;
                    private int end = -1;

                    @Override
                    public List<Pair<Variant, AnnotationIndexEntry>> read(int n) {
                        List<Pair<Variant, AnnotationIndexEntry>> annotationMasks = new ArrayList<>(initialCapacity);

                        // Read next batch
                        if (nextPair == null && iterator.hasNext()) {
                            nextPair = iterator.next();
                        }
                        if (nextPair != null) {
                            annotationMasks.add(nextPair);
                            Variant firstVariant = nextPair.getKey();
                            chromosome = firstVariant.getChromosome();
                            start = firstVariant.getStart() - (firstVariant.getStart() % SampleIndexSchema.BATCH_SIZE);
                            end = start + SampleIndexSchema.BATCH_SIZE;
                            nextPair = null;
                        }
                        while (iterator.hasNext()) {
                            Pair<Variant, AnnotationIndexEntry> pair = iterator.next();
                            Variant variant = pair.getKey();
                            if (variant.getChromosome().equals(chromosome) && variant.getStart() > start && variant.getStart() < end) {
                                annotationMasks.add(pair);
                            } else {
//                                logger.info("Variant " + variant
//                                        + "(" + variant.getChromosome() + ":" + variant.getStart() + "-" + variant.getEnd() + ")"
//                                        + " not in batch " + chromosome + ":" + start + "-" + end);
                                nextPair = pair;
                                break;
                            }
                        }

                        return annotationMasks;
                    }
                },
                annotationMasks -> {
                    // Ensure is sorted as expected
                    annotationMasks.sort(Comparator.comparing(Pair::getKey,
                            SampleIndexSchema.INTRA_CHROMOSOME_VARIANT_COMPARATOR));

                    Variant firstVariant = annotationMasks.get(0).getKey();
                    String chromosome = firstVariant.getChromosome();
                    int start = firstVariant.getStart() - (firstVariant.getStart() % SampleIndexSchema.BATCH_SIZE);
                    int end = start + SampleIndexSchema.BATCH_SIZE;

                    progressLogger.increment(annotationMasks.size(), () -> "Up to batch " + chromosome + ":" + start + "-" + end);
                    List<Put> puts = new ArrayList<>(samples.size());

                    for (Integer sampleId : samples) {
                        Map<String, List<Variant>> map = sampleDBAdaptor.queryByGt(studyId, sampleId, chromosome, start);
                        Put put = annotate(chromosome, start, sampleId, map, annotationMasks);
                        if (!put.isEmpty()) {
                            puts.add(put);
                        }
//                else logger.warn("Empty put for sample " + sampleId + " -> "  + chromosome + ":" + start + ":" + end);
                    }

                    return puts;
                },
                new HBaseDataWriter<>(hBaseManager, sampleIndexTableName),
                ParallelTaskRunner.Config.builder().setNumTasks(8).setCapacity(2).build()
        );

        try {
            ptr.run();
        } catch (ExecutionException e) {
            throw new StorageEngineException("Error", e);
        }

        postAnnotationLoad(studyId, samples);
    }

    public void updateSampleAnnotationMultiSampleIterator(int studyId, List<Integer> samples) throws IOException, StorageEngineException {
        String sampleIndexTableName = tableNameGenerator.getSampleIndexTableName(studyId);
        Map<Integer, Iterator<Map<String, List<Variant>>>> sampleIterators = new HashMap<>(samples.size());

        for (Integer sample : samples) {
            sampleIterators.put(sample, sampleDBAdaptor.iteratorByGt(studyId, sample));
        }

        BufferedMutator mutator = hBaseManager.getConnection().getBufferedMutator(TableName.valueOf(sampleIndexTableName));

        String chromosome = "";
        int start = -1;
        int end = -1;
        List<Pair<Variant, AnnotationIndexEntry>> annotationEntries = null;
        do {
            for (Map.Entry<Integer, Iterator<Map<String, List<Variant>>>> sampleIteratorPair : sampleIterators.entrySet()) {
                Iterator<Map<String, List<Variant>>> sampleIterator = sampleIteratorPair.getValue();
                Integer sampleId = sampleIteratorPair.getKey();
                if (sampleIterator.hasNext()) {
                    Map<String, List<Variant>> next = sampleIterator.next();

                    Variant firstVariant = next.values().iterator().next().get(0);
                    if (annotationEntries == null
                            || !chromosome.equals(firstVariant.getChromosome())
                            || firstVariant.getStart() < start
                            || firstVariant.getStart() > end) {
                        chromosome = firstVariant.getChromosome();
                        start = firstVariant.getStart() - firstVariant.getStart() % SampleIndexSchema.BATCH_SIZE;
                        end = start + SampleIndexSchema.BATCH_SIZE;
                        annotationEntries = annotationIndexDBAdaptor.get(chromosome, start, end);
                    }

                    Put put = annotate(chromosome, start, sampleId, next, annotationEntries);
                    mutator.mutate(put);
                }
            }

            // Remove exhausted iterators
            sampleIterators.entrySet().removeIf(e -> !e.getValue().hasNext());
        } while (!sampleIterators.isEmpty());

        mutator.close();

        postAnnotationLoad(studyId, samples);
    }

    private Put annotate(String chromosome, int start, Integer sampleId,
                        Map<String, List<Variant>> sampleIndex, List<Pair<Variant, AnnotationIndexEntry>> annotationMasks) {
        byte[] rk = SampleIndexSchema.toRowKey(sampleId, chromosome, start);
        Put put = new Put(rk);

        for (Map.Entry<String, List<Variant>> entry : sampleIndex.entrySet()) {
            String gt = entry.getKey();
            List<Variant> variantsToAnnotate = entry.getValue();
            if (!SampleIndexSchema.isAnnotatedGenotype(gt)) {
                continue;
            }

            ListIterator<Pair<Variant, AnnotationIndexEntry>> iterator = annotationMasks.listIterator();
            AnnotationIndexPutBuilder builder = new AnnotationIndexPutBuilder(variantsToAnnotate.size());
            int missingVariants = 0;
            // Assume both lists are ordered, and "variantsToAnnotate" is fully contained in "annotationMasks"
            for (Variant variantToAnnotate : variantsToAnnotate) {
                boolean restarted = false;
                while (iterator.hasNext()) {
                    Pair<Variant, AnnotationIndexEntry> annotationPair = iterator.next();
                    if (annotationPair.getKey().sameGenomicVariant(variantToAnnotate)) {
                        builder.add(annotationPair.getRight());
                        break;
                    } else if (annotationPair.getKey().getStart() > variantToAnnotate.getStart()) {
                        if (!restarted) {
                            logger.warn("Missing variant to annotate " + variantToAnnotate + " RESTART ITERATOR");
                            while (iterator.hasPrevious()) {
                                iterator.previous();
                            }
                            restarted = true;
                        } else {
                            logger.error("Missing variant to annotate " + variantToAnnotate);
                            builder.add(AnnotationIndexEntry.empty(
                                    SampleIndexConfiguration.PopulationFrequencyRange.DEFAULT_THRESHOLDS.length));
                            missingVariants++;
                            break;
                        }
                    }
                }
            }
            if (missingVariants > 0) {
                // TODO: What if a variant is not annotated?
                String msg = "Error annotating batch. " + missingVariants + " missing variants";
                logger.error(msg);
//                            throw new IllegalStateException(msg);
            }

            builder.buildAndReset(put, gt, family);
        }
        return put;
    }

    private void postAnnotationLoad(int studyId, List<Integer> samples) throws StorageEngineException {
        postAnnotationLoad(studyId, samples, metadataManager);
    }

    public static void postAnnotationLoad(int studyId, List<Integer> samples, VariantStorageMetadataManager metadataManager)
            throws StorageEngineException {
        for (Integer sampleId : samples) {
            metadataManager.updateSampleMetadata(studyId, sampleId, sampleMetadata -> {
                return SampleIndexDBAdaptor.setSampleIndexAnnotationStatus(sampleMetadata, TaskMetadata.Status.READY);
            });
        }
    }


}
