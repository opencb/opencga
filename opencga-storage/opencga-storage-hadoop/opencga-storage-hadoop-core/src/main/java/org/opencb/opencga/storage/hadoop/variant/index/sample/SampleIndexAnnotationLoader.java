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
import org.opencb.opencga.storage.hadoop.utils.HBaseDataWriter;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.executors.MRExecutor;
import org.opencb.opencga.storage.hadoop.variant.index.IndexUtils;
import org.opencb.opencga.storage.hadoop.variant.index.annotation.AnnotationIndexDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.index.annotation.mr.SampleIndexAnnotationLoaderDriver;
import org.opencb.opencga.storage.hadoop.variant.utils.HBaseVariantTableNameGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * Created by jacobo on 04/01/19.
 */
public class SampleIndexAnnotationLoader {

    public static final String SAMPLE_INDEX_STATUS = "sampleIndex";
    private final HBaseManager hBaseManager;
    private final HBaseVariantTableNameGenerator tableNameGenerator;
    private final MRExecutor mrExecutor;
    private final AnnotationIndexDBAdaptor annotationIndexDBAdaptor;
    private final SampleIndexDBAdaptor sampleDBAdaptor;
    private final byte[] family;
    private final VariantStorageMetadataManager metadataManager;
    private Logger logger = LoggerFactory.getLogger(SampleIndexAnnotationLoader.class);

    public SampleIndexAnnotationLoader(GenomeHelper helper, HBaseManager hBaseManager, HBaseVariantTableNameGenerator tableNameGenerator,
                                       VariantStorageMetadataManager metadataManager, MRExecutor mrExecutor) {
        this.hBaseManager = hBaseManager;
        this.tableNameGenerator = tableNameGenerator;
        this.mrExecutor = mrExecutor;
        this.annotationIndexDBAdaptor = new AnnotationIndexDBAdaptor(hBaseManager, tableNameGenerator.getAnnotationIndexTableName());
        this.metadataManager = metadataManager;
        this.sampleDBAdaptor = new SampleIndexDBAdaptor(helper, hBaseManager, tableNameGenerator, this.metadataManager);
        family = helper.getColumnFamily();
    }

    public void updateSampleAnnotation(int studyId, List<Integer> samples, ObjectMap options) throws IOException, StorageEngineException {
        samples = new ArrayList<>(samples);
        samples.removeIf(sampleId -> {
            SampleMetadata sampleMetadata = metadataManager.getSampleMetadata(studyId, sampleId);
            if (!sampleMetadata.isAnnotated()) {
                logger.info("Unable to update sample index from sample '" + sampleMetadata.getName() + "'");
                return true;
            } else {
                return false;
            }
        });

//        updateSampleAnnotationBatchMultiThread(studyId, samples);
        updateSampleAnnotationBatchMapreduce(studyId, samples, options);
    }

    private void updateSampleAnnotationBatchMapreduce(int studyId, List<Integer> samples, ObjectMap options)
            throws IOException, StorageEngineException {
        mrExecutor.run(SampleIndexAnnotationLoaderDriver.class, SampleIndexAnnotationLoaderDriver.buildArgs(
                tableNameGenerator.getArchiveTableName(studyId),
                tableNameGenerator.getVariantTableName(), studyId, samples, options), options, "Update sample annotatin batch");
    }


    private void updateSampleAnnotationBatchMultiThread(int studyId, List<Integer> samples) throws IOException, StorageEngineException {
        logger.info("Update sample index annotation of " + samples.size() + " samples");

        String sampleIndexTableName = tableNameGenerator.getSampleIndexTableName(studyId);

        ProgressLogger progressLogger = new ProgressLogger("Sample index annotation updated variants");

        ParallelTaskRunner<Pair<Variant, Byte>, Put> ptr = new ParallelTaskRunner<>(
                new DataReader<Pair<Variant, Byte>>() {

                    private Iterator<Pair<Variant, Byte>> iterator = annotationIndexDBAdaptor.iterator();
                    private int initialCapacity = 200000;
                    private Pair<Variant, Byte> nextPair = null;

                    private String chromosome = "";
                    private int start = -1;
                    private int end = -1;

                    @Override
                    public List<Pair<Variant, Byte>> read(int n) {
                        List<Pair<Variant, Byte>> annotationMasks = new ArrayList<>(initialCapacity);

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
                            Pair<Variant, Byte> pair = iterator.next();
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
                        Map<String, List<Variant>> map = sampleDBAdaptor.rawQuery(studyId, sampleId, chromosome, start);
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
            sampleIterators.put(sample, sampleDBAdaptor.rawIterator(studyId, sample));
        }

        BufferedMutator mutator = hBaseManager.getConnection().getBufferedMutator(TableName.valueOf(sampleIndexTableName));

        String chromosome = "";
        int start = -1;
        int end = -1;
        List<Pair<Variant, Byte>> annotationMasks = null;
        do {
            for (Map.Entry<Integer, Iterator<Map<String, List<Variant>>>> sampleIteratorPair : sampleIterators.entrySet()) {
                Iterator<Map<String, List<Variant>>> sampleIterator = sampleIteratorPair.getValue();
                Integer sampleId = sampleIteratorPair.getKey();
                if (sampleIterator.hasNext()) {
                    Map<String, List<Variant>> next = sampleIterator.next();

                    Variant firstVariant = next.values().iterator().next().get(0);
                    if (annotationMasks == null
                            || !chromosome.equals(firstVariant.getChromosome())
                            || firstVariant.getStart() < start
                            || firstVariant.getStart() > end) {
                        chromosome = firstVariant.getChromosome();
                        start = firstVariant.getStart() - firstVariant.getStart() % SampleIndexSchema.BATCH_SIZE;
                        end = start + SampleIndexSchema.BATCH_SIZE;
                        annotationMasks = annotationIndexDBAdaptor.get(chromosome, start, end);
                    }

                    Put put = annotate(chromosome, start, sampleId, next, annotationMasks);
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
                        Map<String, List<Variant>> sampleIndex, List<Pair<Variant, Byte>> annotationMasks) {
        byte[] rk = SampleIndexSchema.toRowKey(sampleId, chromosome, start);
        Put put = new Put(rk);

        for (Map.Entry<String, List<Variant>> entry : sampleIndex.entrySet()) {
            String gt = entry.getKey();
            List<Variant> variantsToAnnotate = entry.getValue();
            if (!isAnnotatedGenotype(gt)) {
                continue;
            }

            ListIterator<Pair<Variant, Byte>> iterator = annotationMasks.listIterator();
            byte[] annotations = new byte[variantsToAnnotate.size()];
            int i = 0;
            int missingVariants = 0;
            // Assume both lists are ordered, and "variantsToAnnotate" is fully contained in "annotationMasks"
            for (Variant variantToAnnotate : variantsToAnnotate) {
                boolean restarted = false;
                while (iterator.hasNext()) {
                    Pair<Variant, Byte> annotationPair = iterator.next();
                    if (annotationPair.getKey().sameGenomicVariant(variantToAnnotate)) {
                        annotations[i] = annotationPair.getValue();
                        i++;
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
                            annotations[i] = (byte) 0xFF;
                            i++;
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

            put.addColumn(family, SampleIndexSchema.toAnnotationIndexColumn(gt), annotations);
            put.addColumn(family, SampleIndexSchema.toAnnotationIndexCountColumn(gt),
                    IndexUtils.countPerBitToBytes(IndexUtils.countPerBit(annotations)));
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
                sampleMetadata.setStatus(SAMPLE_INDEX_STATUS, TaskMetadata.Status.READY);
                return sampleMetadata;
            });
        }
    }

    public static boolean isAnnotatedGenotype(String gt) {
        return gt.contains("1");
    }


}
