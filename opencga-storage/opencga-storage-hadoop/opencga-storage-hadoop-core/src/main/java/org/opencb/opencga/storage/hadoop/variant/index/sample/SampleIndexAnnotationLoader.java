package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Put;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.index.annotation.AnnotationIndexDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.utils.HBaseVariantTableNameGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * Created by jacobo on 04/01/19.
 */
public class SampleIndexAnnotationLoader {

    private final HBaseManager hBaseManager;
    private final HBaseVariantTableNameGenerator tableNameGenerator;
    private final AnnotationIndexDBAdaptor annotationIndexDBAdaptor;
    private final SampleIndexDBAdaptor sampleDBAdaptor;
    private final byte[] family;
    private Logger logger = LoggerFactory.getLogger(SampleIndexAnnotationLoader.class);

    public SampleIndexAnnotationLoader(GenomeHelper helper, HBaseManager hBaseManager, HBaseVariantTableNameGenerator tableNameGenerator,
                                       VariantStorageMetadataManager metadataManager) {
        this.hBaseManager = hBaseManager;
        this.tableNameGenerator = tableNameGenerator;
        this.annotationIndexDBAdaptor = new AnnotationIndexDBAdaptor(hBaseManager, tableNameGenerator.getAnnotationIndexTableName());
        this.sampleDBAdaptor = new SampleIndexDBAdaptor(helper, hBaseManager, tableNameGenerator, metadataManager);
        family = helper.getColumnFamily();
    }

    public void updateSampleAnnotation(int studyId, List<Integer> samples) throws IOException {
        String sampleIndexTableName = tableNameGenerator.getSampleIndexTableName(studyId);

        BufferedMutator mutator = hBaseManager.getConnection().getBufferedMutator(TableName.valueOf(sampleIndexTableName));

        String chromosome = "";
        int start = -1;
        int end = -1;
        List<Pair<Variant, Byte>> annotationMasks = new ArrayList<>();

        Iterator<Pair<Variant, Byte>> iterator = annotationIndexDBAdaptor.iterator();
        Pair<Variant, Byte> nextPair = null;
        while (iterator.hasNext()) {

            // Read next batch
            annotationMasks.clear();
            if (nextPair == null && iterator.hasNext()) {
                nextPair = iterator.next();
            }
            if (nextPair != null) {
                annotationMasks.add(nextPair);
                Variant firstVariant = nextPair.getKey();
                chromosome = firstVariant.getChromosome();
                start = firstVariant.getStart() - firstVariant.getStart() % SampleIndexDBLoader.BATCH_SIZE;
                end = start + SampleIndexDBLoader.BATCH_SIZE;
                nextPair = null;
            }
            while (iterator.hasNext()) {
                Pair<Variant, Byte> pair = iterator.next();
                Variant variant = pair.getKey();
                if (variant.getChromosome().equals(chromosome) && variant.getStart() > start && variant.getEnd() < end) {
                    annotationMasks.add(pair);
                } else {
                    nextPair = pair;
                    break;
                }
            }

            // Ensure is sorted as expected
            annotationMasks.sort(Comparator.comparing(Pair::getKey, HBaseToSampleIndexConverter.INTRA_CHROMOSOME_VARIANT_COMPARATOR));

            for (Integer sampleId : samples) {
                Map<String, List<Variant>> map = sampleDBAdaptor.rawQuery(studyId, sampleId, chromosome, start);
                Put put = annotate(chromosome, start, sampleId, map, annotationMasks);
                if (!put.isEmpty()) {
                    mutator.mutate(put);
                }
//                else logger.warn("Empty put for sample " + sampleId + " -> "  + chromosome + ":" + start + ":" + end);

            }
        }

        mutator.close();
    }

    public void updateSampleAnnotationMultiSampleIterator(int studyId, List<Integer> samples) throws IOException {
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
                        start = firstVariant.getStart() - firstVariant.getStart() % SampleIndexDBLoader.BATCH_SIZE;
                        end = start + SampleIndexDBLoader.BATCH_SIZE;
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
    }

    private Put annotate(String chromosome, int start, Integer sampleId,
                        Map<String, List<Variant>> sampleIndex, List<Pair<Variant, Byte>> annotationMasks) {
        byte[] rk = HBaseToSampleIndexConverter.toRowKey(sampleId, chromosome, start);
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
                while (iterator.hasNext()) {
                    Pair<Variant, Byte> annotationPair = iterator.next();
                    if (annotationPair.getKey().sameGenomicVariant(variantToAnnotate)) {
                        annotations[i] = annotationPair.getValue();
                        i++;
                        break;
                    } else if (annotationPair.getKey().getStart() > variantToAnnotate.getStart()) {
                        logger.error("Missing variant to annotate " + variantToAnnotate);
                        iterator.previous();
                        annotations[i] = (byte) 0xFF;
                        i++;
                        missingVariants++;
                        break;
                    }
                }
            }
            if (missingVariants > 0) {
                // TODO: What if a variant is not annotated?
                String msg = "Error annotating batch. " + missingVariants + " missing variants";
                logger.error(msg);
//                            throw new IllegalStateException(msg);
            }

            put.addColumn(family, HBaseToSampleIndexConverter.toAnnotationIndexColumn(gt), annotations);
        }
        return put;
    }

    public boolean isAnnotatedGenotype(String gt) {
        return gt.contains("1");
    }


}
