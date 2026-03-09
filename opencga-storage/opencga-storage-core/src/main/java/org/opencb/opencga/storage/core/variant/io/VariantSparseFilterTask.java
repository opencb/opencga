package org.opencb.opencga.storage.core.variant.io;

import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.SampleEntry;
import org.opencb.commons.run.Task;
import org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass;

import java.util.ArrayList;
import java.util.List;

/**
 * Filters out samples with HOM_REF or MISS genotypes from variant study entries.
 * Sets samplesPosition to null (sparse output has no positional mapping).
 *
 * <p>Acts as a safety net in the export pipeline for all backends. Backend converters
 * (e.g. MongoDB's DocumentToSamplesConverter, Hadoop's HBaseToStudyEntryConverter)
 * may perform their own early sparse filtering for performance; this task ensures
 * consistent output regardless of backend implementation.
 */
public class VariantSparseFilterTask implements Task<Variant, Variant> {

    @Override
    public List<Variant> apply(List<Variant> batch) {
        for (Variant variant : batch) {
            for (StudyEntry study : variant.getStudies()) {
                List<SampleEntry> sparseSamples = new ArrayList<>();
                if (study.getSamples() != null) {
                    for (SampleEntry sample : study.getSamples()) {
                        if (sample != null && sample.getFileIndex() != null
                                && sample.getData() != null && !sample.getData().isEmpty()) {
                            String gt = sample.getData().get(0);
                            if (gt != null
                                    && !GenotypeClass.HOM_REF.test(gt)
                                    && !GenotypeClass.MISS.test(gt)) {
                                sparseSamples.add(sample);
                            }
                        }
                    }
                }
                study.setSamplesPosition(null);
                study.setSamples(sparseSamples);
            }
        }
        return batch;
    }
}
