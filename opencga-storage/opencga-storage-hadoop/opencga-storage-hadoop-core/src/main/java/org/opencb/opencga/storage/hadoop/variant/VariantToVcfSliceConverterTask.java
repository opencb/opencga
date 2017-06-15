package org.opencb.opencga.storage.hadoop.variant;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos;
import org.opencb.biodata.tools.variant.converters.proto.VariantToVcfSliceConverter;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.run.ParallelTaskRunner.Task;

import java.util.ArrayList;
import java.util.List;

/**
 * Created on 06/06/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantToVcfSliceConverterTask implements Task<ImmutablePair<Long, List<Variant>>, VcfSliceProtos.VcfSlice> {
    private final VariantToVcfSliceConverter converter;
    private final ProgressLogger progressLogger;

    public VariantToVcfSliceConverterTask() {
        this(null);
    }

    public VariantToVcfSliceConverterTask(ProgressLogger progressLogger) {
        this.progressLogger = progressLogger;
        this.converter = new VariantToVcfSliceConverter();
    }

    @Override
    public List<VcfSliceProtos.VcfSlice> apply(List<ImmutablePair<Long, List<Variant>>> batch) {
        List<VcfSliceProtos.VcfSlice> slices = new ArrayList<>(batch.size());
        for (ImmutablePair<Long, List<Variant>> pair : batch) {
            slices.add(converter.convert(pair.getRight(), pair.getLeft().intValue()));
            if (progressLogger != null) {
                progressLogger.increment(pair.getRight().size());
            }
        }
        return slices;
    }
}
