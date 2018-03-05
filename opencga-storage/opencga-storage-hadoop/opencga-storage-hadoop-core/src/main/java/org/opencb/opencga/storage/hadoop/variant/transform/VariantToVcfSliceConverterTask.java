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

package org.opencb.opencga.storage.hadoop.variant.transform;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.opencb.biodata.models.variant.StudyEntry;
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
            List<Variant> ref = new ArrayList<>(pair.right.size());
            List<Variant> nonRef = new ArrayList<>(pair.right.size());
            for (Variant variant : pair.right) {
                if (isRefVariant(variant)) {
                    ref.add(variant);
                } else {
                    nonRef.add(variant);
                }
            }
            if (!ref.isEmpty()) {
                slices.add(converter.convert(ref, pair.getLeft().intValue()));
            }
            if (!nonRef.isEmpty()) {
                slices.add(converter.convert(nonRef, pair.getLeft().intValue()));
            }
            if (progressLogger != null) {
                progressLogger.increment(pair.getRight().size());
            }
        }
        return slices;
    }

    /**
     * A variant is RefVariant if all the samples have HomRef genotype (0, 0/0, 0|0, ...).
     * If the variant does not have genotype, or there is any genotype not homRef, the variant is not a RefVariant.
     * @param variant Variant to test
     * @return  True if the variant is a reference variant
     */
    protected static boolean isRefVariant(Variant variant) {
        if (variant.getStudies().size() != 1) {
            throw new IllegalArgumentException("Required one Study per variant. Found " + variant.getStudies().size() + " studies instead");
        }
        StudyEntry studyEntry = variant.getStudies().get(0);
        Integer gtIdx = studyEntry.getFormatPositions().get("GT");
        if (gtIdx == null || gtIdx < 0) {
            return false;
        }
        for (List<String> data : studyEntry.getSamplesData()) {
            if (!isHomRef(data.get(gtIdx))) {
                return false;
            }
        }
        return true;
    }

    public static boolean isHomRef(String gt) {
        for (int i = 0; i < gt.length(); i++) {
            char c = gt.charAt(i);
            if (c != '0' && c != '/' && c != '|') {
                return false;
            }
        }
        return true;
    }

}
