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

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.SampleEntry;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos;
import org.opencb.biodata.tools.variant.converters.proto.VariantToVcfSliceConverter;
import org.opencb.biodata.tools.variant.filters.VariantAvroFilters;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.run.ParallelTaskRunner.Task;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageOptions.ARCHIVE_FIELDS;

/**
 * Created on 06/06/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantToVcfSliceConverterTask implements Task<ImmutablePair<Long, List<Variant>>, VcfSliceProtos.VcfSlice> {
    private final VariantToVcfSliceConverter converterNonRef;
    private final VariantToVcfSliceConverter converterRef;
    private VariantAvroFilters refFilter;
    private final ProgressLogger progressLogger;

    public VariantToVcfSliceConverterTask() {
        this(null, null, null);
    }

    public VariantToVcfSliceConverterTask(ProgressLogger progressLogger) {
        this(progressLogger, null, null);
    }

    public VariantToVcfSliceConverterTask(ProgressLogger progressLogger, String fields, String nonRefFilter) {
        this.progressLogger = progressLogger;
        this.converterNonRef = new VariantToVcfSliceConverter();

        if (StringUtils.isEmpty(fields) || fields.equals(VariantQueryUtils.ALL)) {
            this.converterRef = new VariantToVcfSliceConverter();
        } else if (fields.equals(VariantQueryUtils.NONE)) {
            this.converterRef = null;
        } else {
            HashSet<String> attributeFields = new HashSet<>();
            HashSet<String> formatFields = new HashSet<>();
            parseArchiveFields(attributeFields, formatFields, fields);
            this.converterRef = new VariantToVcfSliceConverter(attributeFields, formatFields);
        }
        this.refFilter = new VariantAvroFilters().addTypeFilter(VariantType.NO_VARIATION)
                .addSampleFormatFilter("GT", VariantToVcfSliceConverterTask::isHomRef);
        if (StringUtils.isNotEmpty(nonRefFilter)) {
            refFilter.addFilter(new VariantAvroFilters().addFilter(true, true, nonRefFilter).negate());
        }
    }

    @Override
    public List<VcfSliceProtos.VcfSlice> apply(List<ImmutablePair<Long, List<Variant>>> batch) {
        List<VcfSliceProtos.VcfSlice> slices = new ArrayList<>(batch.size());
        for (ImmutablePair<Long, List<Variant>> pair : batch) {
            List<Variant> ref = new ArrayList<>(pair.right.size());
            List<Variant> nonRef = new ArrayList<>(pair.right.size());
            for (Variant variant : pair.right) {
                if (refFilter.test(variant)) {
                    ref.add(variant);
                } else {
                    nonRef.add(variant);
                }
            }
            if (converterRef != null && !ref.isEmpty()) {
                slices.add(converterRef.convert(ref, pair.getLeft().intValue()));
            }
            if (!nonRef.isEmpty()) {
                slices.add(converterNonRef.convert(nonRef, pair.getLeft().intValue()));
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
        Integer gtIdx = studyEntry.getSampleDataKeyPosition("GT");
        if (gtIdx == null || gtIdx < 0) {
            return false;
        }
        for (SampleEntry sample : studyEntry.getSamples()) {
            if (!isHomRef(sample.getData().get(gtIdx))) {
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

    private static void parseArchiveFields(Set<String> attributeFields, Set<String> formatFields, String fields) {
        // Always store GT, QUAL and FILTER in archive table!
        formatFields.add("GT");
        formatFields.add("FILTER");
        formatFields.add("QUAL");

        Set<String> currentFieldsSet = null;
        for (String field : fields.split(",")) {
            if (field.contains(":")) {
                String[] split = field.split(":");
                if (split[0].equalsIgnoreCase("INFO") || split[0].equalsIgnoreCase("ATTRIBUTES")) {
                    currentFieldsSet = attributeFields;
                } else if (split[0].equalsIgnoreCase("FORMAT")) {
                    currentFieldsSet = formatFields;
                } else {
                    throw new IllegalArgumentException("Malformed param '" + ARCHIVE_FIELDS + "', Unknown group " + split[0]);
                }
                currentFieldsSet.add(split[1]);
            } else if (field.equalsIgnoreCase(StudyEntry.FILTER)) {
                attributeFields.add(StudyEntry.FILTER);
                // Unset current fields set
                if (currentFieldsSet != attributeFields) {
                    currentFieldsSet = null;
                }
            } else if (field.equalsIgnoreCase(StudyEntry.QUAL)) {
                attributeFields.add(StudyEntry.QUAL);
                // Unset current fields set
                if (currentFieldsSet != attributeFields) {
                    currentFieldsSet = null;
                }
            } else if (field.equals("GT")) {
                formatFields.add("GT");
                // Unset current fields set
                if (currentFieldsSet != formatFields) {
                    currentFieldsSet = null;
                }
            } else {
                if (currentFieldsSet == null) {
                    throw new IllegalArgumentException("Malformed param '" + ARCHIVE_FIELDS + "', unknown field currentFieldsSet");
                } else {
                    currentFieldsSet.add(field);
                }
            }
        }
    }

}
