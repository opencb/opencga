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

package org.opencb.opencga.storage.hadoop.variant.converters.samples;

import org.apache.hadoop.hbase.client.Put;
import org.apache.phoenix.schema.types.PVarbinary;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.AlternateCoordinate;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.protobuf.VariantProto;
import org.opencb.biodata.tools.variant.converters.Converter;
import org.opencb.biodata.tools.variant.merge.VariantMerger;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.hadoop.variant.converters.AbstractPhoenixConverter;
import org.opencb.opencga.storage.hadoop.variant.converters.HBaseToVariantConverter;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.PhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.models.protobuf.OtherSampleData;

import java.util.*;


/**
 * Created on 25/05/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class SamplesDataToHBaseConverter extends AbstractPhoenixConverter implements Converter<Variant, Put> {


    private static final int UNKNOWN_FIELD = -1;
    private static final int FILTER_FIELD = -2;
    private final Set<String> defaultGenotypes = new HashSet<>();
    private final StudyConfiguration studyConfiguration;
    private final List<String> fixedFormat;
    private final Set<String> fixedFormatSet;
    private final PhoenixHelper.Column studyColumn;
    private boolean addSecondaryAlternates;

    public SamplesDataToHBaseConverter(byte[] columnFamily, StudyConfiguration studyConfiguration) {
        this(columnFamily, studyConfiguration, false);
    }

    public SamplesDataToHBaseConverter(byte[] columnFamily, StudyConfiguration studyConfiguration, boolean addSecondaryAlternates) {
        super(columnFamily);
        this.studyConfiguration = studyConfiguration;
        studyColumn = VariantPhoenixHelper.getStudyColumn(studyConfiguration.getStudyId());
        this.addSecondaryAlternates = addSecondaryAlternates;
        defaultGenotypes.add("0/0");
        defaultGenotypes.add("0|0");
        fixedFormat = HBaseToVariantConverter.getFixedFormat(studyConfiguration);
        fixedFormatSet = new HashSet<>(fixedFormat);
    }

    @Override
    public Put convert(Variant variant) {
        byte[] rowKey = VariantPhoenixKeyFactory.generateVariantRowKey(variant);
        Put put = new Put(rowKey);
        add(put, VariantPhoenixHelper.VariantColumn.TYPE, variant.getType().toString());
        add(put, studyColumn, 0);
        return convert(variant, put, null);
    }

    public Put convert(Variant variant, Put put) {
        return convert(variant, put, null);
    }


    public Put convert(Variant variant, Put put, Set<Integer> sampleIds) {
        StudyEntry studyEntry = variant.getStudies().get(0);
        Integer gtIdx = studyEntry.getFormatPositions().get(VariantMerger.GT_KEY);
        int[] formatReMap = buildFormatRemap(studyEntry);
        int sampleIdx = 0;
        for (String sampleName : studyEntry.getOrderedSamplesName()) {
            Integer sampleId = studyConfiguration.getSampleIds().get(sampleName);
            if (sampleIds == null || sampleIds.contains(sampleId)) {
                byte[] column = VariantPhoenixHelper.buildSampleColumnKey(studyConfiguration.getStudyId(), sampleId);
                byte[] columnOther = VariantPhoenixHelper.buildOtherSampleDataColumnKey(studyConfiguration.getStudyId(), sampleId);
                OtherSampleData.Builder builder = OtherSampleData.newBuilder();
                List<String> sampleData = studyEntry.getSamplesData().get(sampleIdx);
                if (!defaultGenotypes.contains(sampleData.get(gtIdx))) {
                    if (formatReMap != null) {
                        sampleData = remapSampleData(studyEntry, formatReMap, sampleData, builder);
                    }
                    addFileAttributes(studyEntry, builder);
                    addSecondaryAlternates(variant, studyEntry, sampleData);
                    addVarcharArray(put, column, sampleData);
                    add(put, columnOther, builder.build().toByteArray(), PVarbinary.INSTANCE);
                }
            }
            sampleIdx++;
        }

        return put;
    }

    private void addFileAttributes(StudyEntry studyEntry, OtherSampleData.Builder builder) {
        FileEntry fileEntry = studyEntry.getFiles().get(0);
        VariantProto.FileEntry.Builder fileBuilder = VariantProto.FileEntry.newBuilder()
                .putAllAttributes(fileEntry.getAttributes())
                .setFileId(fileEntry.getFileId());
        if (fileEntry.getCall() != null) {
            fileBuilder.setCall(fileEntry.getCall());
        }
        builder.setFile(fileBuilder);
    }

    public void addSecondaryAlternates(Variant variant, StudyEntry studyEntry, List<String> sampleData) {
        if (addSecondaryAlternates && studyEntry.getSecondaryAlternates() != null && !studyEntry.getSecondaryAlternates().isEmpty()) {
            StringBuilder sb = new StringBuilder();
            Iterator<AlternateCoordinate> iterator = studyEntry.getSecondaryAlternates().iterator();
            while (iterator.hasNext()) {
                AlternateCoordinate alt = iterator.next();
                sb.append(alt.getChromosome() == null ? variant.getChromosome() : alt.getChromosome());
                sb.append(':');
                sb.append(alt.getStart() == null ? variant.getStart() : alt.getStart());
                sb.append(':');
                sb.append(alt.getEnd() == null ? variant.getEnd() : alt.getEnd());
                sb.append(':');
                sb.append(alt.getReference() == null ? variant.getReference() : alt.getReference());
                sb.append(':');
                sb.append(alt.getAlternate() == null ? variant.getAlternate() : alt.getAlternate());
                sb.append(':');
                sb.append(alt.getType() == null ? variant.getType() : alt.getType());

                if (iterator.hasNext()) {
                    sb.append(',');
                }
            }
            sampleData.add(sb.toString());
        }
    }

    private int[] buildFormatRemap(StudyEntry studyEntry) {
        int[] formatReMap;
        if (!fixedFormatSet.equals(studyEntry.getFormat())) {
            formatReMap = new int[fixedFormat.size()];
            for (int i = 0; i < fixedFormat.size(); i++) {
                String format = fixedFormat.get(i);
                Integer idx = studyEntry.getFormatPositions().get(format);
                if (idx == null) {
                    if (format.equals(VariantMerger.GENOTYPE_FILTER_KEY)) {
                        idx = FILTER_FIELD;
                    } else {
                        idx = UNKNOWN_FIELD;
                    }
                }
                formatReMap[i] = idx;
            }
        } else {
            formatReMap = null;
        }
        return formatReMap;
    }

    private List<String> remapSampleData(StudyEntry studyEntry, int[] formatReMap, List<String> sampleData,
                                         OtherSampleData.Builder builder) {
        List<String> remappedSampleData = new ArrayList<>(formatReMap.length);

        for (int i : formatReMap) {
            switch (i) {
                case UNKNOWN_FIELD:
                    remappedSampleData.add("");
                    break;
                case FILTER_FIELD:
                    remappedSampleData.add(studyEntry.getFiles().get(0).getAttributes().get(StudyEntry.FILTER));
                    break;
                default:
                    remappedSampleData.add(sampleData.get(i));
                    break;
            }
        }

        int sampleDataIdx = 0;
        for (String key : studyEntry.getFormat()) {
            if (!fixedFormat.contains(key)) {
                builder.putSampleData(key, sampleData.get(sampleDataIdx));
            }
            sampleDataIdx++;
        }

        return remappedSampleData;
    }

}
