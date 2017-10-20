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

package org.opencb.opencga.storage.hadoop.variant.converters.study;

import org.apache.hadoop.hbase.client.Put;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.AlternateCoordinate;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.tools.Converter;
import org.opencb.biodata.tools.variant.merge.VariantMerger;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.hadoop.variant.converters.AbstractPhoenixConverter;
import org.opencb.opencga.storage.hadoop.variant.converters.HBaseToVariantConverter;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.PhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixKeyFactory;

import java.util.*;


/**
 * Created on 25/05/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class StudyEntryToHBaseConverter extends AbstractPhoenixConverter implements Converter<Variant, Put> {

    private static final int UNKNOWN_FIELD = -1;
    private static final int FILTER_FIELD = -2;
    private final Set<String> defaultGenotypes = new HashSet<>();
    private final StudyConfiguration studyConfiguration;
    private final List<String> fixedFormat;
    private final Set<String> fixedFormatSet;
    private final List<String> fileAttributes;
    private final PhoenixHelper.Column studyColumn;
    private boolean addSecondaryAlternates;

    public StudyEntryToHBaseConverter(byte[] columnFamily, StudyConfiguration studyConfiguration) {
        this(columnFamily, studyConfiguration, false);
    }

    public StudyEntryToHBaseConverter(byte[] columnFamily, StudyConfiguration studyConfiguration, boolean addSecondaryAlternates) {
        super(columnFamily);
        this.studyConfiguration = studyConfiguration;
        studyColumn = VariantPhoenixHelper.getStudyColumn(studyConfiguration.getStudyId());
        this.addSecondaryAlternates = addSecondaryAlternates;
        defaultGenotypes.add("0/0");
        defaultGenotypes.add("0|0");
        fixedFormat = HBaseToVariantConverter.getFixedFormat(studyConfiguration);
        fixedFormatSet = new HashSet<>(fixedFormat);
        fileAttributes = HBaseToVariantConverter.getFixedAttributes(studyConfiguration);
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
        List<String> samplesName = studyEntry.getOrderedSamplesName();
        // Allways write file attributes if there is no samples (i.e. aggregated files)
        boolean writeFileAttributes = samplesName.isEmpty();
        for (String sampleName : samplesName) {
            Integer sampleId = studyConfiguration.getSampleIds().get(sampleName);
            if (sampleIds == null || sampleIds.contains(sampleId)) {
                byte[] column = VariantPhoenixHelper.buildSampleColumnKey(studyConfiguration.getStudyId(), sampleId);
                List<String> sampleData = studyEntry.getSamplesData().get(sampleIdx);
                if (!defaultGenotypes.contains(sampleData.get(gtIdx))) {
                    if (formatReMap != null) {
                        sampleData = remapSampleData(studyEntry, formatReMap, sampleData);
                    }
                    addVarcharArray(put, column, sampleData);
                    writeFileAttributes = true;
                }
            }
            sampleIdx++;
        }
        if (writeFileAttributes) {
            FileEntry fileEntry = studyEntry.getFiles().get(0);
            byte[] fileColumnKey = VariantPhoenixHelper
                    .buildFileColumnKey(studyConfiguration.getStudyId(), Integer.parseInt(fileEntry.getFileId()));
            List<String> fileColumn = remapFileData(variant, studyEntry, fileEntry);
            addVarcharArray(put, fileColumnKey, fileColumn);
        }

        return put;
    }

    private List<String> remapFileData(Variant variant, StudyEntry studyEntry, FileEntry fileEntry) {
        List<String> fileColumn = new ArrayList<>(fileAttributes.size() + HBaseToStudyEntryConverter.FILE_INFO_START_IDX);

        Map<String, String> attributes = fileEntry.getAttributes();
        fileColumn.add(fileEntry.getCall());
        if (addSecondaryAlternates && studyEntry.getSecondaryAlternates() != null && !studyEntry.getSecondaryAlternates().isEmpty()) {
            fileColumn.add(getSecondaryAlternates(variant, studyEntry));
        } else {
            fileColumn.add(null);
        }
        fileColumn.add(attributes.get(StudyEntry.QUAL));
        fileColumn.add(attributes.get(StudyEntry.FILTER));
        for (String fileAttribute : fileAttributes) {
            fileColumn.add(attributes.get(fileAttribute));
        }

        return fileColumn;
    }

    private String getSecondaryAlternates(Variant variant, StudyEntry studyEntry) {
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
        return sb.toString();
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

    private List<String> remapSampleData(StudyEntry studyEntry, int[] formatReMap, List<String> sampleData) {
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

//        int sampleDataIdx = 0;
//        for (String key : studyEntry.getFormat()) {
//            if (!fixedFormat.contains(key)) {
//                builder.putSampleData(key, sampleData.get(sampleDataIdx));
//            }
//            sampleDataIdx++;
//        }

        return remappedSampleData;
    }

}
