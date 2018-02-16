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

import com.google.common.collect.LinkedListMultimap;
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
import org.opencb.opencga.storage.hadoop.variant.gaps.VariantOverlappingStatus;
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
    private final Set<String> defaultGenotypes;
    private final StudyConfiguration studyConfiguration;
    private final List<String> fixedFormat;
    private final Set<String> fixedFormatSet;
    private final List<String> fileAttributes;
    private final PhoenixHelper.Column studyColumn;
    private final LinkedListMultimap<Integer, Integer> sampleToFileMap;
    private boolean addSecondaryAlternates;

    public StudyEntryToHBaseConverter(byte[] columnFamily, StudyConfiguration studyConfiguration) {
        this(columnFamily, studyConfiguration, false);
    }

    public StudyEntryToHBaseConverter(byte[] columnFamily, StudyConfiguration studyConfiguration, boolean addSecondaryAlternates) {
        this(columnFamily, studyConfiguration, addSecondaryAlternates, new HashSet<>(Arrays.asList("0/0", "0|0")));
    }

    public StudyEntryToHBaseConverter(byte[] columnFamily, StudyConfiguration studyConfiguration,
                                      boolean addSecondaryAlternates, Set<String> defaultGenotypes) {
        super(columnFamily);
        this.studyConfiguration = studyConfiguration;
        studyColumn = VariantPhoenixHelper.getStudyColumn(studyConfiguration.getStudyId());
        this.addSecondaryAlternates = addSecondaryAlternates;
        this.defaultGenotypes = defaultGenotypes;
        fixedFormat = HBaseToVariantConverter.getFixedFormat(studyConfiguration);
        fixedFormatSet = new HashSet<>(fixedFormat);
        fileAttributes = HBaseToVariantConverter.getFixedAttributes(studyConfiguration);

        sampleToFileMap = LinkedListMultimap.create();
        for (Map.Entry<Integer, LinkedHashSet<Integer>> entry : studyConfiguration.getSamplesInFiles().entrySet()) {
            for (Integer sampleId : entry.getValue()) {
                sampleToFileMap.put(sampleId, entry.getKey());
            }
        }
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
        return convert(variant, put, sampleIds, VariantOverlappingStatus.NONE);
    }

    public Put convert(Variant variant, Put put, Set<Integer> sampleIds, VariantOverlappingStatus overlappingStatus) {
        StudyEntry studyEntry = variant.getStudies().get(0);
        Integer gtIdx = studyEntry.getFormatPositions().get(VariantMerger.GT_KEY);
        int[] formatReMap = buildFormatRemap(studyEntry);
        int sampleIdx = 0;
        List<String> samplesName = studyEntry.getOrderedSamplesName();
        // Always write file attributes if there is no samples (i.e. aggregated files)
        boolean writeAllFileAttributes = samplesName.isEmpty();
        boolean writeFileAttributes = writeAllFileAttributes;
        Set<Integer> filesToWrite = new HashSet<>();
        for (String sampleName : samplesName) {
            Integer sampleId = studyConfiguration.getSampleIds().get(sampleName);
            if (sampleIds == null || sampleIds.contains(sampleId)) {
                byte[] column = VariantPhoenixHelper.buildSampleColumnKey(studyConfiguration.getStudyId(), sampleId);
                List<String> sampleData = studyEntry.getSamplesData().get(sampleIdx);
                // Write sample data if the is no genotype information, or if the genotype is equals to the default genotype
                if (gtIdx == null || !defaultGenotypes.contains(sampleData.get(gtIdx))) {
                    if (formatReMap != null) {
                        sampleData = remapSampleData(studyEntry, formatReMap, sampleData);
                    }
                    addVarcharArray(put, column, sampleData);
                    // Write file attributes if at least one sample is written.
                    writeFileAttributes = true;
                    filesToWrite.addAll(sampleToFileMap.get(sampleId));
                }
            }
            sampleIdx++;
        }
        if (writeFileAttributes) {
            for (FileEntry fileEntry : studyEntry.getFiles()) {
                int fileId = Integer.parseInt(fileEntry.getFileId());
                if (writeAllFileAttributes || filesToWrite.contains(fileId)) {
                    byte[] fileColumnKey = VariantPhoenixHelper
                            .buildFileColumnKey(studyConfiguration.getStudyId(), fileId);
                    List<String> fileColumn = remapFileData(variant, studyEntry, fileEntry, overlappingStatus);
                    addVarcharArray(put, fileColumnKey, fileColumn);
                }
            }
        }

        return put;
    }

    private List<String> remapFileData(Variant variant, StudyEntry studyEntry, FileEntry fileEntry,
                                       VariantOverlappingStatus overlappingStatus) {
        int capacity = fileAttributes.size() + HBaseToStudyEntryConverter.FILE_INFO_START_IDX;
        List<String> fileColumn = Arrays.asList(new String[capacity]);

        Map<String, String> attributes = fileEntry.getAttributes();
        fileColumn.set(HBaseToStudyEntryConverter.FILE_CALL_IDX, fileEntry.getCall());
        if (addSecondaryAlternates && studyEntry.getSecondaryAlternates() != null && !studyEntry.getSecondaryAlternates().isEmpty()) {
            fileColumn.set(HBaseToStudyEntryConverter.FILE_SEC_ALTS_IDX, getSecondaryAlternates(variant, studyEntry));
        }
        fileColumn.set(HBaseToStudyEntryConverter.FILE_VARIANT_OVERLAPPING_STATUS_IDX, overlappingStatus.toString());
        fileColumn.set(HBaseToStudyEntryConverter.FILE_QUAL_IDX, attributes.get(StudyEntry.QUAL));
        fileColumn.set(HBaseToStudyEntryConverter.FILE_FILTER_IDX, attributes.get(StudyEntry.FILTER));
        int attributeIdx = HBaseToStudyEntryConverter.FILE_INFO_START_IDX;
        for (String fileAttribute : fileAttributes) {
            fileColumn.set(attributeIdx, attributes.get(fileAttribute));
            attributeIdx++;
        }

        // Trim all leading null values
        for (int i = fileColumn.size() - 1; i >= 0; i--) {
            if (fileColumn.get(i) != null) {
                if (i != fileColumn.size() - 1) {
                    fileColumn = fileColumn.subList(0, i + 1);
                }
                break;
            }
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
                    if (sampleData.size() > i) {
                        remappedSampleData.add(sampleData.get(i));
                    } else {
                        remappedSampleData.add("");
                    }
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
