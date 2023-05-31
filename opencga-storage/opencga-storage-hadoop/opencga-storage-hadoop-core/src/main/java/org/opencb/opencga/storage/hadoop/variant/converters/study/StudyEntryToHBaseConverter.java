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
import org.apache.solr.common.StringUtils;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.AlternateCoordinate;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.tools.commons.Converter;
import org.opencb.biodata.tools.variant.merge.VariantMerger;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.PhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchema;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.converters.AbstractPhoenixConverter;
import org.opencb.opencga.storage.hadoop.variant.converters.HBaseToVariantConverter;
import org.opencb.opencga.storage.hadoop.variant.gaps.VariantOverlappingStatus;

import java.util.*;

import static org.opencb.opencga.storage.hadoop.variant.converters.study.HBaseToStudyEntryConverter.ALTERNATE_COORDINATE_SEPARATOR;


/**
 * Created on 25/05/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class StudyEntryToHBaseConverter extends AbstractPhoenixConverter implements Converter<Variant, Put> {

    private static final int UNKNOWN_FIELD = -1;
    private static final int FILTER_FIELD = -2;
    private final Set<String> defaultGenotypes;
//    private final StudyConfiguration studyConfiguration;
    protected final StudyMetadata studyMetadata;
    private final List<String> fixedFormat;
    private final boolean excludeGenotypes;
    private final List<String> fileAttributes;
    private final PhoenixHelper.Column studyColumn;
    private final Map<String, Integer> sampleIdsMap;
    private boolean addSecondaryAlternates;
    private final PhoenixHelper.Column releaseColumn;

    public StudyEntryToHBaseConverter(byte[] columnFamily, int studyId, VariantStorageMetadataManager metadataManager,
                                      boolean addSecondaryAlternates, Integer release, boolean excludeGenotypes,
                                      boolean includeReferenceVariantsData) {
        this(columnFamily, studyId, metadataManager, addSecondaryAlternates,
                release, excludeGenotypes, includeReferenceVariantsData
                        ? Collections.emptySet()
                        : new HashSet<>(Arrays.asList("0/0", "0|0")));
    }

    private StudyEntryToHBaseConverter(byte[] columnFamily, int studyId, VariantStorageMetadataManager metadataManager,
                                       boolean addSecondaryAlternates, Integer release, boolean excludeGenotypes,
                                       Set<String> defaultGenotypes) {
        super(columnFamily);
        this.studyMetadata = metadataManager.getStudyMetadata(studyId);
        studyColumn = VariantPhoenixSchema.getStudyColumn(studyMetadata.getId());
        this.addSecondaryAlternates = addSecondaryAlternates;
        this.defaultGenotypes = defaultGenotypes;
        fixedFormat = HBaseToVariantConverter.getFixedFormat(studyMetadata);
        this.excludeGenotypes = excludeGenotypes;
        fileAttributes = HBaseToVariantConverter.getFixedAttributes(studyMetadata);

        sampleIdsMap = new HashMap<>();
        metadataManager.sampleMetadataIterator(studyId).forEachRemaining(sampleMetadata -> {
            sampleIdsMap.put(sampleMetadata.getName(), sampleMetadata.getId());
        });
//        for (Map.Entry<Integer, LinkedHashSet<Integer>> entry : studyConfiguration.getSamplesInFiles().entrySet()) {
//            for (Integer sampleId : entry.getValue()) {
//                sampleToFileMap.put(sampleId, entry.getKey());
//            }
//        }
        //        Integer release = studyConfiguration.getAttributes().getInt(VariantStorageEngine.Options.RELEASE.key());
        if (release != null) {
            if (release <= 0) {
                throw new IllegalArgumentException("Invalid RELEASE value. Expected > 0, received " + release);
            }
            releaseColumn = VariantPhoenixSchema.getReleaseColumn(release);
        } else {
            releaseColumn = null;
        }
    }

    @Override
    public Put convert(Variant variant) {
        byte[] rowKey = VariantPhoenixKeyFactory.generateVariantRowKey(variant);
        Put put = new Put(rowKey);
        add(put, VariantPhoenixSchema.VariantColumn.TYPE, variant.getType().toString());
        if (variant.getSv() != null) {
            if (variant.getSv().getCiStartLeft() != null) {
                add(put, VariantPhoenixSchema.VariantColumn.CI_START_L, Math.max(0, variant.getSv().getCiStartLeft()));
            }
            if (variant.getSv().getCiStartRight() != null) {
                add(put, VariantPhoenixSchema.VariantColumn.CI_START_R, Math.max(0, variant.getSv().getCiStartRight()));
            }
            if (variant.getSv().getCiEndLeft() != null) {
                add(put, VariantPhoenixSchema.VariantColumn.CI_END_L, Math.max(0, variant.getSv().getCiEndLeft()));
            }
            if (variant.getSv().getCiEndRight() != null) {
                add(put, VariantPhoenixSchema.VariantColumn.CI_END_R, Math.max(0, variant.getSv().getCiEndRight()));
            }
        }
        add(put, studyColumn, 0);
        if (releaseColumn != null) {
            add(put, releaseColumn, true);
        }
        int size = put.size();
        put = convert(variant, put, null);

        if (size == put.size()) {
            return null;
        } else {
            return put;
        }
    }

    public Put convert(Variant variant, Put put) {
        return convert(variant, put, null);
    }


    public Put convert(Variant variant, Put put, Set<Integer> sampleIds) {
        return convert(variant, put, sampleIds, VariantOverlappingStatus.NONE);
    }

    public Put convert(Variant variant, Put put, Set<Integer> sampleIds, VariantOverlappingStatus overlappingStatus) {
        StudyEntry studyEntry = variant.getStudies().get(0);
        Integer gtIdx = studyEntry.getSampleDataKeyPosition(VariantMerger.GT_KEY);
        int[] formatReMap = buildFormatRemap(studyEntry);
        int sampleIdx = 0;
        List<String> samplesName = studyEntry.getOrderedSamplesName();
        // Always write file attributes if there is no samples (i.e. aggregated files)
        boolean writeAllFileAttributes = samplesName.isEmpty();
        boolean writeFileAttributes = writeAllFileAttributes;
        Set<Integer> filesToWrite = new HashSet<>(1);
        for (String sampleName : samplesName) {
            Integer sampleId = sampleIdsMap.get(sampleName);
            if (sampleIds == null || sampleIds.contains(sampleId)) {
                byte[] column = getSampleColumn(sampleId);
                List<String> sampleData = studyEntry.getSamples().get(sampleIdx).getData();
                // Write sample data if the is no genotype information, or if the genotype is equals to the default genotype
                if (gtIdx == null || !defaultGenotypes.contains(sampleData.get(gtIdx))) {
                    if (formatReMap != null) {
                        sampleData = remapSampleData(studyEntry, formatReMap, sampleData);
                    } else {
                        // Trim all leading null values
                        sampleData = trimLeadingNullValues(sampleData, 1);
                    }
                    addVarcharArray(put, column, sampleData);
                    // Write file attributes if at least one sample is written.
                    writeFileAttributes = true;
                    filesToWrite.addAll(getFilesFromSample(sampleId));
                }
            }
            sampleIdx++;
        }
        if (writeFileAttributes) {
            for (FileEntry fileEntry : studyEntry.getFiles()) {
                int fileId = Integer.parseInt(fileEntry.getFileId());
                if (writeAllFileAttributes || filesToWrite.contains(fileId)) {
                    byte[] fileColumnKey = getFileColumnKey(fileId);
                    List<String> fileColumn = remapFileData(variant, studyEntry, fileEntry, overlappingStatus);
                    addVarcharArray(put, fileColumnKey, fileColumn);
                }
            }
        }

        return put;
    }

    protected abstract byte[] getFileColumnKey(int fileId);

    protected abstract byte[] getSampleColumn(Integer sampleId);

    protected abstract Collection<? extends Integer> getFilesFromSample(Integer sampleId);

    private List<String> remapFileData(Variant variant, StudyEntry studyEntry, FileEntry fileEntry,
                                       VariantOverlappingStatus overlappingStatus) {
        int capacity = fileAttributes.size() + HBaseToStudyEntryConverter.FILE_INFO_START_IDX;
        List<String> fileColumn = Arrays.asList(new String[capacity]);

        Map<String, String> data = fileEntry.getData();
        if (fileEntry.getCall() != null) {
            fileColumn.set(HBaseToStudyEntryConverter.FILE_CALL_IDX,
                    fileEntry.getCall().getVariantId() + ":" + fileEntry.getCall().getAlleleIndex());
        }
        if (addSecondaryAlternates && studyEntry.getSecondaryAlternates() != null && !studyEntry.getSecondaryAlternates().isEmpty()) {
            fileColumn.set(HBaseToStudyEntryConverter.FILE_SEC_ALTS_IDX,
                    getSecondaryAlternates(variant, studyEntry.getSecondaryAlternates()));
        }
        fileColumn.set(HBaseToStudyEntryConverter.FILE_VARIANT_OVERLAPPING_STATUS_IDX, overlappingStatus.toString());
        fileColumn.set(HBaseToStudyEntryConverter.FILE_QUAL_IDX, data.get(StudyEntry.QUAL));
        fileColumn.set(HBaseToStudyEntryConverter.FILE_FILTER_IDX, data.get(StudyEntry.FILTER));
        int attributeIdx = HBaseToStudyEntryConverter.FILE_INFO_START_IDX;
        for (String fileAttribute : fileAttributes) {
            fileColumn.set(attributeIdx, data.get(fileAttribute));
            attributeIdx++;
        }

        // Trim all leading null values
        fileColumn = trimLeadingNullValues(fileColumn, HBaseToStudyEntryConverter.FILE_INFO_START_IDX);

        return fileColumn;
    }

    public static String getSecondaryAlternates(Variant variant, List<AlternateCoordinate> secondaryAlternates) {
        StringBuilder sb = new StringBuilder();
        Iterator<AlternateCoordinate> iterator = secondaryAlternates.iterator();
        while (iterator.hasNext()) {
            AlternateCoordinate alt = iterator.next();
            sb.append(alt.getChromosome() == null ? variant.getChromosome() : alt.getChromosome());
            sb.append(ALTERNATE_COORDINATE_SEPARATOR);
            sb.append(alt.getStart() == null ? variant.getStart() : alt.getStart());
            sb.append(ALTERNATE_COORDINATE_SEPARATOR);
            sb.append(alt.getEnd() == null ? variant.getEnd() : alt.getEnd());
            sb.append(ALTERNATE_COORDINATE_SEPARATOR);
            sb.append(alt.getReference() == null ? variant.getReference() : alt.getReference());
            sb.append(ALTERNATE_COORDINATE_SEPARATOR);
            sb.append(alt.getAlternate() == null ? variant.getAlternate() : alt.getAlternate());
            sb.append(ALTERNATE_COORDINATE_SEPARATOR);
            sb.append(alt.getType() == null ? variant.getType() : alt.getType());

            if (iterator.hasNext()) {
                sb.append(',');
            }
        }
        return sb.toString();
    }

    protected static void buildSecondaryAlternate(String chromosome, int start, int end, String reference, String alternate,
                                                           VariantType type, StringBuilder sb) {
        sb.append(chromosome).append(ALTERNATE_COORDINATE_SEPARATOR)
                .append(start).append(ALTERNATE_COORDINATE_SEPARATOR)
                .append(end).append(ALTERNATE_COORDINATE_SEPARATOR)
                .append(reference).append(ALTERNATE_COORDINATE_SEPARATOR)
                .append(alternate).append(ALTERNATE_COORDINATE_SEPARATOR)
                .append(type);
    }

    private int[] buildFormatRemap(StudyEntry studyEntry) {
        int[] formatReMap;
        if (fixedFormat.equals(studyEntry.getSampleDataKeys()) && !excludeGenotypes) {
            formatReMap = null;
        } else {
            formatReMap = new int[fixedFormat.size()];
            for (int i = 0; i < fixedFormat.size(); i++) {
                String format = fixedFormat.get(i);
                Integer idx = studyEntry.getSampleDataKeyPosition(format);
                if (idx == null) {
                    if (format.equals(VariantMerger.GENOTYPE_FILTER_KEY)) {
                        idx = FILTER_FIELD;
                    } else {
                        idx = UNKNOWN_FIELD;
                    }
                }
                formatReMap[i] = idx;
            }
            if (excludeGenotypes) {
                int gtIdx = fixedFormat.indexOf(VariantMerger.GT_KEY);
                if (gtIdx >= 0) {
                    formatReMap[gtIdx] = UNKNOWN_FIELD;
                }
            }
        }
        return formatReMap;
    }

    private List<String> remapSampleData(StudyEntry studyEntry, int[] formatReMap, List<String> sampleData) {
        List<String> remappedSampleData = new ArrayList<>(formatReMap.length);

        for (int i : formatReMap) {
            switch (i) {
                case UNKNOWN_FIELD:
                    remappedSampleData.add(null);
                    break;
                case FILTER_FIELD:
                    remappedSampleData.add(studyEntry.getFiles().get(0).getData().get(StudyEntry.FILTER));
                    break;
                default:
                    if (sampleData.size() > i) {
                        remappedSampleData.add(sampleData.get(i));
                    } else {
                        remappedSampleData.add(null);
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
        remappedSampleData = trimLeadingNullValues(remappedSampleData, 1);

        return remappedSampleData;
    }

    static List<String> trimLeadingNullValues(List<String> values, int minSize) {
        int i = values.size() - 1;
        while (i >= minSize && StringUtils.isEmpty(values.get(i))) {
            i--;
        }
        if (i != values.size() - 1) {
            values = values.subList(0, i + 1);
        }
        return values;
    }

}
