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

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.types.PVarcharArray;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.AlternateCoordinate;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.tools.variant.merge.VariantMerger;
import org.opencb.opencga.storage.core.metadata.VariantStudyMetadata;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.converters.AbstractPhoenixConverter;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableStudyRow;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.hadoop.variant.index.VariantTableStudyRow.HOM_REF_BYTES;

/**
 * Created on 26/05/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HBaseToSamplesDataConverter extends AbstractPhoenixConverter {

    public HBaseToSamplesDataConverter(GenomeHelper genomeHelper) {
        super(genomeHelper.getColumnFamily());
    }

    public Map<Integer, Map<Integer, List<String>>> convert(ResultSet resultSet) {
        Map<Integer, Map<Integer, List<String>>> samplesData = new HashMap<>();
        try {
            ResultSetMetaData metaData = resultSet.getMetaData();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String columnName = metaData.getColumnName(i);
                if (columnName.endsWith(VariantPhoenixHelper.SAMPLE_DATA_SUFIX)) {
                    Array value = resultSet.getArray(i);
                    if (value != null) {
                        String[] split = columnName.split(VariantPhoenixHelper.COLUMN_KEY_SEPARATOR_STR);
                        Integer studyId = getStudyId(split);
                        Integer sampleId = getSampleId(split);

                        Map<Integer, List<String>> studyMap = samplesData.computeIfAbsent(studyId, k -> new HashMap<>());
                        studyMap.put(sampleId, toList(value));
                    }
                } else if (columnName.endsWith(VariantTableStudyRow.HOM_REF)) {
                    Integer studyId = VariantTableStudyRow.extractStudyId(columnName, true);
                    samplesData.computeIfAbsent(studyId, k -> new HashMap<>());
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return samplesData;
    }

    public Map<Integer, Map<Integer, List<String>>> convert(Result result) {
        NavigableMap<byte[], byte[]> map = result.getFamilyMap(columnFamily);
        Map<Integer, Map<Integer, List<String>>> samplesData = new HashMap<>();

        for (Map.Entry<byte[], byte[]> entry : map.entrySet()) {
            byte[] columnBytes = entry.getKey();
            byte[] value = entry.getValue();
            if (value != null && endsWith(columnBytes, VariantPhoenixHelper.SAMPLE_DATA_SUFIX_BYTES)) {
                String columnName = Bytes.toString(columnBytes);
                String[] split = columnName.split(VariantPhoenixHelper.COLUMN_KEY_SEPARATOR_STR);
                Integer studyId = getStudyId(split);
                Integer sampleId = getSampleId(split);

                Array array = (Array) PVarcharArray.INSTANCE.toObject(value);
                List<String> sampleData = toList(array);

                Map<Integer, List<String>> studyMap = samplesData.computeIfAbsent(studyId, k -> new HashMap<>());
                studyMap.put(sampleId, sampleData);
            } else if (endsWith(columnBytes, HOM_REF_BYTES)) {
                String columnName = Bytes.toString(columnBytes);
                Integer studyId = VariantTableStudyRow.extractStudyId(columnName, true);
                samplesData.computeIfAbsent(studyId, k -> new HashMap<>());
            }
        }

        return samplesData;
    }

    public List<AlternateCoordinate> extractSecondaryAlternates(Variant variant, VariantStudyMetadata variantMetadata,
                                                                List<String> expectedFormat, Map<Integer, List<String>> samplesDataMap) {
        Map<String, List<Integer>> alternateSampleIdMap = new HashMap<>();

        for (Map.Entry<Integer, List<String>> entry : samplesDataMap.entrySet()) {
            Integer sampleId = entry.getKey();
            List<String> sampleData = entry.getValue();
            if (sampleData.size() > expectedFormat.size()) {
                String alternate = sampleData.get(sampleData.size() - 1);
                sampleData = sampleData.subList(0, sampleData.size() - 1);
                entry.setValue(sampleData);

                List<Integer> sampleIds = alternateSampleIdMap.computeIfAbsent(alternate, key -> new ArrayList<>());
                sampleIds.add(sampleId);
            }
        }

        if (alternateSampleIdMap.isEmpty()) {
            return Collections.emptyList();
        } else if (alternateSampleIdMap.size() == 1) {
            return getAlternateCoordinates(alternateSampleIdMap.keySet().iterator().next());
        } else {
            // There are multiple secondary alternates.
            // We need to rearrange the genotypes to match with the secondary alternates order.
            VariantMerger variantMerger = new VariantMerger(false);
            variantMerger.setExpectedFormats(expectedFormat);
            variantMerger.setStudyId("0");
            for (VariantStudyMetadata.VariantMetadataRecord record : variantMetadata.getFormat().values()) {
                variantMerger.configure(record.getId(), record.getNumberType(), record.getType());
            }
            for (VariantStudyMetadata.VariantMetadataRecord record : variantMetadata.getInfo().values()) {
                variantMerger.configure(record.getId(), record.getNumberType(), record.getType());
            }

            // Create one variant for each alternate with the samples data
            List<Variant> variants = new ArrayList<>(alternateSampleIdMap.size());
            for (Map.Entry<String, List<Integer>> entry : alternateSampleIdMap.entrySet()) {
                String secondaryAlternates = entry.getKey();

                Variant sampleVariant = new Variant(
                        variant.getChromosome(),
                        variant.getStart(),
                        variant.getReference(),
                        variant.getAlternate());
                StudyEntry se = new StudyEntry("0");
                se.setSecondaryAlternates(getAlternateCoordinates(secondaryAlternates));
                se.setFormat(expectedFormat);
                for (Integer sampleId : entry.getValue()) {
                    se.addSampleData(sampleId.toString(), samplesDataMap.get(sampleId));
                }
                sampleVariant.addStudyEntry(se);
                variants.add(sampleVariant);
            }

            // Merge the variants in the first variant
            Variant newVariant = variantMerger.merge(variants.get(0), variants.subList(1, variants.size()));

            // Update samplesData information
            StudyEntry se = newVariant.getStudies().get(0);
            for (Map.Entry<String, Integer> entry : se.getSamplesPosition().entrySet()) {
                List<String> data = se.getSamplesData().get(entry.getValue());
                Integer sampleId = Integer.valueOf(entry.getKey());
                samplesDataMap.put(sampleId, data);
            }
            return se.getSecondaryAlternates();
        }
    }

    public List<AlternateCoordinate> getAlternateCoordinates(String s) {
        return Arrays.stream(s.split(","))
                .map(this::getAlternateCoordinate)
                .collect(Collectors.toList());
    }

    public AlternateCoordinate getAlternateCoordinate(String s) {
        String[] split = s.split(":");
        return new AlternateCoordinate(
                split[0],
                Integer.parseInt(split[1]),
                Integer.parseInt(split[2]),
                split[3],
                split[4],
                VariantType.valueOf(split[5])
        );
    }

    @SuppressWarnings("unchecked")
    public <T> List<T> toList(Array value) {
        try {
            return Arrays.asList((T[]) value.getArray());
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    private Integer getStudyId(String[] split) {
        return Integer.valueOf(split[0]);
    }

    private Integer getSampleId(String[] split) {
        return Integer.valueOf(split[1]);
    }

}
