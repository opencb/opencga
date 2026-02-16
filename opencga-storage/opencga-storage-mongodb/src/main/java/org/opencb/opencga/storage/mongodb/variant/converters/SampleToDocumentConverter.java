package org.opencb.opencga.storage.mongodb.variant.converters;

import com.google.common.collect.HashBiMap;
import org.bson.Document;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.avro.SampleEntry;
import org.opencb.commons.utils.CompressionUtils;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageOptions;
import org.opencb.opencga.storage.mongodb.variant.protobuf.VariantMongoDBProto;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;

import static org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageOptions.DEFAULT_GENOTYPE;
import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToSamplesConverter.FLOAT_COMPLEX_TYPE_CONVERTER;
import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToSamplesConverter.INTEGER_COMPLEX_TYPE_CONVERTER;

public class SampleToDocumentConverter {

    private final StudyMetadata studyMetadata;
    private final Set<String> defaultGenotype;
    private final Map<String, Integer> sampleIdsMap;
    /** Sample IDs that are loaded with {@code SplitData.MULTI}. May be empty. */
    private final Set<Integer> multiFileSampleIds;

    public SampleToDocumentConverter(StudyMetadata studyMetadata, Map<String, Integer> sampleIdsMap) {
        this(studyMetadata, sampleIdsMap, Collections.emptySet());
    }

    public SampleToDocumentConverter(StudyMetadata studyMetadata, Map<String, Integer> sampleIdsMap,
                                     Set<Integer> multiFileSampleIds) {
        this.studyMetadata = studyMetadata;
        this.sampleIdsMap = sampleIdsMap;
        this.multiFileSampleIds = multiFileSampleIds;
        List<String> defGenotype = studyMetadata.getAttributes().getAsStringList(DEFAULT_GENOTYPE.key());
        this.defaultGenotype = new HashSet<>(defGenotype);
    }

    private int getSampleId(String sampleName) {
        return sampleIdsMap.get(sampleName);
    }

    public Document convertToStorageType(StudyEntry studyEntry, Document otherFields, LinkedHashSet<String> samplesInFile) {
        Map<String, List<Integer>> genotypeCodes = new HashMap<>();
        Map<String, List<Integer>> multiFileGenotypeCodes = new HashMap<>();

        boolean excludeGenotypes = studyMetadata.getAttributes().getBoolean(VariantStorageOptions.EXCLUDE_GENOTYPES.key(),
                VariantStorageOptions.EXCLUDE_GENOTYPES.defaultValue());
        boolean compressExtraParams = studyMetadata.getAttributes()
                .getBoolean(MongoDBVariantStorageOptions.EXTRA_GENOTYPE_FIELDS_COMPRESS.key(),
                        MongoDBVariantStorageOptions.EXTRA_GENOTYPE_FIELDS_COMPRESS.defaultValue());

        // Classify samples by genotype
        int sampleIdx = 0;
        Integer gtIdx = studyEntry.getSampleDataKeyPosition("GT");
        List<String> studyEntryOrderedSamplesName = studyEntry.getOrderedSamplesName();
        for (SampleEntry sampleEntry : studyEntry.getSamples()) {
            String sampleName = studyEntryOrderedSamplesName.get(sampleIdx);
            sampleIdx++;
            if (!samplesInFile.contains(sampleName)) {
                continue;
            }
            String genotype;
            if (gtIdx == null) {
                genotype = ".";
            } else {
                genotype = sampleEntry.getData().get(gtIdx);
            }
            if (genotype == null) {
                genotype = ".";
            }
            int id = getSampleId(sampleName);
            genotypeCodes.computeIfAbsent(genotype, k -> new ArrayList<>()).add(id);
            // For multi-file samples, also track the per-file genotype (stored in "mgt" on the file document).
            if (multiFileSampleIds.contains(id)) {
                multiFileGenotypeCodes.computeIfAbsent(genotype, k -> new ArrayList<>()).add(id);
            }
        }

        // In Mongo, samples are stored in a map, classified by their genotype.
        // The most common genotype will be marked as "default" and the specific
        // positions where it is shown will not be stored. Example from 1000G:
        // "def" : 0|0,
        // "0|1" : [ 41, 311, 342, 358, 881, 898, 903 ],
        // "1|0" : [ 262, 290, 300, 331, 343, 369, 374, 391, 879, 918, 930 ]
        Document mongoSamples = new Document();
        Document mongoGenotypes = new Document();
        for (Map.Entry<String, List<Integer>> entry : genotypeCodes.entrySet()) {
            String genotypeStr = DocumentToSamplesConverter.genotypeToStorageType(entry.getKey());
            if (!defaultGenotype.contains(entry.getKey())) {
                mongoGenotypes.append(genotypeStr, entry.getValue());
            }
        }

        if (!excludeGenotypes) {
            mongoSamples.append(DocumentToStudyEntryConverter.GENOTYPES_FIELD, mongoGenotypes);
        }

        // Build the per-file mgt map for multi-file samples (same shape as the study-level gt map).
        // This is extracted by StudyEntryToDocumentConverter and stored on the file document directly.
        if (!multiFileGenotypeCodes.isEmpty()) {
            Document mgt = new Document();
            for (Map.Entry<String, List<Integer>> entry : multiFileGenotypeCodes.entrySet()) {
                mgt.append(DocumentToSamplesConverter.genotypeToStorageType(entry.getKey()), entry.getValue());
            }
            mongoSamples.append(DocumentToStudyEntryConverter.MULTI_FILE_GENOTYPE_FIELD, mgt);
        }


        //Position for samples in this file
        HashBiMap<String, Integer> samplesPosition = HashBiMap.create();
        int position = 0;
        for (String sample : samplesInFile) {
            samplesPosition.put(sample, position++);
        }

        List<String> extraFields = studyMetadata.getAttributes()
                .getAsStringList(VariantStorageOptions.EXTRA_FORMAT_FIELDS.key());
        List<String> extraFieldsType = studyMetadata.getAttributes()
                .getAsStringList(VariantStorageOptions.EXTRA_FORMAT_FIELDS_TYPE.key());

        for (int i = 0; i < extraFields.size(); i++) {
            String extraField = extraFields.get(i);
            String extraFieldType = i < extraFieldsType.size() ? extraFieldsType.get(i) : "String";


            VariantMongoDBProto.OtherFields.Builder builder = VariantMongoDBProto.OtherFields.newBuilder();
//            List<Object> values = new ArrayList<>(samplesPosition.size());
//            for (int size = samplesPosition.size(); size > 0; size--) {
//                values.add(UNKNOWN_FIELD);
//            }
            sampleIdx = 0;
            if (studyEntry.getSampleDataKeySet().contains(extraField)) {
                Integer formatIdx = studyEntry.getSampleDataKeyPosition(extraField);
                for (SampleEntry sample : studyEntry.getSamples()) {
                    String sampleName = studyEntryOrderedSamplesName.get(sampleIdx);
                    sampleIdx++;
                    if (!samplesInFile.contains(sampleName)) {
                        continue;
                    }
//                    Integer index = samplesPosition.get(sampleName);
                    String stringValue = sample.getData().get(formatIdx);
//                    Object value;
//                    if (NumberUtils.isNumber(stringValue)) {
//                        try {
//                            value = Integer.parseInt(stringValue);
//                        } catch (NumberFormatException e) {
//                            try {
//                                value = Double.parseDouble(stringValue);
//                            } catch (NumberFormatException e2) {
//                                value = stringValue;
//                            }
//                        }
//                    } else {
//                        value = stringValue;
//                    }
                    switch (extraFieldType) {
                        case "Integer": {
                            builder.addIntValues(INTEGER_COMPLEX_TYPE_CONVERTER.convertToStorageType(stringValue));
                            break;
                        }
                        case "Float": {
                            builder.addFloatValues(FLOAT_COMPLEX_TYPE_CONVERTER.convertToStorageType(stringValue));
                            break;
                        }
                        case "String":
                        default:
                            builder.addStringValues(stringValue);
                            break;
                    }
                }

                byte[] byteArray = builder.build().toByteArray();
                if (compressExtraParams) {
                    if (byteArray.length > 50) {
                        try {
                            byteArray = CompressionUtils.compress(byteArray);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    }
                }
                otherFields.append(extraField.toLowerCase(), byteArray);
            } // else { Don't set this field }
        }

        return mongoSamples;
    }

}
