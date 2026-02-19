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
import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToStudyEntryConverter.SAMPLE_DATA_FIELD;

public class SampleToDocumentConverter {

    private final StudyMetadata studyMetadata;
    private final Set<String> defaultGenotype;
    private final Map<String, Integer> sampleIdsMap;

    public SampleToDocumentConverter(StudyMetadata studyMetadata, Map<String, Integer> sampleIdsMap) {
        this.studyMetadata = studyMetadata;
        this.sampleIdsMap = sampleIdsMap;
        List<String> defGenotype = studyMetadata.getAttributes().getAsStringList(DEFAULT_GENOTYPE.key());
        this.defaultGenotype = new HashSet<>(defGenotype);
    }

    /**
     * @deprecated Use {@link #SampleToDocumentConverter(StudyMetadata, Map)}. The multiFileSampleIds
     *             parameter is no longer needed since mgt is written for all samples in root-level files[].
     * @param studyMetadata study metadata
     * @param sampleIdsMap  sample name → id mapping
     * @param multiFileSampleIds ignored
     */
    @Deprecated
    public SampleToDocumentConverter(StudyMetadata studyMetadata, Map<String, Integer> sampleIdsMap,
                                     Set<Integer> multiFileSampleIds) {
        this(studyMetadata, sampleIdsMap);
    }

    private int getSampleId(String sampleName) {
        return sampleIdsMap.get(sampleName);
    }


    public Document convertToStorageType(StudyEntry studyEntry, LinkedHashSet<String> samplesInFile) {
        Document fileDocument = new Document();
        convertToStorageType(studyEntry, samplesInFile, fileDocument);
        return fileDocument;
    }

    /**
     * Convert the sample data of the given StudyEntry to a Document, and append it to the given fileDocument.
     * Add fields to the fileDocument:
     * - "samplesData": the sample data for the samples in this file, with extra format fields stored as compressed protobuf OtherFields.
     * - "mgt": the per-file genotype map, with sample ids grouped by genotype. This is only added if genotypes are not
     *          excluded and there are genotypes different from the default genotype.
     *
     * @param studyEntry     the StudyEntry containing the sample data
     * @param samplesInFile  the set of sample names that belong to the file being processed. Only these samples will be
     *                       included in the "samplesData" field.
     * @param fileDocument   the Document to which the sample data will be appended. This is modified in-place and also
     *                       returned for convenience.
     */
    public void convertToStorageType(StudyEntry studyEntry, LinkedHashSet<String> samplesInFile, Document fileDocument) {
        Document dataFields = new Document();
        Document mgt = new Document();
        Document mongoSamples = fileDocument
                .append(SAMPLE_DATA_FIELD, dataFields);

        Map<String, List<Integer>> genotypeCodes = new HashMap<>();

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
        }

        // Build the per-file mgt map (FILE_GENOTYPE_FIELD) for ALL samples.
        // The study-level "gt" field is no longer written; all GT data lives in the root-level files[].mgt.
        // This document is extracted by StudyEntryToDocumentConverter and stored on the file document directly.
        if (!excludeGenotypes && !genotypeCodes.isEmpty()) {
            for (Map.Entry<String, List<Integer>> entry : genotypeCodes.entrySet()) {
                if (!defaultGenotype.contains(entry.getKey())) {
                    mgt.append(DocumentToSamplesConverter.genotypeToStorageType(entry.getKey()), entry.getValue());
                }
            }
            mongoSamples.append(DocumentToStudyEntryConverter.FILE_GENOTYPE_FIELD, mgt);
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
                dataFields.append(extraField.toLowerCase(), byteArray);
            } // else { Don't set this field }
        }
    }

}
