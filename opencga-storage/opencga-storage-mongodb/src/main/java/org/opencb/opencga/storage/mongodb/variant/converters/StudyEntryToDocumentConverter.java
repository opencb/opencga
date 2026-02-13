package org.opencb.opencga.storage.mongodb.variant.converters;

import org.bson.Document;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.AlternateCoordinate;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.avro.OriginalCall;
import org.opencb.commons.datastore.mongodb.GenericDocumentComplexConverter;

import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToStudyEntryConverter.*;

public class StudyEntryToDocumentConverter {

    private final SampleToDocumentConverter samplesConverter;
    private final boolean includeSrc;

    public StudyEntryToDocumentConverter(SampleToDocumentConverter samplesConverter, boolean includeSrc) {
        this.samplesConverter = samplesConverter;
        this.includeSrc = includeSrc;
    }

    public Document convertToStorageType(Variant variant, StudyEntry studyEntry) {
        return convertToStorageType(variant, studyEntry, studyEntry.getFiles(), new LinkedHashSet<>(studyEntry.getOrderedSamplesName()));
    }

    public Document convertToStorageType(Variant variant, StudyEntry studyEntry, FileEntry file, LinkedHashSet<String> sampleNames) {
        return convertToStorageType(variant, studyEntry, Collections.singletonList(file), sampleNames);
    }

    public Document convertToStorageType(Variant variant, StudyEntry studyEntry, List<FileEntry> files, LinkedHashSet<String> sampleNames) {

        int studyId = Integer.parseInt(studyEntry.getStudyId());
        Document studyObject = new Document(STUDYID_FIELD, studyId);

        // Alternate alleles
        List<Document> alternates = new LinkedList<>();
        if (studyEntry.getSecondaryAlternates().size() > 0) {   // assuming secondaryAlternates doesn't contain the primary alternate
//            fileObject.append(ALTERNATES_FIELD, studyEntry.getSecondaryAlternatesAlleles());
            for (AlternateCoordinate coordinate : studyEntry.getSecondaryAlternates()) {
                Document alt = new Document();
                alt.put(ALTERNATES_CHR, coordinate.getChromosome() != null ? coordinate.getChromosome() : variant.getChromosome());
                alt.put(ALTERNATES_REF, coordinate.getReference() != null ? coordinate.getReference() : variant.getReference());
                alt.put(ALTERNATES_ALT, coordinate.getAlternate());
                alt.put(ALTERNATES_START, coordinate.getStart() != null ? coordinate.getStart() : variant.getStart());
                alt.put(ALTERNATES_END, coordinate.getEnd() != null ? coordinate.getEnd() : variant.getEnd());
                alt.put(ALTERNATES_TYPE, coordinate.getType() != null ? coordinate.getType().toString() : variant.getType().toString());
                alternates.add(alt);
            }
        }

        final List<Document> fileDocuments;
        if (!files.isEmpty()) {
            fileDocuments = new ArrayList<>(files.size());

            for (FileEntry file : files) {
                Document fileObject = convertFileDocument(studyEntry, file);
                fileDocuments.add(fileObject);

                if (samplesConverter != null) {
                    Document otherFields = new Document();
                    fileObject.append(SAMPLE_DATA_FIELD, otherFields);
                    studyObject.putAll(samplesConverter.convertToStorageType(studyEntry, otherFields, sampleNames));
                }
            }

        } else {
            fileDocuments = Collections.emptyList();
        }

        studyObject.append(FILES_FIELD, fileDocuments);
        if (alternates != null && !alternates.isEmpty()) {
            studyObject.append(ALTERNATES_FIELD, alternates);
        }



        return studyObject;
    }

    protected Document convertFileDocument(StudyEntry studyEntry, FileEntry file) {
        int fileId = Integer.parseInt(file.getFileId());
        Document fileObject = new Document(FILEID_FIELD, fileId);
        // Attributes
        if (file.getData().size() > 0) {
            Document attrs = null;
            for (Map.Entry<String, String> entry : file.getData().entrySet()) {
                String stringValue = entry.getValue();
                String key = entry.getKey().replace(".", GenericDocumentComplexConverter.TO_REPLACE_DOTS);
                Object value = stringValue;
                if (key.equals("src")) {
                    if (includeSrc) {
                        try {
                            value = org.opencb.commons.utils.StringUtils.gzip(stringValue);
                        } catch (IOException ex) {
                            Logger.getLogger(DocumentToStudyEntryConverter.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    } else {
                        continue;
                    }
                } else {
                    try {
                        value = Integer.parseInt(stringValue);
                    } catch (NumberFormatException notAnInt) {
                        try {
                            value = Long.parseLong(stringValue);
                        } catch (NumberFormatException notALong) {
                            try {
                                value = Double.parseDouble(stringValue);
                            } catch (NumberFormatException notADouble) {
                                // leave it as a String
                            }
                        }
                    }
                }

                if (attrs == null) {
                    attrs = new Document(key, value);
                } else {
                    attrs.append(key, value);
                }
            }

            if (attrs != null) {
                fileObject.put(ATTRIBUTES_FIELD, attrs);
            }
        }
        OriginalCall call = studyEntry.getFile(Integer.toString(fileId)).getCall();
        if (call != null) {
            fileObject.append(ORI_FIELD,
                    new Document("s", call.getVariantId())
                            .append("i", call.getAlleleIndex()));
        }
        return fileObject;
    }

}
