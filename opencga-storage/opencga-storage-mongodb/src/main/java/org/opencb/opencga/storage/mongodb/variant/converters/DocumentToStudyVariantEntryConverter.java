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

package org.opencb.opencga.storage.mongodb.variant.converters;

import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.AlternateCoordinate;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.commons.datastore.mongodb.GenericDocumentComplexConverter;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;

import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class DocumentToStudyVariantEntryConverter {

    public static final String STUDYID_FIELD = "sid";
    //    public static final String FORMAT_FIELD = "fm";
    public static final String GENOTYPES_FIELD = "gt";

    public static final String FILES_FIELD = "files";
    public static final String FILEID_FIELD = "fid";
    public static final String SAMPLE_DATA_FIELD = "sampleData";
    public static final String ATTRIBUTES_FIELD = "attrs";
    public static final String ORI_FIELD = "_ori";

    public static final String ALTERNATES_FIELD = "alts";
    public static final String ALTERNATES_CHR = "chr";
    public static final String ALTERNATES_ALT = "alt";
    public static final String ALTERNATES_REF = "ref";
    public static final String ALTERNATES_START = "start";
    public static final String ALTERNATES_END = "end";
    public static final String ALTERNATES_TYPE = "type";

    private boolean includeSrc;
    private Map<Integer, List<Integer>> returnedFiles;
    private Map<Integer, String> fileIds = new HashMap<>();

    //    private Integer fileId;
    private DocumentToSamplesConverter samplesConverter;
    private VariantStorageMetadataManager metadataManager = null;
    private Map<Integer, String> studyIds = new HashMap<>();

    /**
     * Create a converter between VariantSourceEntry and Document entities when
     * there is no need to provide a list of samples or statistics.
     *
     * @param includeSrc If true, will include and gzip the "src" attribute in the Document
     */
    public DocumentToStudyVariantEntryConverter(boolean includeSrc) {
        this.includeSrc = includeSrc;
        this.samplesConverter = null;
        this.returnedFiles = null;
    }


    /**
     * Create a converter from VariantSourceEntry to Document entities. A
     * samples converter and a statistics converter may be provided in case those
     * should be processed during the conversion.
     *
     * @param includeSrc       If true, will include and gzip the "src" attribute in the Document
     * @param samplesConverter The object used to convert the samples. If null, won't convert
     */
    public DocumentToStudyVariantEntryConverter(boolean includeSrc, DocumentToSamplesConverter samplesConverter) {
        this(includeSrc);
        this.samplesConverter = samplesConverter;
    }

    /**
     * Create a converter from VariantSourceEntry to Document entities. A
     * samples converter and a statistics converter may be provided in case those
     * should be processed during the conversion.
     *
     * @param includeSrc       If true, will include and gzip the "src" attribute in the Document
     * @param returnedFiles    If present, reads the information of this files from FILES_FIELD
     * @param samplesConverter The object used to convert the samples. If null, won't convert
     */
    public DocumentToStudyVariantEntryConverter(boolean includeSrc, Map<Integer, List<Integer>> returnedFiles,
                                                DocumentToSamplesConverter samplesConverter) {

        this(includeSrc);
        this.returnedFiles = returnedFiles;
        this.samplesConverter = samplesConverter;
    }


    public DocumentToStudyVariantEntryConverter(boolean includeSrc, int studyId, int fileId,
                                                DocumentToSamplesConverter samplesConverter) {
        this(includeSrc, Collections.singletonMap(studyId, Collections.singletonList(fileId)), samplesConverter);
    }

    public void setMetadataManager(VariantStorageMetadataManager metadataManager) {
        this.metadataManager = metadataManager;
    }

    public void addStudyName(int studyId, String studyName) {
        this.studyIds.put(studyId, studyName);
    }

    public StudyEntry convertToDataModelType(Document document) {
        int studyId = ((Number) document.get(STUDYID_FIELD)).intValue();
//        String fileId = this.fileId == null? null : String.valueOf(this.fileId);
//        String fileId = returnedFiles != null && returnedFiles.size() == 1? returnedFiles.iterator().next().toString() : null;
        StudyEntry study = new StudyEntry(getStudyName(studyId));

//        String fileId = (String) object.get(FILEID_FIELD);
        Document fileObject;
        if (document.containsKey(FILES_FIELD)) {
            List<FileEntry> files = new ArrayList<>(((List) document.get(FILES_FIELD)).size());
            for (Document fileDocument : (List<Document>) document.get(FILES_FIELD)) {
                Integer fid = ((Number) fileDocument.get(FILEID_FIELD)).intValue();
                if (fid < 0) {
                    fid = -fid;
                }
                if (returnedFiles != null && !returnedFiles.getOrDefault(studyId, Collections.emptyList()).contains(fid)) {
                    continue;
                }
                HashMap<String, String> attributes = new HashMap<>();
                FileEntry fileEntry = new FileEntry(getFileName(studyId, fid), null, attributes);
                files.add(fileEntry);

                fileObject = fileDocument;
                // Attributes
                if (fileObject.containsKey(ATTRIBUTES_FIELD)) {
                    Map<String, Object> attrs = ((Document) fileObject.get(ATTRIBUTES_FIELD));
                    for (Map.Entry<String, Object> entry : attrs.entrySet()) {
                        // Unzip the "src" field, if available
                        if (entry.getKey().equals("src")) {
                            if (includeSrc) {
                                byte[] o = (byte[]) entry.getValue();
                                try {
                                    attributes.put(entry.getKey(), org.opencb.commons.utils.StringUtils.gunzip(o));
                                } catch (IOException ex) {
                                    Logger.getLogger(DocumentToStudyVariantEntryConverter.class.getName()).log(Level.SEVERE, null, ex);
                                }
                            }
                        } else {
                            attributes.put(StringUtils.replace(entry.getKey(), GenericDocumentComplexConverter.TO_REPLACE_DOTS, "."),
                                    entry.getValue().toString());
                        }
                    }
                }
                if (fileObject.containsKey(ORI_FIELD)) {
                    Document ori = (Document) fileObject.get(ORI_FIELD);
                    fileEntry.setCall(ori.get("s") + ":" + ori.get("i"));
                }
            }
            study.setFiles(files);
        }

        // Alternate alleles
//        if (fileObject != null && fileObject.containsKey(ALTERNATES_COORDINATES_FIELD)) {
            List<Document> list = (List<Document>) document.get(ALTERNATES_FIELD);
            if (list != null && !list.isEmpty()) {
                for (Document alternateDocument : list) {
                    AlternateCoordinate alternateCoordinate = convertToAlternateCoordinate(alternateDocument);
                    if (study.getSecondaryAlternates() == null) {
                        study.setSecondaryAlternates(new ArrayList<>(list.size()));
                    }
                    study.getSecondaryAlternates().add(alternateCoordinate);
                }
            }
//            String[] alternatives = new String[list.size()];
//            int i = 0;
//            for (Object o : list) {
//                alternatives[i] = o.toString();
//                i++;
//            }
//            study.setSecondaryAlternates(list);
//        }


//        if (fileObject != null && fileObject.containsKey(FORMAT_FIELD)) {
//            study.setFormat((String) fileObject.get(FORMAT_FIELD));
//        } else {

//        }

        // Samples
        if (samplesConverter != null) {
            samplesConverter.convertToDataModelType(document, study, studyId);
        }

        return study;
    }

    public static AlternateCoordinate convertToAlternateCoordinate(Document alternateDocument) {
        VariantType variantType = null;
        String type = (String) alternateDocument.get(ALTERNATES_TYPE);

        if (type != null && !type.isEmpty()) {
            variantType = VariantType.valueOf(type);
        }

        return new AlternateCoordinate(
                (String) alternateDocument.get(ALTERNATES_CHR),
                (Integer) alternateDocument.get(ALTERNATES_START),
                (Integer) alternateDocument.get(ALTERNATES_END),
                (String) alternateDocument.get(ALTERNATES_REF),
                (String) alternateDocument.get(ALTERNATES_ALT),
                variantType);
    }

    public String getStudyName(int studyId) {
        return studyIds.computeIfAbsent(studyId, s -> {
            if (metadataManager == null) {
                return String.valueOf(studyId);
            } else {
                String studyName = metadataManager.getStudyName(studyId);
                if (studyName == null) {
                    return String.valueOf(studyId);
                } else {
                    return studyName;
                }
            }
        });
    }

    public String getFileName(int studyId, int fileId) {
        return fileIds.computeIfAbsent(fileId, f -> {
            if (metadataManager == null) {
                return Integer.toString(fileId);
            } else {
                String fileName = metadataManager.getFileName(studyId, fileId);
                if (fileName == null) {
                    return String.valueOf(fileId);
                } else {
                    return fileName;
                }
            }
        });
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
                    studyObject.putAll(samplesConverter.convertToStorageType(studyEntry, studyId, otherFields, sampleNames));
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
                            Logger.getLogger(DocumentToStudyVariantEntryConverter.class.getName()).log(Level.SEVERE, null, ex);
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
        String call = studyEntry.getFile(Integer.toString(fileId)).getCall();
        if (call != null && !call.isEmpty()) {
            int indexOf = call.lastIndexOf(":");
            fileObject.append(ORI_FIELD,
                    new Document("s", call.substring(0, indexOf))
                            .append("i", Integer.parseInt(call.substring(indexOf + 1))));
        }
        return fileObject;
    }

    public DocumentToSamplesConverter getSamplesConverter() {
        return samplesConverter;
    }

    public void setIncludeSrc(boolean includeSrc) {
        this.includeSrc = includeSrc;
    }
}
