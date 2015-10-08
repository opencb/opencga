/*
 * Copyright 2015 OpenCB
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

package org.opencb.opencga.storage.mongodb.variant;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.opencb.biodata.models.variant.VariantSourceEntry;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.datastore.core.ComplexTypeConverter;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.StudyConfigurationManager;

import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class DBObjectToVariantSourceEntryConverter implements ComplexTypeConverter<VariantSourceEntry, DBObject> {

    public final static String FILEID_FIELD = "fid";
    public final static String STUDYID_FIELD = "sid";
    public final static String ALTERNATES_FIELD = "alts";
    public final static String ATTRIBUTES_FIELD = "attrs";
    //    public final static String FORMAT_FIELD = "fm";
    public final static String GENOTYPES_FIELD = "gt";
    public static final String FILES_FIELD = "files";
    public static final String ORI_FIELD = "_ori";

    private boolean includeSrc;
    private Set<Integer> returnedFiles;

//    private Integer fileId;
    private DBObjectToSamplesConverter samplesConverter;
    private StudyConfigurationManager studyConfigurationManager = null;
    private Map<Integer, String> studyIds = new HashMap<>();

    /**
     * Create a converter between VariantSourceEntry and DBObject entities when
     * there is no need to provide a list of samples or statistics.
     *
     * @param includeSrc If true, will include and gzip the "src" attribute in the DBObject
     */
    public DBObjectToVariantSourceEntryConverter(boolean includeSrc) {
        this.includeSrc = includeSrc;
        this.samplesConverter = null;
        this.returnedFiles = null;
    }


    /**
     * Create a converter from VariantSourceEntry to DBObject entities. A
     * samples converter and a statistics converter may be provided in case those
     * should be processed during the conversion.
     *
     * @param includeSrc       If true, will include and gzip the "src" attribute in the DBObject
     * @param samplesConverter The object used to convert the samples. If null, won't convert
     */
    public DBObjectToVariantSourceEntryConverter(boolean includeSrc,
                                                 DBObjectToSamplesConverter samplesConverter) {
        this(includeSrc);
        this.samplesConverter = samplesConverter;
    }

    /**
     * Create a converter from VariantSourceEntry to DBObject entities. A
     * samples converter and a statistics converter may be provided in case those
     * should be processed during the conversion.
     *
     * @param includeSrc       If true, will include and gzip the "src" attribute in the DBObject
     * @param returnedFiles    If present, reads the information of this files from FILES_FIELD
     * @param samplesConverter The object used to convert the samples. If null, won't convert
     */
    public DBObjectToVariantSourceEntryConverter(boolean includeSrc, List<Integer> returnedFiles,
                                                 DBObjectToSamplesConverter samplesConverter) {
        this(includeSrc);
        this.returnedFiles = (returnedFiles != null)? new HashSet<>(returnedFiles) : null;
        this.samplesConverter = samplesConverter;
    }


    public DBObjectToVariantSourceEntryConverter(boolean includeSrc, Integer returnedFile,
                                                 DBObjectToSamplesConverter samplesConverter) {
        this(includeSrc, Collections.singletonList(returnedFile), samplesConverter);
    }

    public void setStudyConfigurationManager(StudyConfigurationManager studyConfigurationManager) {
        this.studyConfigurationManager = studyConfigurationManager;
    }

    public void addStudyName(int studyId, String studyName) {
        this.studyIds.put(studyId, studyName);
    }

    @Override
    public VariantSourceEntry convertToDataModelType(DBObject object) {
        int studyId = ((Number) object.get(STUDYID_FIELD)).intValue();
//        String fileId = this.fileId == null? null : String.valueOf(this.fileId);
//        String fileId = returnedFiles != null && returnedFiles.size() == 1? returnedFiles.iterator().next().toString() : null;
        VariantSourceEntry study = new VariantSourceEntry(getStudyName(studyId));

//        String fileId = (String) object.get(FILEID_FIELD);
        DBObject fileObject = null;
        if (object.containsField(FILES_FIELD)) {
            List<FileEntry> files = new ArrayList<>(((List) object.get(FILES_FIELD)).size());
            for (DBObject dbObject : (List<DBObject>) object.get(FILES_FIELD)) {
                Integer fid = ((Integer) dbObject.get(FILEID_FIELD));

                if (returnedFiles != null && !returnedFiles.contains(fid)) {
                    continue;
                }
                HashMap<String, String> attributes = new HashMap<>();
                FileEntry fileEntry = new FileEntry(fid.toString(), null, attributes);
                files.add(fileEntry);

                fileObject = dbObject;
                // Attributes
                if (fileObject.containsField(ATTRIBUTES_FIELD)) {
                    Map<String, Object> attrs = ((DBObject) fileObject.get(ATTRIBUTES_FIELD)).toMap();
                    for (Map.Entry<String, Object> entry : attrs.entrySet()) {
                        // Unzip the "src" field, if available
                        if (entry.getKey().equals("src")) {
                            if (includeSrc) {
                                byte[] o = (byte[]) entry.getValue();
                                try {
                                    attributes.put(entry.getKey(), org.opencb.commons.utils.StringUtils.gunzip(o));
                                } catch (IOException ex) {
                                    Logger.getLogger(DBObjectToVariantSourceEntryConverter.class.getName()).log(Level.SEVERE, null, ex);
                                }
                            }
                        } else {
                            attributes.put(entry.getKey().replace(DBObjectToStudyConfigurationConverter.TO_REPLACE_DOTS, "."), entry.getValue().toString());
                        }
                    }
                }
                if (fileObject.containsField(ORI_FIELD)) {
                    DBObject _ori = (DBObject) fileObject.get(ORI_FIELD);
                    String ori = _ori.get("s") + ":" + _ori.get("i");
                    fileEntry.setCall(ori);
                } else {
                    fileEntry.setCall("");
                }
            }
            study.setFiles(files);
        }

        // Alternate alleles
        if (fileObject != null && fileObject.containsField(ALTERNATES_FIELD)) {
            List list = (List) fileObject.get(ALTERNATES_FIELD);
//            String[] alternatives = new String[list.size()];
//            int i = 0;
//            for (Object o : list) {
//                alternatives[i] = o.toString();
//                i++;
//            }
            study.setSecondaryAlternates(list);
        }


//        if (fileObject != null && fileObject.containsField(FORMAT_FIELD)) {
//            study.setFormat((String) fileObject.get(FORMAT_FIELD));
//        } else {

//        }

        // Samples
        if (samplesConverter != null && object.containsField(GENOTYPES_FIELD)) {
            samplesConverter.convertToDataModelType(object, study, studyId);
        } else {
            study.setFormat(Collections.<String>emptyList());
        }

        return study;
    }

    public String getStudyName(int studyId) {
        if (!studyIds.containsKey(studyId)) {
            if (studyConfigurationManager == null) {
                studyIds.put(studyId, Integer.toString(studyId));
            } else {
                QueryResult<StudyConfiguration> queryResult = studyConfigurationManager.getStudyConfiguration(studyId, null);
                if (queryResult.getResult().isEmpty()) {
                    studyIds.put(studyId, Integer.toString(studyId));
                } else {
                    studyIds.put(studyId, queryResult.first().getStudyName());
                }
            }
        }
        return studyIds.get(studyId);
    }

    @Override
    public DBObject convertToStorageType(VariantSourceEntry object) {
        int fileId = Integer.parseInt(object.getFileId());
        BasicDBObject fileObject = new BasicDBObject(FILEID_FIELD, fileId);

        // Alternate alleles
        if (object.getSecondaryAlternates().size() > 0) {   // assuming secondaryAlternates doesn't contain the primary alternate
            fileObject.append(ALTERNATES_FIELD, object.getSecondaryAlternates());
        }

        // Attributes
        if (object.getAttributes().size() > 0) {
            BasicDBObject attrs = null;
            for (Map.Entry<String, String> entry : object.getAttributes().entrySet()) {
                String stringValue = entry.getValue();
                String key = entry.getKey().replace(".", DBObjectToStudyConfigurationConverter.TO_REPLACE_DOTS);
                Object value = stringValue;
                if (key.equals("src")) {
                    if (includeSrc) {
                        try {
                            value = org.opencb.commons.utils.StringUtils.gzip(stringValue);
                        } catch (IOException ex) {
                            Logger.getLogger(DBObjectToVariantSourceEntryConverter.class.getName()).log(Level.SEVERE, null, ex);
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
                            } catch (NumberFormatException NotADouble) {
                                // leave it as a String
                            }
                        }
                    }
                }

                if (attrs == null) {
                    attrs = new BasicDBObject(key, value);
                } else {
                    attrs.append(key, value);
                }
            }

            if (attrs != null) {
                fileObject.put(ATTRIBUTES_FIELD, attrs);
            }
        }
        String call = object.getFile(Integer.toString(fileId)).getCall();
        if (call != null && !call.isEmpty()) {
                int indexOf = call.lastIndexOf(":");
                fileObject.append(ORI_FIELD,
                        new BasicDBObject("s", call.substring(0, indexOf))
                                .append("i", Integer.parseInt(call.substring(indexOf + 1))));
        }

        int studyId = Integer.parseInt(object.getStudyId());
        BasicDBObject mongoFile = new BasicDBObject(STUDYID_FIELD, studyId);
        mongoFile.append(FILES_FIELD, Collections.singletonList(fileObject));

//        if (samples != null && !samples.isEmpty()) {
        if (samplesConverter != null) {
            mongoFile.putAll(samplesConverter.convertToStorageType(object.getSamplesDataAsMap(), studyId, fileId));
        }


        return mongoFile;
    }

    public DBObjectToSamplesConverter getSamplesConverter() {
        return samplesConverter;
    }

    public void setIncludeSrc(boolean includeSrc) {
        this.includeSrc = includeSrc;
    }
}
