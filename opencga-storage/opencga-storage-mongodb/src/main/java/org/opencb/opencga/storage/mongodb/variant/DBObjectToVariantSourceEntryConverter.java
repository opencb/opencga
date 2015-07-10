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
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.opencb.biodata.models.variant.VariantSourceEntry;
import org.opencb.datastore.core.ComplexTypeConverter;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.StudyConfigurationManager;

/**
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class DBObjectToVariantSourceEntryConverter implements ComplexTypeConverter<VariantSourceEntry, DBObject> {

    public final static String FILEID_FIELD = "fid";
    public final static String STUDYID_FIELD = "sid";
    public final static String ALTERNATES_FIELD = "alts";
    public final static String ATTRIBUTES_FIELD = "attrs";
    public final static String FORMAT_FIELD = "fm";
    public final static String GENOTYPES_FIELD = "gt";
    public static final String FILES_FIELD = "files";

    private boolean includeSrc;

    private Integer fileId;
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
        this.fileId = null;
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
     * @param fileId           If present, reads the information of this file from FILES_FIELD
     * @param samplesConverter The object used to convert the samples. If null, won't convert
     */
    public DBObjectToVariantSourceEntryConverter(boolean includeSrc, Integer fileId,
                                                 DBObjectToSamplesConverter samplesConverter) {
        this(includeSrc);
        this.fileId = fileId;
        this.samplesConverter = samplesConverter;
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
        String fileId = this.fileId == null? null : String.valueOf(this.fileId);
        VariantSourceEntry file = new VariantSourceEntry(fileId, getStudyName(studyId));

//        String fileId = (String) object.get(FILEID_FIELD);
        DBObject fileObject = null;
        if (this.fileId != null && object.containsField(FILES_FIELD)) {
            for (DBObject dbObject : (List<DBObject>) object.get(FILES_FIELD)) {
                if (this.fileId.equals(dbObject.get(FILEID_FIELD))) {
                    fileObject = dbObject;
                    break;
                }
            }
        }
        // Alternate alleles
        if (fileObject != null && fileObject.containsField(ALTERNATES_FIELD)) {
            List list = (List) fileObject.get(ALTERNATES_FIELD);
            String[] alternatives = new String[list.size()];
            int i = 0;
            for (Object o : list) {
                alternatives[i] = o.toString();
                i++;
            }
            file.setSecondaryAlternates(alternatives);
        }

        // Attributes
        if (fileObject != null && fileObject.containsField(ATTRIBUTES_FIELD)) {
            Map<String, Object> attrs = ((DBObject) fileObject.get(ATTRIBUTES_FIELD)).toMap();
            for (Map.Entry<String, Object> entry : attrs.entrySet()) {
                // Unzip the "src" field, if available
                if (entry.getKey().equals("src")) {
                    if (includeSrc) {
                        byte[] o = (byte[]) entry.getValue();
                        try {
                            file.addAttribute("src", org.opencb.commons.utils.StringUtils.gunzip(o));
                        } catch (IOException ex) {
                            Logger.getLogger(DBObjectToVariantSourceEntryConverter.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    }
                } else {
                    file.addAttribute(entry.getKey(), entry.getValue().toString());
                }
            }

        }
        if (fileObject != null && fileObject.containsField(FORMAT_FIELD)) {
            file.setFormat((String) fileObject.get(FORMAT_FIELD));
        } else {
            file.setFormat("GT");
        }

        // Samples
        if (samplesConverter != null && object.containsField(GENOTYPES_FIELD)) {
            Map<String, Map<String, String>> samplesData = samplesConverter.convertToDataModelType(object, studyId);

            // Add the samples to the Java object, combining the data structures
            // with the samples' names and the genotypes
            for (Map.Entry<String, Map<String, String>> sampleData : samplesData.entrySet()) {
                file.addSampleData(sampleData.getKey(), sampleData.getValue());
            }
        }

        return file;
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
        if (object.getSecondaryAlternates().length > 0) {   // assuming secondaryAlternates doesn't contain the primary alternate
            fileObject.append(ALTERNATES_FIELD, object.getSecondaryAlternates());
        }

        // Attributes
        if (object.getAttributes().size() > 0) {
            BasicDBObject attrs = null;
            for (Map.Entry<String, String> entry : object.getAttributes().entrySet()) {
                String stringValue = entry.getValue();
                Object value = stringValue;
                if (entry.getKey().equals("src")) {
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
                        value = Double.parseDouble(stringValue);
                    } catch (NumberFormatException ignore) {
                    }
                }

                if (attrs == null) {
                    attrs = new BasicDBObject(entry.getKey(), value);
                } else {
                    attrs.append(entry.getKey(), value);
                }
            }

            if (attrs != null) {
                fileObject.put(ATTRIBUTES_FIELD, attrs);
            }
        }

        int studyId = Integer.parseInt(object.getStudyId());
        BasicDBObject mongoFile = new BasicDBObject(STUDYID_FIELD, studyId);
        mongoFile.append(FILES_FIELD, Collections.singletonList(fileObject));

//        if (samples != null && !samples.isEmpty()) {
        if (samplesConverter != null) {
            fileObject.append(FORMAT_FIELD, object.getFormat()); // Useless field if genotypeCodes are not stored
            mongoFile.put(GENOTYPES_FIELD, samplesConverter.convertToStorageType(object.getSamplesData(), studyId));
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
