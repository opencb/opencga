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
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.opencb.biodata.models.variant.VariantSourceEntry;
import org.opencb.datastore.core.ComplexTypeConverter;

/**
 * 
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

    /**
     * Create a converter between VariantSourceEntry and DBObject entities when 
     * there is no need to provide a list of samples or statistics.
     *
     * @param includeSrc       If true, will include and gzip the "src" attribute in the DBObject
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
     *  @param includeSrc       If true, will include and gzip the "src" attribute in the DBObject
     * @param samplesConverter The object used to convert the samples. If null, won't convert
     *
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
     *  @param includeSrc       If true, will include and gzip the "src" attribute in the DBObject
     *  @param fileId           If present, reads the information of this file from FILES_FIELD
     *  @param samplesConverter The object used to convert the samples. If null, won't convert
     *
     */
    public DBObjectToVariantSourceEntryConverter(boolean includeSrc, Integer fileId,
                                                 DBObjectToSamplesConverter samplesConverter) {
        this(includeSrc);
        this.fileId = fileId;
        this.samplesConverter = samplesConverter;
    }

    @Override
    public VariantSourceEntry convertToDataModelType(DBObject object) {
        int studyId = ((Number) object.get(STUDYID_FIELD)).intValue();
        VariantSourceEntry file = new VariantSourceEntry(String.valueOf(fileId), Integer.toString(studyId));

//        String fileId = (String) object.get(FILEID_FIELD);
        DBObject fileObject = null;
        if (fileId != null && object.containsField(FILES_FIELD)) {
            for (DBObject dbObject : (List<DBObject>) object.get(FILES_FIELD)) {
                if (fileId.equals(dbObject.get(FILEID_FIELD))) {
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
            file.setAttributes(((DBObject) fileObject.get(ATTRIBUTES_FIELD)).toMap());
            // Unzip the "src" field, if available
            if (((DBObject) fileObject.get(ATTRIBUTES_FIELD)).containsField("src")) {
                byte[] o = (byte[]) ((DBObject) fileObject.get(ATTRIBUTES_FIELD)).get("src");
                try {
                    file.addAttribute("src", org.opencb.commons.utils.StringUtils.gunzip(o));
                } catch (IOException ex) {
                    Logger.getLogger(DBObjectToVariantSourceEntryConverter.class.getName()).log(Level.SEVERE, null, ex);
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
                Object value = entry.getValue();
                if (entry.getKey().equals("src")) {
                    if (includeSrc) {
                        try {
                            value = org.opencb.commons.utils.StringUtils.gzip(entry.getValue());
                        } catch (IOException ex) {
                            Logger.getLogger(DBObjectToVariantSourceEntryConverter.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    } else {
                        continue;
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
