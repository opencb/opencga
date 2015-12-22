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

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import java.util.*;

import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.commons.utils.CryptoUtils;
import org.opencb.datastore.core.ComplexTypeConverter;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class DBObjectToVariantConverter implements ComplexTypeConverter<Variant, DBObject> {

    public final static String CHROMOSOME_FIELD = "chromosome";
    public final static String START_FIELD = "start";
    public final static String END_FIELD = "end";
    public final static String LENGTH_FIELD = "length";
    public final static String REFERENCE_FIELD = "reference";
    public final static String ALTERNATE_FIELD = "alternate";
    public final static String IDS_FIELD = "ids";
    public final static String TYPE_FIELD = "type";

    public final static String HGVS_FIELD = "hgvs";
    public final static String HGVS_NAME_FIELD = "name";
    public final static String HGVS_TYPE_FIELD = "type";

    public final static String STUDIES_FIELD = "studies";
    public final static String ANNOTATION_FIELD = "annotation";
    public final static String STATS_FIELD = "stats";

    public static final String _AT_FIELD = "_at";
    public static final String CHUNK_IDS_FIELD = "chunkIds";

//    public final static String ID_FIELD = "id";
//    public final static String FILES_FIELD = "files";
//    public final static String EFFECTS_FIELD = "effs";
//    public final static String SOTERM_FIELD = "so";
//    public final static String GENE_FIELD = "gene";

    public final static Map<String, String> fieldsMap;

    static {
        fieldsMap = new HashMap<>();
        fieldsMap.put("chromosome", CHROMOSOME_FIELD);
        fieldsMap.put("start", START_FIELD);
        fieldsMap.put("end", END_FIELD);
        fieldsMap.put("length", LENGTH_FIELD);
        fieldsMap.put("reference", REFERENCE_FIELD);
        fieldsMap.put("alternate", ALTERNATE_FIELD);
        fieldsMap.put("ids", IDS_FIELD);
        fieldsMap.put("type", TYPE_FIELD);
        fieldsMap.put("hgvs", HGVS_FIELD);
//        fieldsMap.put("hgvs.type", HGVS_FIELD + "." + HGVS_TYPE_FIELD);
//        fieldsMap.put("hgvs.name", HGVS_FIELD + "." + HGVS_NAME_FIELD);
        fieldsMap.put("sourceEntries", STUDIES_FIELD);
        fieldsMap.put("annotation", ANNOTATION_FIELD);
        fieldsMap.put("sourceEntries.cohortStats", STATS_FIELD);
    }

    private DBObjectToStudyVariantEntryConverter variantSourceEntryConverter;
    private DBObjectToVariantAnnotationConverter variantAnnotationConverter;
    private DBObjectToVariantStatsConverter statsConverter;

    /**
     * Create a converter between Variant and DBObject entities when there is 
     * no need to convert the files the variant was read from.
     */
    public DBObjectToVariantConverter() {
        this(null, null);
    }

    /**
     * Create a converter between Variant and DBObject entities. A converter for 
     * the files the variant was read from can be provided in case those 
     * should be processed during the conversion.
     *
     * @param variantSourceEntryConverter The object used to convert the files
     * @param statsConverter
     */
    public DBObjectToVariantConverter(DBObjectToStudyVariantEntryConverter variantSourceEntryConverter, DBObjectToVariantStatsConverter statsConverter) {
        this.variantSourceEntryConverter = variantSourceEntryConverter;
        this.variantAnnotationConverter = new DBObjectToVariantAnnotationConverter();
        this.statsConverter = statsConverter;
    }


    @Override
    public Variant convertToDataModelType(DBObject object) {
        String chromosome = (String) object.get(CHROMOSOME_FIELD);
        int start = (int) object.get(START_FIELD);
        int end = (int) object.get(END_FIELD);
        String reference = (String) object.get(REFERENCE_FIELD);
        String alternate = (String) object.get(ALTERNATE_FIELD);
        Variant variant = new Variant(chromosome, start, end, reference, alternate);
        if (object.containsField(IDS_FIELD)) {
            Object ids = object.get(IDS_FIELD);
            variant.setIds(new LinkedList<>(((Collection<String>) ids)));
        }

        // Transform HGVS: List of map entries -> Map of lists
        List mongoHgvs = (List) object.get(HGVS_FIELD);
        if (mongoHgvs != null) {
            for (Object o : mongoHgvs) {
                DBObject dbo = (DBObject) o;
                variant.addHgvs((String) dbo.get(HGVS_TYPE_FIELD), (String) dbo.get(HGVS_NAME_FIELD));
            }
        }

        // Files
        if (variantSourceEntryConverter != null) {
            List mongoFiles = (List) object.get(STUDIES_FIELD);
            if (mongoFiles != null) {
                for (Object o : mongoFiles) {
                    DBObject dbo = (DBObject) o;
                    variant.addStudyEntry(variantSourceEntryConverter.convertToDataModelType(dbo));
                }
            }
        }

        // Annotations
        DBObject mongoAnnotation;
        Object o = object.get(ANNOTATION_FIELD);
        if (o instanceof List) {
            if(!((List) o).isEmpty()) {
                mongoAnnotation = (DBObject) ((List) o).get(0);
            } else {
                mongoAnnotation = null;
            }
        } else {
            mongoAnnotation = (DBObject) object.get(ANNOTATION_FIELD);
        }
        if (mongoAnnotation != null) {
            VariantAnnotation annotation = variantAnnotationConverter.convertToDataModelType(mongoAnnotation);
            annotation.setChromosome(variant.getChromosome());
            annotation.setAlternate(variant.getAlternate());
            annotation.setReference(variant.getReference());
            annotation.setStart(variant.getStart());
            variant.setAnnotation(annotation);
        }

        // Statistics
        if (statsConverter != null && object.containsField(STATS_FIELD)) {
            DBObject stats = (DBObject) object.get(STATS_FIELD);
            statsConverter.convertCohortsToDataModelType(stats, variant);
        }
        return variant;
    }

    @Override
    public DBObject convertToStorageType(Variant variant) {
        // Attributes easily calculated
        BasicDBObject mongoVariant = new BasicDBObject("_id", buildStorageId(variant))
//                .append(IDS_FIELD, object.getIds())    //Do not include IDs.
                .append(CHROMOSOME_FIELD, variant.getChromosome())
                .append(START_FIELD, variant.getStart())
                .append(END_FIELD, variant.getEnd())
                .append(LENGTH_FIELD, variant.getLength())
                .append(REFERENCE_FIELD, variant.getReference())
                .append(ALTERNATE_FIELD, variant.getAlternate())
                .append(TYPE_FIELD, variant.getType().name());

        // Internal fields used for query optimization (dictionary named "_at")
        BasicDBObject _at = new BasicDBObject();
        mongoVariant.append(_AT_FIELD, _at);

        // Two different chunk sizes are calculated for different resolution levels: 1k and 10k
        BasicDBList chunkIds = new BasicDBList();
        String chunkSmall = variant.getChromosome() + "_" + variant.getStart() / VariantMongoDBWriter.CHUNK_SIZE_SMALL + "_" + VariantMongoDBWriter.CHUNK_SIZE_SMALL / 1000 + "k";
        String chunkBig = variant.getChromosome() + "_" + variant.getStart() / VariantMongoDBWriter.CHUNK_SIZE_BIG + "_" + VariantMongoDBWriter.CHUNK_SIZE_BIG / 1000 + "k";
        chunkIds.add(chunkSmall);
        chunkIds.add(chunkBig);
        _at.append(CHUNK_IDS_FIELD, chunkIds);

        // Transform HGVS: Map of lists -> List of map entries
        BasicDBList hgvs = new BasicDBList();
        for (Map.Entry<String, List<String>> entry : variant.getHgvs().entrySet()) {
            for (String value : entry.getValue()) {
                hgvs.add(new BasicDBObject(HGVS_TYPE_FIELD, entry.getKey()).append(HGVS_NAME_FIELD, value));
            }
        }
        mongoVariant.append(HGVS_FIELD, hgvs);

        // Files
        if (variantSourceEntryConverter != null) {
            BasicDBList mongoFiles = new BasicDBList();
            for (StudyEntry archiveFile : variant.getStudies()) {
                mongoFiles.add(variantSourceEntryConverter.convertToStorageType(archiveFile));
            }
            mongoVariant.append(STUDIES_FIELD, mongoFiles);
        }

//        // Annotations
        mongoVariant.append(ANNOTATION_FIELD, Collections.emptyList());
        if (variantAnnotationConverter != null) {
            if (variant.getAnnotation() != null) {
                DBObject annotation = variantAnnotationConverter.convertToStorageType(variant.getAnnotation());
                mongoVariant.append(ANNOTATION_FIELD, annotation);
            }
        }

        // Statistics
        if (statsConverter != null) {
            List mongoStats = statsConverter.convertCohortsToStorageType(variant.getStudiesMap());
            mongoVariant.put(STATS_FIELD, mongoStats);
        }

        return mongoVariant;
    }

    public String buildStorageId(Variant v) {
        return buildStorageId(v.getChromosome(), v.getStart(), v.getReference(), v.getAlternate());
    }

    public String buildStorageId(String chromosome, int start, String reference, String alternate) {
        StringBuilder builder = new StringBuilder(chromosome);
        builder.append("_");
        builder.append(start);
        builder.append("_");
        if (reference.equals("-")) {

        } else if (reference.length() < Variant.SV_THRESHOLD) {
            builder.append(reference);
        } else {
            builder.append(new String(CryptoUtils.encryptSha1(reference)));
        }

        builder.append("_");

        if (alternate.equals("-")) {

        } else if (alternate.length() < Variant.SV_THRESHOLD) {
            builder.append(alternate);
        } else {
            builder.append(new String(CryptoUtils.encryptSha1(alternate)));
        }

        return builder.toString();
    }


    public static String toShortFieldName(String longFieldName) {
        if(longFieldName.contains(".")) {
            String[] split = longFieldName.split("\\.");
            return fieldsMap.get(split[0]);
        }
        return fieldsMap.get(longFieldName);
    }

}
