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

package org.opencb.opencga.storage.mongodb.variant.converters;

import org.bson.Document;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.commons.datastore.core.ComplexTypeConverter;
import org.opencb.opencga.storage.mongodb.variant.VariantMongoDBWriter;

import java.util.*;

/**
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class DocumentToVariantConverter implements ComplexTypeConverter<Variant, Document> {

    public static final String CHROMOSOME_FIELD = "chromosome";
    public static final String START_FIELD = "start";
    public static final String END_FIELD = "end";
    public static final String LENGTH_FIELD = "length";
    public static final String REFERENCE_FIELD = "reference";
    public static final String ALTERNATE_FIELD = "alternate";
    public static final String IDS_FIELD = "ids";
    public static final String TYPE_FIELD = "type";

    public static final String HGVS_FIELD = "hgvs";
    public static final String HGVS_NAME_FIELD = "name";
    public static final String HGVS_TYPE_FIELD = "type";

    public static final String STUDIES_FIELD = "studies";
    public static final String ANNOTATION_FIELD = "annotation";
    public static final String STATS_FIELD = "stats";

    public static final String AT_FIELD = "_at";
    public static final String CHUNK_IDS_FIELD = "chunkIds";

//    public static final String ID_FIELD = "id";
//    public static final String FILES_FIELD = "files";
//    public static final String EFFECTS_FIELD = "effs";
//    public static final String SOTERM_FIELD = "so";
//    public static final String GENE_FIELD = "gene";

    public static final Map<String, String> FIELDS_MAP;

    static {
        FIELDS_MAP = new HashMap<>();
        FIELDS_MAP.put("chromosome", CHROMOSOME_FIELD);
        FIELDS_MAP.put("start", START_FIELD);
        FIELDS_MAP.put("end", END_FIELD);
        FIELDS_MAP.put("length", LENGTH_FIELD);
        FIELDS_MAP.put("reference", REFERENCE_FIELD);
        FIELDS_MAP.put("alternate", ALTERNATE_FIELD);
        FIELDS_MAP.put("ids", IDS_FIELD);
        FIELDS_MAP.put("type", TYPE_FIELD);
        FIELDS_MAP.put("hgvs", HGVS_FIELD);
//        FIELDS_MAP.put("hgvs.type", HGVS_FIELD + "." + HGVS_TYPE_FIELD);
//        FIELDS_MAP.put("hgvs.name", HGVS_FIELD + "." + HGVS_NAME_FIELD);
        FIELDS_MAP.put("sourceEntries", STUDIES_FIELD);
        FIELDS_MAP.put("annotation", ANNOTATION_FIELD);
        FIELDS_MAP.put("sourceEntries.cohortStats", STATS_FIELD);
    }

    private DocumentToStudyVariantEntryConverter variantSourceEntryConverter;
    private DocumentToVariantAnnotationConverter variantAnnotationConverter;
    private DocumentToVariantStatsConverter statsConverter;
    private final VariantStringIdComplexTypeConverter idConverter = new VariantStringIdComplexTypeConverter();

    // Add default variant ID if it is missing. Use CHR:POS:REF:ALT
    private boolean addDefaultId;

    /**
     * Create a converter between {@link Variant} and {@link Document} entities when there is
     * no need to convert the studies the variant was read from.
     */
    public DocumentToVariantConverter() {
        this(null, null);
    }

    /**
     * Create a converter between {@link Variant} and {@link Document} entities. A converter for
     * the studies the variant was read from can be provided in case those
     * should be processed during the conversion.
     *
     * @param variantSourceEntryConverter The object used to convert the files
     * @param statsConverter Stats converter
     */
    public DocumentToVariantConverter(DocumentToStudyVariantEntryConverter variantSourceEntryConverter,
                                      DocumentToVariantStatsConverter statsConverter) {
        this.variantSourceEntryConverter = variantSourceEntryConverter;
        this.variantAnnotationConverter = new DocumentToVariantAnnotationConverter();
        this.statsConverter = statsConverter;
        addDefaultId = true;
    }


    @Override
    public Variant convertToDataModelType(Document object) {
        String chromosome = (String) object.get(CHROMOSOME_FIELD);
        int start = (int) object.get(START_FIELD);
        int end = (int) object.get(END_FIELD);
        String reference = (String) object.get(REFERENCE_FIELD);
        String alternate = (String) object.get(ALTERNATE_FIELD);
        Variant variant = new Variant(chromosome, start, end, reference, alternate);
        if (object.containsKey(IDS_FIELD)) {
            LinkedList<String> ids = new LinkedList<>(object.get(IDS_FIELD, Collection.class));
            if (ids.isEmpty()) {
                if (addDefaultId) {
                    variant.setId(variant.toString());
                }
                variant.setNames(Collections.emptyList());
            } else {
                variant.setId(ids.get(0));
                variant.setNames(ids.subList(1, ids.size()));
            }
        }
        if (object.containsKey(TYPE_FIELD)) {
            variant.setType(VariantType.valueOf(object.get(TYPE_FIELD).toString()));
        }

        // Transform HGVS: List of map entries -> Map of lists
        List mongoHgvs = (List) object.get(HGVS_FIELD);
        if (mongoHgvs != null) {
            for (Object o : mongoHgvs) {
                Document dbo = (Document) o;
                variant.addHgvs((String) dbo.get(HGVS_TYPE_FIELD), (String) dbo.get(HGVS_NAME_FIELD));
            }
        }

        // Files
        if (variantSourceEntryConverter != null) {
            List mongoFiles = (List) object.get(STUDIES_FIELD);
            if (mongoFiles != null) {
                for (Object o : mongoFiles) {
                    Document dbo = (Document) o;
                    variant.addStudyEntry(variantSourceEntryConverter.convertToDataModelType(dbo));
                }
            }
        }

        // Annotations
        Document mongoAnnotation;
        Object o = object.get(ANNOTATION_FIELD);
        if (o instanceof List) {
            if (!((List) o).isEmpty()) {
                mongoAnnotation = (Document) ((List) o).get(0);
            } else {
                mongoAnnotation = null;
            }
        } else {
            mongoAnnotation = (Document) object.get(ANNOTATION_FIELD);
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
        if (statsConverter != null && object.containsKey(STATS_FIELD)) {
            List<Document> stats = object.get(STATS_FIELD, List.class);
            statsConverter.convertCohortsToDataModelType(stats, variant);
        }
        return variant;
    }

    @Override
    public Document convertToStorageType(Variant variant) {
        // Attributes easily calculated
        Document mongoVariant = new Document("_id", buildStorageId(variant))
//                .append(IDS_FIELD, object.getIds())    //Do not include IDs.
                .append(CHROMOSOME_FIELD, variant.getChromosome())
                .append(START_FIELD, variant.getStart())
                .append(END_FIELD, variant.getEnd())
                .append(LENGTH_FIELD, variant.getLength())
                .append(REFERENCE_FIELD, variant.getReference())
                .append(ALTERNATE_FIELD, variant.getAlternate())
                .append(TYPE_FIELD, variant.getType().name());

        // Internal fields used for query optimization (dictionary named "_at")
        Document at = new Document();
        mongoVariant.append(AT_FIELD, at);

        // Two different chunk sizes are calculated for different resolution levels: 1k and 10k
        List<String> chunkIds = new LinkedList<>();
        String chunkSmall = variant.getChromosome() + "_" + variant.getStart() / VariantMongoDBWriter.CHUNK_SIZE_SMALL + "_"
                + VariantMongoDBWriter.CHUNK_SIZE_SMALL / 1000 + "k";
        String chunkBig = variant.getChromosome() + "_" + variant.getStart() / VariantMongoDBWriter.CHUNK_SIZE_BIG + "_"
                + VariantMongoDBWriter.CHUNK_SIZE_BIG / 1000 + "k";
        chunkIds.add(chunkSmall);
        chunkIds.add(chunkBig);
        at.append(CHUNK_IDS_FIELD, chunkIds);

        // Transform HGVS: Map of lists -> List of map entries
        List<Document> hgvs = new LinkedList<>();
        for (Map.Entry<String, List<String>> entry : variant.getHgvs().entrySet()) {
            for (String value : entry.getValue()) {
                hgvs.add(new Document(HGVS_TYPE_FIELD, entry.getKey()).append(HGVS_NAME_FIELD, value));
            }
        }
        mongoVariant.append(HGVS_FIELD, hgvs);

        // Files
        if (variantSourceEntryConverter != null) {
            List<Document> mongoFiles = new LinkedList<>();
            for (StudyEntry archiveFile : variant.getStudies()) {
                mongoFiles.add(variantSourceEntryConverter.convertToStorageType(archiveFile));
            }
            mongoVariant.append(STUDIES_FIELD, mongoFiles);
        }

//        // Annotations
        mongoVariant.append(ANNOTATION_FIELD, Collections.emptyList());
        if (variantAnnotationConverter != null) {
            if (variant.getAnnotation() != null) {
                Document annotation = variantAnnotationConverter.convertToStorageType(variant.getAnnotation());
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
        return idConverter.buildId(v);
//        return buildStorageId(v.getChromosome(), v.getStart(), v.getReference(), v.getAlternate());
    }

    public String buildStorageId(String chromosome, int start, String reference, String alternate) {
        return idConverter.buildId(chromosome, start, reference, alternate);
//
//        StringBuilder builder = new StringBuilder(chromosome);
//        builder.append("_");
//        builder.append(start);
//        builder.append("_");
//        if (reference.equals("-")) {
//            System.out.println("Empty block");
//        } else if (reference.length() < Variant.SV_THRESHOLD) {
//            builder.append(reference);
//        } else {
//            builder.append(new String(CryptoUtils.encryptSha1(reference)));
//        }
//
//        builder.append("_");
//
//        if (alternate.equals("-")) {
//            System.out.println("Empty block");
//        } else if (alternate.length() < Variant.SV_THRESHOLD) {
//            builder.append(alternate);
//        } else {
//            builder.append(new String(CryptoUtils.encryptSha1(alternate)));
//        }
//
//        return builder.toString();
    }


    public static String toShortFieldName(String longFieldName) {
        if (longFieldName.contains(".")) {
            String[] split = longFieldName.split("\\.");
            return FIELDS_MAP.get(split[0]);
        }
        return FIELDS_MAP.get(longFieldName);
    }

}
