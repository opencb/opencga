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

import org.bson.Document;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.*;
import org.opencb.commons.datastore.core.ComplexTypeConverter;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;

import java.util.*;

import static java.util.Collections.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantField.AdditionalAttributes.GROUP_NAME;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantField.AdditionalAttributes.RELEASE;
import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToStudyEntryConverter.*;
import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToVariantAnnotationConverter.JSON_RAW;
import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToVariantAnnotationConverter.newVariantAnnotation;

/**
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class DocumentToVariantConverter extends AbstractDocumentConverter {

    public static final String ID_FIELD = "id";
    public static final String CHROMOSOME_FIELD = "chromosome";
    public static final String START_FIELD = "start";
    public static final String END_FIELD = "end";
    public static final String LENGTH_FIELD = "length";
    public static final String REFERENCE_FIELD = "reference";
    public static final String ALTERNATE_FIELD = "alternate";
    public static final String IDS_FIELD = "ids";
    public static final String TYPE_FIELD = "type";

    public static final String SV_FIELD = "sv";
    public static final String SV_CISTART_FIELD = "cistart";
    public static final String SV_CIEND_FIELD = "ciend";
    public static final String SV_CN_FIELD = "cn";
    public static final String SV_INS_SEQ = "ins_seq";
    public static final String SV_TYPE = "type";
    public static final String SV_BND = "bnd";
    public static final String SV_BND_ORIENTATION = "orientation";
    public static final String SV_BND_INS_SEQ = "insSeq";
    public static final String SV_BND_MATE = "mate";
    public static final String SV_BND_MATE_CHR = "chr";
    public static final String SV_BND_MATE_POS = "pos";
    public static final String SV_BND_MATE_CI_POS_L = "ciPosL";
    public static final String SV_BND_MATE_CI_POS_R = "ciPosR";

    public static final String STUDIES_FIELD = "studies";
    /** Root-level files array (moved from studies[].files). */
    public static final String FILES_FIELD = "files";
    public static final String ANNOTATION_FIELD = "annotation";
    public static final String CUSTOM_ANNOTATION_FIELD = "customAnnotation";
    public static final String STATS_FIELD = "stats";

    public static final String AT_FIELD = "_at";
    public static final String CHUNK_IDS_FIELD = "chunkIds";
    public static final String RELEASE_FIELD = "_r";
    public static final String INDEX_FIELD = "_index";
    public static final String INDEX_TIMESTAMP_FIELD = "ts";
//    public static final String INDEX_SYNCHRONIZED_FIELD = "sync";
//    public static final String INDEX_STUDIES_FIELD = "st";

//    public static final String ID_FIELD = "id";
//    public static final String EFFECTS_FIELD = "effs";
//    public static final String SOTERM_FIELD = "so";
//    public static final String GENE_FIELD = "gene";

    protected static final Map<VariantField, List<String>> FIELDS_MAP;
    public static final Set<VariantField> REQUIRED_FIELDS_SET;


    static {
        Set<VariantField> requiredFieldsSet = new HashSet<>();
        requiredFieldsSet.add(VariantField.CHROMOSOME);
        requiredFieldsSet.add(VariantField.START);
        requiredFieldsSet.add(VariantField.END);
        requiredFieldsSet.add(VariantField.REFERENCE);
        requiredFieldsSet.add(VariantField.ALTERNATE);
        requiredFieldsSet.add(VariantField.TYPE);
        requiredFieldsSet.add(VariantField.SV);
        REQUIRED_FIELDS_SET = Collections.unmodifiableSet(requiredFieldsSet);

        Map<VariantField, List<String>> map = new EnumMap<>(VariantField.class);
        map.put(VariantField.ID, singletonList(IDS_FIELD));
        map.put(VariantField.NAMES, singletonList(IDS_FIELD));
        map.put(VariantField.STRAND, emptyList());
        map.put(VariantField.CHROMOSOME, singletonList(CHROMOSOME_FIELD));
        map.put(VariantField.START, singletonList(START_FIELD));
        map.put(VariantField.END, singletonList(END_FIELD));
        map.put(VariantField.REFERENCE, singletonList(REFERENCE_FIELD));
        map.put(VariantField.ALTERNATE, singletonList(ALTERNATE_FIELD));
        map.put(VariantField.LENGTH, singletonList(LENGTH_FIELD));
        map.put(VariantField.TYPE, singletonList(TYPE_FIELD));
        map.put(VariantField.SV, singletonList(SV_FIELD));
        // files[] is now at root level (not nested inside studies[]).
        // studies[] only contains {sid} and stats.
        map.put(VariantField.STUDIES, Arrays.asList(STUDIES_FIELD, STATS_FIELD, FILES_FIELD));
        map.put(VariantField.STUDIES_SAMPLES, Arrays.asList(
                FILES_FIELD + '.' + STUDYID_FIELD,
                FILES_FIELD + '.' + FILEID_FIELD,
                FILES_FIELD + '.' + FILE_GENOTYPE_FIELD,
                FILES_FIELD + '.' + SAMPLE_DATA_FIELD,
                FILES_FIELD + '.' + ALTERNATES_FIELD,
                FILES_FIELD + '.' + ORI_FIELD
        ));
        map.put(VariantField.STUDIES_SAMPLE_DATA_KEYS, Arrays.asList());
        map.put(VariantField.STUDIES_SCORES, emptyList());
        map.put(VariantField.STUDIES_ISSUES, emptyList());
        map.put(VariantField.STUDIES_FILES, Arrays.asList(
                FILES_FIELD + '.' + STUDYID_FIELD,
                FILES_FIELD + '.' + FILEID_FIELD,
                FILES_FIELD + '.' + ATTRIBUTES_FIELD,
                FILES_FIELD + '.' + ALTERNATES_FIELD,
                FILES_FIELD + '.' + ORI_FIELD));
        map.put(VariantField.STUDIES_STATS, singletonList(STATS_FIELD));
        map.put(VariantField.STUDIES_SECONDARY_ALTERNATES, emptyList());
        map.put(VariantField.STUDIES_STUDY_ID, singletonList(STUDIES_FIELD + '.' + STUDYID_FIELD));

        List<String> annotationFields = Arrays.asList(ANNOTATION_FIELD + "." + JSON_RAW, CUSTOM_ANNOTATION_FIELD, RELEASE_FIELD);
        map.put(VariantField.ANNOTATION, annotationFields);
        for (VariantField child : VariantField.ANNOTATION.getChildren()) {
            map.put(child, annotationFields);
        }
//        map.put(VariantField.ANNOTATION_ANCESTRAL_ALLELE, emptyList());
//        map.put(VariantField.ANNOTATION_ID, emptyList());
//        map.put(VariantField.ANNOTATION_CHROMOSOME, emptyList());
//        map.put(VariantField.ANNOTATION_START, emptyList());
//        map.put(VariantField.ANNOTATION_END, emptyList());
//        map.put(VariantField.ANNOTATION_REFERENCE, emptyList());
//        map.put(VariantField.ANNOTATION_ALTERNATE, emptyList());
//        map.put(VariantField.ANNOTATION_XREFS, singletonList(ANNOTATION_FIELD + '.' + XREFS_FIELD));
//        map.put(VariantField.ANNOTATION_HGVS, singletonList(ANNOTATION_FIELD + '.' + DocumentToVariantAnnotationConverter.HGVS_FIELD));
//        map.put(VariantField.ANNOTATION_CYTOBAND, singletonList(ANNOTATION_FIELD + '.' + CYTOBANDS_FIELD));
//        map.put(VariantField.ANNOTATION_DISPLAY_CONSEQUENCE_TYPE, singletonList(ANNOTATION_FIELD + '.' + DISPLAY_CONSEQUENCE_TYPE_FIELD));
//        map.put(VariantField.ANNOTATION_CONSEQUENCE_TYPES, singletonList(ANNOTATION_FIELD + '.' + CONSEQUENCE_TYPE_FIELD));
//        map.put(VariantField.ANNOTATION_POPULATION_FREQUENCIES, singletonList(ANNOTATION_FIELD + '.' + POPULATION_FREQUENCIES_FIELD));
//        map.put(VariantField.ANNOTATION_MINOR_ALLELE, emptyList());
//        map.put(VariantField.ANNOTATION_MINOR_ALLELE_FREQ, emptyList());
//        map.put(VariantField.ANNOTATION_CONSERVATION, Arrays.asList(
//                ANNOTATION_FIELD + '.' + CONSERVED_REGION_PHYLOP_FIELD,
//                ANNOTATION_FIELD + '.' + CONSERVED_REGION_PHASTCONS_FIELD,
//                ANNOTATION_FIELD + '.' + CONSERVED_REGION_GERP_FIELD
//        ));
//        map.put(VariantField.ANNOTATION_GENE_EXPRESSION, emptyList());
//        map.put(VariantField.ANNOTATION_GENE_TRAIT_ASSOCIATION, singletonList(ANNOTATION_FIELD + '.' + GENE_TRAIT_FIELD));
//        map.put(VariantField.ANNOTATION_GENE_DRUG_INTERACTION, singletonList(ANNOTATION_FIELD + '.' + DRUG_FIELD));
////        map.put(VariantField.ANNOTATION_VARIANT_TRAIT_ASSOCIATION, singletonList(ANNOTATION_FIELD + '.' + CLINICAL_DATA_FIELD));
//        map.put(VariantField.ANNOTATION_TRAIT_ASSOCIATION, singletonList(ANNOTATION_FIELD + '.' + CLINICAL_DATA_FIELD));
//        map.put(VariantField.ANNOTATION_FUNCTIONAL_SCORE, Arrays.asList(
//                ANNOTATION_FIELD + '.' + FUNCTIONAL_CADD_RAW_FIELD,
//                ANNOTATION_FIELD + '.' + FUNCTIONAL_CADD_SCALED_FIELD));
//        map.put(VariantField.ANNOTATION_REPEAT, singletonList(ANNOTATION_FIELD + '.' + REPEATS_FIELD));
//        map.put(VariantField.ANNOTATION_DRUGS, emptyList());
//        map.put(VariantField.ANNOTATION_ADDITIONAL_ATTRIBUTES, Arrays.asList(CUSTOM_ANNOTATION_FIELD, RELEASE_FIELD));

        FIELDS_MAP = unmodifiableMap(map);

    }

    private final DocumentToStudyEntryConverter variantStudyEntryConverter;
    private Set<Integer> returnStudies;
    private final DocumentToVariantAnnotationConverter variantAnnotationConverter;
    private final DocumentToVariantStatsConverter statsConverter;

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
     * @param variantStudyEntryConverter The object used to convert the files
     * @param statsConverter Stats converter
     */
    public DocumentToVariantConverter(DocumentToStudyEntryConverter variantStudyEntryConverter,
                                      DocumentToVariantStatsConverter statsConverter) {
        this(variantStudyEntryConverter, statsConverter, null, null);
    }

    /**
     * Create a converter between {@link Variant} and {@link Document} entities. A converter for
     * the studies the variant was read from can be provided in case those
     * should be processed during the conversion.
     *
     * @param variantStudyEntryConverter The object used to convert the files
     * @param statsConverter Stats converter
     * @param returnStudies List of studies to return
     * @param annotationIds Map of annotationIds
     */
    public DocumentToVariantConverter(DocumentToStudyEntryConverter variantStudyEntryConverter,
                                      DocumentToVariantStatsConverter statsConverter, Collection<Integer> returnStudies,
                                      Map<Integer, String> annotationIds) {
        this.variantStudyEntryConverter = variantStudyEntryConverter;
        this.variantAnnotationConverter = new DocumentToVariantAnnotationConverter(annotationIds);
        this.statsConverter = statsConverter;
        addDefaultId = true;
        if (returnStudies != null) {
            if (returnStudies instanceof Set) {
                this.returnStudies = (Set<Integer>) returnStudies;
            } else {
                this.returnStudies = new HashSet<>(returnStudies);
            }
        }
    }

    public ComplexTypeConverter<Variant, Document> asComplexTypeConverter() {
        return new ComplexTypeConverter<Variant, Document>() {
            @Override
            public Document convertToStorageType(Variant object) {
                throw new UnsupportedOperationException("Not implemented yet");
            }

            @Override
            public Variant convertToDataModelType(Document object) {
                return DocumentToVariantConverter.this.convertToDataModelType(object);
            }
        };
    }

    public Variant convertToDataModelType(Document variantObject) {
        String chromosome = (String) variantObject.get(CHROMOSOME_FIELD);
        int start = (int) variantObject.get(START_FIELD);
        int end = (int) variantObject.get(END_FIELD);
        String reference = (String) variantObject.get(REFERENCE_FIELD);
        String alternate = (String) variantObject.get(ALTERNATE_FIELD);
        Variant variant = new Variant(chromosome, start, end, reference, alternate);
        if (addDefaultId) {
            variant.setId(variant.toString());
        }
//        if (object.containsKey(IDS_FIELD)) {
//            LinkedList<String> names = new LinkedList<>(object.get(IDS_FIELD, Collection.class));
//            variant.setNames(names);
//        }
        if (variantObject.containsKey(TYPE_FIELD)) {
            variant.setType(VariantType.valueOf(variantObject.get(TYPE_FIELD).toString()));
        }

        // SV
        Document mongoSv = variantObject.get(SV_FIELD, Document.class);
        if (mongoSv != null) {
            StructuralVariation sv = new StructuralVariation();
            List<Integer> ciStart = getDefault(mongoSv, SV_CISTART_FIELD, Arrays.asList(null, null));
            List<Integer> ciEnd = getDefault(mongoSv, SV_CIEND_FIELD, Arrays.asList(null, null));
            List<String> insSeq = getDefault(mongoSv, SV_INS_SEQ, Arrays.asList(null, null));
            sv.setCiStartLeft(ciStart.get(0));
            sv.setCiStartRight(ciStart.get(1));
            sv.setCiEndLeft(ciEnd.get(0));
            sv.setCiEndRight(ciEnd.get(1));
            sv.setLeftSvInsSeq(insSeq.get(0));
            sv.setRightSvInsSeq(insSeq.get(1));
            sv.setCopyNumber(mongoSv.getInteger(SV_CN_FIELD));

            String type = mongoSv.getString(SV_TYPE);
            if (type != null) {
                sv.setType(StructuralVariantType.valueOf(type));
            }

            Document mongoBnd = mongoSv.get(SV_BND, Document.class);
            if (mongoBnd != null) {
                Breakend bnd = new Breakend();
                bnd.setOrientation(BreakendOrientation.valueOf(mongoBnd.getString(SV_BND_ORIENTATION)));
                bnd.setInsSeq(mongoBnd.getString(SV_BND_INS_SEQ));
                Document mongoBndMate = mongoBnd.get(SV_BND_MATE, Document.class);
                if (mongoBndMate != null) {
                    BreakendMate mate = new BreakendMate();
                    mate.setChromosome(mongoBndMate.getString(SV_BND_MATE_CHR));
                    mate.setPosition(mongoBndMate.getInteger(SV_BND_MATE_POS));
                    mate.setCiPositionLeft(mongoBndMate.getInteger(SV_BND_MATE_CI_POS_L));
                    mate.setCiPositionRight(mongoBndMate.getInteger(SV_BND_MATE_CI_POS_R));
                    bnd.setMate(mate);
                }
                sv.setBreakend(bnd);
            }

            variant.setSv(sv);
        }

        // Files
        if (variantStudyEntryConverter != null) {
            List<Document> studies = variantObject.get(STUDIES_FIELD, List.class);
            // Root-level files (Stage 2 format). If absent, fall back to study-embedded files (Stage 1).
            List<Document> rootFiles = variantObject.get(FILES_FIELD, List.class);

            if (studies != null) {
                for (Document studyDocument : studies) {
                    int sid = ((Number) studyDocument.get(STUDYID_FIELD)).intValue();
                    if (returnStudies == null || returnStudies.contains(sid)) {
                        List<Document> studyFiles = new ArrayList<>();
                        if (rootFiles != null) {
                            for (Document rootFile : rootFiles) {
                                if (rootFile == null || rootFile.get(STUDYID_FIELD) == null) {
                                    System.out.println("WARNING: Found file without study ID. Skipping file " + rootFile.toJson());
                                }
                                int fileSid = rootFile.get(STUDYID_FIELD, Number.class).intValue();
                                if (fileSid == sid) {
                                    studyFiles.add(rootFile);
                                }
                            }
                        }
                        variant.addStudyEntry(variantStudyEntryConverter.convertToDataModelType(studyDocument, studyFiles, variant));
                    }
                }
            }

            Set<String> names = new HashSet<>();
            for (StudyEntry studyEntry : variant.getStudies()) {
                List<FileEntry> files = studyEntry.getFiles();
                if (files != null) {
                    for (FileEntry fileEntry : files) {
                        String id = fileEntry.getData().get(StudyEntry.VCF_ID);
                        if (id != null) {
                            names.add(id);
                        }
                    }
                }
            }
            variant.setNames(new ArrayList<>(names));
        }

        // Annotations
        Document mongoAnnotation;
        Object o = variantObject.get(ANNOTATION_FIELD);
        if (o instanceof List) {
            if (!((List) o).isEmpty()) {
                mongoAnnotation = (Document) ((List) o).get(0);
            } else {
                mongoAnnotation = null;
            }
        } else {
            mongoAnnotation = (Document) variantObject.get(ANNOTATION_FIELD);
        }
        Document customAnnotation = variantObject.get(CUSTOM_ANNOTATION_FIELD, Document.class);
        boolean hasRelease = variantObject.containsKey(RELEASE_FIELD);
        boolean hasIndex = variantObject.containsKey(INDEX_FIELD);
        if (mongoAnnotation != null || customAnnotation != null || hasRelease) {
            VariantAnnotation annotation;
            if (mongoAnnotation != null) {
                annotation = variantAnnotationConverter
                        .convertToDataModelType(mongoAnnotation, customAnnotation, variant);
            } else {
                annotation = newVariantAnnotation(variant);
                if (customAnnotation != null) {
                    annotation.setAdditionalAttributes(variantAnnotationConverter
                            .convertAdditionalAttributesToDataModelType(customAnnotation));
                }
            }
            AdditionalAttribute additionalAttribute = null;
            if (hasRelease || hasIndex) {
                if (annotation.getAdditionalAttributes() == null) {
                    annotation.setAdditionalAttributes(new HashMap<>());
                }
                if (annotation.getAdditionalAttributes().containsKey(GROUP_NAME.key())) {
                    additionalAttribute = annotation.getAdditionalAttributes().get(GROUP_NAME.key());
                } else {
                    additionalAttribute = new AdditionalAttribute(new HashMap<>());
                    annotation.getAdditionalAttributes().put(GROUP_NAME.key(), additionalAttribute);
                }
            }
            if (hasRelease) {
                String release = this.<Number>getList(variantObject, RELEASE_FIELD).stream()
                        .map(Number::intValue)
                        .min(Integer::compareTo)
                        .orElse(-1)
                        .toString();

                additionalAttribute.getAttribute().put(RELEASE.key(), release);
            }

            variant.setAnnotation(annotation);
        }

        // Statistics
        if (statsConverter != null && variantObject.containsKey(STATS_FIELD)) {
            List<Document> stats = variantObject.get(STATS_FIELD, List.class);
            statsConverter.convertCohortsToDataModelType(stats, variant);
        }
        return variant;
    }

    public static List<String> toShortFieldName(VariantField field) {
        return FIELDS_MAP.get(field);
    }

}
