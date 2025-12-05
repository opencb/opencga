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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.annotation.ConsequenceTypeMappings;
import org.opencb.biodata.models.variant.avro.*;
import org.opencb.commons.datastore.core.ComplexTypeConverter;
import org.opencb.opencga.core.models.common.mixins.GenericRecordAvroJsonMixin;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.annotation.converters.VariantAnnotationModelUtils;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.opencb.opencga.storage.mongodb.variant.converters.DocumentToVariantConverter.ANNOTATION_FIELD;

/**
 * Created by jacobo on 13/01/15.
 */
public class DocumentToVariantAnnotationConverter
        extends AbstractDocumentConverter
        implements ComplexTypeConverter<VariantAnnotation, Document> {

    private static final String ANNOT_ID_FIELD = "id";
    private static final String GENE_SO_FIELD = "_gn_so";
    private static final String CONSEQUENCE_TYPE_FIELD = "ct";
    private static final String CT_GENE_NAME_FIELD = "gn";
    private static final String CT_ENSEMBL_GENE_ID_FIELD = "ensg";
    private static final String CT_ENSEMBL_TRANSCRIPT_ID_FIELD = "enst";
    private static final String CT_BIOTYPE_FIELD = "bt";
    private static final String CT_TRANSCRIPT_ANNOT_FLAGS_FIELD = "flags";
    private static final String CT_SO_ACCESSION_FIELD = "so";
    private static final String CT_PROTEIN_KEYWORDS_FIELD = "kw";
    private static final String CT_PROTEIN_SUBSTITUTION_SCORE_FIELD = "ps_score";
    private static final String CT_PROTEIN_POLYPHEN_FIELD = "polyphen";
    private static final String CT_PROTEIN_SIFT_FIELD = "sift";

    private static final String XREFS_FIELD = "xrefs";

    private static final String POPULATION_FREQUENCIES_FIELD = "popFq";
    public static final String POPULATION_FREQUENCY_STUDY_FIELD = "study";
    public static final String POPULATION_FREQUENCY_POP_FIELD = "pop";
    public static final String POPULATION_FREQUENCY_REFERENCE_FREQUENCY_FIELD = "refFq";
    public static final String POPULATION_FREQUENCY_ALTERNATE_FREQUENCY_FIELD = "altFq";
    private static final String POPULATION_FREQUENCY_REF_HOM_GT_FIELD = "hetFq";
    private static final String POPULATION_FREQUENCY_HET_GT_FIELD = "refHomFq";
    private static final String POPULATION_FREQUENCY_ALT_HOM_GT_FIELD = "altHomFq";

    private static final String CONSERVED_REGION_SCORE_FIELD = "cr_score";
    private static final String CONSERVED_REGION_PHYLOP_FIELD = "cr_phylop";
    private static final String CONSERVED_REGION_PHASTCONS_FIELD = "cr_phastcons";
    private static final String CONSERVED_REGION_GERP_FIELD = "cr_gerp";

    private static final String GENE_TRAIT_FIELD = "gn_trait";

    private static final String DRUG_FIELD = "drug";
    private static final String DRUG_NAME_FIELD = "dn";

    public static final String SCORE_SCORE_FIELD = "sc";
    private static final String SCORE_SOURCE_FIELD = "src";
    public static final String SCORE_DESCRIPTION_FIELD = "desc";

    private static final String CLINICAL_COMBINATIONS_FIELD = "clinical_c";

    private static final String FUNCTIONAL_SCORE = "fn_score";
    private static final String FUNCTIONAL_CADD_RAW_FIELD = "fn_cadd_r";
    private static final String FUNCTIONAL_CADD_SCALED_FIELD = "fn_cadd_s";

    public static final Map<String, String> SCORE_FIELD_MAP;

    private final ObjectWriter writer;
    private final ObjectReader reader;

    protected static Logger logger = LoggerFactory.getLogger(DocumentToVariantAnnotationConverter.class);

    public static final String POLYPHEN = "polyphen";
    public static final String SIFT = "sift";

    public static final String PHAST_CONS = "phastCons";
    public static final String PHYLOP = "phylop";
    public static final String GERP = "gerp";

    public static final String CADD_SCALED = "cadd_scaled";
    public static final String CADD_RAW = "cadd_raw";

    public static final String JSON_RAW = "jsonRaw";


    public static final String ANNOT_ID = ANNOTATION_FIELD + '.' + ANNOT_ID_FIELD;
    public static final String GENE_SO = ANNOTATION_FIELD + '.' + GENE_SO_FIELD;
    public static final String CONSEQUENCE_TYPE = ANNOTATION_FIELD + '.' + CONSEQUENCE_TYPE_FIELD;
    public static final String CT_ENSEMBL_GENE_ID = CONSEQUENCE_TYPE + '.' + CT_ENSEMBL_GENE_ID_FIELD;
    public static final String CT_ENSEMBL_TRANSCRIPT_ID = CONSEQUENCE_TYPE + '.' + CT_ENSEMBL_TRANSCRIPT_ID_FIELD;
    public static final String CT_TRANSCRIPT_ANNOT_FLAGS = CONSEQUENCE_TYPE + '.' + CT_TRANSCRIPT_ANNOT_FLAGS_FIELD;
    public static final String CT_GENE_NAME = CONSEQUENCE_TYPE + '.' + CT_GENE_NAME_FIELD;
    public static final String CT_BIOTYPE = CONSEQUENCE_TYPE + '.' + CT_BIOTYPE_FIELD;
    public static final String CT_SO_ACCESSION = CONSEQUENCE_TYPE + '.' + CT_SO_ACCESSION_FIELD;
    public static final String CT_PROTEIN_KEYWORDS = CONSEQUENCE_TYPE + '.' + CT_PROTEIN_KEYWORDS_FIELD;
    public static final String CT_PROTEIN_POLYPHEN_SCORE = CONSEQUENCE_TYPE + '.' + CT_PROTEIN_SUBSTITUTION_SCORE_FIELD
            + '.' + CT_PROTEIN_POLYPHEN_FIELD + '.' + SCORE_SCORE_FIELD;
    public static final String CT_PROTEIN_POLYPHEN_DESCRIPTION = CONSEQUENCE_TYPE + '.' + CT_PROTEIN_SUBSTITUTION_SCORE_FIELD
            + '.' + CT_PROTEIN_POLYPHEN_FIELD + '.' + SCORE_DESCRIPTION_FIELD;
    public static final String CT_PROTEIN_SIFT_SCORE = CONSEQUENCE_TYPE + '.' + CT_PROTEIN_SIFT_FIELD + '.' + SCORE_SCORE_FIELD;
    public static final String CT_PROTEIN_SIFT_DESCRIPTION = CONSEQUENCE_TYPE + '.' + CT_PROTEIN_SIFT_FIELD + '.' + SCORE_DESCRIPTION_FIELD;
    public static final String POPULATION_FREQUENCIES = ANNOTATION_FIELD + '.' + POPULATION_FREQUENCIES_FIELD;
    public static final String POPULATION_FREQUENCY_STUDY = POPULATION_FREQUENCIES + '.' + POPULATION_FREQUENCY_STUDY_FIELD;
    public static final String POPULATION_FREQUENCY_POP = POPULATION_FREQUENCIES + '.' + POPULATION_FREQUENCY_POP_FIELD;
    public static final String POPULATION_FREQUENCY_REFERENCE_FREQUENCY = POPULATION_FREQUENCIES
            + '.' + POPULATION_FREQUENCY_REFERENCE_FREQUENCY_FIELD;
    public static final String POPULATION_FREQUENCY_ALTERNATE_FREQUENCY = POPULATION_FREQUENCIES
            + '.' + POPULATION_FREQUENCY_ALTERNATE_FREQUENCY_FIELD;
    public static final String CONSERVED_REGION_PHYLOP_SCORE = ANNOTATION_FIELD + '.' + CONSERVED_REGION_SCORE_FIELD
            + '.' + CONSERVED_REGION_PHYLOP_FIELD + '.' + SCORE_SCORE_FIELD;
    public static final String CONSERVED_REGION_PHASTCONS_SCORE = ANNOTATION_FIELD + '.' + CONSERVED_REGION_SCORE_FIELD
            + '.' + CONSERVED_REGION_PHASTCONS_FIELD + '.' + SCORE_SCORE_FIELD;
    public static final String CONSERVED_REGION_GERP_SCORE = ANNOTATION_FIELD + '.' + CONSERVED_REGION_SCORE_FIELD
            + '.' + CONSERVED_REGION_GERP_FIELD + '.' + SCORE_SCORE_FIELD;
    public static final String GENE_TRAIT_NAME = ANNOTATION_FIELD + '.' + GENE_TRAIT_FIELD;
    public static final String DRUG_NAME = ANNOTATION_FIELD + '.' + DRUG_FIELD + '.' + DRUG_NAME_FIELD;
    public static final String FUNCTIONAL_CADD_RAW_SCORE = ANNOTATION_FIELD + '.' + FUNCTIONAL_CADD_RAW_FIELD + '.' + SCORE_SCORE_FIELD;
    public static final String FUNCTIONAL_CADD_SCALED_SCORE = ANNOTATION_FIELD
            + '.' + FUNCTIONAL_CADD_SCALED_FIELD + '.' + SCORE_SCORE_FIELD;
    public static final String XREFS_ID = ANNOTATION_FIELD + '.' + XREFS_FIELD;
    public static final String CLINICAL_COMBINATIONS = ANNOTATION_FIELD + '.' + CLINICAL_COMBINATIONS_FIELD;

    static {
        Map<String, String> scoreFieldMap = new HashMap<>(7);
        scoreFieldMap.put(SIFT, ANNOTATION_FIELD + "." + CONSEQUENCE_TYPE_FIELD + "." + CT_PROTEIN_SIFT_FIELD);
        scoreFieldMap.put(POLYPHEN, ANNOTATION_FIELD + "." + CONSEQUENCE_TYPE_FIELD + "." + CT_PROTEIN_POLYPHEN_FIELD);
        scoreFieldMap.put(PHAST_CONS, ANNOTATION_FIELD + "." + CONSERVED_REGION_PHASTCONS_FIELD);
        scoreFieldMap.put(PHYLOP, ANNOTATION_FIELD + "." + CONSERVED_REGION_PHYLOP_FIELD);
        scoreFieldMap.put(GERP, ANNOTATION_FIELD + "." + CONSERVED_REGION_GERP_FIELD);
        scoreFieldMap.put(CADD_SCALED, ANNOTATION_FIELD + "." + FUNCTIONAL_CADD_SCALED_FIELD);
        scoreFieldMap.put(CADD_RAW, ANNOTATION_FIELD + "." + FUNCTIONAL_CADD_RAW_FIELD);
        SCORE_FIELD_MAP = Collections.unmodifiableMap(scoreFieldMap);
    }

    private Integer annotationId = null;
    private Map<Integer, String> annotationIds = Collections.emptyMap();

    public DocumentToVariantAnnotationConverter(Map<Integer, String> annotationIds) {
        this();
        this.annotationIds = annotationIds;
    }

    public DocumentToVariantAnnotationConverter(Integer annotationId) {
        this();
        this.annotationId = annotationId;
    }

    public DocumentToVariantAnnotationConverter() {
        ObjectMapper jsonObjectMapper = new ObjectMapper();
        jsonObjectMapper.configure(SerializationFeature.WRITE_NULL_MAP_VALUES, false);
        jsonObjectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        jsonObjectMapper.addMixIn(GenericRecord.class, GenericRecordAvroJsonMixin.class);
        writer = jsonObjectMapper.writerFor(VariantAnnotation.class);
        reader = jsonObjectMapper.readerFor(VariantAnnotation.class);
    }

    @Override
    public VariantAnnotation convertToDataModelType(Document object) {
        return convertToDataModelType(object, null, null);
    }


    public VariantAnnotation convertToDataModelType(Document object, Document customAnnotation, Variant variant) {
//        String chromosome = null;
//        String reference = null;
//        String alternate = null;

        byte[] jsonRaw = object.get(JSON_RAW, byte[].class);
        VariantAnnotation va;
        if (jsonRaw != null && jsonRaw.length > 0) {
            try {
                va = reader.readValue(jsonRaw, VariantAnnotation.class);
                // Even if we have the raw JSON stored, we need to set some fields that
                // could be missing there
                if (variant != null) {
                    va.setChromosome(variant.getChromosome());
                    va.setReference(variant.getReference());
                    va.setAlternate(variant.getAlternate());
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            if (variant != null) {
                va = newVariantAnnotation(variant);
            } else {
                va = new VariantAnnotation();
            }

        }

        if (customAnnotation != null) {
            Map<String, AdditionalAttribute> additionalAttributes = convertAdditionalAttributesToDataModelType(customAnnotation);
            va.setAdditionalAttributes(additionalAttributes);
        } else {
            if (!annotationIds.isEmpty()) {
                Object o = object.get(ANNOT_ID_FIELD);
                if (o != null && o instanceof Number) {
                    if (va.getAdditionalAttributes() == null) {
                        va.setAdditionalAttributes(new HashMap<>());
                    }
                    va.getAdditionalAttributes().compute(VariantField.AdditionalAttributes.GROUP_NAME.key(), (key, value) -> {
                        if (value == null) {
                            HashMap<String, String> map = new HashMap<>(1);
                            value = new AdditionalAttribute(map);
                        }
                        value.getAttribute().put(VariantField.AdditionalAttributes.ANNOTATION_ID.key(),
                                annotationIds.get(((Number) o).intValue()));
                        return value;
                    });
                }
            }
        }
        return va;
    }

    protected static VariantAnnotation newVariantAnnotation(Variant variant) {
        VariantAnnotation va;
        va = new VariantAnnotation();
        va.setChromosome(variant.getChromosome());
        va.setReference(variant.getReference());
        va.setAlternate(variant.getAlternate());
        va.setStart(variant.getStart());
        va.setEnd(variant.getEnd());
        return va;
    }

    public Map<String, AdditionalAttribute> convertAdditionalAttributesToDataModelType(Document customAnnotation) {
        Map<String, AdditionalAttribute> attributeMap = new HashMap<>();
        for (String key : customAnnotation.keySet()) {
            Document document = customAnnotation.get(key, Document.class);
            HashMap<String, String> map = new HashMap<>();
            document.forEach((k, value) -> map.put(k, value.toString()));
            AdditionalAttribute attribute = new AdditionalAttribute(map);
            attributeMap.put(key, attribute);
        }
        return attributeMap;
    }

    @Override
    public Document convertToStorageType(VariantAnnotation variantAnnotation) {
        Document document = new Document();
        Set<String> xrefs = VariantAnnotationModelUtils.extractXRefs(variantAnnotation);
        List<Document> cts = new LinkedList<>();

        try {
            byte[] bytes = writer.writeValueAsBytes(variantAnnotation);
            document.put(JSON_RAW, bytes);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        //Annotation ID
        document.put(ANNOT_ID_FIELD, annotationId);

        //ConsequenceType
        if (variantAnnotation.getConsequenceTypes() != null) {
            Set<String> gnSo = new HashSet<>();
            List<ConsequenceType> consequenceTypes = variantAnnotation.getConsequenceTypes();
            for (ConsequenceType consequenceType : consequenceTypes) {
                Document ct = new Document();

                putNotNull(ct, CT_GENE_NAME_FIELD, consequenceType.getGeneName());
                putNotNull(ct, CT_ENSEMBL_GENE_ID_FIELD, consequenceType.getGeneId());
                putNotNull(ct, CT_ENSEMBL_TRANSCRIPT_ID_FIELD, consequenceType.getTranscriptId());
                putNotNull(ct, CT_BIOTYPE_FIELD, consequenceType.getBiotype());
                putNotNull(ct, CT_TRANSCRIPT_ANNOT_FLAGS_FIELD, consequenceType.getTranscriptFlags());

                ProteinVariantAnnotation proteinVariantAnnotation = consequenceType.getProteinVariantAnnotation();
                if (consequenceType.getSequenceOntologyTerms() != null) {
                    List<Integer> soAccession = new LinkedList<>();
                    for (SequenceOntologyTerm entry : consequenceType.getSequenceOntologyTerms()) {
                        soAccession.add(ConsequenceTypeMappings.termToAccession.get(entry.getName()));
                    }
                    putNotNull(ct, CT_SO_ACCESSION_FIELD, soAccession);

                    for (Integer so : soAccession) {
                        if (StringUtils.isNotEmpty(consequenceType.getGeneName())) {
                            gnSo.add(buildGeneSO(consequenceType.getGeneName(), so));
                        }
                        if (StringUtils.isNotEmpty(consequenceType.getGeneId())) {
                            gnSo.add(buildGeneSO(consequenceType.getGeneId(), so));
                        }
                        if (StringUtils.isNotEmpty(consequenceType.getTranscriptId())) {
                            gnSo.add(buildGeneSO(consequenceType.getTranscriptId(), so));
                        }
                        if (proteinVariantAnnotation != null) {
                            if (StringUtils.isNotEmpty(proteinVariantAnnotation.getUniprotAccession())) {
                                gnSo.add(buildGeneSO(proteinVariantAnnotation.getUniprotAccession(), so));
                            }
                            if (StringUtils.isNotEmpty(proteinVariantAnnotation.getUniprotName())) {
                                gnSo.add(buildGeneSO(proteinVariantAnnotation.getUniprotName(), so));
                            }
                        }
                    }
                }
                //Protein annotation
                if (proteinVariantAnnotation != null) {
                    //Protein substitution region score
                    if (proteinVariantAnnotation.getSubstitutionScores() != null) {
                        List<Document> proteinSubstitutionScores = new LinkedList<>();
                        for (Score score : proteinVariantAnnotation.getSubstitutionScores()) {
                            if (score != null) {
                                if (score.getSource().equals(POLYPHEN)) {
                                    putNotNull(ct, CT_PROTEIN_POLYPHEN_FIELD, convertScoreToStorageNoSource(score));
                                } else if (score.getSource().equals(SIFT)) {
                                    putNotNull(ct, CT_PROTEIN_SIFT_FIELD, convertScoreToStorageNoSource(score));
                                } else {
                                    proteinSubstitutionScores.add(convertScoreToStorage(score));
                                }
                            }
                        }
                        putNotNull(ct, CT_PROTEIN_SUBSTITUTION_SCORE_FIELD, proteinSubstitutionScores);
                    }
                    putNotNull(ct, CT_PROTEIN_KEYWORDS_FIELD, proteinVariantAnnotation.getKeywords());


                }

                cts.add(ct);


            }
            putNotNull(document, GENE_SO_FIELD, gnSo);
            putNotNull(document, CONSEQUENCE_TYPE_FIELD, cts);
        }

        //Conserved region score
        if (variantAnnotation.getConservation() != null) {
            List<Document> conservedRegionScores = new LinkedList<>();
            for (Score score : variantAnnotation.getConservation()) {
                if (score != null) {
                    if (score.getSource().equals(PHYLOP)) {
                        putNotNull(document, CONSERVED_REGION_PHYLOP_FIELD, convertScoreToStorageNoSource(score));
                    } else if (score.getSource().equals(PHAST_CONS)) {
                        putNotNull(document, CONSERVED_REGION_PHASTCONS_FIELD, convertScoreToStorageNoSource(score));
                    } else if (score.getSource().equals(GERP)) {
                        putNotNull(document, CONSERVED_REGION_GERP_FIELD, convertScoreToStorageNoSource(score));
                    } else {
                        conservedRegionScores.add(convertScoreToStorage(score));
                    }
                }
            }
            putNotNull(document, CONSERVED_REGION_SCORE_FIELD, conservedRegionScores);
        }

        // Gene trait association
        if (variantAnnotation.getGeneTraitAssociation() != null) {
            List<String> geneTraitAssociations = new LinkedList<>();
            for (GeneTraitAssociation geneTraitAssociation : variantAnnotation.getGeneTraitAssociation()) {
                if (geneTraitAssociation != null) {
                    if (geneTraitAssociation.getName() != null) {
                        geneTraitAssociations.add(geneTraitAssociation.getName());
                    }
                }
            }
            putNotNull(document, GENE_TRAIT_FIELD, geneTraitAssociations);
        }

        //Population frequencies
        if (variantAnnotation.getPopulationFrequencies() != null) {
            List<Document> populationFrequencies = new LinkedList<>();
            for (PopulationFrequency populationFrequency : variantAnnotation.getPopulationFrequencies()) {
                if (populationFrequency != null) {
                    populationFrequencies.add(convertPopulationFrequencyToStorage(populationFrequency));
                }
            }
            putNotNull(document, POPULATION_FREQUENCIES_FIELD, populationFrequencies);
        }

        // Drug-Gene Interactions
        if (variantAnnotation.getGeneDrugInteraction() != null) {
            List<Document> drugGeneInteractions = new LinkedList<>();
            List<GeneDrugInteraction> geneDrugInteractionList = variantAnnotation.getGeneDrugInteraction();
            if (geneDrugInteractionList != null) {
                for (GeneDrugInteraction geneDrugInteraction : geneDrugInteractionList) {
                    Document drugDbObject = new Document();
                    putNotNull(drugDbObject, DRUG_NAME_FIELD, geneDrugInteraction.getDrugName());
                    drugGeneInteractions.add(drugDbObject);
                }
            }
            putNotNull(document, DRUG_FIELD, drugGeneInteractions);
        }

        //XREFs
        putNotNull(document, XREFS_FIELD, xrefs);

        //Functional score
        if (variantAnnotation.getFunctionalScore() != null) {
            List<Document> scores = new ArrayList<>(variantAnnotation.getFunctionalScore().size());
            for (Score score : variantAnnotation.getFunctionalScore()) {
                if (score != null) {
                    if (score.getSource().equals(CADD_RAW)) {
                        putNotNull(document, FUNCTIONAL_CADD_RAW_FIELD, convertScoreToStorageNoSource(score));
                    } else if (score.getSource().equals(CADD_SCALED)) {
                        putNotNull(document, FUNCTIONAL_CADD_SCALED_FIELD, convertScoreToStorageNoSource(score));
                    } else {
                        scores.add(convertScoreToStorage(score));
                    }
                }
            }
            putNotNull(document, FUNCTIONAL_SCORE, scores);
        }

        //Clinical Data
        if (CollectionUtils.isNotEmpty(variantAnnotation.getTraitAssociation())) {
            document.put(CLINICAL_COMBINATIONS_FIELD, VariantQueryUtils.buildClinicalCombinations(variantAnnotation));
        }

        return document;
    }

    public static String buildGeneSO(String gene, Integer so) {
        return gene == null ? null : gene + '_' + so;
    }


    public Document convertScoreToStorageNoSource(Score score) {
        return convertScoreToStorage(score.getScore(), null, score.getDescription());
    }

    private Document convertScoreToStorage(Score score) {
        return convertScoreToStorage(score.getScore(), score.getSource(), score.getDescription());
    }

    private Document convertScoreToStorage(double score, String source, String description) {
        Document dbObject = new Document(SCORE_SCORE_FIELD, score);
        putNotNull(dbObject, SCORE_SOURCE_FIELD, source);
        putNotNull(dbObject, SCORE_DESCRIPTION_FIELD, description);
        return dbObject;
    }

    private Document convertPopulationFrequencyToStorage(PopulationFrequency populationFrequency) {
        Document dbObject = new Document(POPULATION_FREQUENCY_STUDY_FIELD, populationFrequency.getStudy());
        putNotNull(dbObject, POPULATION_FREQUENCY_POP_FIELD, populationFrequency.getPopulation());
        putNotNull(dbObject, POPULATION_FREQUENCY_REFERENCE_FREQUENCY_FIELD, populationFrequency.getRefAlleleFreq());
        putNotNull(dbObject, POPULATION_FREQUENCY_ALTERNATE_FREQUENCY_FIELD, populationFrequency.getAltAlleleFreq());
        putNotNull(dbObject, POPULATION_FREQUENCY_REF_HOM_GT_FIELD, populationFrequency.getRefHomGenotypeFreq());
        putNotNull(dbObject, POPULATION_FREQUENCY_HET_GT_FIELD, populationFrequency.getHetGenotypeFreq());
        putNotNull(dbObject, POPULATION_FREQUENCY_ALT_HOM_GT_FIELD, populationFrequency.getAltHomGenotypeFreq());
        return dbObject;
    }


    public Document convertToStorageType(Map<String, AdditionalAttribute> attributes) {
        Document document = new Document();
        attributes.forEach((key, attribute) -> {
            document.put(key, convertToStorageType(attribute));
        });
        return document;
    }

    public static Document convertToStorageType(AdditionalAttribute attribute) {
        Document document = new Document();
        document.putAll(attribute.getAttribute());
        return document;
    }

}
