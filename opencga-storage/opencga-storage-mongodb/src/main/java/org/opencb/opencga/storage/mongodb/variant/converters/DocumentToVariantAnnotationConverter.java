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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.avro.generic.GenericRecord;
import org.bson.Document;
import org.opencb.biodata.models.variant.annotation.ConsequenceTypeMappings;
import org.opencb.biodata.models.variant.avro.*;
import org.opencb.commons.datastore.core.ComplexTypeConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by jacobo on 13/01/15.
 */
public class DocumentToVariantAnnotationConverter
        extends AbstractDocumentConverter
        implements ComplexTypeConverter<VariantAnnotation, Document> {

    public static final String ANNOT_ID_FIELD = "id";

    public static final String CONSEQUENCE_TYPE_FIELD = "ct";
    public static final String CT_GENE_NAME_FIELD = "gn";
    public static final String CT_ENSEMBL_GENE_ID_FIELD = "ensg";
    public static final String CT_ENSEMBL_TRANSCRIPT_ID_FIELD = "enst";
    public static final String CT_RELATIVE_POS_FIELD = "relPos";
    public static final String CT_CODON_FIELD = "codon";
    public static final String CT_STRAND_FIELD = "strand";
    public static final String CT_BIOTYPE_FIELD = "bt";
    public static final String CT_TRANSCRIPT_ANNOT_FLAGS = "flags";
    public static final String CT_C_DNA_POSITION_FIELD = "cDnaPos";
    public static final String CT_CDS_POSITION_FIELD = "cdsPos";
    public static final String CT_AA_POSITION_FIELD = "aaPos";
    public static final String CT_AA_REFERENCE_FIELD = "aaRef";
    public static final String CT_AA_ALTERNATE_FIELD = "aaAlt";
    public static final String CT_SO_ACCESSION_FIELD = "so";
    public static final String CT_PROTEIN_KEYWORDS = "kw";
    public static final String CT_PROTEIN_SUBSTITUTION_SCORE_FIELD = "ps_score";
    @Deprecated
    public static final String CT_PROTEIN_POLYPHEN_FIELD = "polyphen";
    @Deprecated
    public static final String CT_PROTEIN_SIFT_FIELD = "sift";
    public static final String CT_PROTEIN_FEATURE_FIELD = "pd";
    public static final String CT_PROTEIN_FEATURE_ID_FIELD = "id";
    public static final String CT_PROTEIN_FEATURE_START_FIELD = "start";
    public static final String CT_PROTEIN_FEATURE_END_FIELD = "end";
    public static final String CT_PROTEIN_FEATURE_TYPE_FIELD = "type";
    public static final String CT_PROTEIN_FEATURE_DESCRIPTION_FIELD = "desc";

    public static final String XREFS_FIELD = "xrefs";
    public static final String XREF_ID_FIELD = "id";
    public static final String XREF_SOURCE_FIELD = "src";

    public static final String POPULATION_FREQUENCIES_FIELD = "popFq";
    public static final String POPULATION_FREQUENCY_STUDY_FIELD = "study";
    public static final String POPULATION_FREQUENCY_POP_FIELD = "pop";
    public static final String POPULATION_FREQUENCY_REFERENCE_ALLELE_FIELD = "ref";
    public static final String POPULATION_FREQUENCY_ALTERNATE_ALLELE_FIELD = "alt";
    public static final String POPULATION_FREQUENCY_REFERENCE_FREQUENCY_FIELD = "refFq";
    public static final String POPULATION_FREQUENCY_ALTERNATE_FREQUENCY_FIELD = "altFq";

    public static final String CONSERVED_REGION_SCORE_FIELD = "cr_score";

    public static final String GENE_TRAIT_FIELD = "gn_trait";
    public static final String GENE_TRAIT_ID_FIELD = "id";
    public static final String GENE_TRAIT_NAME_FIELD = "name";
    public static final String GENE_TRAIT_HPO_FIELD = "hpo";
    public static final String GENE_TRAIT_SCORE_FIELD = "sc";
//    public static final String GENE_TRAIT_PUBMEDS_FIELD = "nPubmed";
    public static final String GENE_TRAIT_TYPES_FIELD = "types";
//    public static final String GENE_TRAIT_SOURCES_FIELD = "srcs";
    public static final String GENE_TRAIT_SOURCE_FIELD = "src";

    public static final String DRUG_FIELD = "drug";
    public static final String DRUG_NAME_FIELD = "dn";
    public static final String DRUG_GENE_FIELD = CT_GENE_NAME_FIELD;
    public static final String DRUG_SOURCE_FIELD = "src";
    public static final String DRUG_STUDY_TYPE_FIELD = "st";

    public static final String SCORE_SCORE_FIELD = "sc";
    public static final String SCORE_SOURCE_FIELD = "src";
    public static final String SCORE_DESCRIPTION_FIELD = "desc";

    public static final String CLINICAL_DATA_FIELD = "clinical";
    public static final String CLINICAL_COSMIC_FIELD = "cosmic";
    public static final String CLINICAL_GWAS_FIELD = "gwas";
    public static final String CLINICAL_CLINVAR_FIELD = "clinvar";

    public static final String FUNCTIONAL_SCORE = "fn_score";

    public static final String DEFAULT_STRAND_VALUE = "+";
    public static final String DEFAULT_DRUB_SOURCE = "dgidb";

    private final ObjectMapper jsonObjectMapper;
    private final ObjectWriter writer;

    protected static Logger logger = LoggerFactory.getLogger(DocumentToVariantAnnotationConverter.class);

    public DocumentToVariantAnnotationConverter() {
        jsonObjectMapper = new ObjectMapper();
        jsonObjectMapper.configure(SerializationFeature.WRITE_NULL_MAP_VALUES, false);
        jsonObjectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        writer = jsonObjectMapper.writer();
    }

    @Override
    public VariantAnnotation convertToDataModelType(Document object) {
        VariantAnnotation va = new VariantAnnotation();

        //ConsequenceType
        List<ConsequenceType> consequenceTypes = new LinkedList<>();
        Object cts = object.get(CONSEQUENCE_TYPE_FIELD);
        if (cts != null && cts instanceof List) {
            for (Object o : ((List) cts)) {
                if (o instanceof Document) {
                    Document ct = (Document) o;

                    //SO accession name
                    List<String> soAccessionNames = new LinkedList<>();
                    if (ct.containsKey(CT_SO_ACCESSION_FIELD)) {
                        if (ct.get(CT_SO_ACCESSION_FIELD) instanceof List) {
                            List<Integer> list = (List) ct.get(CT_SO_ACCESSION_FIELD);
                            for (Integer so : list) {
                                soAccessionNames.add(ConsequenceTypeMappings.accessionToTerm.get(so));
                            }
                        } else {
                            soAccessionNames.add(ConsequenceTypeMappings.accessionToTerm.get(ct.get(CT_SO_ACCESSION_FIELD)));
                        }
                    }

                    //ProteinSubstitutionScores
                    List<Score> proteinSubstitutionScores = new LinkedList<>();
                    if (ct.containsKey(CT_PROTEIN_SUBSTITUTION_SCORE_FIELD)) {
                        List<Document> list = (List) ct.get(CT_PROTEIN_SUBSTITUTION_SCORE_FIELD);
                        for (Document document : list) {
                            proteinSubstitutionScores.add(buildScore(document));
                        }
                    }
                    if (ct.containsKey(CT_PROTEIN_POLYPHEN_FIELD)) {
                        Document dbObject = (Document) ct.get(CT_PROTEIN_POLYPHEN_FIELD);
                        proteinSubstitutionScores.add(new Score(getDefault(dbObject, SCORE_SCORE_FIELD, 0.0),
                                "polyphen",
                                getDefault(dbObject, SCORE_DESCRIPTION_FIELD, "")));
                    }
                    if (ct.containsKey(CT_PROTEIN_SIFT_FIELD)) {
                        Document dbObject = (Document) ct.get(CT_PROTEIN_SIFT_FIELD);
                        proteinSubstitutionScores.add(new Score(getDefault(dbObject, SCORE_SCORE_FIELD, 0.0),
                                "sift",
                                getDefault(dbObject, SCORE_DESCRIPTION_FIELD, "")));
                    }


                    List<ProteinFeature> features = new ArrayList<>();
                    if (ct.containsKey(CT_PROTEIN_FEATURE_FIELD)) {
                        List<Document> featureDocuments = (List) ct.get(CT_PROTEIN_FEATURE_FIELD);
                        for (Document featureDocument : featureDocuments) {
                            features.add(new ProteinFeature(
                                    getDefault(featureDocument, CT_PROTEIN_FEATURE_ID_FIELD, ""),
                                    getDefault(featureDocument, CT_PROTEIN_FEATURE_START_FIELD, 0),
                                    getDefault(featureDocument, CT_PROTEIN_FEATURE_END_FIELD, 0),
                                    getDefault(featureDocument, CT_PROTEIN_FEATURE_TYPE_FIELD, ""),
                                    getDefault(featureDocument, CT_PROTEIN_FEATURE_DESCRIPTION_FIELD, "")
                            ));
                        }
                    }


                    ProteinVariantAnnotation proteinVariantAnnotation = buildProteinVariantAnnotation(
                            getDefault(ct, CT_AA_POSITION_FIELD, 0),
                            getDefault(ct, CT_AA_REFERENCE_FIELD, ""),
                            getDefault(ct, CT_AA_ALTERNATE_FIELD, ""),
                            proteinSubstitutionScores,
                            getDefault(ct, CT_PROTEIN_KEYWORDS, Collections.emptyList()),
                            features);
                    consequenceTypes.add(buildConsequenceType(
                            getDefault(ct, CT_GENE_NAME_FIELD, ""),
                            getDefault(ct, CT_ENSEMBL_GENE_ID_FIELD, ""),
                            getDefault(ct, CT_ENSEMBL_TRANSCRIPT_ID_FIELD, ""),
                            getDefault(ct, CT_STRAND_FIELD, "+"),
                            getDefault(ct, CT_BIOTYPE_FIELD, ""),
                            getDefault(ct, CT_TRANSCRIPT_ANNOT_FLAGS, Collections.emptyList()),
                            getDefault(ct, CT_C_DNA_POSITION_FIELD, 0),
                            getDefault(ct, CT_CDS_POSITION_FIELD, 0),
                            getDefault(ct, CT_CODON_FIELD, ""),
                            soAccessionNames,
                            proteinVariantAnnotation));
                }
            }

        }
        va.setConsequenceTypes(consequenceTypes);

        //Conserved Region Scores
        List<Score> conservedRegionScores = new LinkedList<>();
        if (object.containsKey(CONSERVED_REGION_SCORE_FIELD)) {
            List<Document> list = (List) object.get(CONSERVED_REGION_SCORE_FIELD);
            for (Document dbObject : list) {
                conservedRegionScores.add(buildScore(dbObject));
            }
        }
        va.setConservation(conservedRegionScores);

        //Population frequencies
        List<PopulationFrequency> populationFrequencies = new LinkedList<>();
        if (object.containsKey(POPULATION_FREQUENCIES_FIELD)) {
            List<Document> list = (List) object.get(POPULATION_FREQUENCIES_FIELD);
            for (Document dbObject : list) {
                populationFrequencies.add(new PopulationFrequency(
                        getDefault(dbObject, POPULATION_FREQUENCY_STUDY_FIELD, ""),
                        getDefault(dbObject, POPULATION_FREQUENCY_POP_FIELD, ""),
                        getDefault(dbObject, POPULATION_FREQUENCY_REFERENCE_ALLELE_FIELD, ""),
                        getDefault(dbObject, POPULATION_FREQUENCY_ALTERNATE_ALLELE_FIELD, ""),
                        (float) getDefault(dbObject, POPULATION_FREQUENCY_REFERENCE_FREQUENCY_FIELD, -1.0),
                        (float) getDefault(dbObject, POPULATION_FREQUENCY_ALTERNATE_FREQUENCY_FIELD, -1.0),
                        -1.0f,
                        -1.0f,
                        -1.0f
                ));
            }
        }
        va.setPopulationFrequencies(populationFrequencies);

        // Gene trait association
        List<GeneTraitAssociation> geneTraitAssociations = new LinkedList<>();
        if (object.containsKey(GENE_TRAIT_FIELD)) {
            List<Document> list = (List) object.get(GENE_TRAIT_FIELD);
            for (Document document : list) {
                geneTraitAssociations.add(new GeneTraitAssociation(
                        getDefault(document, GENE_TRAIT_ID_FIELD, ""),
                        getDefault(document, GENE_TRAIT_NAME_FIELD, ""),
                        getDefault(document, GENE_TRAIT_HPO_FIELD, ""),
                        (float) getDefault(document, GENE_TRAIT_SCORE_FIELD, 0F),
                        0, //getDefault(document, GENE_TRAIT_PUBMEDS_FIELD, 0),
                        getDefault(document, GENE_TRAIT_TYPES_FIELD, Collections.emptyList()),
                        Collections.emptyList(),
                        getDefault(document, GENE_TRAIT_SOURCE_FIELD, "")
                ));
            }
        }
        va.setGeneTraitAssociation(geneTraitAssociations);


        // Drug-Gene Interactions
        List<GeneDrugInteraction> drugs = new LinkedList<>();
        if (object.containsKey(DRUG_FIELD)) {
            List<Document> list = (List) object.get(DRUG_FIELD);
            for (Document dbObject : list) {
                //drugs.add(dbObject.toMap());
                drugs.add(new GeneDrugInteraction(
                        getDefault(dbObject, DRUG_GENE_FIELD, ""),
                        getDefault(dbObject, DRUG_NAME_FIELD, ""),
                        getDefault(dbObject, DRUG_SOURCE_FIELD, DEFAULT_DRUB_SOURCE),
                        getDefault(dbObject, DRUG_STUDY_TYPE_FIELD, ""),
                        ""));  // "convertToStorageType" stores the study_type within the
                // DRUG_SOURCE_FIELD

            }
        }
        va.setGeneDrugInteraction(drugs);

        //XREfs
        List<Xref> xrefs = new LinkedList<>();
        Object xrs = object.get(XREFS_FIELD);
        if (xrs != null && xrs instanceof List) {
            for (Object o : (List) xrs) {
                if (o instanceof Document) {
                    Document xref = (Document) o;

                    xrefs.add(new Xref(
                            (String) xref.get(XREF_ID_FIELD),
                            (String) xref.get(XREF_SOURCE_FIELD))
                    );
                }
            }
        }
        va.setXrefs(xrefs);

        //Functional score
        List<Score> functionalScore = new LinkedList<>();
        if (object.containsKey(FUNCTIONAL_SCORE)) {
            List<Document> scores = object.get(FUNCTIONAL_SCORE, List.class);
            for (Document document : scores) {
                functionalScore.add(buildScore(document));
            }
        }
        va.setFunctionalScore(functionalScore);

        //Clinical Data
        if (object.containsKey(CLINICAL_DATA_FIELD)) {
            va.setVariantTraitAssociation(parseClinicalData((Document) object.get(CLINICAL_DATA_FIELD)));
        }

        return va;
    }

    private Score buildScore(Document document) {
        return new Score(
                getDefault(document, SCORE_SCORE_FIELD, 0.0),
                getDefault(document, SCORE_SOURCE_FIELD, ""),
                getDefault(document, SCORE_DESCRIPTION_FIELD, "")
        );
    }

    private ConsequenceType buildConsequenceType(String geneName, String ensemblGeneId, String ensemblTranscriptId, String strand,
                                                 String biotype, List<String> transcriptAnnotationFlags, Integer cDnaPosition,
                                                 Integer cdsPosition, String codon,
                                                 List<String> soNameList, ProteinVariantAnnotation proteinVariantAnnotation) {
        List<SequenceOntologyTerm> soTerms = new ArrayList<>(soNameList.size());
        for (String soName : soNameList) {
            soTerms.add(new SequenceOntologyTerm(ConsequenceTypeMappings.getSoAccessionString(soName), soName));
        }
        return new ConsequenceType(geneName, ensemblGeneId, ensemblTranscriptId, strand, biotype, transcriptAnnotationFlags, cDnaPosition,
                cdsPosition, codon, proteinVariantAnnotation, soTerms);
    }

    private ProteinVariantAnnotation buildProteinVariantAnnotation(Integer aaPosition, String aaReference, String aaAlternate,
                                                                   List<Score> proteinSubstitutionScores, List<String> keywords,
                                                                   List<ProteinFeature> features) {
        return new ProteinVariantAnnotation(null, null, aaPosition,
                    aaReference, aaAlternate, null, null, proteinSubstitutionScores, keywords, features);
    }

    private VariantTraitAssociation parseClinicalData(Document clinicalData) {
        if (clinicalData != null) {
            int size = 0;
            VariantTraitAssociation variantTraitAssociation = new VariantTraitAssociation();
            List cosmicDBList = (List) clinicalData.get(CLINICAL_COSMIC_FIELD);
            if (cosmicDBList != null) {
                List<Cosmic> cosmicList = new ArrayList<>(cosmicDBList.size());
                for (Object object : cosmicDBList) {
                    cosmicList.add(jsonObjectMapper.convertValue(object, Cosmic.class));
                }
                size += cosmicList.size();
                variantTraitAssociation.setCosmic(cosmicList);
            }
            List gwasDBList = (List) clinicalData.get(CLINICAL_GWAS_FIELD);
            if (gwasDBList != null) {
                List<Gwas> gwasList = new ArrayList<>(gwasDBList.size());
                for (Object object : gwasDBList) {
                    gwasList.add(jsonObjectMapper.convertValue(object, Gwas.class));
                }
                size += gwasList.size();
                variantTraitAssociation.setGwas(gwasList);
            }
            List clinvarDBList = (List) clinicalData.get(CLINICAL_CLINVAR_FIELD);
            if (clinvarDBList != null) {
                List<ClinVar> clinvarList = new ArrayList<>(clinvarDBList.size());
                for (Object object : clinvarDBList) {
                    clinvarList.add(jsonObjectMapper.convertValue(object, ClinVar.class));
                }
                size += clinvarList.size();
                variantTraitAssociation.setClinvar(clinvarList);
            }
            if (size > 0) {
                return variantTraitAssociation;
            } else {
                return null;
            }
        }

        return null;

    }

    @Override
    public Document convertToStorageType(VariantAnnotation variantAnnotation) {
        Document document = new Document();
        Set<Document> xrefs = new HashSet<>();
        List<Document> cts = new LinkedList<>();

        //Annotation ID
        document.put(ANNOT_ID_FIELD, "?");

        //Variant ID
        if (variantAnnotation.getId() != null && !variantAnnotation.getId().isEmpty()) {
            xrefs.add(convertXrefToStorage(variantAnnotation.getId(), "dbSNP"));
        }

        //ConsequenceType
        if (variantAnnotation.getConsequenceTypes() != null) {
            List<ConsequenceType> consequenceTypes = variantAnnotation.getConsequenceTypes();
            for (ConsequenceType consequenceType : consequenceTypes) {
                Document ct = new Document();

                putNotNull(ct, CT_GENE_NAME_FIELD, consequenceType.getGeneName());
                putNotNull(ct, CT_ENSEMBL_GENE_ID_FIELD, consequenceType.getEnsemblGeneId());
                putNotNull(ct, CT_ENSEMBL_TRANSCRIPT_ID_FIELD, consequenceType.getEnsemblTranscriptId());
//                putNotNull(ct, RELATIVE_POS_FIELD, consequenceType.getRelativePosition());
                putNotNull(ct, CT_CODON_FIELD, consequenceType.getCodon());
                putNotDefault(ct, CT_STRAND_FIELD, consequenceType.getStrand(), DEFAULT_STRAND_VALUE);
                putNotNull(ct, CT_BIOTYPE_FIELD, consequenceType.getBiotype());
                putNotNull(ct, CT_TRANSCRIPT_ANNOT_FLAGS, consequenceType.getTranscriptAnnotationFlags());
                putNotNull(ct, CT_C_DNA_POSITION_FIELD, consequenceType.getCdnaPosition());
                putNotNull(ct, CT_CDS_POSITION_FIELD, consequenceType.getCdsPosition());

                if (consequenceType.getSequenceOntologyTerms() != null) {
                    List<Integer> soAccession = new LinkedList<>();
                    for (SequenceOntologyTerm entry : consequenceType.getSequenceOntologyTerms()) {
                        soAccession.add(ConsequenceTypeMappings.termToAccession.get(entry.getName()));
                    }
                    putNotNull(ct, CT_SO_ACCESSION_FIELD, soAccession);
                }
                //Protein annotation
                if (consequenceType.getProteinVariantAnnotation() != null) {
                    putNotNull(ct, CT_AA_POSITION_FIELD, consequenceType.getProteinVariantAnnotation().getPosition());
                    putNotNull(ct, CT_AA_REFERENCE_FIELD, consequenceType.getProteinVariantAnnotation().getReference());
                    putNotNull(ct, CT_AA_ALTERNATE_FIELD, consequenceType.getProteinVariantAnnotation().getAlternate());
                    //Protein substitution region score
                    if (consequenceType.getProteinVariantAnnotation().getSubstitutionScores() != null) {
                        List<Document> proteinSubstitutionScores = new LinkedList<>();
                        for (Score score : consequenceType.getProteinVariantAnnotation().getSubstitutionScores()) {
                            if (score != null) {
//                                if (score.getSource().equals("polyphen")) {
//                                    putNotNull(ct, CT_PROTEIN_POLYPHEN_FIELD, convertScoreToStorage(score.getScore(), null,
//                                            score.getDescription()));
//                                } else if (score.getSource().equals("sift")) {
//                                    putNotNull(ct, CT_PROTEIN_SIFT_FIELD, convertScoreToStorage(score.getScore(), null,
//                                            score.getDescription()));
//                                } else {
                                    proteinSubstitutionScores.add(convertScoreToStorage(score));
//                                }
                            }
                        }
                        putNotNull(ct, CT_PROTEIN_SUBSTITUTION_SCORE_FIELD, proteinSubstitutionScores);
                    }
                    putNotNull(ct, CT_PROTEIN_KEYWORDS, consequenceType.getProteinVariantAnnotation().getKeywords());

                    List<ProteinFeature> features = consequenceType.getProteinVariantAnnotation().getFeatures();
                    if (features != null) {
                        List<Document> documentFeatures = new ArrayList<>(features.size());
                        for (ProteinFeature feature : features) {
                            Document documentFeature = new Document();
                            putNotNull(documentFeature, CT_PROTEIN_FEATURE_ID_FIELD, feature.getId());
                            putNotNull(documentFeature, CT_PROTEIN_FEATURE_START_FIELD, feature.getStart());
                            putNotNull(documentFeature, CT_PROTEIN_FEATURE_END_FIELD, feature.getEnd());
                            putNotNull(documentFeature, CT_PROTEIN_FEATURE_TYPE_FIELD, feature.getType());
                            putNotNull(documentFeature, CT_PROTEIN_FEATURE_DESCRIPTION_FIELD, feature.getDescription());
                            documentFeatures.add(documentFeature);
                        }
                        putNotNull(ct, CT_PROTEIN_FEATURE_FIELD, documentFeatures);
                    }

                }

                cts.add(ct);

                if (consequenceType.getGeneName() != null && !consequenceType.getGeneName().isEmpty()) {
                    xrefs.add(convertXrefToStorage(consequenceType.getGeneName(), "HGNC"));
                }
                if (consequenceType.getEnsemblGeneId() != null && !consequenceType.getEnsemblGeneId().isEmpty()) {
                    xrefs.add(convertXrefToStorage(consequenceType.getEnsemblGeneId(), "ensemblGene"));
                }
                if (consequenceType.getEnsemblTranscriptId() != null && !consequenceType.getEnsemblTranscriptId().isEmpty()) {
                    xrefs.add(convertXrefToStorage(consequenceType.getEnsemblTranscriptId(), "ensemblTranscript"));
                }

            }
            putNotNull(document, CONSEQUENCE_TYPE_FIELD, cts);
        }

        //Conserved region score
        if (variantAnnotation.getConservation() != null) {
            List<Document> conservedRegionScores = new LinkedList<>();
            for (Score score : variantAnnotation.getConservation()) {
                if (score != null) {
                    conservedRegionScores.add(convertScoreToStorage(score));
                }
            }
            putNotNull(document, CONSERVED_REGION_SCORE_FIELD, conservedRegionScores);
        }

        // Gene trait association
        if (variantAnnotation.getGeneTraitAssociation() != null) {
            List<Document> geneTraitAssociations = new LinkedList<>();
            for (GeneTraitAssociation geneTraitAssociation : variantAnnotation.getGeneTraitAssociation()) {
                if (geneTraitAssociation != null) {
                    Document d = new Document();
                    putNotNull(d, GENE_TRAIT_ID_FIELD, geneTraitAssociation.getId());
                    putNotNull(d, GENE_TRAIT_NAME_FIELD, geneTraitAssociation.getName());
                    putNotNull(d, GENE_TRAIT_SCORE_FIELD, geneTraitAssociation.getScore());
                    putNotNull(d, GENE_TRAIT_HPO_FIELD, geneTraitAssociation.getHpo());
//                    putNotNull(d, GENE_TRAIT_PUBMEDS_FIELD, geneTraitAssociation.getNumberOfPubmeds());
                    putNotNull(d, GENE_TRAIT_TYPES_FIELD, geneTraitAssociation.getAssociationTypes());
//                    putNotNull(d, GENE_TRAIT_SOURCES_FIELD, geneTraitAssociation.getSources());
                    putNotNull(d, GENE_TRAIT_SOURCE_FIELD, geneTraitAssociation.getSource());

                    geneTraitAssociations.add(d);
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
                    Document drugDbObject = new Document(DRUG_GENE_FIELD, geneDrugInteraction.getGeneName());
                    putNotNull(drugDbObject, DRUG_NAME_FIELD, geneDrugInteraction.getDrugName());
                    putNotDefault(drugDbObject, DRUG_SOURCE_FIELD, geneDrugInteraction.getSource(), DEFAULT_DRUB_SOURCE);
                    putNotNull(drugDbObject, DRUG_STUDY_TYPE_FIELD, geneDrugInteraction.getStudyType());
                    drugGeneInteractions.add(drugDbObject);
                }
            }
            putNotNull(document, DRUG_FIELD, drugGeneInteractions);
        }

        //XREFs
        if (variantAnnotation.getXrefs() != null) {
            for (Xref xref : variantAnnotation.getXrefs()) {
                xrefs.add(convertXrefToStorage(xref.getId(), xref.getSource()));
            }
        }
        putNotNull(document, XREFS_FIELD, xrefs);

        //Functional score
        if (variantAnnotation.getFunctionalScore() != null) {
            List<Document> scores = new ArrayList<>(variantAnnotation.getFunctionalScore().size());
            for (Score score : variantAnnotation.getFunctionalScore()) {
                Document documentScore = new Document();
                putNotNull(documentScore, SCORE_SCORE_FIELD, score.getScore());
                putNotNull(documentScore, SCORE_SOURCE_FIELD, score.getSource());
                scores.add(documentScore);
            }
            putNotNull(document, FUNCTIONAL_SCORE, scores);
        }

        //Clinical Data
        Document clinicalDocument = new Document();
        if (variantAnnotation.getVariantTraitAssociation() != null) {
            putNotNull(clinicalDocument, CLINICAL_COSMIC_FIELD,
                    generateClinicalDBList(variantAnnotation.getVariantTraitAssociation().getCosmic()));
            putNotNull(clinicalDocument, CLINICAL_GWAS_FIELD,
                    generateClinicalDBList(variantAnnotation.getVariantTraitAssociation().getGwas()));
            putNotNull(clinicalDocument, CLINICAL_CLINVAR_FIELD,
                    generateClinicalDBList(variantAnnotation.getVariantTraitAssociation().getClinvar()));
        }
        if (!clinicalDocument.isEmpty()) {
            document.put(CLINICAL_DATA_FIELD, clinicalDocument);
        }

        return document;
    }

    private <T> List generateClinicalDBList(List<T> objectList) {
        List list = new ArrayList(objectList.size());
        if (objectList != null) {
            for (T object : objectList) {
                try {
                    if (object instanceof GenericRecord) {
                        list.add(Document.parse(object.toString()));
                    } else {
                        list.add(Document.parse(writer.writeValueAsString(object)));
                    }
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                    logger.error("Error serializing Clinical Data " + object.getClass(), e);
                }
            }
            return list;
        }
        return null;
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
        return dbObject;
    }

    private Document convertXrefToStorage(String id, String source) {
        Document dbObject = new Document(XREF_ID_FIELD, id);
        dbObject.put(XREF_SOURCE_FIELD, source);
        return dbObject;
    }


}
