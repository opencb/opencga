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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import org.apache.avro.generic.GenericRecord;
import org.opencb.biodata.models.variant.annotation.ConsequenceTypeMappings;
import org.opencb.biodata.models.variant.avro.*;
import org.opencb.datastore.core.ComplexTypeConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by jacobo on 13/01/15.
 */
public class DBObjectToVariantAnnotationConverter implements ComplexTypeConverter<VariantAnnotation, DBObject> {

    public static final String ANNOT_ID_FIELD = "id";

    public static final String CONSEQUENCE_TYPE_FIELD = "ct";
    public static final String GENE_NAME_FIELD = "gn";
    public static final String ENSEMBL_GENE_ID_FIELD = "ensg";
    public static final String ENSEMBL_TRANSCRIPT_ID_FIELD = "enst";
    public static final String RELATIVE_POS_FIELD = "relPos";
    public static final String CODON_FIELD = "codon";
    public static final String STRAND_FIELD = "strand";
    public static final String BIOTYPE_FIELD = "bt";
    public static final String C_DNA_POSITION_FIELD = "cDnaPos";
    public static final String CDS_POSITION_FIELD = "cdsPos";
    public static final String AA_POSITION_FIELD = "aaPos";
    public static final String AA_REFERENCE_FIELD = "aaRef";
    public static final String AA_ALTERNATE_FIELD = "aaAlt";
    public static final String SO_ACCESSION_FIELD = "so";
    public static final String PROTEIN_SUBSTITUTION_SCORE_FIELD = "ps_score";
    public static final String POLYPHEN_FIELD = "polyphen";
    public static final String SIFT_FIELD = "sift";

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
    public static final String DRUG_FIELD = "drug";
    public static final String DRUG_NAME_FIELD = "dn";
    public static final String DRUG_SOURCE_FIELD = "src";
    public static final String SCORE_SCORE_FIELD = "sc";
    public static final String SCORE_SOURCE_FIELD = "src";
    public static final String SCORE_DESCRIPTION_FIELD = "desc";

    public static final String CLINICAL_DATA_FIELD = "clinical";
    public static final String COSMIC_FIELD = "cosmic";
    public static final String GWAS_FIELD = "gwas";
    public static final String CLINVAR_FIELD = "clinvar";

    public static final String DEFAULT_STRAND_VALUE = "+";

    private final ObjectMapper jsonObjectMapper;
    private final ObjectWriter writer;

    protected static Logger logger = LoggerFactory.getLogger(DBObjectToVariantAnnotationConverter.class);

    public DBObjectToVariantAnnotationConverter() {
        jsonObjectMapper = new ObjectMapper();
        jsonObjectMapper.configure(SerializationFeature.WRITE_NULL_MAP_VALUES, false);
        jsonObjectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        writer = jsonObjectMapper.writer();
    }

    @Override
    public VariantAnnotation convertToDataModelType(DBObject object) {
        VariantAnnotation va = new VariantAnnotation();

        //ConsequenceType
        List<ConsequenceType> consequenceTypes = new LinkedList<>();
        Object cts = object.get(CONSEQUENCE_TYPE_FIELD);
        if (cts != null && cts instanceof BasicDBList) {
            for (Object o : ((BasicDBList) cts)) {
                if (o instanceof DBObject) {
                    DBObject ct = (DBObject) o;

                    //SO accession name
                    List<String> soAccessionNames = new LinkedList<>();
                    if (ct.containsField(SO_ACCESSION_FIELD)) {
                        if (ct.get(SO_ACCESSION_FIELD) instanceof List) {
                            List<Integer> list = (List) ct.get(SO_ACCESSION_FIELD);
                            for (Integer so : list) {
                                soAccessionNames.add(ConsequenceTypeMappings.accessionToTerm.get(so));
                            }
                        } else {
                            soAccessionNames.add(ConsequenceTypeMappings.accessionToTerm.get(ct.get(SO_ACCESSION_FIELD)));
                        }
                    }

                    //ProteinSubstitutionScores
                    List<Score> proteinSubstitutionScores = new LinkedList<>();
                    if (ct.containsField(PROTEIN_SUBSTITUTION_SCORE_FIELD)) {
                        List<DBObject> list = (List) ct.get(PROTEIN_SUBSTITUTION_SCORE_FIELD);
                        for (DBObject dbObject : list) {
                            proteinSubstitutionScores.add(new Score(
                                    getDefault(dbObject, SCORE_SCORE_FIELD, 0.0),
                                    getDefault(dbObject, SCORE_SOURCE_FIELD, ""),
                                    getDefault(dbObject, SCORE_DESCRIPTION_FIELD, "")
                            ));
                        }
                    }
                    if (ct.containsField(POLYPHEN_FIELD)) {
                        DBObject dbObject = (DBObject) ct.get(POLYPHEN_FIELD);
                        proteinSubstitutionScores.add(new Score(getDefault(dbObject, SCORE_SCORE_FIELD, 0.0),
                                "polyphen",
                                getDefault(dbObject, SCORE_DESCRIPTION_FIELD, "")));
                    }
                    if (ct.containsField(SIFT_FIELD)) {
                        DBObject dbObject = (DBObject) ct.get(SIFT_FIELD);
                        proteinSubstitutionScores.add(new Score(getDefault(dbObject, SCORE_SCORE_FIELD, 0.0),
                                "sift",
                                getDefault(dbObject, SCORE_DESCRIPTION_FIELD, "")));
                    }


                    consequenceTypes.add(buildConsequenceType(
                            getDefault(ct, GENE_NAME_FIELD, "") /*.toString()*/,
                            getDefault(ct, ENSEMBL_GENE_ID_FIELD, "") /*.toString()*/,
                            getDefault(ct, ENSEMBL_TRANSCRIPT_ID_FIELD, "") /*.toString()*/,
                            getDefault(ct, STRAND_FIELD, "+") /*.toString()*/,
                            getDefault(ct, BIOTYPE_FIELD, "") /*.toString()*/,
                            getDefault(ct, C_DNA_POSITION_FIELD, 0),
                            getDefault(ct, CDS_POSITION_FIELD, 0),
                            getDefault(ct, CODON_FIELD, ""), getDefault(ct, AA_POSITION_FIELD, 0),
                            getDefault(ct, AA_REFERENCE_FIELD, "") /*.toString() */,
                            getDefault(ct, AA_ALTERNATE_FIELD, "") /*.toString() */,
                            /*.toString() */
                            proteinSubstitutionScores,
                            soAccessionNames));
                }
            }

        }
        va.setConsequenceTypes(consequenceTypes);

        //Conserved Region Scores
        List<Score> conservedRegionScores = new LinkedList<>();
        if (object.containsField(CONSERVED_REGION_SCORE_FIELD)) {
            List<DBObject> list = (List) object.get(CONSERVED_REGION_SCORE_FIELD);
            for (DBObject dbObject : list) {
                conservedRegionScores.add(new Score(
                        getDefault(dbObject, SCORE_SCORE_FIELD, 0.0),
                        getDefault(dbObject, SCORE_SOURCE_FIELD, ""),
                        getDefault(dbObject, SCORE_DESCRIPTION_FIELD, "")
                ));
            }
        }
        va.setConservation(conservedRegionScores);

        //Population frequencies
        List<PopulationFrequency> populationFrequencies = new LinkedList<>();
        if (object.containsField(POPULATION_FREQUENCIES_FIELD)) {
            List<DBObject> list = (List) object.get(POPULATION_FREQUENCIES_FIELD);
            for (DBObject dbObject : list) {
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

        // Drug-Gene Interactions
        List<GeneDrugInteraction> drugs = new LinkedList<>();
        if (object.containsField(DRUG_FIELD)) {
            List<DBObject> list = (List) object.get(DRUG_FIELD);
            for (DBObject dbObject : list) {
                //drugs.add(dbObject.toMap());
                drugs.add(new GeneDrugInteraction(getDefault(dbObject, GENE_NAME_FIELD, ""),
                        getDefault(dbObject, DRUG_NAME_FIELD, ""), "dgidb",
                        getDefault(dbObject, DRUG_SOURCE_FIELD, ""), ""));  // "convertToStorageType" stores the study_type within the
                // DRUG_SOURCE_FIELD

            }
        }
        va.setGeneDrugInteraction(drugs);

        //XREfs
        List<Xref> xrefs = new LinkedList<>();
        Object xrs = object.get(XREFS_FIELD);
        if (xrs != null && xrs instanceof BasicDBList) {
            for (Object o : (BasicDBList) xrs) {
                if (o instanceof DBObject) {
                    DBObject xref = (DBObject) o;

                    xrefs.add(new Xref(
                            (String) xref.get(XREF_ID_FIELD),
                            (String) xref.get(XREF_SOURCE_FIELD))
                    );
                }
            }
        }
        va.setXrefs(xrefs);


        //Clinical Data
        if (object.containsField(CLINICAL_DATA_FIELD)) {
            va.setVariantTraitAssociation(parseClinicalData((DBObject) object.get(CLINICAL_DATA_FIELD)));
        }

        return va;
    }

    private ConsequenceType buildConsequenceType(String geneName, String ensemblGeneId, String ensemblTranscriptId, String strand,
                                                 String biotype, Integer cDnaPosition, Integer cdsPosition, String codon,
                                                 Integer aaPosition, String aaReference, String aaAlternate,
                                                 List<Score> proteinSubstitutionScores, List<String> soNameList) {
        List<SequenceOntologyTerm> soTerms = new ArrayList<>(soNameList.size());
        for (String soName : soNameList) {
            soTerms.add(new SequenceOntologyTerm(ConsequenceTypeMappings.getSoAccessionString(soName), soName));
        }
        ProteinVariantAnnotation proteinVariantAnnotation = new ProteinVariantAnnotation(null, null, aaPosition,
                aaReference, aaAlternate, null, null, proteinSubstitutionScores, null, null);
        return new ConsequenceType(geneName, ensemblGeneId, ensemblTranscriptId, strand, biotype, Arrays.asList(), cDnaPosition,
                cdsPosition, codon, proteinVariantAnnotation, soTerms);
    }

    private VariantTraitAssociation parseClinicalData(DBObject clinicalData) {
        if (clinicalData != null) {
            int size = 0;
            VariantTraitAssociation variantTraitAssociation = new VariantTraitAssociation();
            BasicDBList cosmicDBList = (BasicDBList) clinicalData.get(COSMIC_FIELD);
            if (cosmicDBList != null) {
                List<Cosmic> cosmicList = new ArrayList<>(cosmicDBList.size());
                for (Object object : cosmicDBList) {
                    cosmicList.add(jsonObjectMapper.convertValue(object, Cosmic.class));
                }
                size += cosmicList.size();
                variantTraitAssociation.setCosmic(cosmicList);
            }
            BasicDBList gwasDBList = (BasicDBList) clinicalData.get(GWAS_FIELD);
            if (gwasDBList != null) {
                List<Gwas> gwasList = new ArrayList<>(gwasDBList.size());
                for (Object object : gwasDBList) {
                    gwasList.add(jsonObjectMapper.convertValue(object, Gwas.class));
                }
                size += gwasList.size();
                variantTraitAssociation.setGwas(gwasList);
            }
            BasicDBList clinvarDBList = (BasicDBList) clinicalData.get(CLINVAR_FIELD);
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
    public DBObject convertToStorageType(VariantAnnotation variantAnnotation) {
        DBObject dbObject = new BasicDBObject();
        Set<DBObject> xrefs = new HashSet<>();
        List<DBObject> cts = new LinkedList<>();

        //Annotation ID
        dbObject.put(ANNOT_ID_FIELD, "?");

        //Variant ID
        if (variantAnnotation.getId() != null && !variantAnnotation.getId().isEmpty()) {
            xrefs.add(convertXrefToStorage(variantAnnotation.getId(), "dbSNP"));
        }

        //ConsequenceType
        if (variantAnnotation.getConsequenceTypes() != null) {
            List<ConsequenceType> consequenceTypes = variantAnnotation.getConsequenceTypes();
            for (ConsequenceType consequenceType : consequenceTypes) {
                DBObject ct = new BasicDBObject();

                putNotNull(ct, GENE_NAME_FIELD, consequenceType.getGeneName());
                putNotNull(ct, ENSEMBL_GENE_ID_FIELD, consequenceType.getEnsemblGeneId());
                putNotNull(ct, ENSEMBL_TRANSCRIPT_ID_FIELD, consequenceType.getEnsemblTranscriptId());
//                putNotNull(ct, RELATIVE_POS_FIELD, consequenceType.getRelativePosition());
                putNotNull(ct, CODON_FIELD, consequenceType.getCodon());
                putNotDefault(ct, STRAND_FIELD, consequenceType.getStrand(), DEFAULT_STRAND_VALUE);
                putNotNull(ct, BIOTYPE_FIELD, consequenceType.getBiotype());
                putNotNull(ct, C_DNA_POSITION_FIELD, consequenceType.getCdnaPosition());
                putNotNull(ct, CDS_POSITION_FIELD, consequenceType.getCdsPosition());

                if (consequenceType.getSequenceOntologyTerms() != null) {
                    List<Integer> soAccession = new LinkedList<>();
                    for (SequenceOntologyTerm entry : consequenceType.getSequenceOntologyTerms()) {
                        soAccession.add(ConsequenceTypeMappings.termToAccession.get(entry.getName()));
                    }
                    putNotNull(ct, SO_ACCESSION_FIELD, soAccession);
                }
                //Protein annotation
                if (consequenceType.getProteinVariantAnnotation() != null) {
                    putNotNull(ct, AA_POSITION_FIELD, consequenceType.getProteinVariantAnnotation().getPosition());
                    putNotNull(ct, AA_REFERENCE_FIELD, consequenceType.getProteinVariantAnnotation().getReference());
                    putNotNull(ct, AA_ALTERNATE_FIELD, consequenceType.getProteinVariantAnnotation().getAlternate());
                    //Protein substitution region score
                    if (consequenceType.getProteinVariantAnnotation().getSubstitutionScores() != null) {
                        List<DBObject> proteinSubstitutionScores = new LinkedList<>();
                        for (Score score : consequenceType.getProteinVariantAnnotation().getSubstitutionScores()) {
                            if (score != null) {
                                if (score.getSource().equals("polyphen")) {
                                    putNotNull(ct, POLYPHEN_FIELD, convertScoreToStorage(score.getScore(), null, score.getDescription()));
                                } else if (score.getSource().equals("sift")) {
                                    putNotNull(ct, SIFT_FIELD, convertScoreToStorage(score.getScore(), null, score.getDescription()));
                                } else {
                                    proteinSubstitutionScores.add(convertScoreToStorage(score));
                                }
                            }
                        }
                        putNotNull(ct, PROTEIN_SUBSTITUTION_SCORE_FIELD, proteinSubstitutionScores);
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
            putNotNull(dbObject, CONSEQUENCE_TYPE_FIELD, cts);
        }

        //Conserved region score
        if (variantAnnotation.getConservation() != null) {
            List<DBObject> conservedRegionScores = new LinkedList<>();
            for (Score score : variantAnnotation.getConservation()) {
                if (score != null) {
                    conservedRegionScores.add(convertScoreToStorage(score));
                }
            }
            putNotNull(dbObject, CONSERVED_REGION_SCORE_FIELD, conservedRegionScores);
        }

        //Population frequencies
        if (variantAnnotation.getPopulationFrequencies() != null) {
            List<DBObject> populationFrequencies = new LinkedList<>();
            for (PopulationFrequency populationFrequency : variantAnnotation.getPopulationFrequencies()) {
                if (populationFrequency != null) {
                    populationFrequencies.add(convertPopulationFrequencyToStorage(populationFrequency));
                }
            }
            putNotNull(dbObject, POPULATION_FREQUENCIES_FIELD, populationFrequencies);
        }

        // Drug-Gene Interactions
        if (variantAnnotation.getGeneDrugInteraction() != null) {
            List<DBObject> drugGeneInteractions = new LinkedList<>();
            List<GeneDrugInteraction> geneDrugInteractionList = variantAnnotation.getGeneDrugInteraction();
            if (geneDrugInteractionList != null) {
                for (GeneDrugInteraction geneDrugInteraction : geneDrugInteractionList) {
                    DBObject drugDbObject = new BasicDBObject(GENE_NAME_FIELD, geneDrugInteraction.getGeneName());
                    putNotNull(drugDbObject, DRUG_NAME_FIELD, geneDrugInteraction.getDrugName());
                    putNotNull(drugDbObject, "src", geneDrugInteraction.getStudyType());
                    drugGeneInteractions.add(drugDbObject);
                }
            }
            putNotNull(dbObject, DRUG_FIELD, drugGeneInteractions);
        }

        //XREFs
        if (variantAnnotation.getXrefs() != null) {
            for (Xref xref : variantAnnotation.getXrefs()) {
                xrefs.add(convertXrefToStorage(xref.getId(), xref.getSource()));
            }
        }
        putNotNull(dbObject, XREFS_FIELD, xrefs);

        //Clinical Data
        BasicDBObject clinicalDBObject = new BasicDBObject();
        if (variantAnnotation.getVariantTraitAssociation() != null) {
            putNotNull(clinicalDBObject, COSMIC_FIELD,
                    generateClinicalDBList(variantAnnotation.getVariantTraitAssociation().getCosmic()));
            putNotNull(clinicalDBObject, GWAS_FIELD,
                    generateClinicalDBList(variantAnnotation.getVariantTraitAssociation().getGwas()));
            putNotNull(clinicalDBObject, CLINVAR_FIELD,
                    generateClinicalDBList(variantAnnotation.getVariantTraitAssociation().getClinvar()));
        }
        if (!clinicalDBObject.isEmpty()) {
            dbObject.put(CLINICAL_DATA_FIELD, clinicalDBObject);
        }

        return dbObject;
    }

    private <T> BasicDBList generateClinicalDBList(List<T> objectList) {
        BasicDBList basicDBList = new BasicDBList();
        if (objectList != null) {
            for (T object : objectList) {
                try {
                    if (object instanceof GenericRecord) {
                        basicDBList.add(JSON.parse(object.toString()));
                    } else {
                        basicDBList.add(JSON.parse(writer.writeValueAsString(object)));
                    }
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                    logger.error("Error serializing Clinical Data " + object.getClass(), e);
                }
            }
            return basicDBList;
        }
        return null;
    }

    private DBObject convertScoreToStorage(Score score) {
        return convertScoreToStorage(score.getScore(), score.getSource(), score.getDescription());
    }

    private DBObject convertScoreToStorage(double score, String source, String description) {
        DBObject dbObject = new BasicDBObject(SCORE_SCORE_FIELD, score);
        putNotNull(dbObject, SCORE_SOURCE_FIELD, source);
        putNotNull(dbObject, SCORE_DESCRIPTION_FIELD, description);
        return dbObject;
    }

    private DBObject convertPopulationFrequencyToStorage(PopulationFrequency populationFrequency) {
        DBObject dbObject = new BasicDBObject(POPULATION_FREQUENCY_STUDY_FIELD, populationFrequency.getStudy());
        putNotNull(dbObject, POPULATION_FREQUENCY_POP_FIELD, populationFrequency.getPopulation());
        putNotNull(dbObject, POPULATION_FREQUENCY_REFERENCE_FREQUENCY_FIELD, populationFrequency.getRefAlleleFreq());
        putNotNull(dbObject, POPULATION_FREQUENCY_ALTERNATE_FREQUENCY_FIELD, populationFrequency.getAltAlleleFreq());
        return dbObject;
    }

    private DBObject convertXrefToStorage(String id, String source) {
        DBObject dbObject = new BasicDBObject(XREF_ID_FIELD, id);
        dbObject.put(XREF_SOURCE_FIELD, source);
        return dbObject;
    }


    //Utils
    private void putNotNull(DBObject dbObject, String key, Object obj) {
        if (obj != null) {
            dbObject.put(key, obj);
        }
    }

    private void putNotNull(DBObject dbObject, String key, Collection obj) {
        if (obj != null && !obj.isEmpty()) {
            dbObject.put(key, obj);
        }
    }

    private void putNotNull(DBObject dbObject, String key, String obj) {
        if (obj != null && !obj.isEmpty()) {
            dbObject.put(key, obj);
        }
    }

    private void putNotNull(DBObject dbObject, String key, Integer obj) {
        if (obj != null && obj != 0) {
            dbObject.put(key, obj);
        }
    }

    private void putNotDefault(DBObject dbObject, String key, String obj, Object defaultValue) {
        if (obj != null && !obj.isEmpty() && !obj.equals(defaultValue)) {
            dbObject.put(key, obj);
        }
    }

    private String getDefault(DBObject object, String key, String defaultValue) {
        Object o = object.get(key);
        if (o != null) {
            return o.toString();
        } else {
            return defaultValue;
        }
    }

    private int getDefault(DBObject object, String key, int defaultValue) {
        Object o = object.get(key);
        if (o != null) {
            if (o instanceof Integer) {
                return (Integer) o;
            } else {
                try {
                    return Integer.parseInt(o.toString());
                } catch (Exception e) {
                    return defaultValue;
                }
            }
        } else {
            return defaultValue;
        }
    }

    private double getDefault(DBObject object, String key, double defaultValue) {
        Object o = object.get(key);
        if (o != null) {
            if (o instanceof Double) {
                return (Double) o;
            } else {
                try {
                    return Double.parseDouble(o.toString());
                } catch (Exception e) {
                    return defaultValue;
                }
            }
        } else {
            return defaultValue;
        }
    }

}
