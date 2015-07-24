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
import org.opencb.biodata.models.variant.annotation.*;
import org.opencb.biodata.models.variation.PopulationFrequency;
import org.opencb.cellbase.core.common.drug.Drug;
import org.opencb.datastore.core.ComplexTypeConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by jacobo on 13/01/15.
 */
public class DBObjectToVariantAnnotationConverter implements ComplexTypeConverter<VariantAnnotation, DBObject> {

    public final static String ANNOT_ID_FIELD = "id";

    public final static String CONSEQUENCE_TYPE_FIELD = "ct";
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
    public static final String AA_CHANGE_FIELD = "aaChange";
    public static final String SO_ACCESSION_FIELD = "so";
    public static final String PROTEIN_SUBSTITUTION_SCORE_FIELD = "ps_score";
    public static final String POLYPHEN_FIELD = "polyphen";
    public static final String SIFT_FIELD = "sift";

    public static final String XREFS_FIELD = "xrefs";
    public final static String XREF_ID_FIELD = "id";
    public final static String XREF_SOURCE_FIELD = "src";

    public static final String POPULATION_FREQUENCIES_FIELD = "popFq";
    public static final String POPULATION_FREQUENCY_STUDY_FIELD = "study";
    public static final String POPULATION_FREQUENCY_POP_FIELD = "pop";
    public static final String POPULATION_FREQUENCY_SUPERPOP_FIELD = "superPop";
    public static final String POPULATION_FREQUENCY_REFERENCE_ALLELE_FIELD = "ref";
    public static final String POPULATION_FREQUENCY_ALTERNATE_ALLELE_FIELD = "alt";
    public static final String POPULATION_FREQUENCY_REFERENCE_FREQUENCY_FIELD = "refFq";
    public static final String POPULATION_FREQUENCY_ALTERNATE_FREQUENCY_FIELD = "altFq";

    public static final String CONSERVED_REGION_SCORE_FIELD = "cr_score";
    public static final String DRUG_FIELD = "drug";
    public final static String SCORE_SCORE_FIELD = "sc";
    public final static String SCORE_SOURCE_FIELD = "src";
    public final static String SCORE_DESCRIPTION_FIELD = "desc";

    public static final String CLINICAL_DATA_FIELD = "clinical";

    private final ObjectWriter writer;

    protected static Logger logger = LoggerFactory.getLogger(DBObjectToVariantAnnotationConverter.class);

    public DBObjectToVariantAnnotationConverter() {
        ObjectMapper jsonObjectMapper = new ObjectMapper();
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
        if(cts != null && cts instanceof BasicDBList) {
            for (Object o : ((BasicDBList) cts)) {
                if(o instanceof DBObject) {
                    DBObject ct = (DBObject) o;

                    //SO accession name
                    List<String> soAccessionNames = new LinkedList<>();
                    if(ct.containsField(SO_ACCESSION_FIELD)) {
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
                    if(ct.containsField(PROTEIN_SUBSTITUTION_SCORE_FIELD)) {
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


                    consequenceTypes.add(new ConsequenceType(
                            getDefault(ct, GENE_NAME_FIELD, "") /*.toString()*/,
                            getDefault(ct, ENSEMBL_GENE_ID_FIELD, "") /*.toString()*/,
                            getDefault(ct, ENSEMBL_TRANSCRIPT_ID_FIELD, "") /*.toString()*/,
                            getDefault(ct, STRAND_FIELD, "") /*.toString()*/,
                            getDefault(ct, BIOTYPE_FIELD, "") /*.toString()*/,
                            getDefault(ct, C_DNA_POSITION_FIELD, 0),
                            getDefault(ct, CDS_POSITION_FIELD, 0),
                            getDefault(ct, AA_POSITION_FIELD, 0),
                            getDefault(ct, AA_CHANGE_FIELD, "") /*.toString() */,
                            getDefault(ct, CODON_FIELD, "") /*.toString() */,
                            proteinSubstitutionScores,
                            soAccessionNames, Collections.<ExpressionValue>emptyList()));
                }
            }

        }
        va.setConsequenceTypes(consequenceTypes);

        //Conserved Region Scores
        List<Score> conservedRegionScores = new LinkedList<>();
        if(object.containsField(CONSERVED_REGION_SCORE_FIELD)) {
            List<DBObject> list = (List) object.get(CONSERVED_REGION_SCORE_FIELD);
            for (DBObject dbObject : list) {
                conservedRegionScores.add(new Score(
                        getDefault(dbObject, SCORE_SCORE_FIELD, 0.0),
                        getDefault(dbObject, SCORE_SOURCE_FIELD, ""),
                        getDefault(dbObject, SCORE_DESCRIPTION_FIELD, "")
                ));
            }
        }
        va.setConservationScores(conservedRegionScores);

        //Population frequencies
        List<PopulationFrequency> populationFrequencies = new LinkedList<>();
        if(object.containsField(POPULATION_FREQUENCIES_FIELD)) {
            List<DBObject> list = (List) object.get(POPULATION_FREQUENCIES_FIELD);
            for (DBObject dbObject : list) {
                populationFrequencies.add(new PopulationFrequency(
                        getDefault(dbObject, POPULATION_FREQUENCY_STUDY_FIELD, ""),
                        getDefault(dbObject, POPULATION_FREQUENCY_POP_FIELD, ""),
                        getDefault(dbObject, POPULATION_FREQUENCY_SUPERPOP_FIELD, ""),
                        getDefault(dbObject, POPULATION_FREQUENCY_REFERENCE_ALLELE_FIELD, ""),
                        getDefault(dbObject, POPULATION_FREQUENCY_ALTERNATE_ALLELE_FIELD, ""),
                        (float) getDefault(dbObject, POPULATION_FREQUENCY_REFERENCE_FREQUENCY_FIELD, -1.0),
                        (float) getDefault(dbObject, POPULATION_FREQUENCY_ALTERNATE_FREQUENCY_FIELD, -1.0)
                ));
            }
        }
        va.setPopulationFrequencies(populationFrequencies);

        // Drug-Gene Interactions
        Map<String, List<Object>> drugGeneInteractionMap = new HashMap<>();
        List<Object> drugs = new LinkedList<>();
        if(object.containsField(DRUG_FIELD)) {
            List<DBObject> list = (List) object.get(DRUG_FIELD);
            for (DBObject dbObject : list) {
                drugs.add(dbObject.toMap());
            }
            drugGeneInteractionMap.put("dgidb", drugs);
        }
        va.setGeneDrugInteraction(drugGeneInteractionMap);

        //XREfs
        List<Xref> xrefs = new LinkedList<>();
        Object xrs = object.get(XREFS_FIELD);
        if(xrs != null && xrs instanceof BasicDBList) {
            for (Object o : (BasicDBList) xrs) {
                if(o instanceof DBObject) {
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
            DBObject clinicalData = ((DBObject) object.get(CLINICAL_DATA_FIELD));
            va.setClinical(clinicalData.toMap());
        }

        return va;
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
                putNotNull(ct, RELATIVE_POS_FIELD, consequenceType.getRelativePosition());
                putNotNull(ct, CODON_FIELD, consequenceType.getCodon());
                putNotNull(ct, STRAND_FIELD, consequenceType.getStrand());
                putNotNull(ct, BIOTYPE_FIELD, consequenceType.getBiotype());
                putNotNull(ct, C_DNA_POSITION_FIELD, consequenceType.getcDnaPosition());
                putNotNull(ct, CDS_POSITION_FIELD, consequenceType.getCdsPosition());
                putNotNull(ct, AA_POSITION_FIELD, consequenceType.getAaPosition());
                putNotNull(ct, AA_CHANGE_FIELD, consequenceType.getAaChange());

                if (consequenceType.getSoTerms() != null) {
                    List<Integer> soAccession = new LinkedList<>();
                    for (ConsequenceType.ConsequenceTypeEntry entry : consequenceType.getSoTerms()) {
                        soAccession.add(ConsequenceTypeMappings.termToAccession.get(entry.getSoName()));
                    }
                    putNotNull(ct, SO_ACCESSION_FIELD, soAccession);
                }

                //Protein substitution region score
                if (consequenceType.getProteinSubstitutionScores() != null) {
                    List<DBObject> proteinSubstitutionScores = new LinkedList<>();
                    for (Score score : consequenceType.getProteinSubstitutionScores()) {
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
        if (variantAnnotation.getConservationScores() != null) {
            List<DBObject> conservedRegionScores = new LinkedList<>();
            for (Score score : variantAnnotation.getConservationScores()) {
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
            List<Object> objectList = variantAnnotation.getGeneDrugInteraction().get("dgidb");
            if (objectList != null) {
                for (Object obj : objectList) {
                    Map geneDrugInteraction = (Map)obj;
                    DBObject drugDbObject = new BasicDBObject("gn", geneDrugInteraction.get("geneName"));
                    putNotNull(drugDbObject, "dn", geneDrugInteraction.get("drugName"));
                    putNotNull(drugDbObject, "src", geneDrugInteraction.get("studyType"));
                    drugGeneInteractions.add(drugDbObject);
                }
            }
            putNotNull(dbObject, DRUG_FIELD, drugGeneInteractions);
        }

        //XREFs
        if(variantAnnotation.getXrefs() != null) {
            for (Xref xref : variantAnnotation.getXrefs()) {
                xrefs.add(convertXrefToStorage(xref.getId(), xref.getSrc()));
            }
        }
        putNotNull(dbObject, XREFS_FIELD, xrefs);

        //Clinical Data
        if (variantAnnotation.getClinical() != null) {
            List<DBObject> clinicalData = new LinkedList<>();
            for (Map.Entry<String, Object> entry : variantAnnotation.getClinical().entrySet()) {
                if (entry.getValue() != null) {
                    try {
                        clinicalData.add(new BasicDBObject(entry.getKey(), JSON.parse(writer.writeValueAsString(entry.getValue()))));
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                        logger.error("Error serializing Clinical Data " + entry.getValue().getClass(), e);
                    }
                }
            }
            putNotNull(dbObject, CLINICAL_DATA_FIELD, clinicalData);
        }

        return dbObject;
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
        putNotNull(dbObject, POPULATION_FREQUENCY_POP_FIELD, populationFrequency.getPop());
        putNotNull(dbObject, POPULATION_FREQUENCY_SUPERPOP_FIELD, populationFrequency.getSuperPop());
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
        if(obj != null) {
            dbObject.put(key, obj);
        }
    }

    private void putNotNull(DBObject dbObject, String key, Collection obj) {
        if(obj != null && !obj.isEmpty()) {
            dbObject.put(key, obj);
        }
    }

    private void putNotNull(DBObject dbObject, String key, String obj) {
        if(obj != null && !obj.isEmpty()) {
            dbObject.put(key, obj);
        }
    }

    private void putNotNull(DBObject dbObject, String key, Integer obj) {
        if(obj != null && obj != 0) {
            dbObject.put(key, obj);
        }
    }

    private String getDefault(DBObject object, String key, String defaultValue) {
        Object o = object.get(key);
        if (o != null ) {
            return o.toString();
        } else {
            return defaultValue;
        }
    }

    private int getDefault(DBObject object, String key, int defaultValue) {
        Object o = object.get(key);
        if (o != null ) {
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
        if (o != null ) {
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
