package org.opencb.opencga.storage.mongodb.variant;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.opencb.biodata.models.variant.annotation.*;
import org.opencb.datastore.core.ComplexTypeConverter;

import java.util.*;

/**
 * Created by jacobo on 13/01/15.
 */
public class DBObjectToVariantAnnotationConverter implements ComplexTypeConverter<VariantAnnotation, DBObject> {

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

    public static final String XREFS_FIELD = "xrefs";
    public final static String XREF_ID_FIELD = "id";
    public final static String XREF_SOURCE_FIELD = "src";

    public static final String PROTEIN_SUBSTITUTION_SCORE_FIELD = "ps_score";
    public static final String CONSERVED_REGION_SCORE_FIELD = "cr_score";
    public final static String SCORE_SCORE_FIELD = "sc";
    public final static String SCORE_SOURCE_FIELD = "src";
    public final static String SCORE_DESCRIPTION_FIELD = "desc";

    private final ObjectMapper jsonObjectMapper;

    public DBObjectToVariantAnnotationConverter() {
        jsonObjectMapper = new ObjectMapper();
        jsonObjectMapper.configure(SerializationFeature.WRITE_NULL_MAP_VALUES, false);
        jsonObjectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
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
                            soAccessionNames));
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
        va.setConservedRegionScores(conservedRegionScores);

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

        return va;
    }

    @Override
    public DBObject convertToStorageType(VariantAnnotation object) {
        DBObject dbObject = new BasicDBObject();
        Set<DBObject> xrefs = new HashSet<>();
        List<DBObject> cts = new LinkedList<>();

        //ID
        if (object.getId() != null && !object.getId().isEmpty()) {
            xrefs.add(convertXrefToStorage(object.getId(), "dbSNP"));
        }

        //ConsequenceType
        if (object.getConsequenceTypes() != null) {
            List<ConsequenceType> consequenceTypes = object.getConsequenceTypes();
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
                            proteinSubstitutionScores.add(convertScoreToStorage(score));
                        }
                    }
                    putNotNull(ct, PROTEIN_SUBSTITUTION_SCORE_FIELD, proteinSubstitutionScores);
                }


                cts.add(ct);

                if (consequenceType.getGeneName() != null) {
                    xrefs.add(convertXrefToStorage(consequenceType.getGeneName(), "HGNC"));
                }
                if (consequenceType.getEnsemblGeneId() != null) {
                    xrefs.add(convertXrefToStorage(consequenceType.getEnsemblGeneId(), "ensemblGene"));
                }
                if (consequenceType.getEnsemblTranscriptId() != null) {
                    xrefs.add(convertXrefToStorage(consequenceType.getEnsemblTranscriptId(), "ensemblTranscript"));
                }

            }
            putNotNull(dbObject, CONSEQUENCE_TYPE_FIELD, cts);
        }

        //Conserved region score
        if (object.getConservedRegionScores() != null) {
            List<DBObject> conservedRegionScores = new LinkedList<>();
            for (Score score : object.getConservedRegionScores()) {
                if (score != null) {
                    conservedRegionScores.add(convertScoreToStorage(score));
                }
            }
            putNotNull(dbObject, CONSERVED_REGION_SCORE_FIELD, conservedRegionScores);
        }


        //XREFs
        if(object.getXrefs() != null) {
            for (Xref xref : object.getXrefs()) {
                xrefs.add(convertXrefToStorage(xref.getId(), xref.getSrc()));
            }
        }
        putNotNull(dbObject, XREFS_FIELD, xrefs);

        return dbObject;
    }

    private DBObject convertScoreToStorage(Score score) {
        DBObject dbObject = new BasicDBObject(SCORE_SCORE_FIELD, score.getScore());
        putNotNull(dbObject, SCORE_SOURCE_FIELD, score.getSource());
        putNotNull(dbObject, SCORE_DESCRIPTION_FIELD, score.getDescription());
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
