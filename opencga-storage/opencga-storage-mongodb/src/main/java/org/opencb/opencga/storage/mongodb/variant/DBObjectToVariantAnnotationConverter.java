package org.opencb.opencga.storage.mongodb.variant;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import org.opencb.biodata.models.variant.annotation.ConsequenceType;
import org.opencb.biodata.models.variant.annotation.ConsequenceTypeMappings;
import org.opencb.biodata.models.variant.annotation.VariantAnnotation;
import org.opencb.biodata.models.variant.annotation.Xref;
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
    public static final String A_POSITION_FIELD = "aPos";
    public static final String A_CHANGE_FIELD = "aChange";
    public static final String SO_ACCESSION_FIELD = "so";

    public static final String XREFS_FIELD = "xrefs";
    public final static String XREF_ID_FIELD = "id";
    public final static String XREF_SOURCE_FIELD = "src";

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

                    String soa = null;
                    if(ct.containsField(SO_ACCESSION_FIELD)) {
                        Integer so = Integer.parseInt("" + ct.get(SO_ACCESSION_FIELD));
                        soa = ConsequenceTypeMappings.accessionToTerm.get(so);
                    }
                    consequenceTypes.add(new ConsequenceType(
                            (String) ct.get(GENE_NAME_FIELD) /*.toString()*/,
                            (String) ct.get(ENSEMBL_GENE_ID_FIELD) /*.toString()*/,
                            (String) ct.get(ENSEMBL_TRANSCRIPT_ID_FIELD) /*.toString()*/,
                            (String) ct.get(STRAND_FIELD) /*.toString()*/,
                            (String) ct.get(BIOTYPE_FIELD) /*.toString()*/,
                            (Integer) ct.get(C_DNA_POSITION_FIELD),
                            (Integer) ct.get(CDS_POSITION_FIELD),
                            (Integer) ct.get(A_POSITION_FIELD),
                            (String) ct.get(A_CHANGE_FIELD) /*.toString() */,
                            (String) ct.get(CODON_FIELD) /*.toString() */,
                            soa));
                }
            }

        }
        va.setConsequenceTypes(consequenceTypes);

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
        if (object.getId() != null) {
            xrefs.add(convertXrefToStorage(object.getId(), "dbSNP"));
        }

        //ConsequenceType
        List<ConsequenceType> consequenceTypes = object.getConsequenceTypes();
        if (consequenceTypes != null) {
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
                putNotNull(ct, A_POSITION_FIELD, consequenceType.getaPosition());
                putNotNull(ct, A_CHANGE_FIELD, consequenceType.getaChange());
                putNotNull(ct, SO_ACCESSION_FIELD, consequenceType.getSOAccession());

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
            dbObject.put(CONSEQUENCE_TYPE_FIELD, cts);
        }

        //XREFs
        if(object.getXrefs() != null) {
            for (Xref xref : object.getXrefs()) {
                xrefs.add(convertXrefToStorage(xref.getId(), xref.getSrc()));
            }
        }
        dbObject.put(XREFS_FIELD, xrefs);

        return dbObject;
    }

    private DBObject convertXrefToStorage(String id, String source) {
        DBObject dbObject = new BasicDBObject(XREF_ID_FIELD, id);
        dbObject.put(XREF_SOURCE_FIELD, source);
        return dbObject;
    }

    private void putNotNull(DBObject dbObject, String key, Object obj) {
        if(obj != null) {
            dbObject.put(key, obj);
        }
    }
}
