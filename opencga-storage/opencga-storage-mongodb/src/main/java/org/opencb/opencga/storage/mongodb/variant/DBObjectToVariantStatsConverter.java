package org.opencb.opencga.storage.mongodb.variant;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSourceEntry;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.datastore.core.ComplexTypeConverter;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class DBObjectToVariantStatsConverter implements ComplexTypeConverter<VariantStats, DBObject> {

    public final static String COHORT_ID = "ci";
    public final static String STUDY_ID = "sid";
    
    public final static String MAF_FIELD = "maf";
    public final static String MGF_FIELD = "mgf";
    public final static String MAFALLELE_FIELD = "mafAl";
    public final static String MGFGENOTYPE_FIELD = "mgfGt";
    public final static String MISSALLELE_FIELD = "missAl";
    public final static String MISSGENOTYPE_FIELD = "missGt";
    public final static String NUMGT_FIELD = "numGt";
    
    
    @Override
    public VariantStats convertToDataModelType(DBObject object) {
        // Basic fields
        VariantStats stats = new VariantStats();
        stats.setMaf(((Double) object.get(MAF_FIELD)).floatValue());
        stats.setMgf(((Double) object.get(MGF_FIELD)).floatValue());
        stats.setMafAllele((String) object.get(MAFALLELE_FIELD));
        stats.setMgfGenotype((String) object.get(MGFGENOTYPE_FIELD));

        stats.setMissingAlleles((int) object.get(MISSALLELE_FIELD));
        stats.setMissingGenotypes((int) object.get(MISSGENOTYPE_FIELD));

        // Genotype counts
        BasicDBObject genotypes = (BasicDBObject) object.get(NUMGT_FIELD);
        for (Map.Entry<String, Object> o : genotypes.entrySet()) {
            String genotypeStr = o.getKey().replace("-1", ".");
            stats.addGenotype(new Genotype(genotypeStr), (int) o.getValue());
        }

        return stats;
    }

    @Override
    public DBObject convertToStorageType(VariantStats vs) {
        // Basic fields
        BasicDBObject mongoStats = new BasicDBObject(MAF_FIELD, vs.getMaf());
        mongoStats.append(MGF_FIELD, vs.getMgf());
        mongoStats.append(MAFALLELE_FIELD, vs.getMafAllele());
        mongoStats.append(MGFGENOTYPE_FIELD, vs.getMgfGenotype());
        mongoStats.append(MISSALLELE_FIELD, vs.getMissingAlleles());
        mongoStats.append(MISSGENOTYPE_FIELD, vs.getMissingGenotypes());

        // Genotype counts
        BasicDBObject genotypes = new BasicDBObject();
        for (Map.Entry<Genotype, Integer> g : vs.getGenotypesCount().entrySet()) {
            String genotypeStr = g.getKey().toString().replace(".", "-1");
            genotypes.append(genotypeStr, g.getValue());
        }
        mongoStats.append(NUMGT_FIELD, genotypes);
        return mongoStats;
    }

    /**
     * As in mongo, a variant is {studies:[],stats:[]} but the data model is {studies:[stats:[]]} this method doesn't 
     * return anything. Instead, the sourceEntries parameter is filled.
     * @param cohortsStats List from mongo containing VariantStats.
     * @param variant contains allele info to fill the VariantStats.
     * @param sourceEntries will be filled with the cohortStats.
     */
    public void convertCohortsToDataModelType(DBObject cohortsStats, Variant variant, Map<String, VariantSourceEntry> sourceEntries) {
        if (cohortsStats instanceof List) {
            List<DBObject> cohortStatsList = ((List) cohortsStats);
            for (DBObject vs : cohortStatsList) {
                VariantStats variantStats = convertToDataModelType(vs);
                if (variant != null) {
                    variantStats.setRefAllele(variant.getReference());
                    variantStats.setAltAllele(variant.getAlternate());
                    variantStats.setVariantType(variant.getType());
                }
                Map<String, VariantStats> cohortStats = sourceEntries.get(vs.get(STUDY_ID)).getCohortStats();
                cohortStats.put((String) vs.get(COHORT_ID), variantStats);
            }
        }
    }
    /*
    public List convertCohortsToStorageType(Map<String, VariantStats> cohortStats) {
        List<DBObject> cohortsStats = new LinkedList<>();
        VariantStats variantStats;
        for (Map.Entry<String, VariantStats> variantStatsEntry : cohortStats.entrySet()) {
            variantStats = variantStatsEntry.getValue();
            DBObject variantStatsDBObject = convertToStorageType(variantStats);
            variantStatsDBObject.put(DBObjectToVariantStatsConverter.COHORT_ID, variantStatsEntry.getKey());
            cohortsStats.add(variantStatsDBObject);
        }
        return cohortsStats;
    }
    */

    public List convertCohortsToStorageType(Map<String, VariantSourceEntry> sourceEntries) {
        List<DBObject> cohortsStatsList = new LinkedList<>();
        VariantStats variantStats;
        for (String studyId : sourceEntries.keySet()) {
            List list = convertCohortsToStorageType(sourceEntries.get(studyId).getCohortStats(), studyId);
            cohortsStatsList.addAll(list);
        }
        return cohortsStatsList;
    }
    
    public List<DBObject> convertCohortsToStorageType(Map<String, VariantStats> cohortStats, String studyId) {
        List<DBObject> cohortsStatsList = new LinkedList<>();
        VariantStats variantStats;
            for (Map.Entry<String, VariantStats> variantStatsEntry : cohortStats.entrySet()) {
                variantStats = variantStatsEntry.getValue();
                DBObject variantStatsDBObject = convertToStorageType(variantStats);
                variantStatsDBObject.put(DBObjectToVariantStatsConverter.COHORT_ID, variantStatsEntry.getKey());
                variantStatsDBObject.put(DBObjectToVariantStatsConverter.STUDY_ID, studyId);
                cohortsStatsList.add(variantStatsDBObject);
            }
        return cohortsStatsList;
    }
}




/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 *//*
public class DBObjectToVariantStatsConverter implements ComplexTypeConverter<Map<String, VariantStats>, DBObject> {

    public final static String COHORT_ID = "ci";

    public final static String MAF_FIELD = "maf";
    public final static String MGF_FIELD = "mgf";
    public final static String MAFALLELE_FIELD = "mafAl";
    public final static String MGFGENOTYPE_FIELD = "mgfGt";
    public final static String MISSALLELE_FIELD = "missAl";
    public final static String MISSGENOTYPE_FIELD = "missGt";
    public final static String NUMGT_FIELD = "numGt";


    @Override
    public Map<String, VariantStats> convertToDataModelType(DBObject object) {
        Map<String, VariantStats> cohortStats = new LinkedHashMap<>();
        if (object instanceof List) {
            List<DBObject> cohortStatsDBList = ((List) object);
            for (DBObject o : cohortStatsDBList) {
                // Basic fields
                VariantStats stats = getVariantStats(o);
                cohortStats.put((String) o.get(COHORT_ID), stats);
            }
        } else {
            VariantStats stats = getVariantStats(object);

            cohortStats.put("all", stats);
        }

        return cohortStats;
    }

    private VariantStats getVariantStats(DBObject object) {
        // Basic fields
        VariantStats stats = new VariantStats();
        stats.setMaf(((Double) object.get(MAF_FIELD)).floatValue());
        stats.setMgf(((Double) object.get(MGF_FIELD)).floatValue());
        stats.setMafAllele((String) object.get(MAFALLELE_FIELD));
        stats.setMgfGenotype((String) object.get(MGFGENOTYPE_FIELD));

        stats.setMissingAlleles((int) object.get(MISSALLELE_FIELD));
        stats.setMissingGenotypes((int) object.get(MISSGENOTYPE_FIELD));

        // Genotype counts
        BasicDBObject genotypes = (BasicDBObject) object.get(NUMGT_FIELD);
        for (Map.Entry<String, Object> o : genotypes.entrySet()) {
            String genotypeStr = o.getKey().replace("-1", ".");
            stats.addGenotype(new Genotype(genotypeStr), (int) o.getValue());
        }
        return stats;
    }

    @Override
    public DBObject convertToStorageType(Map<String, VariantStats> vsMap) {
        BasicDBList mongoCohortStats = new BasicDBList();
        for (Map.Entry<String, VariantStats> variantStatsEntry : vsMap.entrySet()) {
            VariantStats vs = variantStatsEntry.getValue();

            // Basic fields
            BasicDBObject mongoStats = new BasicDBObject(COHORT_ID, variantStatsEntry.getKey());

            mongoStats.append(MAF_FIELD, vs.getMaf());
            mongoStats.append(MGF_FIELD, vs.getMgf());
            mongoStats.append(MAFALLELE_FIELD, vs.getMafAllele());
            mongoStats.append(MGFGENOTYPE_FIELD, vs.getMgfGenotype());
            mongoStats.append(MISSALLELE_FIELD, vs.getMissingAlleles());
            mongoStats.append(MISSGENOTYPE_FIELD, vs.getMissingGenotypes());

            // Genotype counts
            BasicDBObject genotypes = new BasicDBObject();
            for (Map.Entry<Genotype, Integer> g : vs.getGenotypesCount().entrySet()) {
                String genotypeStr = g.getKey().toString().replace(".", "-1");
                genotypes.append(genotypeStr, g.getValue());
            }
            mongoStats.append(NUMGT_FIELD, genotypes);
            mongoCohortStats.add(mongoStats);
        }
        return mongoCohortStats;
    }

}
*/