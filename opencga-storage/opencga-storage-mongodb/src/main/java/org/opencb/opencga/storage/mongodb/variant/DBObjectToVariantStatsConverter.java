package org.opencb.opencga.storage.mongodb.variant;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.datastore.core.ComplexTypeConverter;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class DBObjectToVariantStatsConverter implements ComplexTypeConverter<VariantStats, DBObject> {

    public final static String COHORT_ID = "ci";
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

    public Map<String, VariantStats> convertCohortsToDataModelType(DBObject cohortsStats, Variant variant) {
        Map<String, VariantStats> cohortStats = new LinkedHashMap<>();
        if (cohortsStats instanceof List) {
            List<DBObject> cohortStatsList = ((List) cohortsStats);
            for (DBObject vs : cohortStatsList) {
                VariantStats variantStats = convertToDataModelType(vs);
                if (variant != null) {
                    variantStats.setRefAllele(variant.getReference());
                    variantStats.setAltAllele(variant.getAlternate());
                    variantStats.setVariantType(variant.getType());
                }
                cohortStats.put((String) vs.get(DBObjectToVariantStatsConverter.COHORT_ID), variantStats);
            }
        }
        return cohortStats;
    }
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