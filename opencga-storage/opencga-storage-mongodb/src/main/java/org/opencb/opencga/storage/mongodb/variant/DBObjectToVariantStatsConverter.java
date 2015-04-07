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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Cristina Yenyxe Gonzalez Garcia &lt;cyenyxe@ebi.ac.uk&gt;
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
 */
public class DBObjectToVariantStatsConverter implements ComplexTypeConverter<VariantStats, DBObject> {

    public final static String COHORT_ID = "cid";
    public final static String STUDY_ID = "sid";
    public final static String FILE_ID = "fid";
    
    public final static String MAF_FIELD = "maf";
    public final static String MGF_FIELD = "mgf";
    public final static String MAFALLELE_FIELD = "mafAl";
    public final static String MGFGENOTYPE_FIELD = "mgfGt";
    public final static String MISSALLELE_FIELD = "missAl";
    public final static String MISSGENOTYPE_FIELD = "missGt";
    public final static String NUMGT_FIELD = "numGt";

    protected static Logger logger = LoggerFactory.getLogger(DBObjectToVariantStatsConverter.class);

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
     * return anything. Instead, the sourceEntries within the variant is filled.
     * @param cohortsStats List from mongo containing VariantStats.
     * @param variant contains allele info to fill the VariantStats, and it sourceEntries will be filled.
     */
    public void convertCohortsToDataModelType(DBObject cohortsStats, Variant variant) {
        if (cohortsStats instanceof List) {
            List<DBObject> cohortStatsList = ((List) cohortsStats);
            for (DBObject vs : cohortStatsList) {
                VariantStats variantStats = convertToDataModelType(vs);
                if (variant != null) {
                    variantStats.setRefAllele(variant.getReference());
                    variantStats.setAltAllele(variant.getAlternate());
                    variantStats.setVariantType(variant.getType());
                    String fid = (String) vs.get(FILE_ID);
                    String sid = (String) vs.get(STUDY_ID);
                    String cid = (String) vs.get(COHORT_ID);
                    VariantSourceEntry sourceEntry;
                    if (fid != null && sid != null && cid != null) {
                       sourceEntry = variant.getSourceEntry(fid, sid);
                        if (sourceEntry != null) {
                            Map<String, VariantStats> cohortStats = sourceEntry.getCohortStats();
                            cohortStats.put(cid, variantStats);
                        } else {
                            logger.warn("ignoring non present source entry fileId={}, studyId={}", fid, sid);
                        }
                    } else {
                        logger.error("invalid mongo document: all studyId={}, fileId={}, cohortId={} should be present.", sid, fid, cid);
                    }
                }
            }
        }
    }

    /**
     * converts all the cohortstats within the sourceEntries
     * @param sourceEntries for instance, you can pass in variant.getSourceEntries()
     * @return list of VariantStats (as DBObjects)
     */
    public List<DBObject> convertCohortsToStorageType(Map<String, VariantSourceEntry> sourceEntries) {
        List<DBObject> cohortsStatsList = new LinkedList<>();
        for (String studyIdFileId : sourceEntries.keySet()) {
            VariantSourceEntry sourceEntry = sourceEntries.get(studyIdFileId);
            List<DBObject> list = convertCohortsToStorageType(sourceEntry.getCohortStats(), sourceEntry.getStudyId(), sourceEntry.getFileId());
            cohortsStatsList.addAll(list);
        }
        return cohortsStatsList;
    }

    /**
     * converts just some cohorts stats in one VariantSourceEntry.
     * @param cohortStats for instance, you can pass in sourceEntry.getCohortStats()
     * @param studyId of the source entry
     * @param fileId of the source entry
     * @return list of VariantStats (as DBObjects)
     */
    public List<DBObject> convertCohortsToStorageType(Map<String, VariantStats> cohortStats, String studyId, String fileId) {
        List<DBObject> cohortsStatsList = new LinkedList<>();
        VariantStats variantStats;
        for (Map.Entry<String, VariantStats> variantStatsEntry : cohortStats.entrySet()) {
            variantStats = variantStatsEntry.getValue();
            DBObject variantStatsDBObject = convertToStorageType(variantStats);
            variantStatsDBObject.put(DBObjectToVariantStatsConverter.COHORT_ID, variantStatsEntry.getKey());
            variantStatsDBObject.put(DBObjectToVariantStatsConverter.STUDY_ID, studyId);
            variantStatsDBObject.put(DBObjectToVariantStatsConverter.FILE_ID, fileId);
            cohortsStatsList.add(variantStatsDBObject);
        }
        return cohortsStatsList;
    }
}

