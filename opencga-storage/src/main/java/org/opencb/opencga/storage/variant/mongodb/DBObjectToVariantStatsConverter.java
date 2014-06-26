package org.opencb.opencga.storage.variant.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import java.util.Map;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.datastore.core.ComplexTypeConverter;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class DBObjectToVariantStatsConverter implements ComplexTypeConverter<VariantStats, DBObject> {

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
            stats.addGenotype(new Genotype(o.getKey()), (int) o.getValue());
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
            genotypes.append(g.getKey().toString(), g.getValue());
        }
        mongoStats.append(NUMGT_FIELD, genotypes);
        return mongoStats;
    }
    
}
