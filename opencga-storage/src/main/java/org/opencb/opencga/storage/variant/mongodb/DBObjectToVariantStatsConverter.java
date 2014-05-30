package org.opencb.opencga.storage.variant.mongodb;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import java.util.Iterator;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.datastore.core.ComplexTypeConverter;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class DBObjectToVariantStatsConverter implements ComplexTypeConverter<DBObject, VariantStats> {

    @Override
    public VariantStats convert(DBObject object) {
        /*
        BasicDBObject mongoStats = new BasicDBObject("maf", vs.getMaf());
                mongoStats.append("mgf", vs.getMgf());
                mongoStats.append("alleleMaf", vs.getMafAllele());
                mongoStats.append("genotypeMaf", vs.getMgfGenotype());
                mongoStats.append("missAllele", vs.getMissingAlleles());
                mongoStats.append("missGenotypes", vs.getMissingGenotypes());
                mongoStats.append("mendelErr", vs.getMendelianErrors());
                mongoStats.append("genotypeCount", genotypes);
        */
        VariantStats stats = new VariantStats();
        stats.setMaf(((Double) object.get("maf")).floatValue());
        stats.setMgf(((Double) object.get("mgf")).floatValue());
        stats.setMafAllele((String) object.get("alleleMaf"));
        stats.setMgfGenotype((String) object.get("genotypeMaf"));
        
        stats.setMissingAlleles((int) object.get("missAllele"));
        stats.setMissingGenotypes((int) object.get("missGenotypes"));
        stats.setMendelianErrors((int) object.get("mendelErr"));
        
//        for (Map.Entry<Genotype, Integer> g : stats.getGenotypesCount().entrySet()) {
//            genotypes.append(g.getKey().toString(), g.getValue());
//        }
        
        BasicDBObject genotypes = (BasicDBObject) object.get("genotypeCount");
        for (Object o : genotypes.entrySet()) {
            BasicDBObject gtCount = (BasicDBObject) o;
        }
        
        return stats;
    }
    
}
