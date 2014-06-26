package org.opencb.opencga.storage.variant.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.junit.*;
import static org.junit.Assert.assertEquals;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.stats.VariantStats;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class DBObjectToVariantStatsConverterTest {
    
    private static BasicDBObject mongoStats;
    private static VariantStats stats;
    
    @BeforeClass
    public static void setUp() {
        mongoStats = new BasicDBObject("maf", 0.1);
        mongoStats.append("mgf", 0.01);
        mongoStats.append("alleleMaf", "A");
        mongoStats.append("genotypeMaf", "A/A");
        mongoStats.append("missAllele", 10);
        mongoStats.append("missGenotypes", 5);
        
        BasicDBObject genotypes = new BasicDBObject();
        genotypes.append("0/0", 100);
        genotypes.append("0/1", 50);
        genotypes.append("1/1", 10);
        mongoStats.append("genotypeCount", genotypes);
        
        stats = new VariantStats(null, -1, null, null, 0.1, 0.01, "A", "A/A", 10, 5, -1, false, -1, -1, -1, -1);
        stats.addGenotype(new Genotype("0/0"), 100);
        stats.addGenotype(new Genotype("0/1"), 50);
        stats.addGenotype(new Genotype("1/1"), 10);
    }
    
    @Test
    public void testConvertToDataModelType() {
        DBObjectToVariantStatsConverter converter = new DBObjectToVariantStatsConverter();
        VariantStats converted = converter.convertToDataModelType(mongoStats);
        assertEquals(stats, converted);
    }
    
    @Test
    public void testConvertToStorageType() {
        DBObjectToVariantStatsConverter converter = new DBObjectToVariantStatsConverter();
        DBObject converted = converter.convertToStorageType(stats);
        
        assertEquals(stats.getMaf(), (float) converted.get("maf"), 1e-6);
        assertEquals(stats.getMgf(), (float) converted.get("mgf"), 1e-6);
        assertEquals(stats.getMafAllele(), converted.get("alleleMaf"));
        assertEquals(stats.getMgfGenotype(), converted.get("genotypeMaf"));
        
        assertEquals(stats.getMissingAlleles(), converted.get("missAllele"));
        assertEquals(stats.getMissingGenotypes(), converted.get("missGenotypes"));
        
        assertEquals(100, ((DBObject) converted.get("genotypeCount")).get("0/0"));
        assertEquals(50, ((DBObject) converted.get("genotypeCount")).get("0/1"));
        assertEquals(10, ((DBObject) converted.get("genotypeCount")).get("1/1"));
    }
}
