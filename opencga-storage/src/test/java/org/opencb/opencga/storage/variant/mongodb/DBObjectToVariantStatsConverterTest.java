package org.opencb.opencga.storage.variant.mongodb;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import org.junit.*;
import static org.junit.Assert.assertEquals;
import org.opencb.biodata.models.variant.stats.VariantStats;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class DBObjectToVariantStatsConverterTest {
    
    private BasicDBObject mongoStats;
    
    @Before
    public void setUp() {
        mongoStats = new BasicDBObject("maf", 0.1);
        mongoStats.append("mgf", 0.01);
        mongoStats.append("alleleMaf", "A");
        mongoStats.append("genotypeMaf", "A/A");
        mongoStats.append("missAllele", 10);
        mongoStats.append("missGenotypes", 5);
        mongoStats.append("mendelErr", 1);
        
        BasicDBList genotypes = new BasicDBList();
        genotypes.add(new BasicDBObject("0/0", 100));
        genotypes.add(new BasicDBObject("0/1", 50));
        genotypes.add(new BasicDBObject("1/1", 10));
        mongoStats.append("genotypeCount", genotypes);
    }
    
    @Test
    public void testConvert() {
        DBObjectToVariantStatsConverter converter = new DBObjectToVariantStatsConverter();
        VariantStats stats = converter.convert(mongoStats);
        
        assertEquals((double) mongoStats.get("maf"), stats.getMaf(), 1e-6);
        assertEquals((double) mongoStats.get("mgf"), stats.getMgf(), 1e-6);
        assertEquals(mongoStats.get("alleleMaf"), stats.getMafAllele());
        assertEquals(mongoStats.get("genotypeMaf"), stats.getMgfGenotype());
        
        assertEquals(mongoStats.get("missAllele"), stats.getMissingAlleles());
        assertEquals(mongoStats.get("missGenotypes"), stats.getMissingGenotypes());
        assertEquals(mongoStats.get("mendelErr"), stats.getMendelianErrors());
    }
    
}
