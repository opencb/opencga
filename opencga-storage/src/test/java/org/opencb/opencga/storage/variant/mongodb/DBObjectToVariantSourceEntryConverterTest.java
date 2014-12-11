package org.opencb.opencga.storage.variant.mongodb;

import com.google.common.collect.Lists;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSourceEntry;
import org.opencb.biodata.models.variant.stats.VariantStats;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class DBObjectToVariantSourceEntryConverterTest {
    
    private BasicDBObject mongoFile;
    private VariantSourceEntry file;
    
    @Before
    public void setUp() {
        // Java native class
        file = new VariantSourceEntry("f1", "s1");
        file.addAttribute("QUAL", "0.01");
        file.addAttribute("AN", "2");
        file.setFormat("GT:DP");
        
        Map<String, String> na001 = new HashMap<>();
        na001.put("GT", "0/0");
        na001.put("DP", "4");
        file.addSampleData("NA001", na001);
        Map<String, String> na002 = new HashMap<>();
        na002.put("GT", "0/1");
        na002.put("DP", "5");
        file.addSampleData("NA002", na002);
        
        // MongoDB object
        mongoFile = new BasicDBObject(DBObjectToVariantSourceEntryConverter.FILEID_FIELD, file.getFileId())
                .append(DBObjectToVariantSourceEntryConverter.STUDYID_FIELD, file.getStudyId());
        mongoFile.append(DBObjectToVariantSourceEntryConverter.ATTRIBUTES_FIELD, 
                new BasicDBObject("QUAL", "0.01").append("AN", "2"));
        mongoFile.append(DBObjectToVariantSourceEntryConverter.FORMAT_FIELD, file.getFormat());
        BasicDBObject genotypeCodes = new BasicDBObject();
        genotypeCodes.append("def", "0/0");
        genotypeCodes.append("0/1", Arrays.asList(1));
        mongoFile.append(DBObjectToVariantSourceEntryConverter.SAMPLES_FIELD, genotypeCodes);
    }
    
    @Test
    public void testConvertToDataModelTypeWithStats() {
        VariantStats stats = new VariantStats(null, -1, null, null, Variant.VariantType.SNV, 0.1f, 0.01f, "A", "A/A", 10, 5, -1, -1, -1, -1, -1);
        stats.addGenotype(new Genotype("0/0"), 100);
        stats.addGenotype(new Genotype("0/1"), 50);
        stats.addGenotype(new Genotype("1/1"), 10);
        file.setStats(stats);
        file.getSamplesData().clear(); // TODO Samples can't be tested easily, needs a running Mongo instance
        
        BasicDBObject mongoStats = new BasicDBObject(DBObjectToVariantStatsConverter.MAF_FIELD, 0.1);
        mongoStats.append(DBObjectToVariantStatsConverter.MGF_FIELD, 0.01);
        mongoStats.append(DBObjectToVariantStatsConverter.MAFALLELE_FIELD, "A");
        mongoStats.append(DBObjectToVariantStatsConverter.MGFGENOTYPE_FIELD, "A/A");
        mongoStats.append(DBObjectToVariantStatsConverter.MISSALLELE_FIELD, 10);
        mongoStats.append(DBObjectToVariantStatsConverter.MISSGENOTYPE_FIELD, 5);
        BasicDBObject genotypes = new BasicDBObject();
        genotypes.append("0/0", 100);
        genotypes.append("0/1", 50);
        genotypes.append("1/1", 10);
        mongoStats.append(DBObjectToVariantStatsConverter.NUMGT_FIELD, genotypes);
        mongoFile.append(DBObjectToVariantSourceEntryConverter.STATS_FIELD, mongoStats);
        
        List<String> sampleNames = null;
        DBObjectToVariantSourceEntryConverter converter = new DBObjectToVariantSourceEntryConverter(
                true, new DBObjectToVariantStatsConverter(), sampleNames);
        VariantSourceEntry converted = converter.convertToDataModelType(mongoFile);
        assertEquals(file, converted);
    }

    @Test
    public void testConvertToStorageTypeWithStats() {
        VariantStats stats = new VariantStats(null, -1, null, null, Variant.VariantType.SNV, 0.1f, 0.01f, "A", "A/A", 10, 5, -1, -1, -1, -1, -1);
        stats.addGenotype(new Genotype("0/0"), 100);
        stats.addGenotype(new Genotype("0/1"), 50);
        stats.addGenotype(new Genotype("1/1"), 10);
        file.setStats(stats);
        
        BasicDBObject mongoStats = new BasicDBObject(DBObjectToVariantStatsConverter.MAF_FIELD, 0.1);
        mongoStats.append(DBObjectToVariantStatsConverter.MGF_FIELD, 0.01);
        mongoStats.append(DBObjectToVariantStatsConverter.MAFALLELE_FIELD, "A");
        mongoStats.append(DBObjectToVariantStatsConverter.MGFGENOTYPE_FIELD, "A/A");
        mongoStats.append(DBObjectToVariantStatsConverter.MISSALLELE_FIELD, 10);
        mongoStats.append(DBObjectToVariantStatsConverter.MISSGENOTYPE_FIELD, 5);
        BasicDBObject genotypes = new BasicDBObject();
        genotypes.append("0/0", 100);
        genotypes.append("0/1", 50);
        genotypes.append("1/1", 10);
        mongoStats.append(DBObjectToVariantStatsConverter.NUMGT_FIELD, genotypes);
        mongoFile.append(DBObjectToVariantSourceEntryConverter.STATS_FIELD, mongoStats);
        
        List<String> sampleNames = Lists.newArrayList("NA001", "NA002");
        DBObjectToVariantSourceEntryConverter converter = new DBObjectToVariantSourceEntryConverter(
                true, sampleNames, new DBObjectToVariantStatsConverter());
        DBObject converted = converter.convertToStorageType(file);
        
        assertEquals(mongoFile.get(DBObjectToVariantStatsConverter.MAF_FIELD), converted.get(DBObjectToVariantStatsConverter.MAF_FIELD));
        assertEquals(mongoFile.get(DBObjectToVariantStatsConverter.MGF_FIELD), converted.get(DBObjectToVariantStatsConverter.MGF_FIELD));
        assertEquals(mongoFile.get(DBObjectToVariantStatsConverter.MAFALLELE_FIELD), converted.get(DBObjectToVariantStatsConverter.MAFALLELE_FIELD));
        assertEquals(mongoFile.get(DBObjectToVariantStatsConverter.MGFGENOTYPE_FIELD), converted.get(DBObjectToVariantStatsConverter.MGFGENOTYPE_FIELD));
        assertEquals(mongoFile.get(DBObjectToVariantStatsConverter.MISSALLELE_FIELD), converted.get(DBObjectToVariantStatsConverter.MISSALLELE_FIELD));
        assertEquals(mongoFile.get(DBObjectToVariantStatsConverter.MISSGENOTYPE_FIELD), converted.get(DBObjectToVariantStatsConverter.MISSGENOTYPE_FIELD));
        assertEquals(mongoFile.get(DBObjectToVariantStatsConverter.NUMGT_FIELD), converted.get(DBObjectToVariantStatsConverter.NUMGT_FIELD));
    }
    
    @Test
    public void testConvertToDataModelTypeWithoutStats() {
        file.getSamplesData().clear(); // TODO Samples can't be tested easily, needs a running Mongo instance
        List<String> sampleNames = null;
        
        // Test with no stats converter provided
        DBObjectToVariantSourceEntryConverter converter = new DBObjectToVariantSourceEntryConverter(true, null, sampleNames);
        VariantSourceEntry converted = converter.convertToDataModelType(mongoFile);
        assertEquals(file, converted);
        
        // Test with a stats converter provided but no stats object
        converter = new DBObjectToVariantSourceEntryConverter(true, null, sampleNames);
        converted = converter.convertToDataModelType(mongoFile);
        assertEquals(file, converted);
    }

    @Test
    public void testConvertToStorageTypeWithoutStats() {
        List<String> sampleNames = Lists.newArrayList("NA001", "NA002");
        // Test with no stats converter provided
        DBObjectToVariantSourceEntryConverter converter = new DBObjectToVariantSourceEntryConverter(true, sampleNames, null);
        DBObject converted = converter.convertToStorageType(file);
        assertEquals(mongoFile, converted);
        
        // Test with a stats converter provided but no stats object
        converter = new DBObjectToVariantSourceEntryConverter(true, sampleNames, new DBObjectToVariantStatsConverter());
        converted = converter.convertToStorageType(file);
        assertEquals(mongoFile, converted);
    }
    
}
