package org.opencb.opencga.storage.variant.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import java.util.Calendar;
import java.util.Map;
import static org.junit.Assert.assertEquals;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.stats.VariantGlobalStats;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class DBObjectToVariantSourceConverterTest {
    
    private static BasicDBObject mongoSource;
    
    private static VariantSource source;
    
    
    @BeforeClass
    public static void setUpClass() {
        source = new VariantSource("file.vcf", "f", "s1", "study1");
        source.getSamplesPosition().put("NA000", 0);
        source.getSamplesPosition().put("NA001", 1);
        source.getSamplesPosition().put("NA002", 2);
        source.getSamplesPosition().put("NA003", 3);
        source.addMetadata("header", "##fileformat=v4.1");
        VariantGlobalStats global = new VariantGlobalStats();
        global.setSamplesCount(4);
        global.setVariantsCount(10);
        global.setSnpsCount(7);
        global.setIndelsCount(3);
        global.setPassCount(9);
        global.setTransitionsCount(4);
        global.setTransversionsCount(4);
        global.setMeanQuality(20.5f);
        source.setStats(global);
        
        mongoSource = new BasicDBObject(DBObjectToVariantSourceConverter.FILENAME_FIELD, source.getFileName())
                .append(DBObjectToVariantSourceConverter.FILEID_FIELD, source.getFileId())
                .append(DBObjectToVariantSourceConverter.STUDYNAME_FIELD, source.getStudyName())
                .append(DBObjectToVariantSourceConverter.STUDYID_FIELD, source.getStudyId())
                .append(DBObjectToVariantSourceConverter.DATE_FIELD, Calendar.getInstance().getTime())
                .append(DBObjectToVariantSourceConverter.SAMPLES_FIELD, source.getSamplesPosition());
        // TODO Pending how to manage the consequence type ranking (calculate during reading?)
        
        DBObject mongoStats = new BasicDBObject(DBObjectToVariantSourceConverter.NUMSAMPLES_FIELD, global.getSamplesCount())
                .append(DBObjectToVariantSourceConverter.NUMVARIANTS_FIELD, global.getVariantsCount())
                .append(DBObjectToVariantSourceConverter.NUMSNPS_FIELD, global.getSnpsCount())
                .append(DBObjectToVariantSourceConverter.NUMINDELS_FIELD, global.getIndelsCount())
                .append(DBObjectToVariantSourceConverter.NUMPASSFILTERS_FIELD, global.getPassCount())
                .append(DBObjectToVariantSourceConverter.NUMTRANSITIONS_FIELD, global.getTransitionsCount())
                .append(DBObjectToVariantSourceConverter.NUMTRANSVERSIONS_FIELD, global.getTransversionsCount())
                .append(DBObjectToVariantSourceConverter.MEANQUALITY_FIELD, (double) global.getMeanQuality());

        mongoSource = mongoSource.append(DBObjectToVariantSourceConverter.STATS_FIELD, mongoStats);

        // TODO Save pedigree information
        
        // Metadata
        Map<String, String> meta = source.getMetadata();
        DBObject metadataMongo = new BasicDBObject(DBObjectToVariantSourceConverter.HEADER_FIELD, source.getMetadata().get("header"));
        mongoSource = mongoSource.append(DBObjectToVariantSourceConverter.METADATA_FIELD, metadataMongo);
    }
    
    @Test
    public void testConvertToDataModelType() {
        DBObjectToVariantSourceConverter converter = new DBObjectToVariantSourceConverter();
        DBObject converted = converter.convertToStorageType(source);
       
        assertEquals(mongoSource.get(DBObjectToVariantSourceConverter.FILENAME_FIELD), converted.get(DBObjectToVariantSourceConverter.FILENAME_FIELD));
        assertEquals(mongoSource.get(DBObjectToVariantSourceConverter.FILEID_FIELD), converted.get(DBObjectToVariantSourceConverter.FILEID_FIELD));
        assertEquals(mongoSource.get(DBObjectToVariantSourceConverter.STUDYNAME_FIELD), converted.get(DBObjectToVariantSourceConverter.STUDYNAME_FIELD));
        assertEquals(mongoSource.get(DBObjectToVariantSourceConverter.STUDYID_FIELD), converted.get(DBObjectToVariantSourceConverter.STUDYID_FIELD));
        // Exclude the date
        assertEquals(mongoSource.get(DBObjectToVariantSourceConverter.SAMPLES_FIELD), converted.get(DBObjectToVariantSourceConverter.SAMPLES_FIELD));
        
        DBObject convertedStats = (DBObject) converted.get(DBObjectToVariantSourceConverter.STATS_FIELD);
        DBObject expectedStats = (DBObject) converted.get(DBObjectToVariantSourceConverter.STATS_FIELD);
        assertEquals(expectedStats.get(DBObjectToVariantSourceConverter.NUMSAMPLES_FIELD), convertedStats.get(DBObjectToVariantSourceConverter.NUMSAMPLES_FIELD));
        assertEquals(expectedStats.get(DBObjectToVariantSourceConverter.NUMVARIANTS_FIELD), convertedStats.get(DBObjectToVariantSourceConverter.NUMVARIANTS_FIELD));
        assertEquals(expectedStats.get(DBObjectToVariantSourceConverter.NUMSNPS_FIELD), convertedStats.get(DBObjectToVariantSourceConverter.NUMSNPS_FIELD));
        assertEquals(expectedStats.get(DBObjectToVariantSourceConverter.NUMINDELS_FIELD), convertedStats.get(DBObjectToVariantSourceConverter.NUMINDELS_FIELD));
        assertEquals(expectedStats.get(DBObjectToVariantSourceConverter.NUMPASSFILTERS_FIELD), convertedStats.get(DBObjectToVariantSourceConverter.NUMPASSFILTERS_FIELD));
        assertEquals(expectedStats.get(DBObjectToVariantSourceConverter.NUMTRANSITIONS_FIELD), convertedStats.get(DBObjectToVariantSourceConverter.NUMTRANSITIONS_FIELD));
        assertEquals(expectedStats.get(DBObjectToVariantSourceConverter.NUMTRANSVERSIONS_FIELD), convertedStats.get(DBObjectToVariantSourceConverter.NUMTRANSVERSIONS_FIELD));
        assertEquals(expectedStats.get(DBObjectToVariantSourceConverter.MEANQUALITY_FIELD), convertedStats.get(DBObjectToVariantSourceConverter.MEANQUALITY_FIELD));
        
        DBObject convertedMetadata = (DBObject) converted.get(DBObjectToVariantSourceConverter.METADATA_FIELD);
        DBObject expectedMetadata = (DBObject) converted.get(DBObjectToVariantSourceConverter.METADATA_FIELD);
        assertEquals(expectedMetadata.get(DBObjectToVariantSourceConverter.HEADER_FIELD), convertedMetadata.get(DBObjectToVariantSourceConverter.HEADER_FIELD));
    }

    @Test
    public void testConvertToStorageType() {
        DBObjectToVariantSourceConverter converter = new DBObjectToVariantSourceConverter();
        VariantSource converted = converter.convertToDataModelType(mongoSource);
        assertEquals(source, converted);
    }
    
}
