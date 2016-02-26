package org.opencb.opencga.storage.hadoop.variant;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.config.StorageEngineConfiguration;
import org.opencb.opencga.storage.core.config.StorageEtlConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.VariantStorageManagerTestUtils;
import org.opencb.opencga.storage.core.variant.VariantStorageManagerTestUtils.ETLResult;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageManager;
import org.opencb.opencga.storage.hadoop.variant.index.HBaseToVariantConverter;

public class VariantTableMapperTest extends HadoopVariantStorageManagerTestUtils {

    public VariantSource loadFile(String resourceName, int fileId, StudyConfiguration studyConfiguration, Map<? extends String, ?> otherParams) throws Exception {
        HadoopVariantStorageManager variantStorageManager = getVariantStorageManager();
        URI fileInputUri = VariantStorageManagerTestUtils.getResourceUri(resourceName);

        ObjectMap params = new ObjectMap(VariantStorageManager.Options.TRANSFORM_FORMAT.key(), "avro")
                .append(VariantStorageManager.Options.STUDY_CONFIGURATION.key(), studyConfiguration)
                .append(VariantStorageManager.Options.STUDY_ID.key(), studyConfiguration.getStudyId())
                .append(VariantStorageManager.Options.DB_NAME.key(), DB_NAME)
                .append(VariantStorageManager.Options.ANNOTATE.key(), false)
                .append(VariantAnnotationManager.SPECIES, "hsapiens")
                .append(VariantAnnotationManager.ASSEMBLY, "GRc37")
                .append(VariantStorageManager.Options.CALCULATE_STATS.key(), false)
                .append(HadoopVariantStorageManager.HADOOP_LOAD_ARCHIVE, true)
                .append(HadoopVariantStorageManager.HADOOP_LOAD_VARIANT, true);
        if (otherParams != null) {
            params.putAll(otherParams);
        }

        if (fileId > 0) {
            params.append(VariantStorageManager.Options.FILE_ID.key(), fileId);
        }

        ETLResult etlResult = runETL(variantStorageManager, fileInputUri, outputUri, params, params, params,
                params, params, params, params, true, true, true);

        return variantStorageManager.readVariantSource(etlResult.transformResult, new ObjectMap());
    }

    public VariantSource loadFile(String resourceName, int fileId, StudyConfiguration studyConfiguration) throws Exception {
        return loadFile(resourceName, fileId, studyConfiguration, null);
    }
    public VariantSource loadFile(String resourceName, StudyConfiguration studyConfiguration, Map<? extends String, ?> otherParams) throws Exception {
        return loadFile(resourceName, -1, studyConfiguration, otherParams);
    }
    private VariantHadoopDBAdaptor dbAdaptor;
    @Before
    public void setUp() throws Exception {
        clearDB(DB_NAME);
        //Force HBaseConverter to fail if something goes wrong
        HBaseToVariantConverter.setFailOnWrongVariants(true);
        HadoopVariantStorageManager variantStorageManager = getVariantStorageManager();
        dbAdaptor = variantStorageManager.getDBAdaptor(DB_NAME);
        
    }

    @After
    public void tearDown() throws Exception {
    }

    private HBaseStudyConfigurationManager buildStudyManager() throws IOException{
        StorageEngineConfiguration se = variantStorageManager.getConfiguration().getStorageEngine(variantStorageManager.getStorageEngineId());
        ObjectMap opts = se.getVariant().getOptions();
        return new HBaseStudyConfigurationManager(DB_NAME, configuration.get(), opts);
    }
    
    @Test
    public void testMap() throws Exception {
        StudyConfiguration studyConfiguration = VariantStorageManagerTestUtils.newStudyConfiguration();
        VariantSource source1 = loadFile("s1.genome.vcf", studyConfiguration, Collections.emptyMap());
        System.out.println("Query from HBase : " + DB_NAME);
        Configuration conf = configuration.get();
        HBaseToVariantConverter conv = new HBaseToVariantConverter(dbAdaptor.getGenomeHelper(), buildStudyManager());
        HBaseManager hm = new HBaseManager(conf);
        GenomeHelper genomeHelper = dbAdaptor.getGenomeHelper();
        for(Variant variant : dbAdaptor){
            List<StudyEntry> studies = variant.getStudies();
            StudyEntry se = studies.get(0);
            FileEntry fe = se.getFiles().get(0);
            String passString = fe.getAttributes().get("PASS");
            Integer passCnt = Integer.parseInt(passString);
            System.out.println(String.format("Variant = %s; Studies=%s; Pass=%s;",variant,studies.size(),passCnt));
            assertEquals("Position issue with PASS", variant.getStart().equals(10032), passCnt.equals(1));
        }
        System.out.println("End query from HBase : " + DB_NAME);
//        assertEquals(source.getStats().getVariantTypeCount(VariantType.SNP) + source.getStats().getVariantTypeCount(VariantType.SNV), numVariants);
        
    }

}
