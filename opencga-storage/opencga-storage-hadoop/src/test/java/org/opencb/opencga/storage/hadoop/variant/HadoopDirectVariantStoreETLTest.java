/**
 * 
 */
package org.opencb.opencga.storage.hadoop.variant;

import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageManager.HADOOP_LOAD_ARCHIVE;

import java.net.URI;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.junit.Ignore;
import org.junit.Test;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.config.StorageEngineConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.opencb.opencga.storage.core.variant.VariantStorageManager.Options;
import org.opencb.opencga.storage.core.variant.io.VariantReaderUtils;
import org.opencb.opencga.storage.hadoop.auth.HBaseCredentials;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.executors.MRExecutor;

/**
 * @author Matthias Haimel mh719+git@cam.ac.uk
 *
 */
@Ignore
@Deprecated
public class HadoopDirectVariantStoreETLTest implements HadoopVariantStorageManagerTestUtils {

//    @ClassRule
//    public static ExternalResource externalResource = new HadoopExternalResource();
    
    
    
    @Test
    public void runTest() throws StorageManagerException, Exception{
        String f1 = "/Users/mh719/Projects/git-checkouts/opencb/opencga/opencga-storage/opencga-storage-hadoop/./src/test/resources/s1.genome.vcf";
        String f2 = "";
        URI uri = URI.create(f1);
        List<URI> inputFiles = Arrays.asList(uri);
//        URI outdirUri = null;
//        HadoopVariantStorageManager manager = this.getVariantStorageManager();
//        String storageEngineId = "hadoop";
//        StorageConfiguration config = new StorageConfiguration();
//        ObjectMap options = new ObjectMap(0);
//        options.append(VariantStorageManager.Options.DB_NAME.key(), "x");
//        StorageEtlConfiguration variant = new StorageEtlConfiguration();
//        DatabaseCredentials db = new DatabaseCredentials();
//        db.setPassword("");
//        db.setUser("");
//        db.setHosts(Arrays.asList("who1"));
//        variant.setDatabase(db);
//        variant.setOptions(options);
//        List<StorageEngineConfiguration> asList = Arrays.asList(new StorageEngineConfiguration("hadoop", null, variant, options));
//        config.setStorageEngines(asList);
//        manager.setConfiguration(config, storageEngineId);
//        manager.index(inputFiles, outdirUri, false, false, true);

        Configuration conf = new Configuration();
        ObjectMap options = new ObjectMap();
        options.append(HADOOP_LOAD_ARCHIVE, true);
        options.append(Options.TRANSFORM_FORMAT.key(), "proto");
        StudyConfiguration studyConf = new StudyConfiguration(1, "1");
        StorageConfiguration sconfig = new StorageConfiguration();
        options.append(Options.STUDY_CONFIGURATION.key(), studyConf);
        StorageEngineConfiguration config = new StorageEngineConfiguration();
        MRExecutor mrexec = new TestMRExecutor(conf);
        HBaseCredentials credentials = new HBaseCredentials("who1", "test-table", "user", "pass");
        VariantHadoopDBAdaptor dbAdaptor = null; // new VariantHadoopDBAdaptor(credentials, config, conf);
        VariantReaderUtils variantReaderUtils = new VariantReaderUtils();
        HadoopDirectVariantStorageETL etl = new HadoopDirectVariantStorageETL(sconfig, "hadoop", dbAdaptor , mrexec, conf, credentials, variantReaderUtils, options);
        URI load = etl.load(uri);
        System.out.println(load);
        
    }

    /**
     *
     */
    public HadoopDirectVariantStoreETLTest() {
        // TODO Auto-generated constructor stub
    }

    /* (non-Javadoc)
     * @see org.opencb.opencga.storage.core.variant.VariantStorageTest#getVariantStorageManager()
     */
    @Override
    public HadoopVariantStorageManager getVariantStorageManager() throws Exception {
        HadoopVariantStorageManager manager = new HadoopVariantStorageManager();
        return manager;
    }

    /* (non-Javadoc)
     * @see org.opencb.opencga.storage.core.variant.VariantStorageTest#clearDB(java.lang.String)
     */
    @Override
    public void clearDB(String dbName) throws Exception {
        // TODO Auto-generated method stub

    }

}
