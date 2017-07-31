/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * 
 */
package org.opencb.opencga.storage.hadoop.variant;

import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine.HADOOP_LOAD_ARCHIVE;

import java.net.URI;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.junit.Ignore;
import org.junit.Test;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.config.StorageEngineConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine.Options;
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
public class HadoopDirectVariantStoreETLTest implements HadoopVariantStorageTest {

//    @ClassRule
//    public static ExternalResource externalResource = new HadoopExternalResource();
    
    
    
    @Test
    public void runTest() throws StorageEngineException, Exception{
        String f1 = "/Users/mh719/Projects/git-checkouts/opencb/opencga/opencga-storage/opencga-storage-hadoop/./src/test/resources/s1.genome.vcf";
        String f2 = "";
        URI uri = URI.create(f1);
        List<URI> inputFiles = Arrays.asList(uri);
//        URI outdirUri = null;
//        HadoopVariantStorageEngine manager = this.getVariantStorageManager();
//        String storageEngineId = "hadoop";
//        StorageConfiguration config = new StorageConfiguration();
//        ObjectMap options = new ObjectMap(0);
//        options.append(VariantStorageEngine.Options.DB_NAME.key(), "x");
//        StorageEtlConfiguration variant = new StorageEtlConfiguration();
//        DatabaseCredentials db = new DatabaseCredentials();
//        db.setPassword("");
//        db.setUser("");
//        db.setHosts(Arrays.asList("who1"));
//        variant.setDatabase(db);
//        variant.setOptions(options);
//        List<StorageEngineConfiguration> asList = Arrays.asList(new StorageEngineConfiguration("hadoop", null, variant, options));
//        config.setStorageEngines(asList);
//        manager.init(config, storageEngineId);
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
        HadoopDirectVariantStoragePipeline etl = new HadoopDirectVariantStoragePipeline(sconfig, dbAdaptor , mrexec, conf, credentials, variantReaderUtils, options);
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
    public HadoopVariantStorageEngine getVariantStorageEngine() throws Exception {
        HadoopVariantStorageEngine manager = new HadoopVariantStorageEngine();
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
