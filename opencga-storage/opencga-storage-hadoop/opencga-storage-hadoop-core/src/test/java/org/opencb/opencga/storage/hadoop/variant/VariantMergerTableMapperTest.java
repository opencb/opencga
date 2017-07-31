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

package org.opencb.opencga.storage.hadoop.variant;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.junit.*;
import org.junit.rules.ExternalResource;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.config.StorageEngineConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.converters.HBaseToVariantConverter;
import org.opencb.opencga.storage.hadoop.variant.metadata.HBaseStudyConfigurationDBAdaptor;

public class VariantMergerTableMapperTest extends VariantStorageBaseTest implements HadoopVariantStorageTest {

    @ClassRule
    public static ExternalResource externalResource = new HadoopExternalResource();

    private VariantHadoopDBAdaptor dbAdaptor;

    private VariantSource loadFile(String resource, StudyConfiguration studyConfiguration, Map<? extends String, ?> map) throws Exception {
        return VariantHbaseTestUtils.loadFile(getVariantStorageEngine(), DB_NAME, outputUri, resource, studyConfiguration, map);
    }

    @Before
    public void setUp() throws Exception {
        HadoopVariantStorageEngine variantStorageManager = getVariantStorageEngine();
        clearDB(variantStorageManager.getVariantTableName());
        clearDB(variantStorageManager.getArchiveTableName(STUDY_ID));
        //Force HBaseConverter to fail if something goes wrong
        HBaseToVariantConverter.setFailOnWrongVariants(true);
        dbAdaptor = variantStorageManager.getDBAdaptor();
        
    }

    @After
    public void tearDown() throws Exception {
    }

    private StudyConfigurationManager buildStudyManager() throws IOException{
        StorageEngineConfiguration se = variantStorageEngine.getConfiguration().getStorageEngine(variantStorageEngine.getStorageEngineId());
        ObjectMap opts = se.getVariant().getOptions();
        return new StudyConfigurationManager(new HBaseStudyConfigurationDBAdaptor(DB_NAME, configuration.get(), opts));
    }
    
    @Test
    public void testMap() throws Exception {
        StudyConfiguration studyConfiguration = VariantStorageBaseTest.newStudyConfiguration();
        VariantSource source1 = loadFile("s1.genome.vcf", studyConfiguration, Collections.emptyMap());
        System.out.println("Query from HBase : " + DB_NAME);
        Configuration conf = configuration.get();
        HBaseManager hm = new HBaseManager(conf);
        GenomeHelper genomeHelper = dbAdaptor.getGenomeHelper();
        Set<Integer> passPos = new HashSet<>(Arrays.asList(10032,13488));
        for(Variant variant : dbAdaptor){
            List<StudyEntry> studies = variant.getStudies();
            StudyEntry se = studies.get(0);
            FileEntry fe = se.getFiles().get(0);
            String passString = fe.getAttributes().get("PASS");
            Integer passCnt = Integer.parseInt(passString);
            System.out.println(String.format("Variant = %s; Studies=%s; Pass=%s;",variant,studies.size(),passCnt));
            assertEquals("Position issue with PASS", passPos.contains(variant.getStart()), passCnt.equals(1));
        }
        System.out.println("End query from HBase : " + DB_NAME);
//        assertEquals(source.stats().getVariantTypeCount(VariantType.SNP) + source.stats().getVariantTypeCount(VariantType.SNV), numVariants);
        
    }

    @Test
    public void testFilterMap() throws Exception {
        StudyConfiguration studyConfiguration = VariantStorageBaseTest.newStudyConfiguration();
        VariantSource source1 = loadFile("s1.genome.vcf", studyConfiguration, Collections.emptyMap());
        VariantSource source2 = loadFile("s2.genome.vcf", studyConfiguration, Collections.emptyMap());
        System.out.println("Query from HBase : " + DB_NAME);
        Configuration conf = configuration.get();
        HBaseManager hm = new HBaseManager(conf);
        GenomeHelper genomeHelper = dbAdaptor.getGenomeHelper();
        Set<Integer> passPos = new HashSet<>(Arrays.asList(10032,13488));
        for(Variant variant : dbAdaptor){
            List<StudyEntry> studies = variant.getStudies();
            StudyEntry se = studies.get(0);
            FileEntry fe = se.getFiles().get(0);
            System.out.println(se.getSamplesData());
            String passString = fe.getAttributes().get("PASS");
            Integer passCnt = Integer.parseInt(passString);
            System.out.println(String.format("Variant = %s; Studies=%s; Pass=%s;",variant,studies.size(),passCnt));
            assertEquals("Position issue with PASS", passPos.contains(variant.getStart()), passCnt.equals(1));
        }
        System.out.println("End query from HBase : " + DB_NAME);
//        assertEquals(source.stats().getVariantTypeCount(VariantType.SNP) + source.stats().getVariantTypeCount(VariantType.SNV), numVariants);
        
    }

}
