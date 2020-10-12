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

import org.junit.*;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.common.YesNoAuto;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;

import java.net.URI;

/**
 * Created on 15/10/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantHadoopStoragePipelineNoArchiveTest extends VariantStorageBaseTest implements HadoopVariantStorageTest {

    private VariantHadoopDBAdaptor dbAdaptor;

    @ClassRule
    public static HadoopExternalResource externalResource = new HadoopExternalResource();

    @BeforeClass
    public static void beforeClass() throws Exception {
        HadoopVariantStorageEngine variantStorageManager = externalResource.getVariantStorageEngine();
        externalResource.clearDB(variantStorageManager.getVariantTableName());
        externalResource.clearDB(variantStorageManager.getArchiveTableName(STUDY_ID));
    }

    @Before
    @Override
    public void before() throws Exception {
        dbAdaptor = ((HadoopVariantStorageEngine) variantStorageEngine).getDBAdaptor();
    }

    @After
    public void tearDown() throws Exception {
        URI outDir = newOutputUri();
        System.out.println("print variants at = " + outDir);
        VariantHbaseTestUtils.printVariants(dbAdaptor, outDir);
    }


    @Test
    public void test() throws Exception {
        URI inputUri = VariantStorageBaseTest.getResourceUri("platinum/1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz");

        StudyMetadata studyMetadata = VariantStorageBaseTest.newStudyMetadata();
        VariantStorageBaseTest.runDefaultETL(inputUri, getVariantStorageEngine(), studyMetadata,
                new ObjectMap(VariantStorageOptions.TRANSFORM_FORMAT.key(), "avro")
                        .append(VariantStorageOptions.ANNOTATE.key(), false)
                        .append(VariantStorageOptions.LOAD_ARCHIVE.key(), YesNoAuto.NO)
                        .append(VariantStorageOptions.STATS_CALCULATE.key(), false)
        );

//            fileMetadata = variantStorageManager.getVariantReaderUtils().readVariantFileMetadata(etlResult.getTransformResult());
//            VariantSetStats stats = fileMetadata.getStats();
//            Assert.assertNotNull(stats);
    }

}
