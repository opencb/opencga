/*
 * Copyright 2015 OpenCB
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

package org.opencb.opencga.storage.core.variant.io;


import org.junit.*;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageManagerTest;
import org.opencb.opencga.storage.core.variant.VariantStorageManagerTestUtils;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.List;

/**
 * Created by jmmut on 2015-07-15.
 *
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
 */
@Ignore
public abstract class VariantExporterTest extends VariantStorageManagerTestUtils {

    public static final String VCF_TEST_FILE_NAME = "variant-test-file.vcf.gz";
    public static final String EXPORTED_FILE_NAME = "exported-variant-test-file.vcf.gz";
    private static URI inputUri;
    private static VariantStorageManagerTestUtils.ETLResult etlResult = null;
    private VariantDBAdaptor dbAdaptor;
    protected QueryOptions options;
    protected QueryResult<Variant> queryResult;
    protected static StudyConfiguration studyConfiguration;

    @BeforeClass
    public static void beforeClass() throws IOException {
        etlResult = null;
        Path rootDir = getTmpRootDir();
        Path inputPath = rootDir.resolve(VCF_TEST_FILE_NAME);
        Files.copy(VariantStorageManagerTest.class.getClassLoader().getResourceAsStream(VCF_TEST_FILE_NAME), inputPath, StandardCopyOption.REPLACE_EXISTING);
        inputUri = inputPath.toUri();
    }

    @Override
    @Before
    public void before() throws Exception {
        if (etlResult == null) {
            studyConfiguration = newStudyConfiguration();
//            variantSource = new VariantSource(inputUri.getPath(), "testAlias", "testStudy", "Study for testing purposes");
            clearDB(DB_NAME);
            etlResult = runDefaultETL(inputUri, getVariantStorageManager(), studyConfiguration);
        }
        dbAdaptor = getVariantStorageManager().getDBAdaptor(DB_NAME);
    }

    @After
    public void after() {
        dbAdaptor.close();
    }

    @Test
    public void testVcfHtsExport() throws Exception {
        QueryOptions queryOptions = new QueryOptions();
        List<String> include = Arrays.asList("chromosome", "start", "end", "alternative", "reference", "ids", "sourceEntries");
        queryOptions.add("include", include);

        VariantExporter.VcfHtsExport(dbAdaptor, studyConfiguration, new URI("hts" + EXPORTED_FILE_NAME), queryOptions);

        // compare VCF_TEST_FILE_NAME and EXPORTED_FILE_NAME
    }

    @Test
    public void testVcfExport() throws Exception {

        QueryOptions queryOptions = new QueryOptions();
        List<String> include = Arrays.asList("chromosome", "start", "end", "alternative", "reference", "ids", "sourceEntries");
        queryOptions.add("include", include);
        VariantExporter.vcfExport(dbAdaptor, studyConfiguration, new URI(EXPORTED_FILE_NAME), queryOptions);

        // compare VCF_TEST_FILE_NAME and EXPORTED_FILE_NAME
    }
}
