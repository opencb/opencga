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
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.VariantStorageManagerTest;
import org.opencb.opencga.storage.core.variant.VariantStorageManagerTestUtils;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
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

    public static final String[] VCF_TEST_FILE_NAMES = {
            "1k.chr1.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz",
            "1-500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"};

    public static final String EXPORTED_FILE_NAME = "exported-variant-test-file.vcf.gz";
    private static URI[] inputUri;
    private static VariantStorageManagerTestUtils.ETLResult[] etlResult;
    private VariantDBAdaptor dbAdaptor;
    protected QueryOptions options;
    protected QueryResult<Variant> queryResult;
    protected static StudyConfiguration studyConfiguration;

    @BeforeClass
    public static void beforeClass() throws IOException {
        inputUri = new URI[2];
        etlResult = new VariantStorageManagerTestUtils.ETLResult[2];
        for (int i = 0; i < VCF_TEST_FILE_NAMES.length; i++) {
            etlResult[i] = null;
            Path rootDir = getTmpRootDir();
            Path inputPath = rootDir.resolve(VCF_TEST_FILE_NAMES[i]);
            Files.copy(VariantStorageManagerTest.class.getClassLoader().getResourceAsStream(VCF_TEST_FILE_NAME),
                    inputPath, StandardCopyOption.REPLACE_EXISTING);
            inputUri[i] = inputPath.toUri();
        }
    }

    @Override
    @Before
    public void before() throws Exception {
        clearDB(DB_NAME);

        if (studyConfiguration == null) {
            studyConfiguration = newStudyConfiguration();
        }
        for (int i = 0; i < VCF_TEST_FILE_NAMES.length; i++) {
            if (etlResult[i] == null) {
                //            variantSource = new VariantSource(inputUri.getPath(), "testAlias", "testStudy", "Study for testing purposes");

                etlResult[i] = runDefaultETL(inputUri[i], getVariantStorageManager(), studyConfiguration,
                        new ObjectMap(VariantStorageManager.Options.ANNOTATE.key(), false)
                                .append(VariantStorageManager.Options.FILE_ID.key(), i + 6));
            }
        }
        dbAdaptor = getVariantStorageManager().getDBAdaptor(DB_NAME);
    }

    @After
    public void after() {
        dbAdaptor.close();
    }

    @Test
    public void testVcfHtsExport() throws Exception {
//        QueryOptions queryOptions = new QueryOptions();
//        List<String> include = Arrays.asList("chromosome", "start", "end", "alternative", "reference", "ids", "sourceEntries");
//        queryOptions.add("include", include);

        int indelsFails = 232;   // it is expected that those variants fail, by the moment

        int failedVariants = VariantExporter.VcfHtsExport(dbAdaptor.iterator(), studyConfiguration
                , new FileOutputStream("hts" + EXPORTED_FILE_NAME), null);

        assert (failedVariants <= indelsFails);   // allow indels failing due to the reference base issue: "TA,T" is stored as "A,"
        // compare VCF_TEST_FILE_NAME and EXPORTED_FILE_NAME
    }

    @Ignore
    @Test
    public void testVcfExport() throws Exception {

        QueryOptions queryOptions = new QueryOptions();
        List<String> include = Arrays.asList("chromosome", "start", "end", "alternative", "reference", "ids", "sourceEntries");
        queryOptions.add("include", include);
        VariantExporter.vcfExport(dbAdaptor, studyConfiguration, new URI(EXPORTED_FILE_NAME), queryOptions);

        // compare VCF_TEST_FILE_NAME and EXPORTED_FILE_NAME
    }

}
