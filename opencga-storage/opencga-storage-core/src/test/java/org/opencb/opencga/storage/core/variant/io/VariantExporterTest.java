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
import org.opencb.biodata.formats.variant.vcf4.io.VariantVcfReader;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantSourceEntry;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.Query;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.VariantStorageManagerTestUtils;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.zip.GZIPOutputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Created by jmmut on 2015-07-15.
 *
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
 */
@Ignore
public abstract class VariantExporterTest extends VariantStorageManagerTestUtils {

    public static final String[] VCF_TEST_FILE_NAMES = {
            "1-500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz",
//            "501-1000.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz",
//            "1001-1500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz",
//            "1501-2000.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz",
//            "2001-2504.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz",
    };

    public static final String EXPORTED_FILE_NAME = "exported-variant-test-file.vcf.gz";
    private static URI[] inputUri;
    private static VariantStorageManagerTestUtils.ETLResult[] etlResult;
    private VariantDBAdaptor dbAdaptor;
    protected QueryOptions options;
    protected QueryResult<Variant> queryResult;
    protected static StudyConfiguration studyConfiguration;

    @BeforeClass
    public static void beforeClass() throws IOException {
        inputUri = new URI[VCF_TEST_FILE_NAMES.length];
        etlResult = new VariantStorageManagerTestUtils.ETLResult[VCF_TEST_FILE_NAMES.length];
        for (int i = 0; i < VCF_TEST_FILE_NAMES.length; i++) {
            etlResult[i] = null;
            inputUri[i] = getResourceUri(VCF_TEST_FILE_NAMES[i]);
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
                etlResult[i] = runDefaultETL(inputUri[i], getVariantStorageManager(), studyConfiguration,
                        new ObjectMap(VariantStorageManager.Options.ANNOTATE.key(), false)
                                .append(VariantStorageManager.Options.FILE_ID.key(), i)
                                .append(VariantStorageManager.Options.CALCULATE_STATS.key(), false));
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
        Query query = new Query();
//        Set<Integer> samplesInFile = dbAdaptor.getStudyConfigurationManager().getStudyConfiguration(STUDY_NAME, null).first().getSamplesInFiles().get(0);
//        query.append(VariantDBAdaptor.VariantQueryParams.STUDIES.key(), STUDY_NAME)
//                .append(VariantDBAdaptor.VariantQueryParams.RETURNED_FILES.key(), 0)
//                .append(VariantDBAdaptor.VariantQueryParams.FILES.key(), 0)
//                .append(VariantDBAdaptor.VariantQueryParams.RETURNED_SAMPLES.key(), samplesInFile);
        Path outputVcf = getTmpRootDir().resolve("hts_" + EXPORTED_FILE_NAME);
        int failedVariants = VariantExporter.VcfHtsExport(dbAdaptor.iterator(query, null), studyConfiguration
                , new GZIPOutputStream(new FileOutputStream(outputVcf.toFile())), null);

        assertEquals(0, failedVariants);
        // compare VCF_TEST_FILE_NAME and EXPORTED_FILE_NAME
        checkExportedVCF(Paths.get(getResourceUri("1-500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz")), outputVcf);
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

    public void checkExportedVCF(Path originalVcf, Path exportedVcf) throws IOException {
        Map<Integer, Variant> originalVariants = readVCF(originalVcf);
        Map<Integer, Variant> exportedVariants = readVCF(exportedVcf);

        assertEquals(originalVariants.size(), exportedVariants.size());
        for (Map.Entry<Integer, Variant> entry : originalVariants.entrySet()) {
            Variant originalVariant = entry.getValue();
            Variant exportedVariant = exportedVariants.get(entry.getKey());
            assertNotNull("At position " + entry.getValue(), originalVariant);
            assertNotNull("At variant " + originalVariant, exportedVariant);
            assertEquals("At variant " + originalVariant, originalVariant.getChromosome(), exportedVariant.getChromosome());
            assertEquals("At variant " + originalVariant, originalVariant.getAlternate(), exportedVariant.getAlternate());
            assertEquals("At variant " + originalVariant, originalVariant.getReference(), exportedVariant.getReference());
            assertEquals("At variant " + originalVariant, originalVariant.getStart(), exportedVariant.getStart());
            assertEquals("At variant " + originalVariant, originalVariant.getEnd(), exportedVariant.getEnd());
            assertEquals("At variant " + originalVariant, originalVariant.getIds(), exportedVariant.getIds());
            assertEquals("At variant " + originalVariant, originalVariant.getSourceEntries().size(), exportedVariant.getSourceEntries().size());
            assertEquals("At variant " + originalVariant, originalVariant.getSampleNames("f", "s"), exportedVariant.getSampleNames("f", "s"));
            VariantSourceEntry originalSourceEntry = originalVariant.getSourceEntry("f", "s");
            VariantSourceEntry exportedSourceEntry = exportedVariant.getSourceEntry("f", "s");
            for (String sampleName : originalSourceEntry.getSampleNames()) {
//                if (exportedVariant.getAlternate().startsWith("<")) {
//                    System.out.println("Skipped variant " + originalVariant + " " + exportedVariant);
//                    continue;
//                }
                assertEquals("For sample '" + sampleName + "' " + originalVariant + " " + exportedVariant, originalSourceEntry.getSampleData(sampleName, "GT"), exportedSourceEntry.getSampleData(sampleName, "GT").replace("0/0", "0|0"));
            }
        }
    }

    public Map<Integer, Variant> readVCF(Path outputVcf) {
        Map<Integer, Variant> variantMap;
        variantMap = new LinkedHashMap<>();
        VariantVcfReader variantVcfReader = new VariantVcfReader(new VariantSource(outputVcf.getFileName().toString(), "f", "s", ""), outputVcf.toString());
        variantVcfReader.open();
        variantVcfReader.pre();

        List<Variant> read;
        do {
            read = variantVcfReader.read(100);
            for (Variant variant : read) {
                variantMap.put(variant.getStart(), variant);
            }
        } while (!read.isEmpty());

        variantVcfReader.post();
        variantVcfReader.close();
        return variantMap;
    }

}
