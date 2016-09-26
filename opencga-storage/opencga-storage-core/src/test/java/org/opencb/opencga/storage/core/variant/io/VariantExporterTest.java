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
import org.opencb.biodata.formats.variant.io.VariantReader;
import org.opencb.biodata.formats.variant.vcf4.io.VariantVcfReader;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantNormalizer;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.tools.variant.VariantVcfHtsjdkReader;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.StorageETLResult;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.VariantStorageManagerTestUtils;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;

import java.io.*;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Created by jmmut on 2015-07-15.
 *
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
 */
@Ignore
public abstract class VariantExporterTest extends VariantStorageManagerTestUtils {

    public static final String[] VCF_TEST_FILE_NAMES = {
            "1000g_batches/1-500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz",
            "1000g_batches/501-1000.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz",
            "1000g_batches/1001-1500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz",
            "1000g_batches/1501-2000.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz",
            "1000g_batches/2001-2504.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz",
    };

    public static final String EXPORTED_FILE_NAME = "exported-variant-test-file.vcf.gz";
    private static URI[] inputUri;
    private static StorageETLResult[] etlResult;
    private VariantDBAdaptor dbAdaptor;
    protected QueryOptions options;
    protected QueryResult<Variant> queryResult;
    protected static StudyConfiguration studyConfiguration;

    @BeforeClass
    public static void beforeClass() throws IOException {
        inputUri = new URI[VCF_TEST_FILE_NAMES.length];
        etlResult = new StorageETLResult[VCF_TEST_FILE_NAMES.length];
        for (int i = 0; i < VCF_TEST_FILE_NAMES.length; i++) {
            etlResult[i] = null;
            inputUri[i] = getResourceUri(VCF_TEST_FILE_NAMES[i]);
        }
    }

    @Override
    @Before
    public void before() throws Exception {
        if (studyConfiguration == null) {
            clearDB(DB_NAME);
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
    public void after() throws IOException {
        dbAdaptor.close();
    }

    @Test
    public void testVcfHtsExportSingleFile() throws Exception {
        Query query = new Query();
        LinkedHashSet<Integer> returnedSamplesIds = dbAdaptor.getStudyConfigurationManager().getStudyConfiguration(STUDY_NAME, null)
                .first().getSamplesInFiles().get(0);
        List<String> returnedSamples = new LinkedList<>();
        Map<Integer, String> sampleIdMap = StudyConfiguration.inverseMap(studyConfiguration.getSampleIds());
        for (Integer sampleId : returnedSamplesIds) {
            returnedSamples.add(sampleIdMap.get(sampleId));
        }
        query.append(VariantDBAdaptor.VariantQueryParams.STUDIES.key(), STUDY_NAME)
                .append(VariantDBAdaptor.VariantQueryParams.RETURNED_FILES.key(), 0)
                .append(VariantDBAdaptor.VariantQueryParams.FILES.key(), 0)
                .append(VariantDBAdaptor.VariantQueryParams.RETURNED_SAMPLES.key(), returnedSamples);
        Path outputVcf = getTmpRootDir().resolve("hts_sf_" + EXPORTED_FILE_NAME);
        int failedVariants = VariantVcfExporter.htsExport(dbAdaptor.iterator(query, new QueryOptions(QueryOptions.SORT, true)),
                studyConfiguration, dbAdaptor.getVariantSourceDBAdaptor()
                , new GZIPOutputStream(new FileOutputStream(outputVcf.toFile())), new QueryOptions(VariantDBAdaptor.VariantQueryParams
                        .RETURNED_SAMPLES.key(), returnedSamples));

        assertEquals(0, failedVariants);
        // compare VCF_TEST_FILE_NAME and EXPORTED_FILE_NAME
        checkExportedVCF(Paths.get(getResourceUri("1000g_batches/1-500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf" +
                ".gz")), outputVcf, new Region("22"));
    }

    @Test
    public void testVcfHtsExportMultiFile() throws Exception {
        Query query = new Query();
        query.append(VariantDBAdaptor.VariantQueryParams.STUDIES.key(), STUDY_NAME);
//                .append(VariantDBAdaptor.VariantQueryParams.REGION.key(), region);
        Path outputVcf = getTmpRootDir().resolve("hts_mf_" + EXPORTED_FILE_NAME);
        int failedVariants = VariantVcfExporter.htsExport(dbAdaptor.iterator(query, null), studyConfiguration,
                dbAdaptor.getVariantSourceDBAdaptor(),
                new GZIPOutputStream(new FileOutputStream(outputVcf.toFile())), null);

        assertEquals(0, failedVariants);
        // compare VCF_TEST_FILE_NAME and EXPORTED_FILE_NAME
        Path originalVcf = Paths.get(getResourceUri("filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"));

        VariantVcfReader variantVcfReader = new VariantVcfReader(new VariantSource(originalVcf.getFileName().toString(), "f", "s", ""),
                originalVcf.toString());
        variantVcfReader.open();
        variantVcfReader.pre();

        Region region = new Region("22", 16000000);
        int batchSize = 1000;
        while (checkExportedVCF(originalVcf, variantVcfReader, outputVcf, region, batchSize) != batchSize) {
            region = new Region("22", region.getEnd());
        }

        variantVcfReader.post();
        variantVcfReader.close();
    }

    @Ignore
    @Test
    public void testVcfExport() throws Exception {

        QueryOptions queryOptions = new QueryOptions();
        List<String> include = Arrays.asList("chromosome", "start", "end", "alternative", "reference", "ids", "sourceEntries");
        queryOptions.add("include", include);
        VariantVcfExporter.vcfExport(dbAdaptor, studyConfiguration, new URI(EXPORTED_FILE_NAME), new Query(), queryOptions);

        // compare VCF_TEST_FILE_NAME and EXPORTED_FILE_NAME
    }

    public int checkExportedVCF(Path originalVcf, Path exportedVcf, Region region) throws IOException {
        return checkExportedVCF(originalVcf, null, exportedVcf, region, null);
    }

    /**
     * @return number of read variants
     */
    public int checkExportedVCF(Path originalVcf, VariantVcfReader originalVcfReader, Path exportedVcf, Region region, Integer lim)
            throws IOException {
        Map<String, Variant> originalVariants;
        if (originalVcfReader == null) {
            originalVariants = readVCF(originalVcf, lim, region);
        } else {
            originalVariants = readVCF(originalVcf, lim, region, originalVcfReader);
        }
        Map<String, Variant> exportedVariants = readVCF(exportedVcf, region);

        assertEquals(originalVariants.size(), exportedVariants.size());
        for (Map.Entry<String, Variant> entry : originalVariants.entrySet()) {
            Variant originalVariant = entry.getValue();
            Variant exportedVariant = exportedVariants.get(entry.getKey());
            assertNotNull("At position " + entry.getValue(), originalVariant);
            assertNotNull("At variant " + originalVariant, exportedVariant);
            assertEquals("At variant " + originalVariant, originalVariant.getChromosome(), exportedVariant.getChromosome());
            assertEquals("At variant " + originalVariant, originalVariant.getAlternate(), exportedVariant.getAlternate());
            assertEquals("At variant " + originalVariant, originalVariant.getReference(), exportedVariant.getReference());
            assertEquals("At variant " + originalVariant, originalVariant.getStart(), exportedVariant.getStart());
            assertEquals("At variant " + originalVariant, originalVariant.getEnd(), exportedVariant.getEnd());
            assertWithConflicts(originalVariant, () -> assertEquals("At variant " + originalVariant, originalVariant.getIds(), exportedVariant.getIds()));
            assertEquals("At variant " + originalVariant, originalVariant.getStudies().size(), exportedVariant.getStudies().size());
            assertEquals("At variant " + originalVariant, originalVariant.getSampleNames("f", "s"), exportedVariant.getSampleNames("f",
                    "s"));
            StudyEntry originalSourceEntry = originalVariant.getStudy("s");
            StudyEntry exportedSourceEntry = exportedVariant.getStudy("s");
            for (String sampleName : originalSourceEntry.getSamplesName()) {
                assertWithConflicts(exportedVariant, () -> assertEquals("For sample '" + sampleName + "', id "
                                + studyConfiguration.getSampleIds().get(sampleName)
                                + ", in " + originalVariant,
                        originalSourceEntry.getSampleData(sampleName, "GT"),
                        exportedSourceEntry.getSampleData(sampleName, "GT").replace("0/0", "0|0")));
            }
        }
        return originalVariants.size();
    }

    public Map<String, Variant> readVCF(Path vcfPath, Region region) throws IOException {
        return readVCF(vcfPath, null, region);
    }

    public Map<String, Variant> readVCF(Path vcfPath, Integer lim, Region region) throws IOException {
        if (lim == null) {
            lim = Integer.MAX_VALUE;
        }
        if (region == null) {
            region = new Region();
        }

        InputStream is = new FileInputStream(vcfPath.toFile());
        if (vcfPath.toString().endsWith(".gz")) {
            is = new GZIPInputStream(is);
        }
        VariantReader variantVcfReader = new VariantVcfHtsjdkReader(is, new VariantSource(vcfPath.getFileName().toString(), "f", "s", ""), new VariantNormalizer());
        variantVcfReader.open();
        variantVcfReader.pre();

        Map<String, Variant> variantMap;
        variantMap = readVCF(vcfPath, lim, region, variantVcfReader);

        variantVcfReader.post();
        variantVcfReader.close();
        return variantMap;
    }

    public Map<String, Variant> readVCF(Path vcfPath, Integer lim, Region region, VariantReader variantVcfReader) {
        Map<String, Variant> variantMap;
        variantMap = new LinkedHashMap<>();
        List<Variant> read;
        int lines = 0;
        int batchSize = 100;
        int start = Integer.MAX_VALUE;
        int end = 0;
        int variantsToRead = lines + batchSize > lim ? lim - lines : batchSize;
        do {
            System.err.println("Reading " + variantsToRead + " variants from '" + vcfPath.getFileName().toString() + "' line : " + lines
                    + " variants : " + variantMap.size());
            read = variantVcfReader.read(variantsToRead);
            for (Variant variant : read) {
                lines++;
                if (variant.getType().equals(VariantType.SYMBOLIC) || variant.getAlternate().startsWith("<")) {
                    continue;
                }
                if (variant.getStart() >= region.getStart() && variant.getEnd() <= region.getEnd()) {
                    start = Math.min(start, variant.getStart());
                    end = Math.max(end, variant.getEnd());
                    variantMap.put(variant.getStart() + "_" + variant.getAlternate(), variant);
                    if (variantMap.size() == lim) {
                        break;
                    }
                }
            }
        } while (!read.isEmpty() && variantMap.size() < lim);
        region.setStart(start);
        region.setEnd(end);
        System.out.println("Read " + variantMap.size() + " variants between " + region.toString());

        return variantMap;
    }

}
