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

package org.opencb.opencga.storage.core.variant.io;


import org.junit.*;
import org.opencb.biodata.formats.variant.io.VariantReader;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.models.variant.metadata.VariantStudyMetadata;
import org.opencb.biodata.tools.variant.VariantNormalizer;
import org.opencb.biodata.tools.variant.VariantVcfHtsjdkReader;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.io.DataWriter;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.storage.core.StoragePipelineResult;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
public abstract class VariantVcfExporterTest extends VariantStorageBaseTest {

    public static final String[] VCF_TEST_FILE_NAMES = {
            "1000g_batches/1-500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz",
            "1000g_batches/501-1000.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz",
            "1000g_batches/1001-1500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz",
            "1000g_batches/1501-2000.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz",
            "1000g_batches/2001-2504.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz",
    };

    public static final String EXPORTED_FILE_NAME = "exported-variant-test-file.vcf.gz";
    private static final VariantNormalizer VARIANT_NORMALIZER = new VariantNormalizer();
    private static URI[] inputUri;
    private static StoragePipelineResult[] etlResult;
    private VariantDBAdaptor dbAdaptor;
    protected QueryOptions options;
    protected QueryResult<Variant> queryResult;
    protected static StudyConfiguration studyConfiguration;

    @BeforeClass
    public static void beforeClass() throws IOException {
        inputUri = new URI[VCF_TEST_FILE_NAMES.length];
        etlResult = new StoragePipelineResult[VCF_TEST_FILE_NAMES.length];
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
                etlResult[i] = runDefaultETL(inputUri[i], getVariantStorageEngine(), studyConfiguration,
                        new ObjectMap(VariantStorageEngine.Options.ANNOTATE.key(), false)
                                .append(VariantStorageEngine.Options.CALCULATE_STATS.key(), false));
            }
        }
        dbAdaptor = getVariantStorageEngine().getDBAdaptor();
    }

    @After
    public void after() throws IOException {
        dbAdaptor.close();
    }

    @Test
    public void testVcfHtsExportSingleFile() throws Exception {
        Query query = new Query()
                .append(VariantQueryParam.STUDY.key(), STUDY_NAME)
                .append(VariantQueryParam.FILE.key(), 1);

        Path outputVcf = getTmpRootDir().resolve("hts_sf_" + EXPORTED_FILE_NAME);
        QueryOptions options = new QueryOptions(QueryOptions.SORT, true);
        int failedVariants = export(outputVcf, query, options);

        assertEquals(0, failedVariants);
        // compare VCF_TEST_FILE_NAME and EXPORTED_FILE_NAME
        checkExportedVCF(Paths.get(getResourceUri("1000g_batches/1-500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf" +
                ".gz")), outputVcf, new Region("22"));
    }

    @Test
    public void testVcfHtsExportMultiFile() throws Exception {
        Query query = new Query();
        query.append(VariantQueryParam.STUDY.key(), STUDY_NAME);
//                .append(VariantDBAdaptor.VariantQueryParams.REGION.key(), region);
        Path outputVcf = getTmpRootDir().resolve("hts_mf_" + EXPORTED_FILE_NAME);
        int failedVariants = export(outputVcf, query, new QueryOptions(QueryOptions.SORT, true));

        assertEquals(0, failedVariants);
        // compare VCF_TEST_FILE_NAME and EXPORTED_FILE_NAME
        Path originalVcf = Paths.get(getResourceUri("filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"));

        VariantReader variantReader = new VariantVcfHtsjdkReader(
                FileUtils.newInputStream(originalVcf),
                getMetadata(originalVcf),
                VARIANT_NORMALIZER);
        variantReader.open();
        variantReader.pre();

        Region region = new Region("22", 16000000);
        int batchSize = 2000;
        while (checkExportedVCF(originalVcf, variantReader, outputVcf, region, batchSize) != batchSize) {
            region = new Region("22", region.getEnd());
        }

        variantReader.post();
        variantReader.close();
    }

    public int export(Path outputVcf, Query query, QueryOptions options) throws IOException {
        try (GZIPOutputStream outputStream = new GZIPOutputStream(new FileOutputStream(outputVcf.toFile()))) {
            DataWriter<Variant> writer = new VariantWriterFactory(dbAdaptor).newDataWriter(VariantWriterFactory.VariantOutputFormat.VCF_GZ, outputStream, query, options);

            writer.open();
            writer.pre();
            dbAdaptor.iterator(query, options).forEachRemaining(writer::write);
            writer.post();
            writer.close();
        }
        return 0;
    }

    @Ignore
    @Test
    public void testVcfExport() throws Exception {

        QueryOptions queryOptions = new QueryOptions();
        List<String> include = Arrays.asList("chromosome", "start", "end", "alternative", "reference", "ids", "sourceEntries");
        queryOptions.add("include", include);
        VariantVcfDataWriter.vcfExport(dbAdaptor, studyConfiguration, new URI(EXPORTED_FILE_NAME), new Query(), queryOptions);

        // compare VCF_TEST_FILE_NAME and EXPORTED_FILE_NAME
    }

    public int checkExportedVCF(Path originalVcf, Path exportedVcf, Region region) throws IOException {
        return checkExportedVCF(originalVcf, null, exportedVcf, region, null);
    }

    /**
     * @return number of read variants
     */
    public int checkExportedVCF(Path originalVcf, VariantReader originalVcfReader, Path exportedVcf, Region region, Integer lim)
            throws IOException {
        Map<String, Variant> originalVariants;
        if (originalVcfReader == null) {
            originalVariants = readVCF(originalVcf, lim, region);
        } else {
            originalVariants = readVCF(originalVcf, lim, region, originalVcfReader);
        }
        Map<String, Variant> exportedVariants = readVCF(exportedVcf, region);

//        if (originalVariants.size() != exportedVariants.size()) {
            for (String original : originalVariants.keySet()) {
                if (!exportedVariants.containsKey(original)) {
                    System.out.println("original = " + original);
                }
            }
            for (String exported : exportedVariants.keySet()) {
                if (!originalVariants.containsKey(exported)) {
                    System.out.println("exported = " + exported);
                }
            }
//        }

//        assertEquals(originalVariants.size(), exportedVariants.size());
        for (Map.Entry<String, Variant> entry : originalVariants.entrySet()) {
            Variant originalVariant = entry.getValue();
            Variant exportedVariant = exportedVariants.get(entry.getKey());
            assertNotNull("At position " + entry.getValue(), originalVariant);
            String message = "At original variant: " + originalVariant + ", and exported variant: " + exportedVariant;
            assertNotNull(message, exportedVariant);
            assertEquals(message, originalVariant.getChromosome(), exportedVariant.getChromosome());
            assertEquals(message, originalVariant.getAlternate(), exportedVariant.getAlternate());
            assertEquals(message, originalVariant.getReference(), exportedVariant.getReference());
            assertEquals(message, originalVariant.getStart(), exportedVariant.getStart());
            assertEquals(message, originalVariant.getEnd(), exportedVariant.getEnd());
            assertWithConflicts(originalVariant, () -> assertEquals("At variant " + originalVariant, originalVariant.getIds(), exportedVariant.getIds()));
            assertEquals(message, originalVariant.getStudies().size(), exportedVariant.getStudies().size());
            assertEquals(message, originalVariant.getSampleNames(STUDY_NAME), exportedVariant.getSampleNames(STUDY_NAME));
            StudyEntry originalStudyEntry = originalVariant.getStudy(STUDY_NAME);
            StudyEntry exportedStudyEntry = exportedVariant.getStudy(STUDY_NAME);
            for (String sampleName : originalStudyEntry.getSamplesName()) {
                assertWithConflicts(exportedVariant, () -> assertEquals("For sample '" + sampleName + "', id "
                                + studyConfiguration.getSampleIds().get(sampleName)
                                + ", in " + originalVariant,
                        originalStudyEntry.getSampleData(sampleName, "GT"),
                        exportedStudyEntry.getSampleData(sampleName, "GT").replace("0/0", "0|0")));
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
        VariantReader variantVcfReader = new VariantVcfHtsjdkReader(is, getMetadata(vcfPath), VARIANT_NORMALIZER);
        variantVcfReader.open();
        variantVcfReader.pre();

        Map<String, Variant> variantMap;
        variantMap = readVCF(vcfPath, lim, region, variantVcfReader);

        variantVcfReader.post();
        variantVcfReader.close();
        return variantMap;
    }

    protected static VariantStudyMetadata getMetadata(Path vcfPath) {
        return new VariantFileMetadata(vcfPath.getFileName().toString(), "").toVariantStudyMetadata(STUDY_NAME);
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
                    variantMap.put(variant.toString(), variant);
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
