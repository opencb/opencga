package org.opencb.opencga.storage.core.variant;

import org.junit.Ignore;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.StoragePipelineResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.adaptors.sample.SampleData;
import org.opencb.opencga.storage.core.variant.adaptors.sample.VariantSampleData;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantMatchers.gt;

/**
 * Created on 27/10/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Ignore
public abstract class VariantStorageEngineSomaticTest extends VariantStorageBaseTest {

    public static final String VARIANT_B = "1:734964:T:C";
    public static final String VARIANT_A = "1:819320:A:C";

    @Test
    public void indexWithOtherFieldsNoGTSingleFile() throws Exception {
        //GL:DP:GU:TU:AU:CU
        StudyMetadata studyMetadata = newStudyMetadata();
        VariantStorageEngine engine = getVariantStorageEngine();
        runDefaultETL(getResourceUri("variant-test-somatic.vcf"), engine, studyMetadata,
                new ObjectMap(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS.key(), Arrays.asList("GL", "DP", "AU", "CU", "GU", "TU"))
                        .append(VariantStorageEngine.Options.ANNOTATE.key(), false)
                        .append(VariantStorageEngine.Options.CALCULATE_STATS.key(), false)
        );
        VariantDBIterator iterator = engine.iterator(new Query(VariantQueryParam.UNKNOWN_GENOTYPE.key(), "./."), new QueryOptions());
        while (iterator.hasNext()) {
            Variant variant = iterator.next();
            assertThat(variant.getStudy(STUDY_NAME).getSampleData("SAMPLE_1", "GT"), anyOf(is("./."), is(".")));
            assertNotNull(variant.getStudy(STUDY_NAME).getSampleData("SAMPLE_1", "DP"));
            assertNotNull(variant.getStudy(STUDY_NAME).getSampleData("SAMPLE_1", "GL"));
            assertNotNull(variant.getStudy(STUDY_NAME).getSampleData("SAMPLE_1", "AU"));
            assertNotNull(variant.getStudy(STUDY_NAME).getSampleData("SAMPLE_1", "CU"));
            assertNotNull(variant.getStudy(STUDY_NAME).getSampleData("SAMPLE_1", "GU"));
            assertNotNull(variant.getStudy(STUDY_NAME).getSampleData("SAMPLE_1", "TU"));
        }
        assertThat(iterator.getCount(), gt(0));

        iterator = engine.iterator(new Query(VariantQueryParam.SAMPLE.key(), "SAMPLE_1"), new QueryOptions());
        iterator.forEachRemaining(variant -> {
            assertEquals(1, variant.getStudy(STUDY_NAME).getSamplesData().size());
            assertEquals(Collections.singleton("SAMPLE_1"), variant.getStudy(STUDY_NAME).getSamplesName());
            if (!variant.getStudy(STUDY_NAME).getFiles().isEmpty()) {
                assertEquals("variant-test-somatic.vcf", variant.getStudy(STUDY_NAME).getFiles().get(0).getFileId());
            }
        });
        assertThat(iterator.getCount(), gt(0));

        checkSampleData(engine, VARIANT_A);
    }

    @Test
    public void indexWithOtherFieldsNoGTTwoFiles() throws Exception {
        //GL:DP:GU:TU:AU:CU
        StudyMetadata studyMetadata = newStudyMetadata();
        VariantStorageEngine engine = getVariantStorageEngine();
        runDefaultETL(getResourceUri("variant-test-somatic.vcf"), engine, studyMetadata,
                new ObjectMap(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS.key(), Arrays.asList("GL", "DP", "AU", "CU", "GU", "TU"))
                        .append(VariantStorageEngine.Options.ANNOTATE.key(), false)
                        .append(VariantStorageEngine.Options.CALCULATE_STATS.key(), false)
        );
        runDefaultETL(getResourceUri("variant-test-somatic_2.vcf"), engine, studyMetadata,
                new ObjectMap(VariantStorageEngine.Options.CALCULATE_STATS.key(), false)
                        .append(VariantStorageEngine.Options.ANNOTATE.key(), false)
        );
        VariantDBIterator iterator = engine.iterator(new Query(VariantQueryParam.UNKNOWN_GENOTYPE.key(), "./."), new QueryOptions());
        while (iterator.hasNext()) {
            Variant variant = iterator.next();
            assertThat(variant.getStudy(STUDY_NAME).getSampleData("SAMPLE_1", "GT"), anyOf(is("./."), is(".")));
            assertNotNull(variant.getStudy(STUDY_NAME).getSampleData("SAMPLE_1", "DP"));
            assertNotNull(variant.getStudy(STUDY_NAME).getSampleData("SAMPLE_1", "GL"));
            assertNotNull(variant.getStudy(STUDY_NAME).getSampleData("SAMPLE_1", "AU"));
            assertNotNull(variant.getStudy(STUDY_NAME).getSampleData("SAMPLE_1", "CU"));
            assertNotNull(variant.getStudy(STUDY_NAME).getSampleData("SAMPLE_1", "GU"));
            assertNotNull(variant.getStudy(STUDY_NAME).getSampleData("SAMPLE_1", "TU"));
        }
        assertThat(iterator.getCount(), gt(0));

        iterator = engine.iterator(new Query(VariantQueryParam.SAMPLE.key(), "SAMPLE_1"), new QueryOptions());
        iterator.forEachRemaining(variant -> {
            assertEquals(1, variant.getStudy(STUDY_NAME).getSamplesData().size());
            assertEquals(Collections.singleton("SAMPLE_1"), variant.getStudy(STUDY_NAME).getSamplesName());
            if (!variant.getStudy(STUDY_NAME).getFiles().isEmpty()) {
                assertEquals("variant-test-somatic.vcf", variant.getStudy(STUDY_NAME).getFiles().get(0).getFileId());
            }
        });
        assertThat(iterator.getCount(), gt(0));


        checkSampleData(engine, VARIANT_A);
        checkSampleData(engine, VARIANT_B);
    }

    @Test
    public void indexWithOtherFieldsExcludeGT() throws Exception {
        //GL:DP:GU:TU:AU:CU
        StudyMetadata studyMetadata = newStudyMetadata();
        List<String> extraFields = Arrays.asList("GL", "DP", "AU", "CU", "GU", "TU");
        VariantStorageEngine engine = getVariantStorageEngine();
        StoragePipelineResult etlResult = runDefaultETL(getResourceUri("variant-test-somatic.vcf"), engine, studyMetadata,
                new ObjectMap(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS.key(), extraFields)
                        .append(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS_COMPRESS.key(), false)
                        .append(VariantStorageEngine.Options.EXCLUDE_GENOTYPES.key(), true)
                        .append(VariantStorageEngine.Options.CALCULATE_STATS.key(), false)
//                        .append(VariantStorageEngine.Options.FILE_ID.key(), 2)
                        .append(VariantStorageEngine.Options.ANNOTATE.key(), false)
        );
        etlResult = runDefaultETL(getResourceUri("variant-test-somatic_2.vcf"), engine, studyMetadata,
                new ObjectMap(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS.key(), extraFields)
                        .append(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS_COMPRESS.key(), true)
                        .append(VariantStorageEngine.Options.EXCLUDE_GENOTYPES.key(), false)
                        .append(VariantStorageEngine.Options.CALCULATE_STATS.key(), false)
//                        .append(VariantStorageEngine.Options.FILE_ID.key(), 3)
                        .append(VariantStorageEngine.Options.ANNOTATE.key(), false)
        );
        studyMetadata = engine.getMetadataManager().getStudyMetadata(studyMetadata.getId());
        assertEquals(true, studyMetadata.getAttributes().getBoolean(VariantStorageEngine.Options.EXCLUDE_GENOTYPES.key(), false));
        assertEquals(extraFields, studyMetadata.getAttributes().getAsStringList(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS.key()));

        for (Variant variant : engine) {
//            System.out.println(variant.toJson());
            assertNull(variant.getStudy(STUDY_NAME).getSampleData("SAMPLE_1", "GT"));
            assertNotNull(variant.getStudy(STUDY_NAME).getSampleData("SAMPLE_1", "DP"));
            assertNotNull(variant.getStudy(STUDY_NAME).getSampleData("SAMPLE_1", "GL"));
            assertNotNull(variant.getStudy(STUDY_NAME).getSampleData("SAMPLE_1", "AU"));
            assertNotNull(variant.getStudy(STUDY_NAME).getSampleData("SAMPLE_1", "CU"));
            assertNotNull(variant.getStudy(STUDY_NAME).getSampleData("SAMPLE_1", "GU"));
            assertNotNull(variant.getStudy(STUDY_NAME).getSampleData("SAMPLE_1", "TU"));
        }

        VariantDBIterator iterator = engine.iterator(new Query(VariantQueryParam.INCLUDE_SAMPLE.key(), "SAMPLE_1")
                .append(VariantQueryParam.INCLUDE_FILE.key(), VariantQueryUtils.ALL), new QueryOptions());
        iterator.forEachRemaining(variant -> {
            assertEquals(1, variant.getStudy(STUDY_NAME).getSamplesData().size());
            assertEquals(Collections.singleton("SAMPLE_1"), variant.getStudy(STUDY_NAME).getSamplesName());
            assertTrue(variant.getStudy(STUDY_NAME).getFiles().size() > 0);
            assertTrue(variant.getStudy(STUDY_NAME).getFiles().size() <= 2);

        });
        assertThat(iterator.getCount(), gt(0));

        iterator = engine.iterator(new Query(VariantQueryParam.INCLUDE_SAMPLE.key(), "SAMPLE_2")
                .append(VariantQueryParam.INCLUDE_FILE.key(), VariantQueryUtils.ALL), new QueryOptions());
        iterator.forEachRemaining(variant -> {
            assertEquals(1, variant.getStudy(STUDY_NAME).getSamplesData().size());
            assertEquals(Collections.singleton("SAMPLE_2"), variant.getStudy(STUDY_NAME).getSamplesName());
            assertTrue(variant.getStudy(STUDY_NAME).getFiles().size() > 0);
            assertTrue(variant.getStudy(STUDY_NAME).getFiles().size() <= 2);

        });
        assertThat(iterator.getCount(), gt(0));

        iterator = engine.iterator(new Query(VariantQueryParam.INCLUDE_SAMPLE.key(), "SAMPLE_2")
                .append(VariantQueryParam.FILE.key(), "variant-test-somatic_2.vcf"), new QueryOptions());
        iterator.forEachRemaining(variant -> {
            assertEquals(1, variant.getStudy(STUDY_NAME).getSamplesData().size());
            assertEquals(Collections.singleton("SAMPLE_2"), variant.getStudy(STUDY_NAME).getSamplesName());
            if (!variant.getStudy(STUDY_NAME).getFiles().isEmpty()) {
                assertEquals("variant-test-somatic_2.vcf", variant.getStudy(STUDY_NAME).getFiles().get(0).getFileId());
            }
        });
        assertThat(iterator.getCount(), gt(0));

        iterator = engine.iterator(new Query(VariantQueryParam.SAMPLE.key(), "SAMPLE_1"), new QueryOptions());
        iterator.forEachRemaining(variant -> {
            assertEquals(1, variant.getStudy(STUDY_NAME).getSamplesData().size());
            assertEquals(Collections.singleton("SAMPLE_1"), variant.getStudy(STUDY_NAME).getSamplesName());
            if (!variant.getStudy(STUDY_NAME).getFiles().isEmpty()) {
                assertEquals("variant-test-somatic.vcf", variant.getStudy(STUDY_NAME).getFiles().get(0).getFileId());
            }
        });
        assertThat(iterator.getCount(), gt(0));

        checkSampleData(engine, VARIANT_A);
        checkSampleData(engine, VARIANT_B);

    }

    protected void checkSampleData(VariantStorageEngine engine, String variant) throws StorageEngineException {
        checkSampleData(engine.getSampleData(variant, STUDY_NAME, QueryOptions.empty()).first());
        checkSampleData(engine.getSampleData(variant, STUDY_NAME, new QueryOptions("genotype", "NA")).first());
        checkSampleData(engine.getSampleData(variant, STUDY_NAME, new QueryOptions("genotype", "0/1,NA")).first());
    }

    protected void checkSampleData(VariantSampleData sampleData) {
        System.out.println("sampleData = " + sampleData);
        switch (sampleData.getId()) {
            case VARIANT_A:
                assertEquals(STUDY_NAME, sampleData.getStudyId());
                assertEquals(Collections.singletonList("SAMPLE_1"),
                        sampleData.getSamples().get(GenotypeClass.NA_GT_VALUE).stream().map(SampleData::getId).collect(Collectors.toList()));
                assertEquals(Collections.singleton("variant-test-somatic.vcf"),
                        sampleData.getFiles().keySet());
                break;

            case VARIANT_B:
                assertEquals(STUDY_NAME, sampleData.getStudyId());
                assertEquals(Arrays.asList("SAMPLE_1", "SAMPLE_2"),
                        sampleData.getSamples().get(GenotypeClass.NA_GT_VALUE).stream().map(SampleData::getId).collect(Collectors.toList()));
                assertEquals(new HashSet<>(Arrays.asList("variant-test-somatic.vcf", "variant-test-somatic_2.vcf")),
                        sampleData.getFiles().keySet());
                break;

            default:
                fail();
        }
    }

}
