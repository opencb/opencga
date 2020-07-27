package org.opencb.opencga.storage.core.variant;

import org.junit.Ignore;
import org.junit.Test;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.avro.SampleEntry;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.*;
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
    public static final String RS = "rs182637865";
    public static final String RS_VARIANT = "1:947562:C:T";

    @Test
    public void indexWithOtherFieldsNoGTSingleFile() throws Exception {
        //GL:DP:GU:TU:AU:CU
        StudyMetadata studyMetadata = newStudyMetadata();
        VariantStorageEngine engine = getVariantStorageEngine();
        runDefaultETL(getResourceUri("variant-test-somatic.vcf"), engine, studyMetadata,
                new ObjectMap(VariantStorageOptions.EXTRA_FORMAT_FIELDS.key(), Arrays.asList("GL", "DP", "AU", "CU", "GU", "TU"))
                        .append(VariantStorageOptions.ANNOTATE.key(), false)
                        .append(VariantStorageOptions.STATS_CALCULATE.key(), false)
        );
        VariantDBIterator iterator = engine.iterator(new Query(VariantQueryParam.UNKNOWN_GENOTYPE.key(), "./."), new QueryOptions());
        while (iterator.hasNext()) {
            Variant variant = iterator.next();
            assertThat(variant.getStudy(STUDY_NAME).getSampleData("SAMPLE_1", "GT"), is(GenotypeClass.NA_GT_VALUE));
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
            assertEquals(1, variant.getStudy(STUDY_NAME).getSamples().size());
            assertEquals(Collections.singleton("SAMPLE_1"), variant.getStudy(STUDY_NAME).getSamplesName());
            if (!variant.getStudy(STUDY_NAME).getFiles().isEmpty()) {
                assertEquals("variant-test-somatic.vcf", variant.getStudy(STUDY_NAME).getFiles().get(0).getFileId());
            }
        });
        assertThat(iterator.getCount(), gt(0));

        checkSampleData(engine, VARIANT_A);
        // Do not test VARIANT_B and RS, as they expect to have SAMPLE_2
//        checkSampleData(engine, VARIANT_B);
//        checkSampleData(engine, RS);
    }

    @Test
    public void indexWithOtherFieldsNoGTTwoFiles() throws Exception {
        //GL:DP:GU:TU:AU:CU
        StudyMetadata studyMetadata = newStudyMetadata();
        VariantStorageEngine engine = getVariantStorageEngine();
        runDefaultETL(getResourceUri("variant-test-somatic.vcf"), engine, studyMetadata,
                new ObjectMap(VariantStorageOptions.EXTRA_FORMAT_FIELDS.key(), Arrays.asList("GL", "DP", "AU", "CU", "GU", "TU"))
                        .append(VariantStorageOptions.ANNOTATE.key(), false)
                        .append(VariantStorageOptions.STATS_CALCULATE.key(), false)
        );
        runDefaultETL(getResourceUri("variant-test-somatic_2.vcf"), engine, studyMetadata,
                new ObjectMap(VariantStorageOptions.STATS_CALCULATE.key(), false)
                        .append(VariantStorageOptions.ANNOTATE.key(), false)
        );
        VariantDBIterator iterator = engine.iterator(new Query(), new QueryOptions());
        while (iterator.hasNext()) {
            Variant variant = iterator.next();
            if (variant.getStudy(STUDY_NAME).getFile("variant-test-somatic.vcf") == null) {
                assertThat(variant.getStudy(STUDY_NAME).getSampleData("SAMPLE_1", "GT"), is(GenotypeClass.UNKNOWN_GENOTYPE));
            } else {
                assertThat(variant.getStudy(STUDY_NAME).getSampleData("SAMPLE_1", "GT"), is(GenotypeClass.NA_GT_VALUE));
            }
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
            assertEquals(1, variant.getStudy(STUDY_NAME).getSamples().size());
            assertEquals(Collections.singleton("SAMPLE_1"), variant.getStudy(STUDY_NAME).getSamplesName());
            if (!variant.getStudy(STUDY_NAME).getFiles().isEmpty()) {
                assertEquals("variant-test-somatic.vcf", variant.getStudy(STUDY_NAME).getFiles().get(0).getFileId());
            }
        });
        assertThat(iterator.getCount(), gt(0));


        checkSampleData(engine, VARIANT_A);
        checkSampleData(engine, VARIANT_B);
        checkSampleData(engine, RS);
    }

    @Test
    public void indexExcludeGenotype() throws Exception {
        VariantStorageEngine engine = getVariantStorageEngine();
        runETL(engine, getPlatinumFile(0), STUDY_NAME, new ObjectMap(VariantStorageOptions.EXCLUDE_GENOTYPES.key(), true));
        runETL(engine, getPlatinumFile(1), STUDY_NAME, new ObjectMap(VariantStorageOptions.EXCLUDE_GENOTYPES.key(), false));

        VariantDBIterator iterator = engine.iterator(new Query(VariantQueryParam.UNKNOWN_GENOTYPE.key(), "./."), new QueryOptions());
        while (iterator.hasNext()) {
            Variant variant = iterator.next();
            Variant sampleData = engine.getSampleData(variant.toString(), STUDY_NAME, new QueryOptions()).first();
            StudyEntry study = variant.getStudy(STUDY_NAME);
            StudyEntry study_2 = sampleData.getStudies().get(0);
            if (study.getFile("1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz") != null) {
                assertThat(study.getSampleData("NA12877", "GT"), is(GenotypeClass.NA_GT_VALUE));
                assertThat(study_2.getSampleData("NA12877", "GT"), is(GenotypeClass.NA_GT_VALUE));
            } else {
                assertThat(study.getSampleData("NA12877", "GT"), is("./."));
                assertThat(study_2.getSampleData("NA12877", "GT"), nullValue());
            }
            if (study.getFile("1K.end.platinum-genomes-vcf-NA12878_S1.genome.vcf.gz") != null) {
                assertThat(study.getSampleData("NA12878", "GT"), not(is(GenotypeClass.NA_GT_VALUE)));
                assertThat(study_2.getSampleData("NA12878", "GT"), not(is(GenotypeClass.NA_GT_VALUE)));
            } else {
                assertThat(study.getSampleData("NA12878", "GT"), is("./."));
                assertThat(study_2.getSampleData("NA12878", "GT"), nullValue());
            }
        }
        assertThat(iterator.getCount(), gt(0));
    }

    @Test
    public void indexWithOtherFieldsExcludeGT() throws Exception {
        indexWithOtherFieldsExcludeGT(new ObjectMap(), new ObjectMap());
    }

    protected void indexWithOtherFieldsExcludeGT(ObjectMap params1, ObjectMap params2) throws Exception {
        //GL:DP:GU:TU:AU:CU
        StudyMetadata studyMetadata = newStudyMetadata();
        List<String> extraFields = Arrays.asList("GL", "DP", "AU", "CU", "GU", "TU");
        VariantStorageEngine engine = getVariantStorageEngine();
        runDefaultETL(getResourceUri("variant-test-somatic.vcf"), engine, studyMetadata,
                new ObjectMap(params1)
                        .append(VariantStorageOptions.EXTRA_FORMAT_FIELDS.key(), extraFields)
                        .append(VariantStorageOptions.EXCLUDE_GENOTYPES.key(), true)
                        .append(VariantStorageOptions.STATS_CALCULATE.key(), false)
//                        .append(VariantStorageEngine.Options.FILE_ID.key(), 2)
                        .append(VariantStorageOptions.ANNOTATE.key(), false)
        );
        runDefaultETL(getResourceUri("variant-test-somatic_2.vcf"), engine, studyMetadata,
                new ObjectMap(params2)
                        .append(VariantStorageOptions.EXTRA_FORMAT_FIELDS.key(), extraFields)
                        .append(VariantStorageOptions.EXCLUDE_GENOTYPES.key(), false)
                        .append(VariantStorageOptions.STATS_CALCULATE.key(), false)
//                        .append(VariantStorageEngine.Options.FILE_ID.key(), 3)
                        .append(VariantStorageOptions.ANNOTATE.key(), false)
        );
        studyMetadata = engine.getMetadataManager().getStudyMetadata(studyMetadata.getId());
        assertFalse(studyMetadata.getAttributes().containsKey(VariantStorageOptions.EXCLUDE_GENOTYPES.key()));
        assertEquals(extraFields, studyMetadata.getAttributes().getAsStringList(VariantStorageOptions.EXTRA_FORMAT_FIELDS.key()));

        for (Variant variant : engine) {
//            System.out.println(variant.toJson());
            if (variant.getStudy(STUDY_NAME).getFile("variant-test-somatic.vcf") == null) {
                assertThat(variant.getStudy(STUDY_NAME).getSampleData("SAMPLE_1", "GT"), is(GenotypeClass.UNKNOWN_GENOTYPE));
            } else {
                assertThat(variant.getStudy(STUDY_NAME).getSampleData("SAMPLE_1", "GT"), is(GenotypeClass.NA_GT_VALUE));
            }            assertNotNull(variant.getStudy(STUDY_NAME).getSampleData("SAMPLE_1", "DP"));
            assertNotNull(variant.getStudy(STUDY_NAME).getSampleData("SAMPLE_1", "GL"));
            assertNotNull(variant.getStudy(STUDY_NAME).getSampleData("SAMPLE_1", "AU"));
            assertNotNull(variant.getStudy(STUDY_NAME).getSampleData("SAMPLE_1", "CU"));
            assertNotNull(variant.getStudy(STUDY_NAME).getSampleData("SAMPLE_1", "GU"));
            assertNotNull(variant.getStudy(STUDY_NAME).getSampleData("SAMPLE_1", "TU"));
        }

        VariantDBIterator iterator = engine.iterator(new Query(VariantQueryParam.INCLUDE_SAMPLE.key(), "SAMPLE_1")
                .append(VariantQueryParam.INCLUDE_FILE.key(), VariantQueryUtils.ALL), new QueryOptions());
        iterator.forEachRemaining(variant -> {
            assertEquals(1, variant.getStudy(STUDY_NAME).getSamples().size());
            assertEquals(Collections.singleton("SAMPLE_1"), variant.getStudy(STUDY_NAME).getSamplesName());
            assertTrue(variant.getStudy(STUDY_NAME).getFiles().size() > 0);
            assertTrue(variant.getStudy(STUDY_NAME).getFiles().size() <= 2);

        });
        assertThat(iterator.getCount(), gt(0));

        iterator = engine.iterator(new Query(VariantQueryParam.INCLUDE_SAMPLE.key(), "SAMPLE_2")
                .append(VariantQueryParam.INCLUDE_FILE.key(), VariantQueryUtils.ALL), new QueryOptions());
        iterator.forEachRemaining(variant -> {
            assertEquals(1, variant.getStudy(STUDY_NAME).getSamples().size());
            assertEquals(Collections.singleton("SAMPLE_2"), variant.getStudy(STUDY_NAME).getSamplesName());
            assertTrue(variant.getStudy(STUDY_NAME).getFiles().size() > 0);
            assertTrue(variant.getStudy(STUDY_NAME).getFiles().size() <= 2);

        });
        assertThat(iterator.getCount(), gt(0));

        iterator = engine.iterator(new Query(VariantQueryParam.INCLUDE_SAMPLE.key(), "SAMPLE_2")
                .append(VariantQueryParam.FILE.key(), "variant-test-somatic_2.vcf"), new QueryOptions());
        iterator.forEachRemaining(variant -> {
            assertEquals(1, variant.getStudy(STUDY_NAME).getSamples().size());
            assertEquals(Collections.singleton("SAMPLE_2"), variant.getStudy(STUDY_NAME).getSamplesName());
            if (!variant.getStudy(STUDY_NAME).getFiles().isEmpty()) {
                assertEquals("variant-test-somatic_2.vcf", variant.getStudy(STUDY_NAME).getFiles().get(0).getFileId());
            }
        });
        assertThat(iterator.getCount(), gt(0));

        iterator = engine.iterator(new Query(VariantQueryParam.SAMPLE.key(), "SAMPLE_1"), new QueryOptions());
        iterator.forEachRemaining(variant -> {
            assertEquals(1, variant.getStudy(STUDY_NAME).getSamples().size());
            assertEquals(Collections.singleton("SAMPLE_1"), variant.getStudy(STUDY_NAME).getSamplesName());
            if (!variant.getStudy(STUDY_NAME).getFiles().isEmpty()) {
                assertEquals("variant-test-somatic.vcf", variant.getStudy(STUDY_NAME).getFiles().get(0).getFileId());
            }
        });
        assertThat(iterator.getCount(), gt(0));

        checkSampleData(engine, VARIANT_A);
        checkSampleData(engine, VARIANT_B);
        checkSampleData(engine, RS);

    }

    protected void checkSampleData(VariantStorageEngine engine, String variant) throws StorageEngineException {
        checkSampleData(engine.getSampleData(variant, STUDY_NAME, QueryOptions.empty()).first());
        checkSampleData(engine.getSampleData(variant, STUDY_NAME, new QueryOptions("genotype", "NA")).first());
        checkSampleData(engine.getSampleData(variant, STUDY_NAME, new QueryOptions("genotype", "0/1,NA")).first());
    }

    protected void checkSampleData(Variant variant) {
        System.out.println("variant = " + variant.toJson());
        StudyEntry studyEntry = variant.getStudies().get(0);
        switch (variant.toString()) {
            case VARIANT_A:
                assertEquals(STUDY_NAME, studyEntry.getStudyId());
                assertEquals(Collections.singletonList("SAMPLE_1"),
                        studyEntry.getSamples().stream().map(SampleEntry::getSampleId).collect(Collectors.toList()));
                assertEquals(Collections.singleton("variant-test-somatic.vcf"),
                        studyEntry.getFiles().stream().map(FileEntry::getFileId).collect(Collectors.toSet()));
                break;

            case VARIANT_B:
            case RS_VARIANT:
                assertEquals(STUDY_NAME, studyEntry.getStudyId());
                assertEquals(Arrays.asList("SAMPLE_1", "SAMPLE_2"),
                        studyEntry.getSamples().stream().map(SampleEntry::getSampleId).collect(Collectors.toList()));
                assertEquals(new HashSet<>(Arrays.asList("variant-test-somatic.vcf", "variant-test-somatic_2.vcf")),
                        studyEntry.getFiles().stream().map(FileEntry::getFileId).collect(Collectors.toSet()));
                break;

            default:
                fail();
        }
    }

}
