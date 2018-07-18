package org.opencb.opencga.storage.core.variant;

import org.junit.Ignore;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.StoragePipelineResult;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created on 27/10/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Ignore
public abstract class VariantStorageEngineSomaticTest extends VariantStorageBaseTest {

    @Test
    public void indexWithOtherFieldsNoGT() throws Exception {
        //GL:DP:GU:TU:AU:CU
        StudyConfiguration studyConfiguration = newStudyConfiguration();
        StoragePipelineResult etlResult = runDefaultETL(getResourceUri("variant-test-somatic.vcf"), getVariantStorageEngine(), studyConfiguration,
                new ObjectMap(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS.key(), Arrays.asList("GL", "DP", "AU", "CU", "GU", "TU"))
//                        .append(VariantStorageEngine.Options.FILE_ID.key(), 2)
                        .append(VariantStorageEngine.Options.ANNOTATE.key(), false)
        );

        VariantDBIterator iterator = getVariantStorageEngine().getDBAdaptor().iterator(new Query(VariantQueryParam.UNKNOWN_GENOTYPE.key(), "./."), new QueryOptions());
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

    }

    @Test
    public void indexWithOtherFieldsExcludeGT() throws Exception {
        //GL:DP:GU:TU:AU:CU
        StudyConfiguration studyConfiguration = newStudyConfiguration();
        List<String> extraFields = Arrays.asList("GL", "DP", "AU", "CU", "GU", "TU");
        StoragePipelineResult etlResult = runDefaultETL(getResourceUri("variant-test-somatic.vcf"), getVariantStorageEngine(), studyConfiguration,
                new ObjectMap(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS.key(), extraFields)
                        .append(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS_COMPRESS.key(), false)
                        .append(VariantStorageEngine.Options.EXCLUDE_GENOTYPES.key(), true)
                        .append(VariantStorageEngine.Options.CALCULATE_STATS.key(), false)
//                        .append(VariantStorageEngine.Options.FILE_ID.key(), 2)
                        .append(VariantStorageEngine.Options.ANNOTATE.key(), false)
        );
        etlResult = runDefaultETL(getResourceUri("variant-test-somatic_2.vcf"), getVariantStorageEngine(), studyConfiguration,
                new ObjectMap(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS.key(), extraFields)
                        .append(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS_COMPRESS.key(), true)
                        .append(VariantStorageEngine.Options.EXCLUDE_GENOTYPES.key(), false)
                        .append(VariantStorageEngine.Options.CALCULATE_STATS.key(), false)
//                        .append(VariantStorageEngine.Options.FILE_ID.key(), 3)
                        .append(VariantStorageEngine.Options.ANNOTATE.key(), false)
        );
        VariantDBAdaptor dbAdaptor = getVariantStorageEngine().getDBAdaptor();
        studyConfiguration = dbAdaptor.getStudyConfigurationManager().getStudyConfiguration(studyConfiguration.getStudyId(), null).first();
        assertEquals(true, studyConfiguration.getAttributes().getBoolean(VariantStorageEngine.Options.EXCLUDE_GENOTYPES.key(), false));
        assertEquals(extraFields, studyConfiguration.getAttributes().getAsStringList(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS.key()));

        for (Variant variant : dbAdaptor) {
            System.out.println(variant.toJson());
            assertNull(variant.getStudy(STUDY_NAME).getSampleData("SAMPLE_1", "GT"));
            assertNotNull(variant.getStudy(STUDY_NAME).getSampleData("SAMPLE_1", "DP"));
            assertNotNull(variant.getStudy(STUDY_NAME).getSampleData("SAMPLE_1", "GL"));
            assertNotNull(variant.getStudy(STUDY_NAME).getSampleData("SAMPLE_1", "AU"));
            assertNotNull(variant.getStudy(STUDY_NAME).getSampleData("SAMPLE_1", "CU"));
            assertNotNull(variant.getStudy(STUDY_NAME).getSampleData("SAMPLE_1", "GU"));
            assertNotNull(variant.getStudy(STUDY_NAME).getSampleData("SAMPLE_1", "TU"));
        }

        VariantDBIterator iterator = dbAdaptor.iterator(new Query(VariantQueryParam.INCLUDE_SAMPLE.key(), "SAMPLE_1")
                .append(VariantQueryParam.INCLUDE_FILE.key(), VariantQueryUtils.ALL), new QueryOptions());
        iterator.forEachRemaining(variant -> {
            assertEquals(1, variant.getStudy(STUDY_NAME).getSamplesData().size());
            assertEquals(Collections.singleton("SAMPLE_1"), variant.getStudy(STUDY_NAME).getSamplesName());
            assertTrue(variant.getStudy(STUDY_NAME).getFiles().size() > 0);
            assertTrue(variant.getStudy(STUDY_NAME).getFiles().size() <= 2);

        });

        iterator = dbAdaptor.iterator(new Query(VariantQueryParam.INCLUDE_SAMPLE.key(), "SAMPLE_2")
                .append(VariantQueryParam.INCLUDE_FILE.key(), VariantQueryUtils.ALL), new QueryOptions());
        iterator.forEachRemaining(variant -> {
            assertEquals(1, variant.getStudy(STUDY_NAME).getSamplesData().size());
            assertEquals(Collections.singleton("SAMPLE_2"), variant.getStudy(STUDY_NAME).getSamplesName());
            assertTrue(variant.getStudy(STUDY_NAME).getFiles().size() > 0);
            assertTrue(variant.getStudy(STUDY_NAME).getFiles().size() <= 2);

        });

        iterator = dbAdaptor.iterator(new Query(VariantQueryParam.INCLUDE_SAMPLE.key(), "SAMPLE_2")
                .append(VariantQueryParam.FILE.key(), "variant-test-somatic_2.vcf"), new QueryOptions());
        iterator.forEachRemaining(variant -> {
            System.out.println("variant.toJson() = " + variant.toJson());
            assertEquals(1, variant.getStudy(STUDY_NAME).getSamplesData().size());
            assertEquals(Collections.singleton("SAMPLE_2"), variant.getStudy(STUDY_NAME).getSamplesName());
            if (!variant.getStudy(STUDY_NAME).getFiles().isEmpty()) {
                assertEquals("2", variant.getStudy(STUDY_NAME).getFiles().get(0).getFileId());
            }
        });
    }

}
