package org.opencb.opencga.storage.core.variant.adaptors;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSourceEntry;
import org.opencb.biodata.models.variant.VariantStudy;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.Query;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.VariantStorageManagerTestUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor.VariantQueryParams.*;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Ignore
public abstract class VariantDBAdaptorLargeTest extends VariantStorageManagerTestUtils {


    private static StudyConfiguration studyConfiguration1;
    private static StudyConfiguration studyConfiguration2;
    private static StudyConfiguration studyConfiguration3;
    private static VariantDBAdaptor dbAdaptor;
    private QueryResult<Variant> queryResult;
    static private final int NUM_VARIANTS = 9755;
    private QueryOptions options;
    private Query query;

    @Before
    public void before() throws Exception {
        options = new QueryOptions();
        query = new Query();
        if (studyConfiguration1 == null) {
            clearDB(DB_NAME);
            studyConfiguration1 = new StudyConfiguration(1, "Study1");
            studyConfiguration2 = new StudyConfiguration(2, "Study2");
            studyConfiguration3 = new StudyConfiguration(3, "Study3");

            ObjectMap options = new ObjectMap()
                    .append(VariantStorageManager.Options.STUDY_TYPE.key(), VariantStudy.StudyType.CONTROL)
                    .append(VariantStorageManager.Options.CALCULATE_STATS.key(), false)
                    .append(VariantStorageManager.Options.ANNOTATE.key(), false);
            runDefaultETL(getResourceUri("1-500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"), variantStorageManager, studyConfiguration1, options.append(VariantStorageManager.Options.FILE_ID.key(), 5));
            runDefaultETL(getResourceUri("501-1000.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"), variantStorageManager, studyConfiguration1, options.append(VariantStorageManager.Options.FILE_ID.key(), 6));
            runDefaultETL(getResourceUri("1001-1500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"), variantStorageManager, studyConfiguration2, options.append(VariantStorageManager.Options.FILE_ID.key(), 7));
            runDefaultETL(getResourceUri("1501-2000.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"), variantStorageManager, studyConfiguration2, options.append(VariantStorageManager.Options.FILE_ID.key(), 8));
            runDefaultETL(getResourceUri("2001-2504.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"), variantStorageManager, studyConfiguration3, options.append(VariantStorageManager.Options.FILE_ID.key(), 9));

            dbAdaptor = variantStorageManager.getDBAdaptor(DB_NAME);
        }
    }


    @Test
    public void testGetAllVariants_returnedStudies1() {
        String studyId = Integer.toString(studyConfiguration1.getStudyId());
        query.append(RETURNED_STUDIES.key(), studyConfiguration1.getStudyId());
        queryResult = dbAdaptor.get(query, options);

        assertEquals(NUM_VARIANTS, queryResult.getNumResults());
        assertEquals(NUM_VARIANTS, queryResult.getNumTotalResults());

        for (Variant variant : queryResult.getResult()) {
            for (VariantSourceEntry sourceEntry : variant.getSourceEntries().values()) {
                assertEquals(studyId, sourceEntry.getStudyId());
            }
        }
    }

    @Test
    public void testGetAllVariants_returnedStudies3() {
        String studyId = Integer.toString(studyConfiguration3.getStudyId());
        query.append(RETURNED_STUDIES.key(), studyId);
        queryResult = dbAdaptor.get(query, options);

        assertEquals(NUM_VARIANTS, queryResult.getNumResults());
        assertEquals(NUM_VARIANTS, queryResult.getNumTotalResults());

        for (Variant variant : queryResult.getResult()) {
            for (VariantSourceEntry sourceEntry : variant.getSourceEntries().values()) {
                assertEquals(studyId, sourceEntry.getStudyId());
            }
        }    }

    @Test
    public void testGetAllVariants_returnedStudies2_3() {
        List<String> studyIds = Arrays.asList(
                Integer.toString(studyConfiguration2.getStudyId()),
                Integer.toString(studyConfiguration3.getStudyId()));
        query.append(RETURNED_STUDIES.key(), studyIds);
        queryResult = dbAdaptor.get(query, options);

        assertEquals(NUM_VARIANTS, queryResult.getNumResults());
        assertEquals(NUM_VARIANTS, queryResult.getNumTotalResults());

        for (Variant variant : queryResult.getResult()) {
            for (VariantSourceEntry sourceEntry : variant.getSourceEntries().values()) {
                assertTrue(studyIds.contains(sourceEntry.getStudyId()));
            }
        }
    }

    @Test
    public void testGetAllVariants_returnedStudiesAll() {
        List<String> studyIds = Arrays.asList(
                Integer.toString(studyConfiguration1.getStudyId()),
                Integer.toString(studyConfiguration2.getStudyId()),
                Integer.toString(studyConfiguration3.getStudyId()));
        query.append(RETURNED_STUDIES.key(), studyIds);
        queryResult = dbAdaptor.get(query, options);

        assertEquals(NUM_VARIANTS, queryResult.getNumResults());
        assertEquals(NUM_VARIANTS, queryResult.getNumTotalResults());

        for (Variant variant : queryResult.getResult()) {
            for (VariantSourceEntry sourceEntry : variant.getSourceEntries().values()) {
                assertTrue(studyIds.contains(sourceEntry.getStudyId()));
            }
        }
    }

    @Test
    public void testGetAllVariants_returnedStudiesEmpty() {
        query.append(RETURNED_STUDIES.key(), -1);
        queryResult = dbAdaptor.get(query, options);

        assertEquals(NUM_VARIANTS, queryResult.getNumResults());
        assertEquals(NUM_VARIANTS, queryResult.getNumTotalResults());

        for (Variant variant : queryResult.getResult()) {
            assertEquals(Collections.emptyMap(),variant.getSourceEntries());
        }
    }

    @Test
    public void testGetAllVariants_filterStudy_returnedStudiesEmpty() {
        query.append(STUDIES.key(), -1);

        thrown.expect(IllegalStateException.class); //StudyNotFound exception
        queryResult = dbAdaptor.get(query, options);
    }

    @Test
    public void testGetAllVariants_filterStudy_returnedStudies2_3() {
        List<String> studyIds = Arrays.asList(
                Integer.toString(studyConfiguration2.getStudyId()),
                Integer.toString(studyConfiguration3.getStudyId()));
        query.append(RETURNED_STUDIES.key(), studyIds)
                .append(STUDIES.key(), studyIds);
        queryResult = dbAdaptor.get(query, options);

        assertEquals(7946, queryResult.getNumResults());
        assertEquals(7946, queryResult.getNumTotalResults());

        for (Variant variant : queryResult.getResult()) {
            for (VariantSourceEntry sourceEntry : variant.getSourceEntries().values()) {
                assertTrue(studyIds.contains(sourceEntry.getStudyId()));
            }
        }
    }

    @Test
    public void testGetAllVariants_filterStudies2_3() {
        List<String> studyIds = Arrays.asList(
                Integer.toString(studyConfiguration2.getStudyId()),
                Integer.toString(studyConfiguration3.getStudyId()));
        query.append(STUDIES.key(), studyIds);
        queryResult = dbAdaptor.get(query, options);

        assertEquals(7946, queryResult.getNumResults());
        assertEquals(7946, queryResult.getNumTotalResults());

        for (Variant variant : queryResult.getResult()) {
            List<String> returnedStudyIds = variant.getSourceEntries().values().stream().map(VariantSourceEntry::getStudyId).collect(Collectors.toList());
            assertTrue(returnedStudyIds.contains(Integer.toString(studyConfiguration2.getStudyId()))
                    || returnedStudyIds.contains(Integer.toString(studyConfiguration3.getStudyId())));
        }
    }

//    @Test
//    public void testGetAllVariants_returnedFiles() {
//
//    }

}
