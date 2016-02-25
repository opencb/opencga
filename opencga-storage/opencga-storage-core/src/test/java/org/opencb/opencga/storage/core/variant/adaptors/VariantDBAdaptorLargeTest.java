package org.opencb.opencga.storage.core.variant.adaptors;

import htsjdk.variant.variantcontext.VariantContext;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantStudy;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.Query;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.VariantStorageManagerTestUtils;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor.VariantQueryParams.*;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Ignore
public abstract class VariantDBAdaptorLargeTest extends VariantStorageManagerTestUtils {

    protected static final Integer file1 = 1;
    protected static final Integer file2 = 2;
    protected static final Integer file3 = 3;
    protected static final Integer file4 = 4;
    protected static final Integer file5 = 5;
    protected static StudyConfiguration studyConfiguration1;
    protected static StudyConfiguration studyConfiguration2;
    protected static StudyConfiguration studyConfiguration3;
    protected static VariantDBAdaptor dbAdaptor;
    protected static int NUM_VARIANTS = 9751;
    protected QueryResult<Variant> queryResult;
    protected QueryOptions options;
    protected Query query;

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
                    .append(VariantStorageManager.Options.CALCULATE_STATS.key(), true)
                    .append(VariantStorageManager.Options.ANNOTATE.key(), false);
            //Study1
            runDefaultETL(getResourceUri("1-500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"), variantStorageManager, studyConfiguration1, options.append(VariantStorageManager.Options.FILE_ID.key(), file1));
            assertEquals(500, studyConfiguration1.getCohorts().get(studyConfiguration1.getCohortIds().get(StudyEntry.DEFAULT_COHORT)).size());
            runDefaultETL(getResourceUri("501-1000.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"), variantStorageManager, studyConfiguration1, options.append(VariantStorageManager.Options.FILE_ID.key(), file2));
            assertEquals(1000, studyConfiguration1.getCohorts().get(studyConfiguration1.getCohortIds().get(StudyEntry.DEFAULT_COHORT)).size());
            //Study2
            runDefaultETL(getResourceUri("1001-1500.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"), variantStorageManager, studyConfiguration2, options.append(VariantStorageManager.Options.FILE_ID.key(), file3));
            assertEquals(500, studyConfiguration2.getCohorts().get(studyConfiguration2.getCohortIds().get(StudyEntry.DEFAULT_COHORT)).size());
            runDefaultETL(getResourceUri("1501-2000.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"), variantStorageManager, studyConfiguration2, options.append(VariantStorageManager.Options.FILE_ID.key(), file4));
            assertEquals(1000, studyConfiguration2.getCohorts().get(studyConfiguration2.getCohortIds().get(StudyEntry.DEFAULT_COHORT)).size());
            //Study3
            runDefaultETL(getResourceUri("2001-2504.filtered.10k.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz"), variantStorageManager, studyConfiguration3, options.append(VariantStorageManager.Options.FILE_ID.key(), file5));
            assertEquals(504, studyConfiguration3.getCohorts().get(studyConfiguration3.getCohortIds().get(StudyEntry.DEFAULT_COHORT)).size());

            dbAdaptor = variantStorageManager.getDBAdaptor(DB_NAME);


        }
    }


    @Test
    public void testGetAllVariants_returnedStudies1() {
        query.append(RETURNED_STUDIES.key(), studyConfiguration1.getStudyId());
        queryResult = dbAdaptor.get(query, options);

        assertEquals(NUM_VARIANTS, queryResult.getNumResults());
        assertEquals(NUM_VARIANTS, queryResult.getNumTotalResults());

        for (Variant variant : queryResult.getResult()) {
            for (StudyEntry sourceEntry : variant.getStudies()) {
                assertEquals(studyConfiguration1.getStudyName(), sourceEntry.getStudyId());
            }
        }
    }

    @Test
    public void testGetVariantsByType() {
        Set<Variant> snv = new HashSet<>(dbAdaptor.get(new Query(VariantDBAdaptor.VariantQueryParams.TYPE.key(), VariantType.SNV), new QueryOptions()).getResult());
        System.out.println("SNV = " + snv.size());
        assertEquals(9515, snv.size());
        snv.forEach(variant -> assertTrue(EnumSet.of(VariantType.SNV, VariantType.SNP).contains(variant.getType())));

        Set<Variant> not_snv = new HashSet<>(dbAdaptor.get(new Query(VariantDBAdaptor.VariantQueryParams.TYPE.key(), "!" + VariantType.SNV), new QueryOptions()).getResult());
        System.out.println("!SNV = " + not_snv.size());
        not_snv.forEach(variant -> assertFalse(EnumSet.of(VariantType.SNV, VariantType.SNP).contains(variant.getType())));

        Set<Variant> snv_snp = new HashSet<>(dbAdaptor.get(new Query(VariantDBAdaptor.VariantQueryParams.TYPE.key(), VariantType.SNV + "," + VariantContext.Type.SNP), new QueryOptions()).getResult());
        System.out.println("SNV_SNP = " + snv_snp.size());
        assertEquals(snv_snp, snv);

        Set<Variant> snp = new HashSet<>(dbAdaptor.get(new Query(VariantDBAdaptor.VariantQueryParams.TYPE.key(), VariantType.SNP), new QueryOptions()).getResult());
        snp.forEach(variant -> assertEquals(VariantType.SNP, variant.getType()));
        snp.forEach(variant -> assertTrue(snv.contains(variant)));
        System.out.println("SNP = " + snp.size());

        Set<Variant> indels = new HashSet<>(dbAdaptor.get(new Query(VariantDBAdaptor.VariantQueryParams.TYPE.key(), VariantType.INDEL), new QueryOptions()).getResult());
        indels.forEach(variant -> assertEquals(VariantType.INDEL, variant.getType()));
        System.out.println("INDEL = " + indels.size());

        Set<Variant> indels_snp = new HashSet<>(dbAdaptor.get(new Query(VariantDBAdaptor.VariantQueryParams.TYPE.key(), VariantType.INDEL + "," + VariantType.SNP), new QueryOptions()).getResult());
        indels_snp.forEach(variant -> assertTrue(EnumSet.of(VariantType.INDEL, VariantType.SNP).contains(variant.getType())));
        indels_snp.forEach(variant -> assertTrue(indels.contains(variant) || snp.contains(variant)));
        System.out.println("INDEL_SNP = " + indels_snp.size());

        Set<Variant> indels_snv = new HashSet<>(dbAdaptor.get(new Query(VariantDBAdaptor.VariantQueryParams.TYPE.key(), VariantType.INDEL + "," + VariantType.SNV), new QueryOptions()).getResult());
        indels_snv.forEach(variant -> assertTrue(EnumSet.of(VariantType.INDEL, VariantType.SNP, VariantType.SNV).contains(variant.getType())));
        indels_snv.forEach(variant -> assertTrue(indels.contains(variant) || snv.contains(variant)));
        System.out.println("INDEL_SNV = " + indels_snv.size());
    }

    @Test
    public void testGetAllVariants_returnedStudies3() {
        String studyId = Integer.toString(studyConfiguration3.getStudyId());
        query.put(RETURNED_STUDIES.key(), studyId);
        queryResult = dbAdaptor.get(query, options);

        assertEquals(NUM_VARIANTS, queryResult.getNumResults());
        assertEquals(NUM_VARIANTS, queryResult.getNumTotalResults());

        for (Variant variant : queryResult.getResult()) {
            for (StudyEntry sourceEntry : variant.getStudies()) {
                assertEquals(studyConfiguration3.getStudyName(), sourceEntry.getStudyId());
            }
        }

        query.put(RETURNED_STUDIES.key(), studyConfiguration3.getStudyName());
        queryResult = dbAdaptor.get(query, options);

        assertEquals(NUM_VARIANTS, queryResult.getNumResults());
        assertEquals(NUM_VARIANTS, queryResult.getNumTotalResults());

        for (Variant variant : queryResult.getResult()) {
            for (StudyEntry sourceEntry : variant.getStudies()) {
                assertEquals(studyConfiguration3.getStudyName(), sourceEntry.getStudyId());
            }
        }
    }

    @Test
    public void testGetAllVariants_returnedStudies2_3() {
        List<String> studyIds = Arrays.asList(
                studyConfiguration2.getStudyName(),
                studyConfiguration3.getStudyName());
        query.append(RETURNED_STUDIES.key(), studyIds);
        queryResult = dbAdaptor.get(query, options);

        assertEquals(NUM_VARIANTS, queryResult.getNumResults());
        assertEquals(NUM_VARIANTS, queryResult.getNumTotalResults());

        for (Variant variant : queryResult.getResult()) {
            for (StudyEntry sourceEntry : variant.getStudies()) {
                assertTrue(studyIds.contains(sourceEntry.getStudyId()));
            }
        }
    }

    @Test
    public void testGetAllVariants_returnedStudiesAll() {
        List<String> studyIds = Arrays.asList(
                studyConfiguration1.getStudyName(),
                studyConfiguration2.getStudyName(),
                studyConfiguration3.getStudyName());
        query.append(RETURNED_STUDIES.key(), studyIds);
        queryResult = dbAdaptor.get(query, options);

        assertEquals(NUM_VARIANTS, queryResult.getNumResults());
        assertEquals(NUM_VARIANTS, queryResult.getNumTotalResults());

        for (Variant variant : queryResult.getResult()) {
            for (StudyEntry sourceEntry : variant.getStudies()) {
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
            assertEquals(Collections.emptyList(), variant.getStudies());
        }
    }

    @Test
    public void testGetAllVariants_filterStudy_returnedStudiesEmpty() {
        query.append(STUDIES.key(), -1);

        thrown.expect(IllegalArgumentException.class); //StudyNotFound exception
        queryResult = dbAdaptor.get(query, options);
    }

    @Test
    public void testGetAllVariants_filterStudy_unknownStudy() {
        query.append(GENOTYPE.key(), "HG00258:1/1");

        thrown.expect(IllegalArgumentException.class); //Unspecified study exception
        queryResult = dbAdaptor.get(query, options);
    }

    @Test
    public void testGetAllVariants_filterStudy_returnedStudies2_3() {
        List<String> studyIds = Arrays.asList(
                studyConfiguration2.getStudyName(),
                studyConfiguration3.getStudyName());
        query.append(RETURNED_STUDIES.key(), studyIds)
                .append(STUDIES.key(), studyIds);
        queryResult = dbAdaptor.get(query, options);

        assertEquals(7942, queryResult.getNumResults());
        assertEquals(7942, queryResult.getNumTotalResults());

        for (Variant variant : queryResult.getResult()) {
            for (StudyEntry sourceEntry : variant.getStudies()) {
                assertTrue(studyIds.contains(sourceEntry.getStudyId()));
            }
        }
    }

    @Test
    public void testGetAllVariants_filterStudies2_OR_3() {
        List<String> studyIds = Arrays.asList(
                studyConfiguration2.getStudyName(),
                studyConfiguration3.getStudyName());
        query.append(STUDIES.key(), studyIds);
        queryResult = dbAdaptor.get(query, options);

        assertEquals(7942, queryResult.getNumResults());
        assertEquals(7942, queryResult.getNumTotalResults());

        for (Variant variant : queryResult.getResult()) {
            List<String> returnedStudyIds = variant.getStudies().stream().map(StudyEntry::getStudyId).collect(Collectors.toList());
            assertTrue(returnedStudyIds.contains(studyConfiguration2.getStudyName())
                    || returnedStudyIds.contains(studyConfiguration3.getStudyName()));
        }
    }

    @Test
    public void testGetAllVariants_filterStudies2_AND_3() {
        String studyIds = studyConfiguration2.getStudyName() + ";" + studyConfiguration3.getStudyName();
        query.append(STUDIES.key(), studyIds);
        queryResult = dbAdaptor.get(query, options);

        assertEquals(3298, queryResult.getNumResults());
        assertEquals(3298, queryResult.getNumTotalResults());

        for (Variant variant : queryResult.getResult()) {
            List<String> returnedStudyIds = variant.getStudies().stream().map(StudyEntry::getStudyId).collect(Collectors.toList());
            assertTrue(returnedStudyIds.contains(studyConfiguration2.getStudyName())
                    && returnedStudyIds.contains(studyConfiguration3.getStudyName()));
        }
    }

    @Test
    public void testGetAllVariants_filterStudies2_not_3() {
        String studyIds = studyConfiguration2.getStudyName() + ";!" + studyConfiguration3.getStudyName();
        query.append(STUDIES.key(), studyIds);
        queryResult = dbAdaptor.get(query, options);

        System.out.println(queryResult.getNumResults());
        assertEquals(3292, queryResult.getNumResults());
        assertEquals(3292, queryResult.getNumTotalResults());

        for (Variant variant : queryResult.getResult()) {
            List<String> returnedStudyIds = variant.getStudies().stream().map(StudyEntry::getStudyId).collect(Collectors.toList());
            assertTrue(returnedStudyIds.contains(studyConfiguration2.getStudyName()) && !returnedStudyIds.contains(studyConfiguration3.getStudyName()));
        }
    }

    @Test
    public void testGetAllVariants_filterFiles1_2() {
        List<Integer> files = Arrays.asList(
                file1,
                file2);
        query.append(FILES.key(), files);
        queryResult = dbAdaptor.get(query, options);

        query = new Query(STUDIES.key(), studyConfiguration1.getStudyName());
        QueryResult<Variant> queryResultStudy = dbAdaptor.get(query, options.append("limit", 1).append("skipCount", false));

        assertEquals(queryResultStudy.getNumTotalResults(), queryResult.getNumResults());
        assertEquals(5476, queryResult.getNumResults());
        assertEquals(5476, queryResult.getNumTotalResults());

        for (Variant variant : queryResult.getResult()) {
            Set<String> returnedFileIds = variant.getStudies().stream().map(StudyEntry::getFiles).flatMap(fileEntries -> fileEntries.stream().map(FileEntry::getFileId)).collect(Collectors.toSet());
            assertTrue(returnedFileIds.contains(Integer.toString(file1)) || returnedFileIds.contains(Integer.toString(file2)));
            Set<String> returnedStudiesIds = variant.getStudies().stream().map(StudyEntry::getStudyId).collect(Collectors.toSet());
            assertTrue("Returned studies :" + returnedStudiesIds.toString(), returnedStudiesIds.contains(studyConfiguration1.getStudyName()));
        }
    }

    @Test
    public void testGetAllVariants_filterFiles_not_1() {
        String unknownGenotype = "./.";
        query.append(FILES.key(), "!" + file1)
                .append(STUDIES.key(), studyConfiguration1.getStudyName())
                .append(UNKNOWN_GENOTYPE.key(), unknownGenotype)
                .append(RETURNED_STUDIES.key(), studyConfiguration1.getStudyName());
        queryResult = dbAdaptor.get(query, options);

        for (Variant variant : queryResult.getResult()) {
            Set<String> returnedFileIds = variant.getStudies().stream().map(StudyEntry::getFiles).flatMap(fileEntries -> fileEntries.stream().map(FileEntry::getFileId)).collect(Collectors.toSet());
            assertEquals(Collections.singleton("2"), returnedFileIds);
            Set<String> returnedStudiesIds = variant.getStudies().stream().map(StudyEntry::getStudyId).collect(Collectors.toSet());
            assertTrue("Returned studies :" + returnedStudiesIds.toString(), returnedStudiesIds.contains(studyConfiguration1.getStudyName()));
            StudyEntry sourceEntry = variant.getStudy(studyConfiguration1.getStudyName());
            for (Map.Entry<String, Map<String, String>> entry : sourceEntry.getSamplesDataAsMap().entrySet()) {
                String genotype = entry.getValue().get("GT");
                if (studyConfiguration1.getSamplesInFiles().get(file1).contains(studyConfiguration1.getSampleIds().get(entry.getKey()))
                        && !sourceEntry.getAllAttributes().containsKey(file1 + "_QUAL")) {
                    assertEquals(unknownGenotype, genotype);
                } else {
                    assertFalse(unknownGenotype.equals(genotype));
                }
            }
        }
    }

    @Test
    public void testGetAllVariants_returnedFiles1() {

        query.append(RETURNED_FILES.key(), file1)
                .append(FILES.key(), file1);
        queryResult = dbAdaptor.get(query, options);

        query = new Query(FILES.key(), file1);
        QueryResult<Variant> queryResultFile = dbAdaptor.get(query, options.append("limit", 1).append("skipCount", false));

        assertEquals(queryResultFile.getNumTotalResults(), queryResult.getNumResults());
        assertEquals(3279, queryResult.getNumResults());
        assertEquals(3279, queryResult.getNumTotalResults());

        for (Variant variant : queryResult.getResult()) {
            Set<String> returnedFileIds = variant.getStudies().stream().map(StudyEntry::getFiles).flatMap(fileEntries -> fileEntries.stream().map(FileEntry::getFileId)).collect(Collectors.toSet());
            Set<String> returnedStudiesIds = variant.getStudies().stream().map(StudyEntry::getStudyId).collect(Collectors.toSet());

//            assertEquals("Returned files :" + returnedFileIds.toString(), Collections.singleton(file1.toString()), returnedFileIds);
            assertEquals("Returned studies :" + returnedStudiesIds.toString(), Collections.singleton(studyConfiguration1.getStudyName()), returnedStudiesIds);
        }
    }


    @Test
    public void testGetAllVariants_returnedSamples() {

        int i = 0;
        Set<String> sampleSet = new HashSet<>();
        Iterator<String> iterator = studyConfiguration1.getSampleIds().keySet().iterator();
        while (i++ < 5 && iterator.hasNext()) {
            sampleSet.add(iterator.next());
        }

        query.append(RETURNED_SAMPLES.key(), new ArrayList<>(sampleSet));
        queryResult = dbAdaptor.get(query, options);

        assertEquals(NUM_VARIANTS, queryResult.getNumResults());
        assertEquals(NUM_VARIANTS, queryResult.getNumTotalResults());

        for (Variant variant : queryResult.getResult()) {
            for (StudyEntry sourceEntry : variant.getStudies()) {
                if (sourceEntry.getStudyId().equals(studyConfiguration1.getStudyName())) {
                    assertEquals("StudyId:" + sourceEntry.getStudyId() + ", SampleNames " + sourceEntry.getSamplesName(), sampleSet, sourceEntry.getSamplesName());
                } else {
                    assertEquals("StudyId:" + sourceEntry.getStudyId() + ", SampleNames " + sourceEntry.getSamplesName(), Collections.<String>emptySet(), sourceEntry.getSamplesName());
                }
            }
        }
    }

}
