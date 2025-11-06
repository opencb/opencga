package org.opencb.opencga.catalog.managers;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang3.time.StopWatch;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.biodata.models.common.Status;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.CohortDBAdaptor;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.cohort.Cohort;
import org.opencb.opencga.core.models.cohort.CohortUpdateParams;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.InternalStatus;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.sample.SampleReferenceParam;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.testclassification.duration.MediumTests;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

@Category(MediumTests.class)
public class CohortManagerTest extends AbstractManagerTest {

    @Test
    public void testCreateCohort() throws CatalogException {
        Sample sampleId1 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_1"), INCLUDE_RESULT, ownerToken).first();
        Sample sampleId2 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_2"), INCLUDE_RESULT, ownerToken).first();
        Sample sampleId3 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_3"), INCLUDE_RESULT, ownerToken).first();
        Cohort myCohort = catalogManager.getCohortManager().create(studyFqn, new Cohort().setId("MyCohort")
                .setSamples(Arrays.asList(sampleId1, sampleId2, sampleId3))
                .setStatus(new Status("custom", "custom", "description", TimeUtils.getTime())), INCLUDE_RESULT, ownerToken).first();

        assertEquals("MyCohort", myCohort.getId());
        assertEquals(3, myCohort.getSamples().size());
        assertTrue(myCohort.getSamples().stream().map(Sample::getUid).collect(Collectors.toList()).contains(sampleId1.getUid()));
        assertTrue(myCohort.getSamples().stream().map(Sample::getUid).collect(Collectors.toList()).contains(sampleId2.getUid()));
        assertTrue(myCohort.getSamples().stream().map(Sample::getUid).collect(Collectors.toList()).contains(sampleId3.getUid()));
        assertNotNull(myCohort.getStatus());
        assertEquals("custom", myCohort.getStatus().getName());
        assertEquals("description", myCohort.getStatus().getDescription());
    }

    @Test
    public void createSampleCohortTest() throws Exception {
        Sample sampleId1 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_1"), INCLUDE_RESULT, ownerToken).first();
        Sample sampleId2 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_2"), INCLUDE_RESULT, ownerToken).first();
        Sample sampleId3 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_3"), INCLUDE_RESULT, ownerToken).first();
        catalogManager.getCohortManager().create(studyFqn, new Cohort().setId("MyCohort1")
                .setSamples(Arrays.asList(sampleId1, sampleId2)), null, ownerToken).first();
        catalogManager.getCohortManager().create(studyFqn, new Cohort().setId("MyCohort2")
                .setSamples(Arrays.asList(sampleId2, sampleId3)), null, ownerToken).first();

        List<String> ids = new ArrayList<>();
        ids.add("SAMPLE_1");
        ids.add("SAMPLE_2");
        ids.add("SAMPLE_3");

        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.COHORT_IDS.key());

        OpenCGAResult<Sample> sampleDataResult = catalogManager.getSampleManager().get(studyFqn, ids, options, ownerToken);
        assertEquals(3, sampleDataResult.getNumResults());
        for (Sample sample : sampleDataResult.getResults()) {
            switch (sample.getId()) {
                case "SAMPLE_1":
                    assertEquals(1, sample.getCohortIds().size());
                    assertEquals("MyCohort1", sample.getCohortIds().get(0));
                    break;
                case "SAMPLE_2":
                    assertEquals(2, sample.getCohortIds().size());
                    assertTrue(sample.getCohortIds().containsAll(Arrays.asList("MyCohort1", "MyCohort2")));
                    break;
                case "SAMPLE_3":
                    assertEquals(1, sample.getCohortIds().size());
                    assertEquals("MyCohort2", sample.getCohortIds().get(0));
                    break;
                default:
                    fail();
            }
        }
    }

    @Test
    public void updateSampleCohortTest() throws Exception {
        Sample sampleId1 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_1"), INCLUDE_RESULT, ownerToken).first();
        Sample sampleId2 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_2"), INCLUDE_RESULT, ownerToken).first();
        Sample sampleId3 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_3"), INCLUDE_RESULT, ownerToken).first();
        catalogManager.getCohortManager().create(studyFqn, new Cohort().setId("MyCohort1")
                .setSamples(Arrays.asList(sampleId1)), null, ownerToken).first();
        catalogManager.getCohortManager().create(studyFqn, new Cohort().setId("MyCohort2")
                .setSamples(Arrays.asList(sampleId2, sampleId3)), null, ownerToken).first();

        catalogManager.getCohortManager().update(studyFqn, "MyCohort1",
                new CohortUpdateParams().setSamples(Collections.singletonList(new SampleReferenceParam().setId(sampleId3.getId()))),
                QueryOptions.empty(), ownerToken);

        List<String> ids = new ArrayList<>();
        ids.add("SAMPLE_1");
        ids.add("SAMPLE_2");
        ids.add("SAMPLE_3");

        QueryOptions optionsSample = new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.COHORT_IDS.key());
        QueryOptions optionsCohort = new QueryOptions(QueryOptions.INCLUDE, CohortDBAdaptor.QueryParams.SAMPLES.key());

        OpenCGAResult<Sample> sampleDataResult = catalogManager.getSampleManager().get(studyFqn, ids, optionsSample, ownerToken);

        Cohort cohortResult = catalogManager.getCohortManager().get(studyFqn, "MyCohort1", optionsCohort, ownerToken).first();

        assertEquals(2, cohortResult.getSamples().size());
        assertTrue(cohortResult.getSamples().stream().map(Sample::getId).collect(Collectors.toSet())
                .containsAll(Arrays.asList("SAMPLE_1", "SAMPLE_3")));

        for (Sample sample : sampleDataResult.getResults()) {

            switch (sample.getId()) {
                case "SAMPLE_1":
                    assertEquals(1, sample.getCohortIds().size());
                    assertEquals("MyCohort1", sample.getCohortIds().get(0));
                    break;
                case "SAMPLE_2":
                    assertEquals(1, sample.getCohortIds().size());
                    assertTrue(sample.getCohortIds().containsAll(Arrays.asList("MyCohort2")));
                    break;
                case "SAMPLE_3":
                    assertEquals(2, sample.getCohortIds().size());
                    assertTrue(sample.getCohortIds().containsAll(Arrays.asList("MyCohort1", "MyCohort2")));
                    break;
                default:
                    fail();
            }
        }
    }

    @Test
    public void updateSampleCohortWithThreadsTest() throws Exception {
        Sample sampleId1 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_1"), INCLUDE_RESULT, ownerToken).first();
        Sample sampleId2 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_2"), INCLUDE_RESULT, ownerToken).first();
        Sample sampleId3 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_3"), INCLUDE_RESULT, ownerToken).first();
        catalogManager.getCohortManager().create(studyFqn, new Cohort().setId("MyCohort1")
                .setSamples(Arrays.asList(sampleId1)), null, ownerToken).first();
        catalogManager.getCohortManager().create(studyFqn, new Cohort().setId("MyCohort2")
                .setSamples(Arrays.asList(sampleId2, sampleId3)), null, ownerToken).first();

        ExecutorService executorService = Executors.newFixedThreadPool(10,
                new ThreadFactoryBuilder()
                        .setNameFormat("executor-service-%d")
                        .build());

        StopWatch stopWatch = StopWatch.createStarted();
        List<List<String>> sampleIds = new ArrayList<>(5);
        List<String> innerArray = new ArrayList<>(50);
        for (int i = 0; i < 250; i++) {
            if (i % 50 == 0) {
                System.out.println("i = " + i);
            }

            String sampleId = "SAMPLE_AUTO_" + i;
            executorService.submit(() -> {
                try {
                    catalogManager.getSampleManager().create(studyFqn, new Sample().setId(sampleId), QueryOptions.empty(), ownerToken);
                } catch (CatalogException e) {
                    throw new RuntimeException(e);
                }
            });
            if (innerArray.size() == 50) {
                sampleIds.add(new ArrayList<>(innerArray));
                innerArray.clear();
            }
            innerArray.add(sampleId);
        }
        sampleIds.add(new ArrayList<>(innerArray));
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.MINUTES);

        System.out.println("Creating 250 samples took " + stopWatch.getTime(TimeUnit.SECONDS) + " seconds");

        stopWatch.stop();
        stopWatch.reset();
        stopWatch.start();
        executorService = Executors.newFixedThreadPool(3);
        int execution = 0;
        Map<String, Object> actionMap = new HashMap<>();
        actionMap.put(CohortDBAdaptor.QueryParams.SAMPLES.key(), ParamUtils.BasicUpdateAction.SET);
        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(Constants.ACTIONS, actionMap);
        for (List<String> innerSampleIds : sampleIds) {
            Cohort myCohort1 = catalogManager.getCohortManager().get(studyFqn, "MyCohort1", null, ownerToken).first();
            List<SampleReferenceParam> sampleReferenceParamList = new ArrayList<>(myCohort1.getNumSamples() + innerSampleIds.size());
            sampleReferenceParamList.addAll(myCohort1.getSamples().stream().map(s -> new SampleReferenceParam().setId(s.getId())).collect(Collectors.toList()));
            sampleReferenceParamList.addAll(innerSampleIds.stream().map(s -> new SampleReferenceParam().setId(s)).collect(Collectors.toList()));
            int executionId = execution++;
            executorService.submit(() -> {
                try {
                    catalogManager.getCohortManager().update(studyFqn, "MyCohort1",
                            new CohortUpdateParams().setSamples(sampleReferenceParamList),
                            queryOptions, ownerToken);
                    System.out.println("Execution: " + executionId);
                } catch (CatalogException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.MINUTES);
        System.out.println("Attaching 250 samples took " + stopWatch.getTime(TimeUnit.SECONDS) + " seconds");

        // Ensure persistence
        Query sampleQuery = new Query(SampleDBAdaptor.QueryParams.COHORT_IDS.key(), "MyCohort1");
        OpenCGAResult<Sample> search = catalogManager.getSampleManager().search(studyFqn, sampleQuery, SampleManager.INCLUDE_SAMPLE_IDS, ownerToken);
        Cohort myCohort1 = catalogManager.getCohortManager().get(studyFqn, "MyCohort1", null, ownerToken).first();
        assertEquals(search.getNumResults(), myCohort1.getNumSamples());
        Set<String> sampleIdSet = search.getResults().stream().map(Sample::getId).collect(Collectors.toSet());
        assertTrue(myCohort1.getSamples().stream().map(Sample::getId).collect(Collectors.toSet()).containsAll(sampleIdSet));
    }

    @Test
    public void deleteSampleCohortTest() throws Exception {
        Sample sampleId1 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_1"), INCLUDE_RESULT, ownerToken).first();
        Sample sampleId2 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_2"), INCLUDE_RESULT, ownerToken).first();
        Sample sampleId3 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_3"), INCLUDE_RESULT, ownerToken).first();
        catalogManager.getCohortManager().create(studyFqn, new Cohort().setId("MyCohort1")
                .setSamples(Arrays.asList(sampleId1)), null, ownerToken).first();
        catalogManager.getCohortManager().create(studyFqn, new Cohort().setId("MyCohort2")
                .setSamples(Arrays.asList(sampleId2, sampleId3)), null, ownerToken).first();

        catalogManager.getCohortManager().update(studyFqn, "MyCohort1",
                new CohortUpdateParams().setSamples(Arrays.asList(new SampleReferenceParam().setId(sampleId3.getId()))),
                QueryOptions.empty(), ownerToken);

        Map<String, Object> actionMap = new HashMap<>();
        actionMap.put(CohortDBAdaptor.QueryParams.SAMPLES.key(), ParamUtils.BasicUpdateAction.REMOVE);
        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(Constants.ACTIONS, actionMap);
        catalogManager.getCohortManager().update(studyFqn, "MyCohort1",
                new CohortUpdateParams().setSamples(Arrays.asList(new SampleReferenceParam().setId(sampleId1.getId()))),
                queryOptions, ownerToken);

        List<String> ids = new ArrayList<>();
        ids.add("SAMPLE_1");
        ids.add("SAMPLE_2");
        ids.add("SAMPLE_3");

        QueryOptions optionsSample = new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.COHORT_IDS.key());
        QueryOptions optionsCohort = new QueryOptions(QueryOptions.INCLUDE, CohortDBAdaptor.QueryParams.SAMPLES.key());

        OpenCGAResult<Sample> sampleDataResult = catalogManager.getSampleManager().get(studyFqn, ids, optionsSample, ownerToken);

        Cohort cohortResult = catalogManager.getCohortManager().get(studyFqn, "MyCohort1", optionsCohort, ownerToken).first();

        assertEquals(1, cohortResult.getSamples().size());
        assertTrue(cohortResult.getSamples().stream().map(Sample::getId).collect(Collectors.toSet())
                .containsAll(Arrays.asList("SAMPLE_3")));

        for (Sample sample : sampleDataResult.getResults()) {

            switch (sample.getId()) {
                case "SAMPLE_1":
                    assertEquals(0, sample.getCohortIds().size());
                    break;
                case "SAMPLE_2":
                    assertEquals(1, sample.getCohortIds().size());
                    assertTrue(sample.getCohortIds().containsAll(Arrays.asList("MyCohort2")));
                    break;
                case "SAMPLE_3":
                    assertEquals(2, sample.getCohortIds().size());
                    assertTrue(sample.getCohortIds().containsAll(Arrays.asList("MyCohort1", "MyCohort2")));
                    break;
                default:
                    fail();
            }
        }
    }

    @Test
    public void setSampleCohortTest() throws Exception {
        Sample sampleId1 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_1"), INCLUDE_RESULT, ownerToken).first();
        Sample sampleId2 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_2"), INCLUDE_RESULT, ownerToken).first();
        Sample sampleId3 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_3"), INCLUDE_RESULT, ownerToken).first();
        catalogManager.getCohortManager().create(studyFqn, new Cohort().setId("MyCohort1")
                .setSamples(Arrays.asList(sampleId1)), null, ownerToken).first();

        Map<String, Object> actionMap = new HashMap<>();
        actionMap.put(CohortDBAdaptor.QueryParams.SAMPLES.key(), ParamUtils.BasicUpdateAction.SET);
        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(Constants.ACTIONS, actionMap);
        catalogManager.getCohortManager().update(studyFqn, "MyCohort1",
                new CohortUpdateParams().setSamples(Arrays.asList(
                        new SampleReferenceParam().setId(sampleId2.getId()),
                        new SampleReferenceParam().setId(sampleId3.getId()))),
                queryOptions, ownerToken);

        List<String> ids = new ArrayList<>();
        ids.add("SAMPLE_1");
        ids.add("SAMPLE_2");
        ids.add("SAMPLE_3");

        QueryOptions optionsSample = new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.COHORT_IDS.key());
        QueryOptions optionsCohort = new QueryOptions(QueryOptions.INCLUDE, CohortDBAdaptor.QueryParams.SAMPLES.key());

        OpenCGAResult<Sample> sampleDataResult = catalogManager.getSampleManager().get(studyFqn, ids, optionsSample, ownerToken);

        Cohort cohortResult = catalogManager.getCohortManager().get(studyFqn, "MyCohort1", optionsCohort, ownerToken).first();

        assertEquals(2, cohortResult.getSamples().size());
        assertTrue(cohortResult.getSamples().stream().map(Sample::getId).collect(Collectors.toSet())
                .containsAll(Arrays.asList("SAMPLE_2", "SAMPLE_3")));

        for (Sample sample : sampleDataResult.getResults()) {

            switch (sample.getId()) {
                case "SAMPLE_1":
                    assertEquals(0, sample.getCohortIds().size());
                    break;
                case "SAMPLE_2":
                case "SAMPLE_3":
                    assertEquals(1, sample.getCohortIds().size());
                    assertTrue(sample.getCohortIds().containsAll(Arrays.asList("MyCohort1")));
                    break;
                default:
                    fail();
            }
        }
    }
    @Test
    public void testGetAllCohorts() throws CatalogException {
        Sample sampleId1 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_1"), INCLUDE_RESULT, ownerToken).first();
        Sample sampleId2 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_2"), INCLUDE_RESULT, ownerToken).first();
        Sample sampleId3 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_3"), INCLUDE_RESULT, ownerToken).first();
        Sample sampleId4 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_4"), INCLUDE_RESULT, ownerToken).first();
        Sample sampleId5 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_5"), INCLUDE_RESULT, ownerToken).first();
        Cohort myCohort1 = catalogManager.getCohortManager().create(studyFqn,
                new Cohort().setId("MyCohort1").setType(Enums.CohortType.FAMILY).setSamples(Arrays.asList(sampleId1, sampleId2, sampleId3)),
                INCLUDE_RESULT, ownerToken).first();
        Cohort myCohort2 = catalogManager.getCohortManager().create(studyFqn,
                new Cohort().setId("MyCohort2").setType(Enums.CohortType.FAMILY)
                        .setSamples(Arrays.asList(sampleId1, sampleId2, sampleId3, sampleId4)), INCLUDE_RESULT, ownerToken).first();
        Cohort myCohort3 = catalogManager.getCohortManager().create(studyFqn, new Cohort().setId("MyCohort3")
                .setType(Enums.CohortType.CASE_CONTROL).setSamples(Arrays.asList(sampleId3, sampleId4)), INCLUDE_RESULT, ownerToken).first();
        catalogManager.getCohortManager().create(studyFqn, new Cohort().setId("MyCohort4").setType(Enums.CohortType.TRIO)
                .setSamples(Arrays.asList(sampleId5, sampleId3)), null, ownerToken).first();

        long numResults;
        numResults = catalogManager.getCohortManager().search(studyFqn, new Query(CohortDBAdaptor.QueryParams.SAMPLES.key(),
                sampleId1.getId()), new QueryOptions(), ownerToken).getNumResults();
        assertEquals(2, numResults);

        numResults = catalogManager.getCohortManager().search(studyFqn, new Query(CohortDBAdaptor.QueryParams.SAMPLES.key(),
                sampleId1.getId()
                        + "," + sampleId5.getId()), new QueryOptions(), ownerToken).getNumResults();
        assertEquals(3, numResults);

        numResults = catalogManager.getCohortManager().search(studyFqn, new Query(CohortDBAdaptor.QueryParams.ID.key(), "MyCohort2"), new
                QueryOptions(), ownerToken).getNumResults();
        assertEquals(1, numResults);

        numResults = catalogManager.getCohortManager().search(studyFqn, new Query(CohortDBAdaptor.QueryParams.ID.key(), "~MyCohort."), new
                QueryOptions(), ownerToken).getNumResults();
        assertEquals(4, numResults);

        numResults = catalogManager.getCohortManager().search(studyFqn, new Query(CohortDBAdaptor.QueryParams.TYPE.key(),
                Enums.CohortType.FAMILY), new QueryOptions(), ownerToken).getNumResults();
        assertEquals(2, numResults);

        numResults = catalogManager.getCohortManager().search(studyFqn, new Query(CohortDBAdaptor.QueryParams.TYPE.key(), "CASE_CONTROL"),
                new QueryOptions(), ownerToken).getNumResults();
        assertEquals(1, numResults);

        numResults = catalogManager.getCohortManager().search(studyFqn, new Query(CohortDBAdaptor.QueryParams.UID.key(), myCohort1.getUid() +
                "," + myCohort2.getUid() + "," + myCohort3.getUid()), new QueryOptions(), ownerToken).getNumResults();
        assertEquals(3, numResults);
    }

    @Test
    public void testCreateCohortFail() throws CatalogException {
        thrown.expect(CatalogException.class);
        List<Sample> sampleList = Arrays.asList(new Sample().setId("a"), new Sample().setId("b"), new Sample().setId("c"));
        catalogManager.getCohortManager().create(studyFqn,
                new Cohort().setId("MyCohort").setType(Enums.CohortType.FAMILY).setSamples(sampleList),
                null, ownerToken);
    }

    @Test
    public void testCreateCohortAlreadyExisting() throws CatalogException {
        Sample sampleId1 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_1"), INCLUDE_RESULT, ownerToken).first();
        catalogManager.getCohortManager().create(studyFqn, new Cohort().setId("MyCohort").setType(Enums.CohortType.FAMILY)
                .setSamples(Collections.singletonList(sampleId1)), null, ownerToken).first();

        thrown.expect(CatalogDBException.class);
        catalogManager.getCohortManager().create(studyFqn, new Cohort().setId("MyCohort").setType(Enums.CohortType.FAMILY)
                .setSamples(Collections.singletonList(sampleId1)), null, ownerToken).first();
    }

    @Test
    public void testUpdateCohort() throws CatalogException {
        Sample sampleId1 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_1"), INCLUDE_RESULT, ownerToken).first();
        Sample sampleId2 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_2"), INCLUDE_RESULT, ownerToken).first();
        Sample sampleId3 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_3"), INCLUDE_RESULT, ownerToken).first();
        Sample sampleId4 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_4"), INCLUDE_RESULT, ownerToken).first();
        Sample sampleId5 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_5"), INCLUDE_RESULT, ownerToken).first();

        Cohort myCohort = catalogManager.getCohortManager().create(studyFqn, new Cohort().setId("MyCohort").setType(Enums.CohortType.FAMILY)
                .setSamples(Arrays.asList(sampleId1, sampleId2, sampleId3, sampleId1)), INCLUDE_RESULT, ownerToken).first();

        assertEquals("MyCohort", myCohort.getId());
        assertEquals(3, myCohort.getSamples().size());
        assertEquals(3, myCohort.getNumSamples());
        assertTrue(myCohort.getSamples().stream().map(Sample::getUid).collect(Collectors.toList()).contains(sampleId1.getUid()));
        assertTrue(myCohort.getSamples().stream().map(Sample::getUid).collect(Collectors.toList()).contains(sampleId2.getUid()));
        assertTrue(myCohort.getSamples().stream().map(Sample::getUid).collect(Collectors.toList()).contains(sampleId3.getUid()));

        QueryOptions options = new QueryOptions(Constants.ACTIONS, new ObjectMap(CohortDBAdaptor.QueryParams.SAMPLES.key(),
                ParamUtils.BasicUpdateAction.SET.name()));

        DataResult<Cohort> result = catalogManager.getCohortManager().update(studyFqn, myCohort.getId(),
                new CohortUpdateParams()
                        .setId("myModifiedCohort")
                        .setSamples(Arrays.asList(
                                new SampleReferenceParam().setId(sampleId1.getId()),
                                new SampleReferenceParam().setId(sampleId3.getId()),
                                new SampleReferenceParam().setId(sampleId3.getId()),
                                new SampleReferenceParam().setId(sampleId4.getId()),
                                new SampleReferenceParam().setId(sampleId5.getId()))),
                options, ownerToken);
        assertEquals(1, result.getNumUpdated());

        Cohort myModifiedCohort = catalogManager.getCohortManager().get(studyFqn, "myModifiedCohort", QueryOptions.empty(), ownerToken).first();
        assertEquals("myModifiedCohort", myModifiedCohort.getId());
        assertEquals(4, myModifiedCohort.getSamples().size());
        assertEquals(4, myModifiedCohort.getNumSamples());
        assertTrue(myModifiedCohort.getSamples().stream().map(Sample::getUid).collect(Collectors.toList()).contains(sampleId1.getUid()));
        assertTrue(myModifiedCohort.getSamples().stream().map(Sample::getUid).collect(Collectors.toList()).contains(sampleId3.getUid()));
        assertTrue(myModifiedCohort.getSamples().stream().map(Sample::getUid).collect(Collectors.toList()).contains(sampleId4.getUid()));
        assertTrue(myModifiedCohort.getSamples().stream().map(Sample::getUid).collect(Collectors.toList()).contains(sampleId5.getUid()));

        QueryOptions options1 = new QueryOptions(QueryOptions.INCLUDE, CohortDBAdaptor.QueryParams.NUM_SAMPLES.key());
        myModifiedCohort = catalogManager.getCohortManager().get(studyFqn, "myModifiedCohort", options1, ownerToken).first();
        assertEquals(4, myModifiedCohort.getNumSamples());
        assertNull(myModifiedCohort.getSamples());

        options = new QueryOptions(Constants.ACTIONS, new ObjectMap(CohortDBAdaptor.QueryParams.SAMPLES.key(),
                ParamUtils.BasicUpdateAction.SET.name()));
        result = catalogManager.getCohortManager().update(studyFqn, myModifiedCohort.getId(),
                new CohortUpdateParams()
                        .setSamples(Collections.emptyList()),
                options, ownerToken);
        assertEquals(1, result.getNumUpdated());

        myModifiedCohort = catalogManager.getCohortManager().get(studyFqn, "myModifiedCohort", QueryOptions.empty(), ownerToken).first();
        assertEquals(0, myModifiedCohort.getSamples().size());
        assertEquals(0, myModifiedCohort.getNumSamples());

        options = new QueryOptions(Constants.ACTIONS, new ObjectMap(CohortDBAdaptor.QueryParams.SAMPLES.key(),
                ParamUtils.BasicUpdateAction.ADD.name()));
        result = catalogManager.getCohortManager().update(studyFqn, myModifiedCohort.getId(),
                new CohortUpdateParams()
                        .setSamples(Arrays.asList(
                                new SampleReferenceParam().setId(sampleId1.getId()),
                                new SampleReferenceParam().setId(sampleId3.getId()),
                                new SampleReferenceParam().setId(sampleId1.getId()),
                                new SampleReferenceParam().setId(sampleId3.getId()))),
                options, ownerToken);
        assertEquals(1, result.getNumUpdated());
        myModifiedCohort = catalogManager.getCohortManager().get(studyFqn, "myModifiedCohort", QueryOptions.empty(), ownerToken).first();
        assertEquals(2, myModifiedCohort.getSamples().size());
        assertEquals(2, myModifiedCohort.getNumSamples());

        options = new QueryOptions(Constants.ACTIONS, new ObjectMap(CohortDBAdaptor.QueryParams.SAMPLES.key(),
                ParamUtils.BasicUpdateAction.REMOVE.name()));
        result = catalogManager.getCohortManager().update(studyFqn, myModifiedCohort.getId(),
                new CohortUpdateParams()
                        .setSamples(Arrays.asList(
                                new SampleReferenceParam().setId(sampleId3.getId()),
                                new SampleReferenceParam().setId(sampleId3.getId()))),
                options, ownerToken);
        assertEquals(1, result.getNumUpdated());
        myModifiedCohort = catalogManager.getCohortManager().get(studyFqn, "myModifiedCohort", QueryOptions.empty(), ownerToken).first();
        assertEquals(1, myModifiedCohort.getSamples().size());
        assertEquals(1, myModifiedCohort.getNumSamples());
        assertTrue(myModifiedCohort.getSamples().stream().map(Sample::getUid).collect(Collectors.toList()).contains(sampleId1.getUid()));
    }

    @Test
    public void testDeleteCohort() throws CatalogException {
        Sample sampleId1 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_1"), INCLUDE_RESULT, ownerToken).first();
        Sample sampleId2 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_2"), INCLUDE_RESULT, ownerToken).first();
        Sample sampleId3 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_3"), INCLUDE_RESULT, ownerToken).first();

        Cohort myCohort = catalogManager.getCohortManager().create(studyFqn, new Cohort().setId("MyCohort").setType(Enums.CohortType.FAMILY)
                .setSamples(Arrays.asList(sampleId1, sampleId2, sampleId3)), INCLUDE_RESULT, ownerToken).first();

        assertEquals("MyCohort", myCohort.getId());
        assertEquals(3, myCohort.getSamples().size());
        assertTrue(myCohort.getSamples().stream().map(Sample::getUid).collect(Collectors.toList()).contains(sampleId1.getUid()));
        assertTrue(myCohort.getSamples().stream().map(Sample::getUid).collect(Collectors.toList()).contains(sampleId2.getUid()));
        assertTrue(myCohort.getSamples().stream().map(Sample::getUid).collect(Collectors.toList()).contains(sampleId3.getUid()));

        DataResult deleteResult = catalogManager.getCohortManager().delete(studyFqn,
                new Query(CohortDBAdaptor.QueryParams.UID.key(), myCohort.getUid()), null, ownerToken);
        assertEquals(1, deleteResult.getNumDeleted());

        Query query = new Query()
                .append(CohortDBAdaptor.QueryParams.UID.key(), myCohort.getUid())
                .append(CohortDBAdaptor.QueryParams.DELETED.key(), true);
        Cohort cohort = catalogManager.getCohortManager().search(studyFqn, query, null, ownerToken).first();
        assertEquals(InternalStatus.DELETED, cohort.getInternal().getStatus().getId());
    }

    @Test
    public void getSamplesFromCohort() throws CatalogException {
        Sample sampleId1 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_1"), INCLUDE_RESULT,
                ownerToken).first();
        Sample sampleId2 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_2"), INCLUDE_RESULT,
                ownerToken).first();
        Sample sampleId3 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_3"), INCLUDE_RESULT,
                ownerToken).first();

        Cohort myCohort = catalogManager.getCohortManager().create(studyFqn, new Cohort().setId("MyCohort").setType(Enums.CohortType.FAMILY)
                .setSamples(Arrays.asList(sampleId1, sampleId2, sampleId3)), INCLUDE_RESULT, ownerToken).first();

        DataResult<Sample> myCohort1 = catalogManager.getCohortManager().getSamples(studyFqn, "MyCohort", ownerToken);
        assertEquals(3, myCohort1.getNumResults());

        thrown.expect(CatalogParameterException.class);
        catalogManager.getCohortManager().getSamples(studyFqn, "MyCohort,AnotherCohort", ownerToken);

        thrown.expect(CatalogParameterException.class);
        catalogManager.getCohortManager().getSamples(studyFqn, "MyCohort,MyCohort", ownerToken);
    }

    @Test
    public void generateCohortFromSampleQuery() throws CatalogException {
        catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_1"), INCLUDE_RESULT, ownerToken);
        catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_2"), INCLUDE_RESULT, ownerToken);
        catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_3"), INCLUDE_RESULT, ownerToken);

        Query query = new Query();
        Cohort myCohort = catalogManager.getCohortManager().generate(studyFqn, query, new Cohort().setId("MyCohort"), INCLUDE_RESULT, ownerToken).first();
        assertEquals(12, myCohort.getSamples().size());

        query = new Query(SampleDBAdaptor.QueryParams.ID.key(), "~^SAM");
        myCohort = catalogManager.getCohortManager().generate(studyFqn, query, new Cohort()
                .setId("MyCohort2")
                .setStatus(new Status("custom", "custom", "description", TimeUtils.getTime())), INCLUDE_RESULT, ownerToken).first();
        assertEquals(3, myCohort.getSamples().size());
        assertNotNull(myCohort.getStatus());
        assertEquals("custom", myCohort.getStatus().getName());
        assertEquals("description", myCohort.getStatus().getDescription());
    }

    @Test
    public void removeCohortReferencesAfterDeleteTest() throws CatalogException {
        Sample sampleId1 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_1"), INCLUDE_RESULT,
                ownerToken).first();
        Sample sampleId2 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_2"), INCLUDE_RESULT,
                ownerToken).first();
        Sample sampleId3 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_3"), INCLUDE_RESULT,
                ownerToken).first();

        Cohort myCohort = catalogManager.getCohortManager().create(studyFqn, new Cohort().setId("MyCohort").setType(Enums.CohortType.FAMILY)
                .setSamples(Arrays.asList(sampleId1, sampleId2, sampleId3)), INCLUDE_RESULT, ownerToken).first();
        assertEquals(3, myCohort.getNumSamples());
        // Assert samples are sampleId1, sampleId2 and sampleId3
        List<String> sampleIds = myCohort.getSamples().stream().map(Sample::getId).collect(Collectors.toList());
        assertTrue(sampleIds.contains(sampleId1.getId()));
        assertTrue(sampleIds.contains(sampleId2.getId()));
        assertTrue(sampleIds.contains(sampleId3.getId()));

        DataResult<Sample> sampleResult = catalogManager.getCohortManager().getSamples(studyFqn, "MyCohort", ownerToken);
        assertEquals(3, sampleResult.getNumResults());
        for (Sample sample : sampleResult.getResults()) {
            assertEquals(1, sample.getCohortIds().size());
            assertEquals(myCohort.getId(), sample.getCohortIds().get(0));
        }

        // Delete cohort
        DataResult<Cohort> deleteResult = catalogManager.getCohortManager().delete(studyFqn, myCohort.getId(), null, ownerToken);
        assertEquals(1, deleteResult.getNumDeleted());

        // Search cohort
        assertThrows(CatalogException.class, () -> catalogManager.getCohortManager().get(studyFqn, myCohort.getId(), null, ownerToken));

        // Search samples and check that cohort references have been removed
        sampleResult = catalogManager.getSampleManager().search(studyFqn, new Query(SampleDBAdaptor.QueryParams.ID.key(), sampleIds),
                QueryOptions.empty(), ownerToken);
        assertEquals(3, sampleResult.getNumResults());
        for (Sample sample : sampleResult.getResults()) {
            assertEquals(0, sample.getCohortIds().size());
        }
    }


}
