/*
 * Copyright 2015-2020 OpenCB
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

package org.opencb.opencga.catalog.db.mongodb;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.CohortDBAdaptor;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.managers.SampleManager;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.cohort.Cohort;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.InternalStatus;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.individual.IndividualInternal;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.sample.SampleInternal;
import org.opencb.opencga.core.testclassification.duration.MediumTests;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created on 24/08/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Category(MediumTests.class)
public class SampleMongoDBAdaptorTest extends AbstractMongoDBAdaptorTest {

//    @Test
//    public void testAnnotateSample() throws Exception {
//        long sampleId = s1.getId();
//
//        Map<String, Object> annotations = new HashMap<>();
//        annotations.put("key", "value");
//        annotations.put("key2", "value2");
//        annotations.put("key3", 3);
//        annotations.put("key4", true);
//
//        AnnotationSet annot1 = new AnnotationSet("annot1", 3, annotations, "", 1, Collections.emptyMap());
//        AnnotationSet annot2 = new AnnotationSet("annot2", 3, annotations, "", 1, Collections.emptyMap());
//        catalogSampleDBAdaptor.createAnnotationSet(sampleId, , annot1);
//        catalogSampleDBAdaptor.createAnnotationSet(sampleId, , annot2);
//
//        Sample sample = catalogSampleDBAdaptor.get(sampleId, new QueryOptions()).first();
//        Map<String, AnnotationSet> annotationSets = sample.getAnnotationSets().stream()
//                .collect(Collectors.toMap(AnnotationSet::getName, Function.identity()));
//        assertEquals(2, annotationSets.size());
//        assertEquals(annot1, annotationSets.get(annot1.getName()));
//        assertEquals(annot2, annotationSets.get(annot2.getName()));
//
//        catalogSampleDBAdaptor.deleteAnnotationSet(sampleId, annot1.getName());
//
//        sample = catalogSampleDBAdaptor.get(sampleId, new QueryOptions()).first();
//        annotationSets = sample.getAnnotationSets().stream().collect(Collectors.toMap(AnnotationSet::getName, Function.identity()));
//        assertEquals(1, annotationSets.size());
//        assertFalse(annotationSets.containsKey(annot1.getName()));
//        assertEquals(annot2, annotationSets.get(annot2.getName()));
//
//    }

//    @Test
//    public void testAddVariableToAnnotations() throws Exception {
//        createAnnotationExample();
//        Variable variable = new Variable("ANOTHER_KEY", "", Variable.VariableType.BOOLEAN, false, true, false, null, -1, null, null, null,
//                null);
//        assertEquals(3, catalogSampleDBAdaptor.addVariableToAnnotations(3, variable).first().longValue());
//    }
//
//    @Test
//    public void testRenameFieldFromAnnotation() throws Exception {
//        createAnnotationExample();
//        // Rename
//        DataResult<Long> longDataResult = catalogSampleDBAdaptor.renameAnnotationField(3, "key4", "newKey4");
//        assertEquals(3, longDataResult.first().longValue());
//
//        longDataResult = catalogSampleDBAdaptor.renameAnnotationField(1, "key4", "newKey4");
//        assertEquals(0, longDataResult.first().longValue());
//
//        longDataResult = catalogSampleDBAdaptor.renameAnnotationField(3, "key4", "renewkey4");
//        assertEquals(0, longDataResult.first().longValue());
//
//    }

//    private void createAnnotationExample() throws CatalogDBException {
//        long sampleId = s1.getId();
//
//        Map<String, Object> annotations = new HashMap<>();
//        annotations.put("key", "value");
//        annotations.put("key2", "value2");
//        annotations.put("key3", 3);
//        annotations.put("key4", true);
//
//        AnnotationSet annot1 = new AnnotationSet("annot1", 3, annotations, "", 1, Collections.emptyMap());
//        AnnotationSet annot2 = new AnnotationSet("annot2", 3, annotations, "", 1, Collections.emptyMap());
//        AnnotationSet annot3 = new AnnotationSet("annot3", 2, annotations, "", 1, Collections.emptyMap());
//        catalogSampleDBAdaptor.createAnnotationSet(sampleId, , annot3);
//        catalogSampleDBAdaptor.createAnnotationSet(sampleId, , annot1);
//        catalogSampleDBAdaptor.createAnnotationSet(sampleId, , annot2);
//
//        AnnotationSet annot4 = new AnnotationSet("annot4", 3, annotations, "", 1, Collections.emptyMap());
//        catalogSampleDBAdaptor.createAnnotationSet(s2.getId(), , annot4);
//    }

//    @Test
//    public void testRemoveFieldFromAnnotations() throws Exception {
//        createAnnotationExample();
//        DataResult<Long> longDataResult = catalogSampleDBAdaptor.removeAnnotationField(3, "key4");
//        assertEquals(3, longDataResult.first().longValue());
//
//        longDataResult = catalogSampleDBAdaptor.removeAnnotationField(1, "key3");
//        assertEquals(0, longDataResult.first().longValue());
//
//        longDataResult = catalogSampleDBAdaptor.removeAnnotationField(3, "nonExistentKey");
//        assertEquals(0, longDataResult.first().longValue());
//    }

//    @Test
//    public void testOverwriteAnnotateSample() throws Exception {
//        long sampleId = s1.getId();
//
//        Map<String, Object> annotations = new HashMap<>();
//        annotations.put("key", "value");
//        annotations.put("key2", "value2");
//        annotations.put("key3", 3);
//        annotations.put("key4", true);
//
//        AnnotationSet expectedAnnot = new AnnotationSet("annot1", 3, annotations, "", 1, Collections.emptyMap());
//
//        catalogSampleDBAdaptor.createAnnotationSet(sampleId, , expectedAnnot);
//        AnnotationSet annot = catalogSampleDBAdaptor.get(sampleId, null).first().getAnnotationSets().get(0);
//        assertEquals(expectedAnnot, annot);
//
//        annotations = new HashMap<>();
//        annotations.put("key2", "value2");
//        annotations.put("key3", 8);
//        annotations.put("key4", false);
//        annotations.put("key5", 2.3);
//        expectedAnnot = new AnnotationSet("annot1", 3, annotations, "", 1, Collections.emptyMap());
//        catalogSampleDBAdaptor.updateAnnotationSet(sampleId, expectedAnnot);
//        annot = catalogSampleDBAdaptor.get(sampleId, null).first().getAnnotationSets().get(0);
//        assertEquals(expectedAnnot, annot);
//
//    }

    @Test
    public void searchByOntology() throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        List<Phenotype> ontologyList = Arrays.asList(
                new Phenotype("hpo:123", "One hpo term", "hpo", Phenotype.Status.UNKNOWN),
                new Phenotype("hpo:456", "Another hpo term", "hpo", Phenotype.Status.UNKNOWN),
                new Phenotype("go:123", "My go term", "go", Phenotype.Status.UNKNOWN)
        );
        new InternalStatus();
        Sample sample1 = new Sample().setId("sample1").setPhenotypes(ontologyList).setInternal(SampleInternal.init());

        ontologyList = Arrays.asList(
                new Phenotype("hpo:789", "One hpo term", "hpo", Phenotype.Status.UNKNOWN),
                new Phenotype("hpo:xxx", "Another hpo term", "hpo", Phenotype.Status.UNKNOWN),
                new Phenotype("hpo:456", "Another hpo term", "hpo", Phenotype.Status.UNKNOWN),
                new Phenotype("go:yyy", "My go term", "go", Phenotype.Status.UNKNOWN)
        );
        new InternalStatus();
        Sample sample2 = new Sample().setId("sample2").setPhenotypes(ontologyList).setInternal(SampleInternal.init());

        catalogSampleDBAdaptor.insert(studyUid, sample1, Collections.emptyList(), new QueryOptions());
        catalogSampleDBAdaptor.insert(studyUid, sample2, Collections.emptyList(), new QueryOptions());

        // Start the search
        Query query = new Query()
                .append(SampleDBAdaptor.QueryParams.PHENOTYPES.key(), "=hpo:456,hpo:xxx")
                .append(SampleDBAdaptor.QueryParams.ID.key(), "=sample2");

        DataResult<Sample> sampleDataResult = catalogSampleDBAdaptor.get(query, new QueryOptions());
        assertEquals(1, sampleDataResult.getNumResults());
        assertEquals(sample2.getId(), sampleDataResult.first().getId());

        query = new Query()
                .append(SampleDBAdaptor.QueryParams.PHENOTYPES.key(), "hpo:456,hpo:xxx");
        sampleDataResult = catalogSampleDBAdaptor.get(query, new QueryOptions());
        assertEquals(2, sampleDataResult.getNumResults());

        query = new Query()
                .append(SampleDBAdaptor.QueryParams.PHENOTYPES.key(), "My go term");
        sampleDataResult = catalogSampleDBAdaptor.get(query, new QueryOptions());
        assertEquals(2, sampleDataResult.getNumResults());

        query = new Query()
                .append(SampleDBAdaptor.QueryParams.PHENOTYPES.key(), "go:123");
        sampleDataResult = catalogSampleDBAdaptor.get(query, new QueryOptions());
        assertEquals(1, sampleDataResult.getNumResults());
        assertEquals(sample1.getId(), sampleDataResult.first().getId());

        query = new Query()
                .append(SampleDBAdaptor.QueryParams.PHENOTYPES.key(), "=hpo:456,My go term");
        sampleDataResult = catalogSampleDBAdaptor.get(query, new QueryOptions());
        assertEquals(2, sampleDataResult.getNumResults());
    }

    @Test
    public void createSampleCohortTest() throws Exception {
        Sample sample = new Sample("test1", null, "A description", 1);
        dbAdaptorFactory.getCatalogSampleDBAdaptor(organizationId).insert(studyUid, sample, Collections.emptyList(), null);

        List<Sample> samples = new ArrayList<>();
        samples.add(sample);
        Cohort cohort = new Cohort("id_cohort_1", "", Enums.CohortType.CASE_CONTROL, "20210702",
                "20210702", "Test cohort", samples, 5, Collections.emptyMap());

        dbAdaptorFactory.getCatalogCohortDBAdaptor(organizationId).insert(studyUid, cohort, Collections.emptyList(), null);

        Query query = new Query()
                .append(SampleDBAdaptor.QueryParams.ID.key(), "test1")
                .append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);
        DataResult<Sample> result = catalogSampleDBAdaptor.get(query, QueryOptions.empty());

        assertEquals(sample.getId(), result.first().getId());
        assertEquals(sample.getDescription(), result.first().getDescription());
        assertTrue(result.first().getUid() > 0);
        assertTrue(result.first().getCohortIds().size() > 0);
    }

    @Test
    public void createSampleTest() throws Exception {
        Sample hg0097 = new Sample("HG0097", null, "A description", 1);
        dbAdaptorFactory.getCatalogSampleDBAdaptor(organizationId).insert(studyUid, hg0097, Collections.emptyList(), null);

        Query query = new Query()
                .append(SampleDBAdaptor.QueryParams.ID.key(), "HG0097")
                .append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);
        DataResult<Sample> result = catalogSampleDBAdaptor.get(query, QueryOptions.empty());

        assertEquals(hg0097.getId(), result.first().getId());
        assertEquals(hg0097.getDescription(), result.first().getDescription());
        assertTrue(result.first().getUid() > 0);
    }

    @Test
    public void searchSampleByDateTest() throws Exception {
        // We create two samples "created" in different years
        Calendar calendar = Calendar.getInstance();
        calendar.set(2004, Calendar.MARCH, 24);
        Sample sample1 = new Sample("sample1", null, "A description", 1)
                .setCreationDate(TimeUtils.getTime(calendar.getTime()));

        calendar.set(2016, Calendar.DECEMBER, 17);
        Sample sample2 = new Sample("sample2", null, "A description", 1)
                .setCreationDate(TimeUtils.getTime(calendar.getTime()));

        dbAdaptorFactory.getCatalogSampleDBAdaptor(organizationId).insert(studyUid, sample1, null, null);
        dbAdaptorFactory.getCatalogSampleDBAdaptor(organizationId).insert(studyUid, sample2, null, null);

        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.ID.key());

        Query query = new Query()
                .append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(SampleDBAdaptor.QueryParams.ID.key(), "~^sample");

        query.put(SampleDBAdaptor.QueryParams.CREATION_DATE.key(), ">2005");
        DataResult<Sample> sampleDataResult = dbAdaptorFactory.getCatalogSampleDBAdaptor(organizationId).get(query, options);
        assertEquals(1, sampleDataResult.getNumResults());
        assertEquals("sample2", sampleDataResult.first().getId());

        query.put(SampleDBAdaptor.QueryParams.CREATION_DATE.key(), ">200401");
        sampleDataResult = dbAdaptorFactory.getCatalogSampleDBAdaptor(organizationId).get(query, options);
        assertEquals(2, sampleDataResult.getNumResults());

        query.put(SampleDBAdaptor.QueryParams.CREATION_DATE.key(), "2003-2005");
        sampleDataResult = dbAdaptorFactory.getCatalogSampleDBAdaptor(organizationId).get(query, options);
        assertEquals(1, sampleDataResult.getNumResults());
        assertEquals("sample1", sampleDataResult.first().getId());

        query.put(SampleDBAdaptor.QueryParams.CREATION_DATE.key(), "2003-2018");
        sampleDataResult = dbAdaptorFactory.getCatalogSampleDBAdaptor(organizationId).get(query, options);
        assertEquals(2, sampleDataResult.getNumResults());

        query.put(SampleDBAdaptor.QueryParams.CREATION_DATE.key(), "<201611");
        sampleDataResult = dbAdaptorFactory.getCatalogSampleDBAdaptor(organizationId).get(query, options);
        assertEquals(1, sampleDataResult.getNumResults());
        assertEquals("sample1", sampleDataResult.first().getId());
    }

    @Test
    public void caseInsensitiveSearchTest() throws Exception {
        Sample sample1 = new Sample("sample1", null, "A description", 1);
        Sample sample2 = new Sample("sample2", null, "A description", 1);
        Sample sample2Ext = new Sample("sample2Ext", null, "A description", 1);

        dbAdaptorFactory.getCatalogSampleDBAdaptor(organizationId).insert(studyUid, sample1, null, null);
        dbAdaptorFactory.getCatalogSampleDBAdaptor(organizationId).insert(studyUid, sample2, null, null);
        dbAdaptorFactory.getCatalogSampleDBAdaptor(organizationId).insert(studyUid, sample2Ext, null, null);

        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.ID.key());

        Query query = new Query()
                .append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(SampleDBAdaptor.QueryParams.ID.key(), "~/SAMpl*/i");
        DataResult<Sample> sampleDataResult = dbAdaptorFactory.getCatalogSampleDBAdaptor(organizationId).get(query, options);
        assertEquals(3, sampleDataResult.getNumResults());
        assertTrue(Arrays.asList("sample1", "sample2", "sample2Ext")
                .containsAll(sampleDataResult.getResults().stream().map(Sample::getId).collect(Collectors.toList())));

        query = new Query()
                .append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(SampleDBAdaptor.QueryParams.ID.key(), "~/SAMpl*/");
        sampleDataResult = dbAdaptorFactory.getCatalogSampleDBAdaptor(organizationId).get(query, options);
        assertEquals(0, sampleDataResult.getNumResults());

        query = new Query()
                .append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(SampleDBAdaptor.QueryParams.ID.key(), "~/SAMple2ext/");
        sampleDataResult = dbAdaptorFactory.getCatalogSampleDBAdaptor(organizationId).get(query, options);
        assertEquals(0, sampleDataResult.getNumResults());

        query = new Query()
                .append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(SampleDBAdaptor.QueryParams.ID.key(), "~/SAMple2ext/i");
        sampleDataResult = dbAdaptorFactory.getCatalogSampleDBAdaptor(organizationId).get(query, options);
        assertEquals(1, sampleDataResult.getNumResults());
        assertEquals("sample2Ext", sampleDataResult.first().getId());

        query = new Query()
                .append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(SampleDBAdaptor.QueryParams.ID.key(), "~/^SAMple/i");
        sampleDataResult = dbAdaptorFactory.getCatalogSampleDBAdaptor(organizationId).get(query, options);
        assertEquals(3, sampleDataResult.getNumResults());
        assertTrue(Arrays.asList("sample1", "sample2", "sample2Ext")
                .containsAll(sampleDataResult.getResults().stream().map(Sample::getId).collect(Collectors.toList())));

        query = new Query()
                .append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(SampleDBAdaptor.QueryParams.ID.key(), "~/^SAMple/");
        sampleDataResult = dbAdaptorFactory.getCatalogSampleDBAdaptor(organizationId).get(query, options);
        assertEquals(0, sampleDataResult.getNumResults());

        query = new Query()
                .append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(SampleDBAdaptor.QueryParams.ID.key(), "~/^AMple/i");
        sampleDataResult = dbAdaptorFactory.getCatalogSampleDBAdaptor(organizationId).get(query, options);
        assertEquals(0, sampleDataResult.getNumResults());

        query = new Query()
                .append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(SampleDBAdaptor.QueryParams.ID.key(), "~/eXt$/i");
        sampleDataResult = dbAdaptorFactory.getCatalogSampleDBAdaptor(organizationId).get(query, options);
        assertEquals(1, sampleDataResult.getNumResults());
        assertEquals("sample2Ext", sampleDataResult.first().getId());

        query = new Query()
                .append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(SampleDBAdaptor.QueryParams.ID.key(), "~/^Sam\\w+eXt$/i");
        sampleDataResult = dbAdaptorFactory.getCatalogSampleDBAdaptor(organizationId).get(query, options);
        assertEquals(1, sampleDataResult.getNumResults());
        assertEquals("sample2Ext", sampleDataResult.first().getId());

    }

    // Test if we can search for samples of an individual
    @Test
    public void getSampleWithIndividual() throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        QueryOptions queryOptions = new QueryOptions();

        // We create a new sample with the individual
        new InternalStatus();
        Sample sample = new Sample().setId("sample1").setInternal(SampleInternal.init());
        catalogSampleDBAdaptor.insert(studyUid, sample, Collections.emptyList(), queryOptions);
        long sampleId = getSample(studyUid, "sample1", queryOptions).getUid();

        // We create an individual
        String individualName = "individualName";
        new InternalStatus();
        Individual individual = new Individual()
                .setId(individualName)
                .setInternal(IndividualInternal.init())
                .setSamples(Arrays.asList(new Sample().setUid(sampleId).setVersion(1).setInternal(SampleInternal.init())));
        dbAdaptorFactory.getCatalogIndividualDBAdaptor(organizationId).insert(studyUid, individual, null, queryOptions);

        // Get the sample
        Query query = new Query()
                .append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key(), "individualName");
        DataResult<Sample> individualQuery = catalogSampleDBAdaptor.get(query, queryOptions);
        assertEquals("sample1", individualQuery.first().getId());

        query = new Query()
                .append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key(), "non-existing");
        DataResult<Sample> inexistentIndividualQuery = catalogSampleDBAdaptor.get(query, queryOptions);
        assertEquals(0, inexistentIndividualQuery.getNumResults());
    }

    @Test
    public void deleteSampleTest() throws Exception {
        Sample hg0097 = new Sample("HG0097", null, "A description", 1)
                .setInternal(SampleInternal.init());
        dbAdaptorFactory.getCatalogSampleDBAdaptor(organizationId).insert(studyUid, hg0097, Collections.emptyList(), null);

        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(SampleDBAdaptor.QueryParams.ID.key(), SampleDBAdaptor.QueryParams.UID.key(),
                        SampleDBAdaptor.QueryParams.VERSION.key(), SampleDBAdaptor.QueryParams.STUDY_UID.key()));
        Sample sample = getSample(studyUid, "HG0097", options);
        dbAdaptorFactory.getCatalogSampleDBAdaptor(organizationId).delete(sample);

        Query query = new Query()
                .append(SampleDBAdaptor.QueryParams.UID.key(), sample.getUid())
                .append(SampleDBAdaptor.QueryParams.DELETED.key(), true);
        DataResult<Sample> jobResult = catalogSampleDBAdaptor.get(query, QueryOptions.empty());
        assertEquals(1, jobResult.getNumResults());

        thrown.expect(CatalogDBException.class);
        thrown.expectMessage("not exist");
        catalogSampleDBAdaptor.get(sample.getUid(), QueryOptions.empty());
    }

    @Test
    public void deleteSampleFail1Test() throws Exception {
        Sample sample = new Sample().setId("non-existing").setUid(55555555);
        thrown.expect(CatalogDBException.class);
        dbAdaptorFactory.getCatalogSampleDBAdaptor(organizationId).delete(sample);
    }

    @Test
    @Ignore
    public void deleteSampleFail2Test() throws Exception {
        long fileId = catalogFileDBAdaptor.getId(studyUid, "data/file.vcf");

        Sample hg0097 = new Sample("HG0097", null, "A description", 1);
        dbAdaptorFactory.getCatalogSampleDBAdaptor(organizationId).insert(studyUid, hg0097, Collections.emptyList(), null);

        Sample sample = getSample(studyUid, "HG0097", SampleManager.INCLUDE_SAMPLE_IDS);
        dbAdaptorFactory.getCatalogFileDBAdaptor(organizationId).update(fileId, new ObjectMap(FileDBAdaptor.QueryParams.SAMPLE_IDS.key(), sample.getId()),
                QueryOptions.empty());

        dbAdaptorFactory.getCatalogSampleDBAdaptor(organizationId).delete(sample);
    }

    @Test
    @Ignore
    // TODO: This should be tested in the sample manager, not here !!!
    public void deleteSampleFail3Test() throws Exception {
        Sample hg0097 = new Sample("HG0097", null, "A description", 1);
        dbAdaptorFactory.getCatalogSampleDBAdaptor(organizationId).insert(studyUid, hg0097, Collections.emptyList(), null);
        Sample sample = getSample(studyUid, "HG0097", SampleManager.INCLUDE_SAMPLE_IDS);

        dbAdaptorFactory.getCatalogCohortDBAdaptor(organizationId).insert(studyUid, new Cohort("Cohort", "", Enums.CohortType.COLLECTION, "", "",
                "", Collections.singletonList(sample), 1, null), null, null);

        thrown.expect(CatalogDBException.class);
        dbAdaptorFactory.getCatalogSampleDBAdaptor(organizationId).delete(sample);
    }

    @Test
    public void createMultipleCohorts() throws Exception {
        AtomicInteger numFailures = new AtomicInteger();
        Function<Integer, String> getCohortName = c -> "Cohort_" + c;
        int numThreads = 10;
        int numCohorts = 10;
        for (int c = 0; c < numCohorts; c++) {
            List<Thread> threads = new LinkedList<>();
            String cohortName = getCohortName.apply(c);
            for (int i = 0; i < numThreads; i++) {
                threads.add(new Thread(() -> {
                    try {
                        dbAdaptorFactory.getCatalogCohortDBAdaptor(organizationId).insert(studyUid, new Cohort(cohortName, "",
                                Enums.CohortType.COLLECTION, "", "", "", Collections.emptyList(), 1, null), null, null);
                    } catch (CatalogException ignore) {
                        numFailures.incrementAndGet();
                    }
                }));
            }
            threads.parallelStream().forEach(Thread::run);
            threads.parallelStream().forEach((thread) -> {
                try {
                    thread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });
        }

        assertEquals(numCohorts * numThreads - numCohorts, numFailures.intValue());
        List<Cohort> cohorts = dbAdaptorFactory.getCatalogCohortDBAdaptor(organizationId).get(
                new Query(CohortDBAdaptor.QueryParams.STUDY_UID.key(), studyUid), null).getResults();
        assertEquals(numCohorts, cohorts.size());
        Set<String> names = cohorts.stream().map(Cohort::getId).collect(Collectors.toSet());
        for (int c = 0; c < numCohorts; c++) {
            String cohortName = getCohortName.apply(c);
            names.contains(cohortName);
        }
    }
}
