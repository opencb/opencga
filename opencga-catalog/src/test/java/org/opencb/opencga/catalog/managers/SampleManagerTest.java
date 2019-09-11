package org.opencb.opencga.catalog.managers;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.bson.Document;
import org.junit.Test;
import org.opencb.biodata.models.pedigree.IndividualProperty;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.core.result.WriteResult;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.IndividualDBAdaptor;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.update.IndividualUpdateParams;
import org.opencb.opencga.catalog.models.update.SampleUpdateParams;
import org.opencb.opencga.catalog.utils.CatalogAnnotationsValidatorTest;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.core.models.acls.AclParams;
import org.opencb.opencga.core.models.acls.permissions.IndividualAclEntry;
import org.opencb.opencga.core.models.acls.permissions.SampleAclEntry;
import org.opencb.opencga.core.models.summaries.FeatureCount;
import org.opencb.opencga.core.models.summaries.VariableSetSummary;

import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static org.opencb.opencga.catalog.db.api.SampleDBAdaptor.QueryParams.ANNOTATION;

public class SampleManagerTest extends AbstractManagerTest {

    @Test
    public void testSampleVersioning() throws CatalogException {
        Query query = new Query(ProjectDBAdaptor.QueryParams.USER_ID.key(), "user");
        String projectId = catalogManager.getProjectManager().get(query, null, sessionIdUser).first().getId();

        catalogManager.getSampleManager().create(studyFqn,
                new Sample().setId("testSample").setDescription("description"), null, sessionIdUser);
        catalogManager.getSampleManager().update(studyFqn, "testSample", new SampleUpdateParams(),
                new QueryOptions(Constants.INCREMENT_VERSION, true), sessionIdUser);
        catalogManager.getSampleManager().update(studyFqn, "testSample", new SampleUpdateParams(),
                new QueryOptions(Constants.INCREMENT_VERSION, true), sessionIdUser);

        catalogManager.getProjectManager().incrementRelease(projectId, sessionIdUser);
        // We create something to have a gap in the release
        catalogManager.getSampleManager().create(studyFqn, new Sample().setId("dummy"), null, sessionIdUser);

        catalogManager.getProjectManager().incrementRelease(projectId, sessionIdUser);
        catalogManager.getSampleManager().update(studyFqn, "testSample", new SampleUpdateParams(),
                new QueryOptions(Constants.INCREMENT_VERSION, true), sessionIdUser);

        catalogManager.getSampleManager().update(studyFqn, "testSample", new SampleUpdateParams().setDescription("new description"),
                null, sessionIdUser);

        // We want the whole history of the sample
        query = new Query()
                .append(SampleDBAdaptor.QueryParams.ID.key(), "testSample")
                .append(Constants.ALL_VERSIONS, true);
        QueryResult<Sample> sampleQueryResult = catalogManager.getSampleManager().get(studyFqn, query, null, sessionIdUser);
        assertEquals(4, sampleQueryResult.getNumResults());
        assertEquals("description", sampleQueryResult.getResult().get(0).getDescription());
        assertEquals("description", sampleQueryResult.getResult().get(1).getDescription());
        assertEquals("description", sampleQueryResult.getResult().get(2).getDescription());
        assertEquals("new description", sampleQueryResult.getResult().get(3).getDescription());

        // We want the last version of release 1
        query = new Query()
                .append(SampleDBAdaptor.QueryParams.ID.key(), "testSample")
                .append(SampleDBAdaptor.QueryParams.SNAPSHOT.key(), 1);
        sampleQueryResult = catalogManager.getSampleManager().get(studyFqn, query, null, sessionIdUser);
        assertEquals(1, sampleQueryResult.getNumResults());
        assertEquals(3, sampleQueryResult.first().getVersion());

        // We want the last version of release 2 (must be the same of release 1)
        query = new Query()
                .append(SampleDBAdaptor.QueryParams.ID.key(), "testSample")
                .append(SampleDBAdaptor.QueryParams.SNAPSHOT.key(), 2);
        sampleQueryResult = catalogManager.getSampleManager().get(studyFqn, query, null, sessionIdUser);
        assertEquals(1, sampleQueryResult.getNumResults());
        assertEquals(3, sampleQueryResult.first().getVersion());

        // We want the last version of the sample
        query = new Query()
                .append(SampleDBAdaptor.QueryParams.ID.key(), "testSample");
        sampleQueryResult = catalogManager.getSampleManager().get(studyFqn, query, null, sessionIdUser);
        assertEquals(1, sampleQueryResult.getNumResults());
        assertEquals(4, sampleQueryResult.first().getVersion());

        // We want the version 2 of the sample
        query = new Query()
                .append(SampleDBAdaptor.QueryParams.ID.key(), "testSample")
                .append(SampleDBAdaptor.QueryParams.VERSION.key(), 2);
        sampleQueryResult = catalogManager.getSampleManager().get(studyFqn, query, null, sessionIdUser);
        assertEquals(1, sampleQueryResult.getNumResults());
        assertEquals(2, sampleQueryResult.first().getVersion());

        // We want the version 1 of the sample
        query = new Query()
                .append(SampleDBAdaptor.QueryParams.ID.key(), "testSample")
                .append(SampleDBAdaptor.QueryParams.VERSION.key(), 1);
        sampleQueryResult = catalogManager.getSampleManager().get(studyFqn, query, null, sessionIdUser);
        assertEquals(1, sampleQueryResult.getNumResults());
        assertEquals(1, sampleQueryResult.first().getVersion());

        List<QueryResult<Sample>> testSample = catalogManager.getSampleManager()
                .get(studyFqn, Collections.singletonList("testSample"), new Query(Constants.ALL_VERSIONS, true), null, false, sessionIdUser);
        assertEquals(1, testSample.size());
        assertEquals(4, testSample.get(0).getResult().size());
    }

    @Test
    public void updateProcessingField() throws CatalogException {
        catalogManager.getSampleManager().create(studyFqn,
                new Sample().setId("testSample").setDescription("description"), null, sessionIdUser);

        SampleProcessing processing = new SampleProcessing("product", "preparationMethod", "extractionMethod", "labSampleId", "quantity",
                "date", Collections.emptyMap());
        catalogManager.getSampleManager().update(studyFqn, "testSample", new SampleUpdateParams().setProcessing(processing),
                new QueryOptions(Constants.INCREMENT_VERSION, true), sessionIdUser);

        QueryResult<Sample> testSample = catalogManager.getSampleManager().get(studyFqn, "testSample", new QueryOptions(), sessionIdUser);
        assertEquals("product", testSample.first().getProcessing().getProduct());
        assertEquals("preparationMethod", testSample.first().getProcessing().getPreparationMethod());
        assertEquals("extractionMethod", testSample.first().getProcessing().getExtractionMethod());
        assertEquals("labSampleId", testSample.first().getProcessing().getLabSampleId());
        assertEquals("quantity", testSample.first().getProcessing().getQuantity());
        assertEquals("date", testSample.first().getProcessing().getDate());
        assertTrue(testSample.first().getProcessing().getAttributes().isEmpty());
    }

    @Test
    public void updateCollectionField() throws CatalogException {
        catalogManager.getSampleManager().create(studyFqn,
                new Sample().setId("testSample").setDescription("description"), null, sessionIdUser);

        SampleCollection collection = new SampleCollection("tissue", "organ", "quantity", "method", "date", Collections.emptyMap());
        ObjectMap params = new ObjectMap(SampleDBAdaptor.QueryParams.COLLECTION.key(), collection);
        catalogManager.getSampleManager().update(studyFqn, "testSample", new SampleUpdateParams().setCollection(collection),
                new QueryOptions(Constants.INCREMENT_VERSION, true),                 sessionIdUser);

        QueryResult<Sample> testSample = catalogManager.getSampleManager().get(studyFqn, "testSample", new QueryOptions(), sessionIdUser);
        assertEquals("tissue", testSample.first().getCollection().getTissue());
        assertEquals("organ", testSample.first().getCollection().getOrgan());
        assertEquals("quantity", testSample.first().getCollection().getQuantity());
        assertEquals("method", testSample.first().getCollection().getMethod());
        assertEquals("date", testSample.first().getCollection().getDate());
        assertTrue(testSample.first().getCollection().getAttributes().isEmpty());
    }

    @Test
    public void testCreateSample() throws CatalogException {
        QueryResult<Sample> sampleQueryResult = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("HG007"), null,
                sessionIdUser);
        assertEquals(1, sampleQueryResult.getNumResults());
    }

//    @Test
//    public void testUpdateSampleStats() throws CatalogException {
//        catalogManager.getSampleManager().create(studyFqn, new Sample().setId("HG007"), null, sessionIdUser);
//        QueryResult<Sample> update = catalogManager.getSampleManager().update(studyFqn, "HG007",
//                new ObjectMap(SampleDBAdaptor.QueryParams.STATS.key(), new ObjectMap("one", "two")), new QueryOptions(), sessionIdUser);
//        assertEquals(1, update.first().getStats().size());
//        assertTrue(update.first().getStats().containsKey("one"));
//        assertEquals("two", update.first().getStats().get("one"));
//
//        update = catalogManager.getSampleManager().update(studyFqn, "HG007",
//                new ObjectMap(SampleDBAdaptor.QueryParams.STATS.key(), new ObjectMap("two", "three")), new QueryOptions(), sessionIdUser);
//        assertEquals(2, update.first().getStats().size());
//    }

    @Test
    public void testCreateSampleWithDotInName() throws CatalogException {
        String name = "HG007.sample";
        QueryResult<Sample> sampleQueryResult = catalogManager.getSampleManager().create(studyFqn, new Sample().setId(name), null,
                sessionIdUser);
        assertEquals(name, sampleQueryResult.first().getId());
    }

    @Test
    public void testAnnotate() throws CatalogException {
        List<Variable> variables = new ArrayList<>();
        variables.add(new Variable("NAME", "", Variable.VariableType.TEXT, "", true, false, Collections.emptyList(), 0, "", "",
                null, Collections.emptyMap()));
        variables.add(new Variable("AGE", "", Variable.VariableType.INTEGER, "", false, false, Collections.emptyList(), 0, "", "",
                null, Collections.emptyMap()));
        variables.add(new Variable("HEIGHT", "", Variable.VariableType.DOUBLE, "", false, false, Collections.emptyList(), 0, "",
                "", null, Collections.emptyMap()));
        VariableSet vs1 = catalogManager.getStudyManager().createVariableSet(studyFqn, "vs1", "vs1", false, false, "", null, variables,
                Collections.singletonList(VariableSet.AnnotableDataModels.SAMPLE), sessionIdUser).first();

        HashMap<String, Object> annotations = new HashMap<>();
        annotations.put("NAME", "Joe");
        annotations.put("AGE", 25);
        annotations.put("HEIGHT", 180);

        catalogManager.getSampleManager().update(studyFqn, s_1, new SampleUpdateParams()
                        .setAnnotationSets(Collections.singletonList(new AnnotationSet("annotation1", vs1.getId(), annotations))),
                QueryOptions.empty(), sessionIdUser);

        QueryResult<Sample> sampleQueryResult = catalogManager.getSampleManager().get(studyFqn, s_1,
                new QueryOptions(QueryOptions.INCLUDE, Constants.ANNOTATION_SET_NAME + ".annotation1"), sessionIdUser);
        assertEquals(1, sampleQueryResult.getNumResults());
        assertEquals(1, sampleQueryResult.first().getAnnotationSets().size());

//        QueryResult<AnnotationSet> annotationSetQueryResult = catalogManager.getSampleManager().getAnnotationSet(s_1,
//                studyFqn, "annotation1", sessionIdUser);
//        assertEquals(1, annotationSetQueryResult.getNumResults());
        Map<String, Object> map = sampleQueryResult.first().getAnnotationSets().get(0).getAnnotations();
        assertEquals(3, map.size());
        assertEquals("Joe", map.get("NAME"));
        assertEquals(25, map.get("AGE"));
        assertEquals(180.0, map.get("HEIGHT"));
    }

    @Test
    public void searchSamples() throws CatalogException {
        catalogManager.getStudyManager().createGroup(studyFqn, "myGroup", "myGroup", "user2,user3", sessionIdUser);
        catalogManager.getStudyManager().createGroup(studyFqn, "myGroup2", "myGroup2", "user2,user3", sessionIdUser);
        catalogManager.getStudyManager().updateAcl(Arrays.asList(studyFqn), "@myGroup",
                new Study.StudyAclParams("", AclParams.Action.SET, null), sessionIdUser);

        catalogManager.getSampleManager().updateAcl(studyFqn, Arrays.asList("s_1"), "@myGroup", new Sample.SampleAclParams("VIEW",
                AclParams.Action.SET, null, null, null), sessionIdUser);

        QueryResult<Sample> search = catalogManager.getSampleManager().search(studyFqn, new Query(), new QueryOptions(),
                sessionIdUser2);
        assertEquals(1, search.getNumResults());
    }

    @Test
    public void testDeleteAnnotationset() throws CatalogException, JsonProcessingException {
        List<Variable> variables = new ArrayList<>();
        variables.add(new Variable("var_name", "", "", Variable.VariableType.TEXT, "", true, false, Collections.emptyList(), 0, "", "",
                null, Collections.emptyMap()));
        variables.add(new Variable("AGE", "", "", Variable.VariableType.INTEGER, "", false, false, Collections.emptyList(), 0, "", "",
                null, Collections.emptyMap()));
        variables.add(new Variable("HEIGHT", "", "", Variable.VariableType.DOUBLE, "", false, false, Collections.emptyList(), 0, "",
                "", null, Collections.emptyMap()));
        VariableSet vs1 = catalogManager.getStudyManager().createVariableSet(studyFqn, "vs1", "vs1", false, false, "", null, variables,
                Collections.singletonList(VariableSet.AnnotableDataModels.SAMPLE), sessionIdUser).first();

        ObjectMap annotations = new ObjectMap()
                .append("var_name", "Joe")
                .append("AGE", 25)
                .append("HEIGHT", 180);
        AnnotationSet annotationSet = new AnnotationSet("annotation1", vs1.getId(), annotations);
        AnnotationSet annotationSet1 = new AnnotationSet("annotation2", vs1.getId(), annotations);

        QueryResult<Sample> update = catalogManager.getSampleManager().update(studyFqn, s_1, new SampleUpdateParams()
                        .setAnnotationSets(Arrays.asList(annotationSet, annotationSet1)), QueryOptions.empty(), sessionIdUser);
        assertEquals(3, update.first().getAnnotationSets().size());

        catalogManager.getSampleManager().removeAnnotationSet(studyFqn, s_1, "annotation1", QueryOptions.empty(), sessionIdUser);
        QueryResult<Sample> sampleQueryResult = catalogManager.getSampleManager()
                .removeAnnotationSet(studyFqn, s_1, "annotation2", QueryOptions.empty(), sessionIdUser);
        assertEquals(1, sampleQueryResult.first().getAnnotationSets().size());

        thrown.expect(CatalogDBException.class);
        thrown.expectMessage("not found");
        catalogManager.getSampleManager().removeAnnotationSet(studyFqn, s_1, "non_existing", QueryOptions.empty(), sessionIdUser);
    }

    @Test
    public void testSearchAnnotation() throws CatalogException, JsonProcessingException {
        List<Variable> variables = new ArrayList<>();
        variables.add(new Variable("var_name", "", "", Variable.VariableType.TEXT, "", true, false, Collections.emptyList(), 0, "", "",
                null, Collections.emptyMap()));
        variables.add(new Variable("AGE", "", "", Variable.VariableType.INTEGER, "", false, false, Collections.emptyList(), 0, "", "",
                null, Collections.emptyMap()));
        variables.add(new Variable("HEIGHT", "", "", Variable.VariableType.DOUBLE, "", false, false, Collections.emptyList(), 0, "",
                "", null, Collections.emptyMap()));
        variables.add(new Variable("OTHER", "", "", Variable.VariableType.OBJECT, null, false, false, null, 1, "", "", null,
                Collections.emptyMap()));
        VariableSet vs1 = catalogManager.getStudyManager().createVariableSet(studyFqn, "vs1", "vs1", false, false, "", null, variables,
                Collections.singletonList(VariableSet.AnnotableDataModels.SAMPLE), sessionIdUser).first();

        ObjectMap annotations = new ObjectMap()
                .append("var_name", "Joe")
                .append("AGE", 25)
                .append("HEIGHT", 180);
        AnnotationSet annotationSet = new AnnotationSet("annotation1", vs1.getId(), annotations);

        catalogManager.getSampleManager().update(studyFqn, s_1, new SampleUpdateParams()
                        .setAnnotationSets(Collections.singletonList(annotationSet)), QueryOptions.empty(), sessionIdUser);

        Query query = new Query(Constants.ANNOTATION, "var_name=Joe;" + vs1.getId() + ":AGE=25");
        QueryResult<Sample> annotQueryResult = catalogManager.getSampleManager().search(studyFqn, query, QueryOptions.empty(),
                sessionIdUser);
        assertEquals(1, annotQueryResult.getNumResults());

        query.put(Constants.ANNOTATION, "var_name=Joe;" + vs1.getId() + ":AGE=23");
        annotQueryResult = catalogManager.getSampleManager().search(studyFqn, query, QueryOptions.empty(), sessionIdUser);
        assertEquals(0, annotQueryResult.getNumResults());

        query.put(Constants.ANNOTATION, "var_name=Joe;" + vs1.getId() + ":AGE=25;variableSet!=" + vs1.getId());
        annotQueryResult = catalogManager.getSampleManager().search(studyFqn, query, QueryOptions.empty(), sessionIdUser);
        assertEquals(1, annotQueryResult.getNumResults());

        query.put(Constants.ANNOTATION, "var_name=Joe;" + vs1.getId() + ":AGE=25;variableSet!==" + vs1.getId());
        annotQueryResult = catalogManager.getSampleManager().search(studyFqn, query, QueryOptions.empty(), sessionIdUser);
        assertEquals(0, annotQueryResult.getNumResults());

        query.put(Constants.ANNOTATION, "var_name=Joe;" + vs1.getId() + ":AGE=25;variableSet==" + vs1.getId());
        annotQueryResult = catalogManager.getSampleManager().search(studyFqn, query, QueryOptions.empty(), sessionIdUser);
        assertEquals(1, annotQueryResult.getNumResults());

        query.put(Constants.ANNOTATION, "var_name=Joe;" + vs1.getId() + ":AGE=25;variableSet===" + vs1.getId());
        annotQueryResult = catalogManager.getSampleManager().search(studyFqn, query, QueryOptions.empty(), sessionIdUser);
        assertEquals(0, annotQueryResult.getNumResults());

        Study study = catalogManager.getStudyManager().get(studyFqn, null, sessionIdUser).first();
        query.put(Constants.ANNOTATION, "variableSet===" + study.getVariableSets().get(0).getId());
        annotQueryResult = catalogManager.getSampleManager().search(studyFqn, query, QueryOptions.empty(), sessionIdUser);
        assertEquals(7, annotQueryResult.getNumResults());

        query.put(Constants.ANNOTATION, "variableSet!=" + vs1.getId());
        annotQueryResult = catalogManager.getSampleManager().search(studyFqn, query, QueryOptions.empty(), sessionIdUser);
        assertEquals(9, annotQueryResult.getNumResults());

        query.put(Constants.ANNOTATION, "variableSet!==" + vs1.getId());
        annotQueryResult = catalogManager.getSampleManager().search(studyFqn, query, QueryOptions.empty(), sessionIdUser);
        assertEquals(8, annotQueryResult.getNumResults());

        query.put(Constants.ANNOTATION, "variableSet=" + vs1.getId());
        annotQueryResult = catalogManager.getSampleManager().search(studyFqn, query,
                new QueryOptions(QueryOptions.INCLUDE, Constants.VARIABLE_SET + "." + vs1.getId()), sessionIdUser);
        assertEquals(1, annotQueryResult.getNumResults());
        assertEquals(1, annotQueryResult.first().getAnnotationSets().size());
        assertEquals(vs1.getId(), annotQueryResult.first().getAnnotationSets().get(0).getVariableSetId());
    }

    @Test
    public void testProjections() throws CatalogException {
        Study study = catalogManager.getStudyManager().get("1000G:phase1", null, sessionIdUser).first();

        Query query = new Query(Constants.ANNOTATION, "variableSet===" + study.getVariableSets().get(0).getId());
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, "annotationSets");
        QueryResult<Sample> annotQueryResult = catalogManager.getSampleManager().search(studyFqn, query, options,
                sessionIdUser);
        assertEquals(8, annotQueryResult.getNumResults());

        for (Sample sample : annotQueryResult.getResult()) {
            assertEquals(null, sample.getId());
            assertTrue(!sample.getAnnotationSets().isEmpty());
        }
    }

    @Test
    public void testAnnotateMulti() throws CatalogException {
        String sampleId = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_1"), new QueryOptions(),
                sessionIdUser).first().getId();

        List<Variable> variables = new ArrayList<>();
        variables.add(new Variable("NAME", "NAME", "", Variable.VariableType.TEXT, "", true, false, Collections.emptyList(), 0, "", "",
                null, Collections.emptyMap()));
        VariableSet vs1 = catalogManager.getStudyManager().createVariableSet(studyFqn, "vs1", "vs1", false, false, "", null, variables,
                Collections.singletonList(VariableSet.AnnotableDataModels.SAMPLE), sessionIdUser).first();


        HashMap<String, Object> annotations = new HashMap<>();
        annotations.put("NAME", "Luke");

        catalogManager.getSampleManager().update(studyFqn, sampleId, new SampleUpdateParams()
                        .setAnnotationSets(Collections.singletonList(new AnnotationSet("annotation1", vs1.getId(), annotations))),
                QueryOptions.empty(), sessionIdUser);
        QueryResult<Sample> sampleQueryResult = catalogManager.getSampleManager().get(studyFqn, sampleId,
                new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.ANNOTATION_SETS.key()), sessionIdUser);
        assertEquals(1, sampleQueryResult.first().getAnnotationSets().size());

        annotations = new HashMap<>();
        annotations.put("NAME", "Lucas");
        catalogManager.getSampleManager().update(studyFqn, sampleId, new SampleUpdateParams()
                        .setAnnotationSets(Collections.singletonList(new AnnotationSet("annotation2", vs1.getId(), annotations))),
                QueryOptions.empty(), sessionIdUser);
        sampleQueryResult = catalogManager.getSampleManager().get(studyFqn, sampleId,
                new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.ANNOTATION_SETS.key()), sessionIdUser);
        assertEquals(2, sampleQueryResult.first().getAnnotationSets().size());

        assertTrue(Arrays.asList("annotation1", "annotation2")
                .containsAll(sampleQueryResult.first().getAnnotationSets().stream().map(AnnotationSet::getId).collect(Collectors.toSet())));
    }

    @Test
    public void testAnnotateUnique() throws CatalogException {
        String sampleId = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_1"), new QueryOptions(),
                sessionIdUser).first().getId();

        List<Variable> variables = new ArrayList<>();
        variables.add(new Variable("NAME", "NAME", "", Variable.VariableType.TEXT, "", true, false, Collections.emptyList(), 0, "", "",
                null, Collections.emptyMap()));
        VariableSet vs1 = catalogManager.getStudyManager().createVariableSet(studyFqn, "vs1", "vs1", true, false, "", null, variables,
                Collections.singletonList(VariableSet.AnnotableDataModels.SAMPLE), sessionIdUser).first();


        HashMap<String, Object> annotations = new HashMap<>();
        annotations.put("NAME", "Luke");

        catalogManager.getSampleManager().update(studyFqn, sampleId, new SampleUpdateParams()
                        .setAnnotationSets(Collections.singletonList(new AnnotationSet("annotation1", vs1.getId(), annotations))),
                QueryOptions.empty(), sessionIdUser);
        QueryResult<Sample> sampleQueryResult = catalogManager.getSampleManager().get(studyFqn, sampleId,
                new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.ANNOTATION_SETS.key()), sessionIdUser);
        assertEquals(1, sampleQueryResult.first().getAnnotationSets().size());

        annotations.put("NAME", "Lucas");
        thrown.expect(CatalogException.class);
        thrown.expectMessage("unique");
        catalogManager.getSampleManager().update(studyFqn, sampleId, new SampleUpdateParams()
                        .setAnnotationSets(Collections.singletonList(new AnnotationSet("annotation2", vs1.getId(), annotations))),
                QueryOptions.empty(), sessionIdUser);
    }

    @Test
    public void testAnnotateIndividualUnique() throws CatalogException {
        String individualId = catalogManager.getIndividualManager().create(studyFqn, new Individual().setId("INDIVIDUAL_1"),
                new QueryOptions(), sessionIdUser).first().getId();

        List<Variable> variables = new ArrayList<>();
        variables.add(new Variable("NAME", "NAME", "", Variable.VariableType.TEXT, "", true, false, Collections.emptyList(), 0, "", "",
                null, Collections.emptyMap()));
        VariableSet vs1 = catalogManager.getStudyManager().createVariableSet(studyFqn, "vs1", "vs1", true, false, "", null, variables,
                Collections.singletonList(VariableSet.AnnotableDataModels.INDIVIDUAL), sessionIdUser).first();


        HashMap<String, Object> annotations = new HashMap<>();
        annotations.put("NAME", "Luke");
        catalogManager.getIndividualManager().update(studyFqn, individualId, new IndividualUpdateParams()
                        .setAnnotationSets(Collections.singletonList(new AnnotationSet("annotation1", vs1.getId(), annotations))),
                QueryOptions.empty(), sessionIdUser);
        QueryResult<Individual> individualQueryResult = catalogManager.getIndividualManager().get(studyFqn, individualId,
                new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.ANNOTATION_SETS.key()), sessionIdUser);
        assertEquals(1, individualQueryResult.first().getAnnotationSets().size());

        annotations.put("NAME", "Lucas");
        thrown.expect(CatalogException.class);
        thrown.expectMessage("unique");
        catalogManager.getIndividualManager().update(studyFqn, individualId, new IndividualUpdateParams()
                        .setAnnotationSets(Collections.singletonList(new AnnotationSet("annotation2", vs1.getId(), annotations))),
                QueryOptions.empty(), sessionIdUser);
    }

    @Test
    public void testAnnotateIncorrectType() throws CatalogException {
        String sampleId = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_1"), new QueryOptions(),
                sessionIdUser).first().getId();

        List<Variable> variables = new ArrayList<>();
        variables.add(new Variable("NUM", "NUM", "", Variable.VariableType.DOUBLE, "", true, false, null, 0, "", "", null,
                Collections.emptyMap()));
        VariableSet vs1 = catalogManager.getStudyManager().createVariableSet(studyFqn, "vs1", "vs1", false, false, "", null, variables,
                Collections.singletonList(VariableSet.AnnotableDataModels.SAMPLE), sessionIdUser).first();


        HashMap<String, Object> annotations = new HashMap<>();
        annotations.put("NUM", "5");
        catalogManager.getSampleManager().update(studyFqn, sampleId, new SampleUpdateParams()
                .setAnnotationSets(Collections.singletonList(new AnnotationSet("annotation1", vs1.getId(), annotations))),
                QueryOptions.empty(), sessionIdUser);
        QueryResult<Sample> sampleQueryResult = catalogManager.getSampleManager().get(studyFqn, sampleId,
                new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.ANNOTATION_SETS.key()), sessionIdUser);
        assertEquals(1, sampleQueryResult.first().getAnnotationSets().size());

        annotations.put("NUM", "6.8");
        catalogManager.getSampleManager().update(studyFqn, sampleId, new SampleUpdateParams()
                .setAnnotationSets(Collections.singletonList(new AnnotationSet("annotation2", vs1.getId(), annotations))),
                QueryOptions.empty(), sessionIdUser);
        sampleQueryResult = catalogManager.getSampleManager().get(studyFqn, sampleId,
                new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.ANNOTATION_SETS.key()), sessionIdUser);
        assertEquals(2, sampleQueryResult.first().getAnnotationSets().size());

        annotations.put("NUM", "five polong five");
        thrown.expect(CatalogException.class);
        catalogManager.getSampleManager().update(studyFqn, sampleId, new SampleUpdateParams()
                        .setAnnotationSets(Collections.singletonList(new AnnotationSet("annotation3", vs1.getId(), annotations))),
                QueryOptions.empty(), sessionIdUser);
    }

    @Test
    public void testAnnotateRange() throws CatalogException {
        String sampleId = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_1"), new QueryOptions(),
                sessionIdUser).first().getId();

        List<Variable> variables = new ArrayList<>();
        variables.add(new Variable("RANGE_NUM", "RANGE_NUM", "", Variable.VariableType.DOUBLE, "", true, false, Arrays.asList("1:14",
                "16:22", "50:"), 0, "", "", null, Collections.<String, Object>emptyMap()));
        VariableSet vs1 = catalogManager.getStudyManager().createVariableSet(studyFqn, "vs1", "vs1", false, false, "", null, variables,
                Collections.singletonList(VariableSet.AnnotableDataModels.SAMPLE), sessionIdUser).first();

        HashMap<String, Object> annotations = new HashMap<>();
        annotations.put("RANGE_NUM", "1");  // 1:14
        catalogManager.getSampleManager().update(studyFqn, sampleId, new SampleUpdateParams()
                        .setAnnotationSets(Collections.singletonList(new AnnotationSet("annotation1", vs1.getId(), annotations))),
                QueryOptions.empty(), sessionIdUser);
        QueryResult<Sample> sampleQueryResult = catalogManager.getSampleManager().get(studyFqn, sampleId,
                new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.ANNOTATION_SETS.key()), sessionIdUser);
        assertEquals(1, sampleQueryResult.first().getAnnotationSets().size());

        annotations.put("RANGE_NUM", "14"); // 1:14
        catalogManager.getSampleManager().update(studyFqn, sampleId, new SampleUpdateParams()
                .setAnnotationSets(Collections.singletonList(new AnnotationSet("annotation2", vs1.getId(), annotations))),
                QueryOptions.empty(), sessionIdUser);
        sampleQueryResult = catalogManager.getSampleManager().get(studyFqn, sampleId,
                new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.ANNOTATION_SETS.key()), sessionIdUser);
        assertEquals(2, sampleQueryResult.first().getAnnotationSets().size());

        annotations.put("RANGE_NUM", "20");  // 16:20
        catalogManager.getSampleManager().update(studyFqn, sampleId, new SampleUpdateParams()
                        .setAnnotationSets(Collections.singletonList(new AnnotationSet("annotation3", vs1.getId(), annotations))),
                QueryOptions.empty(), sessionIdUser);
        sampleQueryResult = catalogManager.getSampleManager().get(studyFqn, sampleId,
                new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.ANNOTATION_SETS.key()), sessionIdUser);
        assertEquals(3, sampleQueryResult.first().getAnnotationSets().size());

        annotations.put("RANGE_NUM", "100000"); // 50:
        catalogManager.getSampleManager().update(studyFqn, sampleId, new SampleUpdateParams()
                        .setAnnotationSets(Collections.singletonList(new AnnotationSet("annotation4", vs1.getId(), annotations))),
                QueryOptions.empty(), sessionIdUser);
        sampleQueryResult = catalogManager.getSampleManager().get(studyFqn, sampleId,
                new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.ANNOTATION_SETS.key()), sessionIdUser);
        assertEquals(4, sampleQueryResult.first().getAnnotationSets().size());

        annotations.put("RANGE_NUM", "14.1");
        thrown.expect(CatalogException.class);
        catalogManager.getSampleManager().update(studyFqn, sampleId, new SampleUpdateParams()
                        .setAnnotationSets(Collections.singletonList(new AnnotationSet("annotation5", vs1.getId(), annotations))),
                QueryOptions.empty(), sessionIdUser);
    }

    @Test
    public void testAnnotateCategorical() throws CatalogException {
        String sampleId = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_1"), new QueryOptions(),
                sessionIdUser).first().getId();

        List<Variable> variables = new ArrayList<>();
        variables.add(new Variable("COOL_NAME", "COOL_NAME", "", Variable.VariableType.CATEGORICAL, "", true, false, Arrays.asList("LUKE",
                "LEIA", "VADER", "YODA"), 0, "", "", null, Collections.<String, Object>emptyMap()));
        VariableSet vs1 = catalogManager.getStudyManager().createVariableSet(studyFqn, "vs1", "vs1", false, false, "", null, variables,
                Collections.singletonList(VariableSet.AnnotableDataModels.SAMPLE), sessionIdUser).first();

        Map<String, Object> actionMap = new HashMap<>();
        actionMap.put(AnnotationSetManager.ANNOTATION_SETS, ParamUtils.UpdateAction.ADD);
        QueryOptions options = new QueryOptions(Constants.ACTIONS, actionMap);

        HashMap<String, Object> annotations = new HashMap<>();
        annotations.put("COOL_NAME", "LUKE");
        catalogManager.getSampleManager().update(studyFqn, sampleId, new SampleUpdateParams()
                        .setAnnotationSets(Collections.singletonList(new AnnotationSet("annotation1", vs1.getId(), annotations))),
                options, sessionIdUser);
        QueryResult<Sample> sampleQueryResult = catalogManager.getSampleManager().get(studyFqn, sampleId,
                new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.ANNOTATION_SETS.key()), sessionIdUser);
        assertEquals(1, sampleQueryResult.first().getAnnotationSets().size());

        annotations.put("COOL_NAME", "LEIA");
        catalogManager.getSampleManager().update(studyFqn, sampleId, new SampleUpdateParams()
                        .setAnnotationSets(Collections.singletonList(new AnnotationSet("annotation2", vs1.getId(), annotations))),
                options, sessionIdUser);
        sampleQueryResult = catalogManager.getSampleManager().get(studyFqn, sampleId,
                new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.ANNOTATION_SETS.key()), sessionIdUser);
        assertEquals(2, sampleQueryResult.first().getAnnotationSets().size());

        annotations.put("COOL_NAME", "VADER");
        catalogManager.getSampleManager().update(studyFqn, sampleId, new SampleUpdateParams()
                        .setAnnotationSets(Collections.singletonList(new AnnotationSet("annotation3", vs1.getId(), annotations))),
                options, sessionIdUser);
        sampleQueryResult = catalogManager.getSampleManager().get(studyFqn, sampleId,
                new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.ANNOTATION_SETS.key()), sessionIdUser);
        assertEquals(3, sampleQueryResult.first().getAnnotationSets().size());

        annotations.put("COOL_NAME", "YODA");
        catalogManager.getSampleManager().update(studyFqn, sampleId, new SampleUpdateParams()
                        .setAnnotationSets(Collections.singletonList(new AnnotationSet("annotation4", vs1.getId(), annotations))),
                options, sessionIdUser);
        sampleQueryResult = catalogManager.getSampleManager().get(studyFqn, sampleId,
                new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.ANNOTATION_SETS.key()), sessionIdUser);
        assertEquals(4, sampleQueryResult.first().getAnnotationSets().size());

        annotations.put("COOL_NAME", "SPOCK");
        thrown.expect(CatalogException.class);
        catalogManager.getSampleManager().update(studyFqn, sampleId, new SampleUpdateParams()
                        .setAnnotationSets(Collections.singletonList(new AnnotationSet("annotation5", vs1.getId(), annotations))),
                options, sessionIdUser);
    }

    @Test
    public void testAnnotateNested() throws CatalogException {
        String sampleId1 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_1"),
                new QueryOptions(), sessionIdUser).first().getId();
        String sampleId2 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_2"),
                new QueryOptions(), sessionIdUser).first().getId();

        VariableSet vs1 = catalogManager.getStudyManager().createVariableSet(studyFqn, "vs1", "vs1", false, false, "", null,
                Collections.singletonList(CatalogAnnotationsValidatorTest.nestedObject),
                Collections.singletonList(VariableSet.AnnotableDataModels.SAMPLE), sessionIdUser).first();

        HashMap<String, Object> annotations = new HashMap<>();
        annotations.put("nestedObject", new ObjectMap()
                .append("stringList", Arrays.asList("li", "lu"))
                .append("object", new ObjectMap()
                        .append("string", "my value")
                        .append("numberList", Arrays.asList(2, 3, 4))));
        catalogManager.getSampleManager().update(studyFqn, sampleId1, new SampleUpdateParams()
                    .setAnnotationSets(Collections.singletonList(new AnnotationSet("annotation1", vs1.getId(), annotations))),
                QueryOptions.empty(), sessionIdUser);
        QueryResult<Sample> sampleQueryResult = catalogManager.getSampleManager().get(studyFqn, sampleId1,
                new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.ANNOTATION_SETS.key()), sessionIdUser);
        assertEquals(1, sampleQueryResult.first().getAnnotationSets().size());

        annotations.put("nestedObject", new ObjectMap()
                .append("stringList", Arrays.asList("lo", "lu"))
                .append("object", new ObjectMap()
                        .append("string", "stringValue")
                        .append("numberList", Arrays.asList(3, 4, 5))));
        catalogManager.getSampleManager().update(studyFqn, sampleId2, new SampleUpdateParams()
                        .setAnnotationSets(Collections.singletonList(new AnnotationSet("annotation1", vs1.getId(), annotations))),
                QueryOptions.empty(), sessionIdUser);
        sampleQueryResult = catalogManager.getSampleManager().get(studyFqn, sampleId2,
                new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.ANNOTATION_SETS.key()), sessionIdUser);
        assertEquals(1, sampleQueryResult.first().getAnnotationSets().size());

        List<Sample> samples;
        Query query = new Query(SampleDBAdaptor.QueryParams.ANNOTATION.key(), vs1.getId() + ":nestedObject.stringList=li");
        samples = catalogManager.getSampleManager().get(studyFqn, query, null, sessionIdUser).getResult();
        assertEquals(1, samples.size());

        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key(), vs1.getId() + ":nestedObject.stringList=lo");
        samples = catalogManager.getSampleManager().get(studyFqn, query, null, sessionIdUser).getResult();
        assertEquals(1, samples.size());

        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key(), vs1.getId() + ":nestedObject.stringList=LL");
        samples = catalogManager.getSampleManager().get(studyFqn, query, null, sessionIdUser).getResult();
        assertEquals(0, samples.size());

        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key(), vs1.getId() + ":nestedObject.stringList=lo,li,LL");
        samples = catalogManager.getSampleManager().get(studyFqn, query, null, sessionIdUser).getResult();
        assertEquals(2, samples.size());

        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key(), vs1.getId() + ":nestedObject.object.string=my value");
        samples = catalogManager.getSampleManager().get(studyFqn, query, null, sessionIdUser).getResult();
        assertEquals(1, samples.size());

        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key(), vs1.getId() + ":nestedObject.stringList=lo,lu,LL;" + vs1.getId()
                + ":nestedObject.object.string=my value");
        samples = catalogManager.getSampleManager().get(studyFqn, query, null, sessionIdUser).getResult();
        assertEquals(1, samples.size());

        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key(), vs1.getId() + ":nestedObject.stringList=lo,lu,LL;" + vs1.getId()
                + ":nestedObject.object.numberList=7");
        samples = catalogManager.getSampleManager().get(studyFqn, query, null, sessionIdUser).getResult();
        assertEquals(0, samples.size());

        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key(), vs1.getId() + ":nestedObject.stringList=lo,lu,LL;"
                + vs1.getId() + ":nestedObject.object.numberList=3");
        samples = catalogManager.getSampleManager().get(studyFqn, query, null, sessionIdUser).getResult();
        assertEquals(2, samples.size());

        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key(), vs1.getId() + ":nestedObject.stringList=lo,lu,LL;" + vs1.getId()
                + ":nestedObject.object.numberList=5;" + vs1.getId() + ":nestedObject.object.string=stringValue");
        samples = catalogManager.getSampleManager().get(studyFqn, query, null, sessionIdUser).getResult();
        assertEquals(1, samples.size());

        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key(), vs1.getId() + ":nestedObject.stringList=lo,lu,LL;" + vs1.getId()
                + ":nestedObject.object.numberList=2,5");
        samples = catalogManager.getSampleManager().get(studyFqn, query, null, sessionIdUser).getResult();
        assertEquals(2, samples.size());

        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key(), vs1.getId() + ":nestedObject.stringList=lo,lu,LL;" + vs1.getId()
                + ":nestedObject.object.numberList=0");
        samples = catalogManager.getSampleManager().get(studyFqn, query, null, sessionIdUser).getResult();
        assertEquals(0, samples.size());


        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key(), vs1.getId() + ":unexisting=lo,lu,LL");
        thrown.expect(CatalogException.class);
        thrown.expectMessage("does not exist");
        catalogManager.getSampleManager().get(studyFqn, query, null, sessionIdUser).getResult();
    }

//    @Test
//    public void testQuerySampleAnnotationFail1() throws CatalogException {
//        Query query = new Query();
//        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key() + ":nestedObject.stringList", "lo,lu,LL");
//
//        thrown.expect(CatalogDBException.class);
//        thrown.expectMessage("annotation:nestedObject does not exist");
//        QueryResult<Sample> search = catalogManager.getSampleManager().search(studyFqn, query, null, sessionIdUser);
//        catalogManager.getAllSamples(studyId, query, null, sessionIdUser).getResult();
//    }

//    @Test
//    public void testQuerySampleAnnotationFail2() throws CatalogException {
//        Query query = new Query();
//        query.put(CatalogSampleDBAdaptor.QueryParams.ANNOTATION.key(), "nestedObject.stringList:lo,lu,LL");
//
//        thrown.expect(CatalogDBException.class);
//        thrown.expectMessage("Wrong annotation query");
//        catalogManager.getAllSamples(studyId, query, null, sessionIdUser).getResult();
//    }

    @Test
    public void testGroupByAnnotations() throws Exception {
        AbstractManager.MyResourceId vs1 = catalogManager.getStudyManager().getVariableSetId("vs", studyFqn, sessionIdUser);

        QueryResult queryResult = catalogManager.getSampleManager().groupBy(studyFqn, new Query(),
                Collections.singletonList(Constants.ANNOTATION + ":" + vs1.getResourceId() + ":annot1:PHEN"), QueryOptions.empty(),
                sessionIdUser);

        assertEquals(3, queryResult.getNumResults());
        for (Document document : (List<Document>) queryResult.getResult()) {
            Document id = (Document) document.get("_id");
            List<String> value = ((ArrayList<String>) id.values().iterator().next());

            List<String> items = (List<String>) document.get("items");

            if (value.isEmpty()) {
                assertEquals(4, items.size());
                assertTrue(items.containsAll(Arrays.asList("s_6", "s_7", "s_8", "s_9")));
            } else if ("CONTROL".equals(value.get(0))) {
                assertEquals(3, items.size());
                assertTrue(items.containsAll(Arrays.asList("s_1", "s_3", "s_4")));
            } else if ("CASE".equals(value.get(0))) {
                assertEquals(2, items.size());
                assertTrue(items.containsAll(Arrays.asList("s_2", "s_5")));
            } else {
                fail("It should not get into this condition");
            }
        }
    }

    @Test
    public void testIteratorSamples() throws CatalogException {
        Query query = new Query();

        DBIterator<Sample> iterator = catalogManager.getSampleManager().iterator(studyFqn, query, null, sessionIdUser);
        int count = 0;
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }
        assertEquals(9, count);
    }

    @Test
    public void testQuerySamples() throws CatalogException {
        Study study = catalogManager.getStudyManager().get(studyFqn, QueryOptions.empty(), sessionIdUser).first();

        VariableSet variableSet = study.getVariableSets().get(0);

        List<Sample> samples;
        Query query = new Query();

        samples = catalogManager.getSampleManager().get(studyFqn, query, null, sessionIdUser).getResult();
        assertEquals(9, samples.size());

        query = new Query(ANNOTATION.key(), Constants.VARIABLE_SET + "=" + variableSet.getId());
        samples = catalogManager.getSampleManager().get(studyFqn, query, null, sessionIdUser).getResult();
        assertEquals(8, samples.size());

        query = new Query(ANNOTATION.key(), Constants.ANNOTATION_SET_NAME + "=annot2");
        samples = catalogManager.getSampleManager().get(studyFqn, query, null, sessionIdUser).getResult();
        assertEquals(3, samples.size());

        query = new Query(ANNOTATION.key(), Constants.ANNOTATION_SET_NAME + "=noExist");
        samples = catalogManager.getSampleManager().get(studyFqn, query, null, sessionIdUser).getResult();
        assertEquals(0, samples.size());

        query = new Query(ANNOTATION.key(), variableSet.getId() + ":NAME=s_1,s_2,s_3");
        samples = catalogManager.getSampleManager().get(studyFqn, query, null, sessionIdUser).getResult();
        assertEquals(3, samples.size());

        query = new Query(ANNOTATION.key(), variableSet.getId() + ":AGE>30;" + Constants.VARIABLE_SET + "=" + variableSet.getId());
        samples = catalogManager.getSampleManager().get(studyFqn, query, null, sessionIdUser).getResult();
        assertEquals(3, samples.size());

        query = new Query(ANNOTATION.key(), variableSet.getId() + ":AGE>30;" + Constants.VARIABLE_SET + "=" + variableSet.getId());
        samples = catalogManager.getSampleManager().get(studyFqn, query, null, sessionIdUser).getResult();
        assertEquals(3, samples.size());

        query = new Query(ANNOTATION.key(), variableSet.getId() + ":AGE>30;" + variableSet.getId() + ":ALIVE=true;"
                + Constants.VARIABLE_SET + "=" + variableSet.getId());
        samples = catalogManager.getSampleManager().get(studyFqn, query, null, sessionIdUser).getResult();
        assertEquals(2, samples.size());
    }

    @Test
    public void testUpdateAnnotation() throws CatalogException {
        Sample sample = catalogManager.getSampleManager().get(studyFqn, s_1, null, sessionIdUser).first();
        AnnotationSet annotationSet = sample.getAnnotationSets().get(0);

        Individual ind = new Individual()
                .setId("INDIVIDUAL_1")
                .setSex(IndividualProperty.Sex.UNKNOWN);
        ind.setAnnotationSets(Collections.singletonList(annotationSet));
        ind = catalogManager.getIndividualManager().create(studyFqn, ind, QueryOptions.empty(), sessionIdUser).first();

        // First update
        annotationSet.getAnnotations().put("NAME", "SAMPLE1");
        annotationSet.getAnnotations().put("AGE", 38);
        annotationSet.getAnnotations().put("EXTRA", "extra");
        annotationSet.getAnnotations().remove("HEIGHT");

        // Update annotation set
        catalogManager.getIndividualManager().updateAnnotations(studyFqn, ind.getId(), annotationSet.getId(),
                annotationSet.getAnnotations(), ParamUtils.CompleteUpdateAction.SET, new QueryOptions(Constants.INCREMENT_VERSION, true),
                sessionIdUser);
        catalogManager.getSampleManager().updateAnnotations(studyFqn, s_1, annotationSet.getId(), annotationSet.getAnnotations(),
                ParamUtils.CompleteUpdateAction.SET, new QueryOptions(Constants.INCREMENT_VERSION, true), sessionIdUser);

        Consumer<AnnotationSet> check = as -> {
            Map<String, Object> auxAnnotations = as.getAnnotations();

            assertEquals(5, auxAnnotations.size());
            assertEquals("SAMPLE1", auxAnnotations.get("NAME"));
            assertEquals(38, auxAnnotations.get("AGE"));
            assertEquals("extra", auxAnnotations.get("EXTRA"));
        };

        sample = catalogManager.getSampleManager().get(studyFqn, s_1, null, sessionIdUser).first();
        ind = catalogManager.getIndividualManager().get(studyFqn, ind.getId(), null, sessionIdUser).first();
        check.accept(sample.getAnnotationSets().get(0));
        check.accept(ind.getAnnotationSets().get(0));

        // Call again to the update to check that nothing changed
        catalogManager.getIndividualManager().updateAnnotations(studyFqn, ind.getId(), annotationSet.getId(),
                annotationSet.getAnnotations(), ParamUtils.CompleteUpdateAction.SET, new QueryOptions(Constants.INCREMENT_VERSION, true),
                sessionIdUser);
        check.accept(ind.getAnnotationSets().get(0));

        // Update mandatory annotation
        annotationSet.getAnnotations().put("NAME", "SAMPLE 1");
        annotationSet.getAnnotations().remove("EXTRA");

        catalogManager.getIndividualManager().updateAnnotations(studyFqn, ind.getId(), annotationSet.getId(),
                annotationSet.getAnnotations(), ParamUtils.CompleteUpdateAction.SET, new QueryOptions(Constants.INCREMENT_VERSION, true),
                sessionIdUser);
        catalogManager.getSampleManager().updateAnnotations(studyFqn, s_1, annotationSet.getId(), annotationSet.getAnnotations(),
                ParamUtils.CompleteUpdateAction.SET, new QueryOptions(Constants.INCREMENT_VERSION, true), sessionIdUser);

        check = as -> {
            Map<String, Object> auxAnnotations = as.getAnnotations();

            assertEquals(4, auxAnnotations.size());
            assertEquals("SAMPLE 1", auxAnnotations.get("NAME"));
            assertEquals(false, auxAnnotations.containsKey("EXTRA"));
        };

        sample = catalogManager.getSampleManager().get(studyFqn, s_1, null, sessionIdUser).first();
        ind = catalogManager.getIndividualManager().get(studyFqn, ind.getId(), null, sessionIdUser).first();
        check.accept(sample.getAnnotationSets().get(0));
        check.accept(ind.getAnnotationSets().get(0));

        // Update non-mandatory annotation
        annotationSet.getAnnotations().put("EXTRA", "extra");
        catalogManager.getIndividualManager().updateAnnotations(studyFqn, ind.getId(), annotationSet.getId(),
                annotationSet.getAnnotations(), ParamUtils.CompleteUpdateAction.SET, new QueryOptions(Constants.INCREMENT_VERSION, true),
                sessionIdUser);
        catalogManager.getSampleManager().updateAnnotations(studyFqn, s_1, annotationSet.getId(), annotationSet.getAnnotations(),
                ParamUtils.CompleteUpdateAction.SET, new QueryOptions(Constants.INCREMENT_VERSION, true), sessionIdUser);

        check = as -> {
            Map<String, Object> auxAnnotations = as.getAnnotations();

            assertEquals(5, auxAnnotations.size());
            assertEquals("SAMPLE 1", auxAnnotations.get("NAME"));
            assertEquals("extra", auxAnnotations.get("EXTRA"));
        };

        sample = catalogManager.getSampleManager().get(studyFqn, s_1, null, sessionIdUser).first();
        ind = catalogManager.getIndividualManager().get(studyFqn, ind.getId(), null, sessionIdUser).first();
        check.accept(sample.getAnnotationSets().get(0));
        check.accept(ind.getAnnotationSets().get(0));

        // Update non-mandatory annotation
        Map<String, Object> annotationUpdate = new ObjectMap("EXTRA", "extraa");
        // Action now is ADD, we only want to change that annotation
        catalogManager.getIndividualManager().updateAnnotations(studyFqn, ind.getId(), annotationSet.getId(), annotationUpdate,
                ParamUtils.CompleteUpdateAction.ADD, new QueryOptions(Constants.INCREMENT_VERSION, true), sessionIdUser);
        catalogManager.getSampleManager().updateAnnotations(studyFqn, s_1, annotationSet.getId(), annotationUpdate,
                ParamUtils.CompleteUpdateAction.ADD, new QueryOptions(Constants.INCREMENT_VERSION, true), sessionIdUser);

        check = as -> {
            Map<String, Object> auxAnnotations = as.getAnnotations();

            assertEquals(5, auxAnnotations.size());
            assertEquals("SAMPLE 1", auxAnnotations.get("NAME"));
            assertEquals("extraa", auxAnnotations.get("EXTRA"));
        };

        sample = catalogManager.getSampleManager().get(studyFqn, s_1, null, sessionIdUser).first();
        ind = catalogManager.getIndividualManager().get(studyFqn, ind.getId(), null, sessionIdUser).first();
        check.accept(sample.getAnnotationSets().get(0));
        check.accept(ind.getAnnotationSets().get(0));

        thrown.expect(CatalogException.class);
        thrown.expectMessage("not found");
        catalogManager.getIndividualManager().updateAnnotations(studyFqn, ind.getId(), "blabla", annotationUpdate,
                ParamUtils.CompleteUpdateAction.ADD, new QueryOptions(Constants.INCREMENT_VERSION, true), sessionIdUser);
    }

    @Test
    public void testUpdateAnnotationFail() throws CatalogException {
        Sample sample = catalogManager.getSampleManager().get(studyFqn, s_1, null, sessionIdUser).first();
        AnnotationSet annotationSet = sample.getAnnotationSets().get(0);

        thrown.expect(CatalogException.class); //Can not delete required fields
        thrown.expectMessage("required variable");
        catalogManager.getSampleManager().removeAnnotations(studyFqn, s_1, annotationSet.getId(), Collections.singletonList("NAME"),
                QueryOptions.empty(), sessionIdUser);
    }

    @Test
    public void testDeleteAnnotation() throws CatalogException {
        // We add one of the non mandatory annotations

        // First update
        catalogManager.getSampleManager().updateAnnotations(studyFqn, s_1, "annot1", new ObjectMap("EXTRA", "extra"),
                ParamUtils.CompleteUpdateAction.ADD, QueryOptions.empty(), sessionIdUser);

        Sample sample = catalogManager.getSampleManager().get(studyFqn, s_1, null, sessionIdUser).first();
        AnnotationSet annotationSet = sample.getAnnotationSets().get(0);
        assertEquals("extra", annotationSet.getAnnotations().get("EXTRA"));

        // Now we remove that non mandatory annotation
        catalogManager.getSampleManager().removeAnnotations(studyFqn, s_1, annotationSet.getId(), Collections.singletonList("EXTRA"),
                QueryOptions.empty(), sessionIdUser);

        sample = catalogManager.getSampleManager().get(studyFqn, s_1, null, sessionIdUser).first();
        annotationSet = sample.getAnnotationSets().get(0);
        assertTrue(!annotationSet.getAnnotations().containsKey("EXTRA"));

        // Now we attempt to remove one mandatory annotation
        thrown.expect(CatalogException.class); //Can not delete required fields
        thrown.expectMessage("required variable");
        catalogManager.getSampleManager().removeAnnotations(studyFqn, s_1, annotationSet.getId(), Collections.singletonList("AGE"),
                QueryOptions.empty(), sessionIdUser);
    }

    @Test
    public void testDeleteAnnotationSet() throws CatalogException {
        catalogManager.getSampleManager().removeAnnotationSet(studyFqn, s_1, "annot1", QueryOptions.empty(), sessionIdUser);

        QueryResult<Sample> sampleQueryResult = catalogManager.getSampleManager().get(studyFqn, s_1,
                new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.ANNOTATION_SETS.key()), sessionIdUser);
        assertEquals(0, sampleQueryResult.first().getAnnotationSets().size());
    }

    @Test
    public void getVariableSetSummary() throws CatalogException {
        Study study = catalogManager.getStudyManager().get(studyFqn, null, sessionIdUser).first();

        long variableSetId = study.getVariableSets().get(0).getUid();

        QueryResult<VariableSetSummary> variableSetSummary = catalogManager.getStudyManager()
                .getVariableSetSummary(studyFqn, Long.toString(variableSetId), sessionIdUser);

        assertEquals(1, variableSetSummary.getNumResults());
        VariableSetSummary summary = variableSetSummary.first();

        assertEquals(5, summary.getSamples().size());

        // PHEN
        int i;
        for (i = 0; i < summary.getSamples().size(); i++) {
            if ("PHEN".equals(summary.getSamples().get(i).getName())) {
                break;
            }
        }
        List<FeatureCount> annotations = summary.getSamples().get(i).getAnnotations();
        assertEquals("PHEN", summary.getSamples().get(i).getName());
        assertEquals(2, annotations.size());

        for (i = 0; i < annotations.size(); i++) {
            if ("CONTROL".equals(annotations.get(i).getName())) {
                break;
            }
        }
        assertEquals("CONTROL", annotations.get(i).getName());
        assertEquals(5, annotations.get(i).getCount());

        for (i = 0; i < annotations.size(); i++) {
            if ("CASE".equals(annotations.get(i).getName())) {
                break;
            }
        }
        assertEquals("CASE", annotations.get(i).getName());
        assertEquals(3, annotations.get(i).getCount());

    }

    @Test
    public void testModifySample() throws CatalogException {
        String sampleId1 = catalogManager.getSampleManager()
                .create(studyFqn, new Sample().setId("SAMPLE_1"), new QueryOptions(), sessionIdUser).first().getId();
        String individualId = catalogManager.getIndividualManager().create(studyFqn, new Individual().setId("Individual1"),
                new QueryOptions(), sessionIdUser).first().getId();

        Sample sample = catalogManager.getSampleManager()
                .update(studyFqn, sampleId1, new SampleUpdateParams().setIndividualId(individualId), null, sessionIdUser).first();

        assertEquals(individualId, sample.getIndividualId());
    }

    @Test
    public void testGetSampleAndIndividualWithPermissionsChecked() throws CatalogException {
        String sampleId1 = catalogManager.getSampleManager()
                .create(studyFqn, new Sample().setId("SAMPLE_1"), new QueryOptions(), sessionIdUser).first().getId();
        String individualId = catalogManager.getIndividualManager().create(studyFqn, new Individual().setId("Individual1"),
                new QueryOptions(), sessionIdUser).first().getId();

        Sample sample = catalogManager.getSampleManager()
                .update(studyFqn, sampleId1, new SampleUpdateParams().setIndividualId(individualId), null, sessionIdUser).first();

        assertEquals(individualId, sample.getIndividualId());

        catalogManager.getSampleManager().updateAcl(studyFqn, Collections.singletonList("SAMPLE_1"), "user2",
                new Sample.SampleAclParams(SampleAclEntry.SamplePermissions.VIEW.name(), AclParams.Action.SET, null, null, null),
                sessionIdUser);

        sample = catalogManager.getSampleManager().get(studyFqn, "SAMPLE_1", new QueryOptions("lazy", false), sessionIdUser2).first();
        assertEquals(null, sample.getAttributes().get("individual"));

        catalogManager.getSampleManager().updateAcl(studyFqn, Collections.singletonList("SAMPLE_1"), "user2",
                new Sample.SampleAclParams(SampleAclEntry.SamplePermissions.VIEW.name(), AclParams.Action.SET, null, null, null, true),
                sessionIdUser);
        sample = catalogManager.getSampleManager().get(studyFqn, "SAMPLE_1", new QueryOptions("lazy", false), sessionIdUser2).first();
        assertEquals(individualId, ((Individual) sample.getAttributes().get("OPENCGA_INDIVIDUAL")).getId());
        assertEquals(sampleId1, sample.getId());

        sample = catalogManager.getSampleManager().get(studyFqn, new Query("individual", "Individual1"), new QueryOptions("lazy", false),
                sessionIdUser2).first();
        assertEquals(individualId, ((Individual) sample.getAttributes().get("OPENCGA_INDIVIDUAL")).getId());
        assertEquals(sampleId1, sample.getId());

    }

    @Test
    public void searchSamplesByIndividual() throws CatalogException {
        catalogManager.getIndividualManager().create(studyFqn, new Individual().setId("Individual1")
                .setSamples(Arrays.asList(new Sample().setId("sample1"), new Sample().setId("sample2"))), new QueryOptions(), sessionIdUser);

        QueryResult<Sample> sampleQueryResult = catalogManager.getSampleManager().search(studyFqn,
                new Query(SampleDBAdaptor.QueryParams.INDIVIDUAL.key(), "Individual1"), QueryOptions.empty(), sessionIdUser);

        assertEquals(2, sampleQueryResult.getNumResults());

        sampleQueryResult = catalogManager.getSampleManager().search(studyFqn,
                new Query().append(SampleDBAdaptor.QueryParams.INDIVIDUAL.key(), "Individual1")
                        .append(SampleDBAdaptor.QueryParams.ID.key(), "sample1"), QueryOptions.empty(), sessionIdUser);
        assertEquals(1, sampleQueryResult.getNumResults());

        catalogManager.getIndividualManager().create(studyFqn, new Individual().setId("Individual2"), new QueryOptions(), sessionIdUser);
        sampleQueryResult = catalogManager.getSampleManager().search(studyFqn,
                new Query().append(SampleDBAdaptor.QueryParams.INDIVIDUAL.key(), "Individual2"), QueryOptions.empty(), sessionIdUser);
        assertEquals(0, sampleQueryResult.getNumResults());
    }

    @Test
    public void searchSamplesDifferentVersions() throws CatalogException {
        catalogManager.getSampleManager().create(studyFqn, new Sample().setId("sample1"), QueryOptions.empty(), sessionIdUser);
        catalogManager.getSampleManager().create(studyFqn, new Sample().setId("sample2"), QueryOptions.empty(), sessionIdUser);
        catalogManager.getSampleManager().create(studyFqn, new Sample().setId("sample3"), QueryOptions.empty(), sessionIdUser);

        // Generate 4 versions of sample1
        catalogManager.getSampleManager().update(studyFqn, "sample1", new SampleUpdateParams(),
                new QueryOptions(Constants.INCREMENT_VERSION, true), sessionIdUser);
        catalogManager.getSampleManager().update(studyFqn, "sample1", new SampleUpdateParams(),
                new QueryOptions(Constants.INCREMENT_VERSION, true), sessionIdUser);
        catalogManager.getSampleManager().update(studyFqn, "sample1", new SampleUpdateParams(),
                new QueryOptions(Constants.INCREMENT_VERSION, true), sessionIdUser);

        // Generate 3 versions of sample2
        catalogManager.getSampleManager().update(studyFqn, "sample2", new SampleUpdateParams(),
                new QueryOptions(Constants.INCREMENT_VERSION, true), sessionIdUser);
        catalogManager.getSampleManager().update(studyFqn, "sample2", new SampleUpdateParams(),
                new QueryOptions(Constants.INCREMENT_VERSION, true), sessionIdUser);

        // Generate 1 versions of sample3
        catalogManager.getSampleManager().update(studyFqn, "sample3", new SampleUpdateParams(),
                new QueryOptions(Constants.INCREMENT_VERSION, true), sessionIdUser);

        Query query = new Query()
                .append(SampleDBAdaptor.QueryParams.ID.key(), "sample1,sample2,sample3")
                .append(SampleDBAdaptor.QueryParams.VERSION.key(), "3,2,1");
        QueryResult<Sample> sampleQueryResult = catalogManager.getSampleManager().get(studyFqn, query, QueryOptions.empty(), sessionIdUser);
        assertEquals(3, sampleQueryResult.getNumResults());
        for (Sample sample : sampleQueryResult.getResult()) {
            switch (sample.getId()) {
                case "sample1":
                    assertEquals(3, sample.getVersion());
                    break;
                case "sample2":
                    assertEquals(2, sample.getVersion());
                    break;
                case "sample3":
                    assertEquals(1, sample.getVersion());
                    break;
                default:
                    fail("One of the three samples above should always be present");
            }
        }

        query.put(SampleDBAdaptor.QueryParams.VERSION.key(), "2");
        sampleQueryResult = catalogManager.getSampleManager().get(studyFqn, query, QueryOptions.empty(), sessionIdUser);
        assertEquals(3, sampleQueryResult.getNumResults());
        sampleQueryResult.getResult().forEach(
                s -> assertEquals(2, s.getVersion())
        );

        query.put(SampleDBAdaptor.QueryParams.VERSION.key(), "1,2");
        thrown.expect(CatalogException.class);
        thrown.expectMessage("size of the array");
        catalogManager.getSampleManager().get(studyFqn, query, QueryOptions.empty(), sessionIdUser);
    }

    @Test
    public void getSharedProject() throws CatalogException, IOException {
        catalogManager.getUserManager().create("dummy", "dummy", "asd@asd.asd", "dummy", "", 50000L,
                Account.Type.GUEST, QueryOptions.empty(), null);
        catalogManager.getStudyManager().updateGroup(studyFqn, "@members", new GroupParams("dummy",
                GroupParams.Action.ADD), sessionIdUser);

        String token = catalogManager.getUserManager().login("dummy", "dummy");
        QueryResult<Project> queryResult = catalogManager.getProjectManager().getSharedProjects("dummy", QueryOptions.empty(), token);
        assertEquals(1, queryResult.getNumResults());

        catalogManager.getStudyManager().updateGroup(studyFqn, "@members", new GroupParams("*", GroupParams.Action.ADD),
                sessionIdUser);
        queryResult = catalogManager.getProjectManager().getSharedProjects("*", QueryOptions.empty(), null);
        assertEquals(1, queryResult.getNumResults());
    }

    @Test
    public void smartResolutorStudyAliasFromAnonymousUser() throws CatalogException {
        catalogManager.getStudyManager().updateGroup(studyFqn, "@members", new GroupParams("*", GroupParams.Action.ADD),
                sessionIdUser);
        Study study = catalogManager.getStudyManager().resolveId(studyFqn, "*");
        assertTrue(study != null);
    }

    @Test
    public void testCreateSampleWithIndividual() throws CatalogException {
        String individualId = catalogManager.getIndividualManager().create(studyFqn, new Individual().setId("Individual1"),
                new QueryOptions(), sessionIdUser).first().getId();
        String sampleId1 = catalogManager.getSampleManager().create(studyFqn, new Sample()
                        .setId("SAMPLE_1")
                        .setIndividualId(individualId),
                new QueryOptions(), sessionIdUser).first().getId();

        QueryResult<Individual> individualQueryResult = catalogManager.getIndividualManager().get(studyFqn, individualId,
                QueryOptions.empty(), sessionIdUser);
        assertEquals(sampleId1, individualQueryResult.first().getSamples().get(0).getId());

        // Create sample linking to individual based on the individual name
        String sampleId2 = catalogManager.getSampleManager().create(studyFqn, new Sample()
                        .setId("SAMPLE_2")
                        .setIndividualId("Individual1"),
                new QueryOptions(), sessionIdUser).first().getId();

        individualQueryResult = catalogManager.getIndividualManager().get(studyFqn, individualId, QueryOptions.empty(), sessionIdUser);
        assertEquals(2, individualQueryResult.first().getSamples().size());
        assertTrue(individualQueryResult.first().getSamples().stream().map(Sample::getId).collect(Collectors.toSet()).containsAll(
                Arrays.asList(sampleId1, sampleId2)
        ));
    }

    @Test
    public void testModifySampleBadIndividual() throws CatalogException {
        String sampleId1 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_1"), new QueryOptions(),
                sessionIdUser).first().getId();

        thrown.expect(CatalogException.class);
        thrown.expectMessage("not found");
        catalogManager.getSampleManager().update(studyFqn, sampleId1, new SampleUpdateParams().setIndividualId("ind"), null, sessionIdUser);
    }

    @Test
    public void testDeleteSample() throws CatalogException {
        long sampleUid = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_1"), new QueryOptions(),
                sessionIdUser).first().getUid();

        Query query = new Query(SampleDBAdaptor.QueryParams.ID.key(), "SAMPLE_1");
        WriteResult delete = catalogManager.getSampleManager().delete("1000G:phase1", query, null, sessionIdUser);
        assertEquals(1, delete.getNumUpdated());

        query = new Query()
                .append(SampleDBAdaptor.QueryParams.UID.key(), sampleUid)
                .append(SampleDBAdaptor.QueryParams.STATUS_NAME.key(), Status.DELETED);

        QueryResult<Sample> sampleQueryResult = catalogManager.getSampleManager().get("1000G:phase1", query, new QueryOptions(),
                sessionIdUser);
//        QueryResult<Sample> sample = catalogManager.getSample(sampleId, new QueryOptions(), sessionIdUser);
        assertEquals(1, sampleQueryResult.getNumResults());
        assertTrue(sampleQueryResult.first().getId().contains("DELETED"));
    }

    @Test
    public void testAssignPermissionsWithPropagationAndNoIndividual() throws CatalogException {
        Sample sample = new Sample().setId("sample");
        catalogManager.getSampleManager().create(studyFqn, sample, QueryOptions.empty(), sessionIdUser);

        List<QueryResult<SampleAclEntry>> queryResults = catalogManager.getSampleManager().updateAcl(studyFqn,
                Arrays.asList("sample"), "user2", new Sample.SampleAclParams("VIEW", AclParams.Action.SET, null, null, null, true),
                sessionIdUser);
        assertEquals(1, queryResults.size());
        assertEquals(1, queryResults.get(0).getNumResults());
        assertEquals(1, queryResults.get(0).first().getPermissions().size());
        assertTrue(queryResults.get(0).first().getPermissions().contains(SampleAclEntry.SamplePermissions.VIEW));
    }

    // Two samples, one related to one individual and the other does not have any individual associated
    @Test
    public void testAssignPermissionsWithPropagationWithIndividualAndNoIndividual() throws CatalogException {
        Individual individual = new Individual().setId("individual").setSamples(Collections.singletonList(new Sample().setId("sample")));
        catalogManager.getIndividualManager().create(studyFqn, individual, QueryOptions.empty(), sessionIdUser);

        Sample sample2 = new Sample().setId("sample2");
        catalogManager.getSampleManager().create(studyFqn, sample2, QueryOptions.empty(), sessionIdUser);

        List<QueryResult<SampleAclEntry>> queryResults = catalogManager.getSampleManager().updateAcl(studyFqn,
                Arrays.asList("sample", "sample2"), "user2", new Sample.SampleAclParams("VIEW", AclParams.Action.SET, null, null, null,
                        true), sessionIdUser);
        assertEquals(2, queryResults.size());
        assertEquals(1, queryResults.get(0).getNumResults());
        assertEquals(1, queryResults.get(0).first().getPermissions().size());
        assertTrue(queryResults.get(0).first().getPermissions().contains(SampleAclEntry.SamplePermissions.VIEW));
        assertTrue(queryResults.get(1).first().getPermissions().contains(SampleAclEntry.SamplePermissions.VIEW));

        List<QueryResult<IndividualAclEntry>> individualAcl = catalogManager.getIndividualManager().getAcls(studyFqn,
                Collections.singletonList("individual"), "user2", false, sessionIdUser);
        assertEquals(1, individualAcl.size());
        assertEquals(1, individualAcl.get(0).getNumResults());
        assertEquals(1, individualAcl.get(0).first().getPermissions().size());
        assertTrue(individualAcl.get(0).first().getPermissions().contains(IndividualAclEntry.IndividualPermissions.VIEW));
    }

}