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

package org.opencb.opencga.catalog.managers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.bson.Document;
import org.junit.Test;
import org.opencb.biodata.formats.sequence.fastqc.FastQc;
import org.opencb.biodata.formats.sequence.fastqc.Summary;
import org.opencb.biodata.models.clinical.Disorder;
import org.opencb.biodata.models.clinical.qc.SampleQcVariantStats;
import org.opencb.biodata.models.pedigree.IndividualProperty;
import org.opencb.biodata.models.variant.metadata.SampleVariantStats;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.IndividualDBAdaptor;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.CatalogAnnotationsValidatorTest;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysisUpdateParams;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.common.CustomStatus;
import org.opencb.opencga.core.models.common.CustomStatusParams;
import org.opencb.opencga.core.models.common.Status;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.individual.IndividualAclEntry;
import org.opencb.opencga.core.models.individual.IndividualUpdateParams;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.sample.*;
import org.opencb.opencga.core.models.study.*;
import org.opencb.opencga.core.models.summaries.FeatureCount;
import org.opencb.opencga.core.models.summaries.VariableSetSummary;
import org.opencb.opencga.core.models.user.Account;
import org.opencb.opencga.core.response.OpenCGAResult;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static org.opencb.opencga.catalog.db.api.SampleDBAdaptor.QueryParams.ANNOTATION;
import static org.opencb.opencga.catalog.utils.ParamUtils.AclAction.SET;
import static org.opencb.opencga.core.api.ParamConstants.SAMPLE_INCLUDE_INDIVIDUAL_PARAM;

public class SampleManagerTest extends AbstractManagerTest {

    @Test
    public void testSampleVersioning() throws CatalogException {
        Query query = new Query(ProjectDBAdaptor.QueryParams.USER_ID.key(), "user");
        String projectId = catalogManager.getProjectManager().get(query, null, token).first().getId();

        catalogManager.getSampleManager().create(studyFqn,
                new Sample().setId("testSample").setDescription("description"), null, token);
        catalogManager.getSampleManager().update(studyFqn, "testSample", new SampleUpdateParams(),
                new QueryOptions(Constants.INCREMENT_VERSION, true), token);
        catalogManager.getSampleManager().update(studyFqn, "testSample", new SampleUpdateParams(),
                new QueryOptions(Constants.INCREMENT_VERSION, true), token);

        catalogManager.getProjectManager().incrementRelease(projectId, token);
        // We create something to have a gap in the release
        catalogManager.getSampleManager().create(studyFqn, new Sample().setId("dummy"), null, token);

        catalogManager.getProjectManager().incrementRelease(projectId, token);
        catalogManager.getSampleManager().update(studyFqn, "testSample", new SampleUpdateParams(),
                new QueryOptions(Constants.INCREMENT_VERSION, true), token);

        catalogManager.getSampleManager().update(studyFqn, "testSample",
                new SampleUpdateParams().setDescription("new description"), null, token);

        // We want the whole history of the sample
        query = new Query()
                .append(SampleDBAdaptor.QueryParams.ID.key(), "testSample")
                .append(Constants.ALL_VERSIONS, true);
        DataResult<Sample> sampleDataResult = catalogManager.getSampleManager().search(studyFqn, query, null, token);
        assertEquals(4, sampleDataResult.getNumResults());
        assertEquals("description", sampleDataResult.getResults().get(0).getDescription());
        assertEquals("description", sampleDataResult.getResults().get(1).getDescription());
        assertEquals("description", sampleDataResult.getResults().get(2).getDescription());
        assertEquals("new description", sampleDataResult.getResults().get(3).getDescription());

        query = new Query()
                .append(SampleDBAdaptor.QueryParams.VERSION.key(), "all");
        sampleDataResult = catalogManager.getSampleManager().get(studyFqn, Collections.singletonList("testSample"),
                query, null, false, token);
        assertEquals(4, sampleDataResult.getNumResults());
        assertEquals("description", sampleDataResult.getResults().get(0).getDescription());
        assertEquals("description", sampleDataResult.getResults().get(1).getDescription());
        assertEquals("description", sampleDataResult.getResults().get(2).getDescription());
        assertEquals("new description", sampleDataResult.getResults().get(3).getDescription());

        // We want the last version of release 1
        query = new Query()
                .append(SampleDBAdaptor.QueryParams.ID.key(), "testSample")
                .append(SampleDBAdaptor.QueryParams.SNAPSHOT.key(), 1);
        sampleDataResult = catalogManager.getSampleManager().search(studyFqn, query, null, token);
        assertEquals(1, sampleDataResult.getNumResults());
        assertEquals(3, sampleDataResult.first().getVersion());

        // We want the last version of release 2 (must be the same of release 1)
        query = new Query()
                .append(SampleDBAdaptor.QueryParams.ID.key(), "testSample")
                .append(SampleDBAdaptor.QueryParams.SNAPSHOT.key(), 2);
        sampleDataResult = catalogManager.getSampleManager().search(studyFqn, query, null, token);
        assertEquals(1, sampleDataResult.getNumResults());
        assertEquals(3, sampleDataResult.first().getVersion());

        // We want the last version of the sample
        query = new Query()
                .append(SampleDBAdaptor.QueryParams.ID.key(), "testSample");
        sampleDataResult = catalogManager.getSampleManager().search(studyFqn, query, null, token);
        assertEquals(1, sampleDataResult.getNumResults());
        assertEquals(4, sampleDataResult.first().getVersion());

        // We want the version 2 of the sample
        query = new Query()
                .append(SampleDBAdaptor.QueryParams.ID.key(), "testSample")
                .append(SampleDBAdaptor.QueryParams.VERSION.key(), 2);
        sampleDataResult = catalogManager.getSampleManager().search(studyFqn, query, null, token);
        assertEquals(1, sampleDataResult.getNumResults());
        assertEquals(2, sampleDataResult.first().getVersion());

        // We want the version 1 of the sample
        query = new Query()
                .append(SampleDBAdaptor.QueryParams.ID.key(), "testSample")
                .append(SampleDBAdaptor.QueryParams.VERSION.key(), 1);
        sampleDataResult = catalogManager.getSampleManager().search(studyFqn, query, null, token);
        assertEquals(1, sampleDataResult.getNumResults());
        assertEquals(1, sampleDataResult.first().getVersion());

        DataResult<Sample> testSample = catalogManager.getSampleManager()
                .get(studyFqn, Collections.singletonList("testSample"), new Query(Constants.ALL_VERSIONS, true), null, false, token);
        assertEquals(4, testSample.getResults().size());
    }

    @Test
    public void searchByInternalAnnotationSetTest() throws CatalogException {
        Set<Variable> variables = new HashSet<>();
        variables.add(new Variable().setId("a").setType(Variable.VariableType.STRING));
        variables.add(new Variable().setId("b").setType(Variable.VariableType.MAP_INTEGER).setAllowedKeys(Arrays.asList("b1", "b2")));
        VariableSet variableSet = new VariableSet("myInternalVset", "", false, false, true, "", variables, null, 1, null);
        catalogManager.getStudyManager().createVariableSet(studyFqn, variableSet, token);

        Map<String, Object> annotations = new HashMap<>();
        annotations.put("a", "hello");
        annotations.put("b", new ObjectMap("b1", 2).append("b2", 3));
        AnnotationSet annotationSet = new AnnotationSet("annSet", variableSet.getId(), annotations);

        annotations = new HashMap<>();
        annotations.put("a", "bye");
        annotations.put("b", new ObjectMap("b1", 4).append("b2", 5));
        AnnotationSet annotationSet2 = new AnnotationSet("annSet2", variableSet.getId(), annotations);

        Sample sample = new Sample()
                .setId("sample")
                .setAnnotationSets(Arrays.asList(annotationSet, annotationSet2));
        Sample sampleResult = catalogManager.getSampleManager().create(studyFqn, sample, QueryOptions.empty(), token).first();
        for (AnnotationSet aSet : sampleResult.getAnnotationSets()) {
            assertNotEquals(variableSet.getId(), aSet.getVariableSetId());
        }

        // Create a different sample with different annotations
        annotations = new HashMap<>();
        annotations.put("a", "hi");
        annotations.put("b", new ObjectMap("b1", 12).append("b2", 13));
        annotationSet = new AnnotationSet("annSet", variableSet.getId(), annotations);

        annotations = new HashMap<>();
        annotations.put("a", "goodbye");
        annotations.put("b", new ObjectMap("b1", 14).append("b2", 15));
        annotationSet2 = new AnnotationSet("annSet2", variableSet.getId(), annotations);

        Sample sample2 = new Sample()
                .setId("sample2")
                .setAnnotationSets(Arrays.asList(annotationSet, annotationSet2));
        sampleResult = catalogManager.getSampleManager().create(studyFqn, sample2, QueryOptions.empty(), token).first();
        for (AnnotationSet aSet : sampleResult.getAnnotationSets()) {
            assertNotEquals(variableSet.getId(), aSet.getVariableSetId());
        }

        // Query by one of the annotations
        Query query = new Query(Constants.ANNOTATION, "myInternalVset:a=hello");
        assertEquals(1, catalogManager.getSampleManager().count(studyFqn, query, token).getNumMatches());
        assertEquals("sample", catalogManager.getSampleManager().search(studyFqn, query, SampleManager.INCLUDE_SAMPLE_IDS, token).first().getId());

        query = new Query(Constants.ANNOTATION, "myInternalVset:b.b1=4");
        assertEquals(1, catalogManager.getSampleManager().count(studyFqn, query, token).getNumMatches());
        assertEquals("sample", catalogManager.getSampleManager().search(studyFqn, query, SampleManager.INCLUDE_SAMPLE_IDS, token).first().getId());

        query = new Query(Constants.ANNOTATION, "b.b1=14");
        assertEquals(1, catalogManager.getSampleManager().count(studyFqn, query, token).getNumMatches());
        assertEquals("sample2", catalogManager.getSampleManager().search(studyFqn, query, SampleManager.INCLUDE_SAMPLE_IDS, token).first().getId());

        query = new Query(Constants.ANNOTATION, "a=goodbye");
        assertEquals(1, catalogManager.getSampleManager().count(studyFqn, query, token).getNumMatches());
        assertEquals("sample2", catalogManager.getSampleManager().search(studyFqn, query, SampleManager.INCLUDE_SAMPLE_IDS, token).first().getId());

        // Update sample annotation to be exactly the same as sample2
        ObjectMap action = new ObjectMap(SampleDBAdaptor.QueryParams.ANNOTATION_SETS.key(), ParamUtils.BasicUpdateAction.SET);
        QueryOptions options = new QueryOptions(Constants.ACTIONS, action);
        catalogManager.getSampleManager().update(studyFqn, sample.getId(),
                new SampleUpdateParams().setAnnotationSets(Arrays.asList(annotationSet, annotationSet2)), options, token);

        query = new Query(Constants.ANNOTATION, "myInternalVset:a=hello");
        assertEquals(0, catalogManager.getSampleManager().count(studyFqn, query, token).getNumMatches());

        query = new Query(Constants.ANNOTATION, "myInternalVset:b.b1=4");
        assertEquals(0, catalogManager.getSampleManager().count(studyFqn, query, token).getNumMatches());

        query = new Query(Constants.ANNOTATION, "b.b1=14");
        assertEquals(2, catalogManager.getSampleManager().count(studyFqn, query, token).getNumMatches());
        assertTrue(Arrays.asList("sample", "sample2")
                .containsAll(catalogManager.getSampleManager().search(studyFqn, query, SampleManager.INCLUDE_SAMPLE_IDS, token)
                        .getResults().stream().map(Sample::getId).collect(Collectors.toList())));

        query = new Query(Constants.ANNOTATION, "a=goodbye");
        assertEquals(2, catalogManager.getSampleManager().count(studyFqn, query, token).getNumMatches());
        assertTrue(Arrays.asList("sample", "sample2")
                .containsAll(catalogManager.getSampleManager().search(studyFqn, query, SampleManager.INCLUDE_SAMPLE_IDS, token)
                        .getResults().stream().map(Sample::getId).collect(Collectors.toList())));
    }

    @Test
    public void updateQualityControlTest1() throws CatalogException {
        Sample sample = new Sample().setId("sample");
        catalogManager.getSampleManager().create(studyFqn, sample, QueryOptions.empty(), token);

        List<SampleQcVariantStats> sampleQcVariantStats = new ArrayList<>();
        SampleVariantStats sampleVariantStats = new SampleVariantStats();
        sampleVariantStats.setVariantCount(20);
        sampleVariantStats.setTiTvRatio((float) 13.2);
        sampleQcVariantStats.add(new SampleQcVariantStats("v1", "", null, sampleVariantStats));

        sampleVariantStats = new SampleVariantStats();
        sampleVariantStats.setVariantCount(10);
        sampleVariantStats.setTiTvRatio((float) 15.2);
        sampleQcVariantStats.add(new SampleQcVariantStats("v2", "", null, sampleVariantStats));

        SampleVariantQualityControlMetrics metrics = new SampleVariantQualityControlMetrics(sampleQcVariantStats, null, null);
        SampleQualityControl qualityControl = new SampleQualityControl(null, null, null, metrics);

        OpenCGAResult<Sample> result = catalogManager.getSampleManager().update(studyFqn, "sample",
                new SampleUpdateParams().setQualityControl(qualityControl), QueryOptions.empty(), token);
        assertEquals(1, result.getNumUpdated());

        Query query = new Query()
                .append(SampleDBAdaptor.STATS_ID, "v1")
                .append(SampleDBAdaptor.STATS_VARIANT_COUNT, 20);
        assertEquals(1, catalogManager.getSampleManager().count(studyFqn, query, token).getNumMatches());

        query = new Query()
                .append(SampleDBAdaptor.STATS_ID, "v2")
                .append(SampleDBAdaptor.STATS_VARIANT_COUNT, 10);
        assertEquals(1, catalogManager.getSampleManager().count(studyFqn, query, token).getNumMatches());

        query = new Query()
                .append(SampleDBAdaptor.STATS_ID, "v1")
                .append(SampleDBAdaptor.STATS_VARIANT_COUNT, 15);
        assertEquals(0, catalogManager.getSampleManager().count(studyFqn, query, token).getNumMatches());

        query = new Query()
                .append(SampleDBAdaptor.STATS_ID, "v1")
                .append("stats.tiTvRatio", 13.2);
        assertEquals(1, catalogManager.getSampleManager().count(studyFqn, query, token).getNumMatches());

        query = new Query()
                .append(SampleDBAdaptor.STATS_ID, "v2")
                .append("stats.tiTvRatio", 15.2);
        assertEquals(1, catalogManager.getSampleManager().count(studyFqn, query, token).getNumMatches());

        query = new Query()
                .append(SampleDBAdaptor.STATS_ID, "v2")
                .append("stats.tiTvRatio", 3.5);
        assertEquals(0, catalogManager.getSampleManager().count(studyFqn, query, token).getNumMatches());

        // Change values
        sampleQcVariantStats = new ArrayList<>();
        sampleVariantStats = new SampleVariantStats();
        sampleVariantStats.setVariantCount(15);
        sampleVariantStats.setTiTvRatio((float) 3.5);
        sampleQcVariantStats.add(new SampleQcVariantStats("v1", "", null, sampleVariantStats));
        metrics = new SampleVariantQualityControlMetrics(sampleQcVariantStats, null, null);
        qualityControl = new SampleQualityControl(null, null, null, metrics);

        // And update sample
        result = catalogManager.getSampleManager().update(studyFqn, "sample", new SampleUpdateParams().setQualityControl(qualityControl),
                QueryOptions.empty(), token);
        assertEquals(1, result.getNumUpdated());

        // Check same values as before but the results should be now different
        query = new Query()
                .append(SampleDBAdaptor.STATS_ID, "v1")
                .append(SampleDBAdaptor.STATS_VARIANT_COUNT, 20);
        assertEquals(0, catalogManager.getSampleManager().count(studyFqn, query, token).getNumMatches());

        query = new Query()
                .append(SampleDBAdaptor.STATS_ID, "v2")
                .append(SampleDBAdaptor.STATS_VARIANT_COUNT, 10);
        assertEquals(0, catalogManager.getSampleManager().count(studyFqn, query, token).getNumMatches());

        query = new Query()
                .append(SampleDBAdaptor.STATS_ID, "v1")
                .append(SampleDBAdaptor.STATS_VARIANT_COUNT, 15);
        assertEquals(1, catalogManager.getSampleManager().count(studyFqn, query, token).getNumMatches());

        query = new Query()
                .append(SampleDBAdaptor.STATS_ID, "v1")
                .append("stats.tiTvRatio", 13.2);
        assertEquals(0, catalogManager.getSampleManager().count(studyFqn, query, token).getNumMatches());

        query = new Query()
                .append(SampleDBAdaptor.STATS_ID, "v1")
                .append("stats.tiTvRatio", 15.2);
        assertEquals(0, catalogManager.getSampleManager().count(studyFqn, query, token).getNumMatches());

        query = new Query()
                .append(SampleDBAdaptor.STATS_ID, "v1")
                .append("stats.tiTvRatio", 3.5);
        assertEquals(1, catalogManager.getSampleManager().count(studyFqn, query, token).getNumMatches());

        // Update any other sample field to validate it doesn't affect quality control
        result = catalogManager.getSampleManager().update(studyFqn, "sample", new SampleUpdateParams().setDescription("my description"),
                QueryOptions.empty(), token);
        assertEquals(1, result.getNumUpdated());

        // Check same values as before but the results should be now different
        query = new Query()
                .append(SampleDBAdaptor.STATS_ID, "v1")
                .append(SampleDBAdaptor.STATS_VARIANT_COUNT, 20);
        assertEquals(0, catalogManager.getSampleManager().count(studyFqn, query, token).getNumMatches());

        query = new Query()
                .append(SampleDBAdaptor.STATS_ID, "v2")
                .append(SampleDBAdaptor.STATS_VARIANT_COUNT, 10);
        assertEquals(0, catalogManager.getSampleManager().count(studyFqn, query, token).getNumMatches());

        query = new Query()
                .append(SampleDBAdaptor.STATS_ID, "v1")
                .append(SampleDBAdaptor.STATS_VARIANT_COUNT, 15);
        assertEquals(1, catalogManager.getSampleManager().count(studyFqn, query, token).getNumMatches());

        query = new Query()
                .append(SampleDBAdaptor.STATS_ID, "v1")
                .append("stats.tiTvRatio", 13.2);
        assertEquals(0, catalogManager.getSampleManager().count(studyFqn, query, token).getNumMatches());

        query = new Query()
                .append(SampleDBAdaptor.STATS_ID, "v1")
                .append("stats.tiTvRatio", 15.2);
        assertEquals(0, catalogManager.getSampleManager().count(studyFqn, query, token).getNumMatches());

        query = new Query()
                .append(SampleDBAdaptor.STATS_ID, "v1")
                .append("stats.tiTvRatio", 3.5);
        assertEquals(1, catalogManager.getSampleManager().count(studyFqn, query, token).getNumMatches());

        // Remove SampleQcVariantStats values
        qualityControl = new SampleQualityControl(Arrays.asList("file1", "file2"), null, null, null);

        // And update sample
        result = catalogManager.getSampleManager().update(studyFqn, "sample", new SampleUpdateParams().setQualityControl(qualityControl),
                QueryOptions.empty(), token);
        assertEquals(1, result.getNumUpdated());

        // None of the previous queries should give any result
        query = new Query()
                .append(SampleDBAdaptor.STATS_ID, "v1")
                .append(SampleDBAdaptor.STATS_VARIANT_COUNT, 20);
        assertEquals(0, catalogManager.getSampleManager().count(studyFqn, query, token).getNumMatches());

        query = new Query()
                .append(SampleDBAdaptor.STATS_ID, "v2")
                .append(SampleDBAdaptor.STATS_VARIANT_COUNT, 10);
        assertEquals(0, catalogManager.getSampleManager().count(studyFqn, query, token).getNumMatches());

        query = new Query()
                .append(SampleDBAdaptor.STATS_ID, "v1")
                .append(SampleDBAdaptor.STATS_VARIANT_COUNT, 15);
        assertEquals(0, catalogManager.getSampleManager().count(studyFqn, query, token).getNumMatches());

        query = new Query()
                .append(SampleDBAdaptor.STATS_ID, "v1")
                .append("stats.tiTvRatio", 13.2);
        assertEquals(0, catalogManager.getSampleManager().count(studyFqn, query, token).getNumMatches());

        query = new Query()
                .append(SampleDBAdaptor.STATS_ID, "v1")
                .append("stats.tiTvRatio", 15.2);
        assertEquals(0, catalogManager.getSampleManager().count(studyFqn, query, token).getNumMatches());

        query = new Query()
                .append(SampleDBAdaptor.STATS_ID, "v1")
                .append("stats.tiTvRatio", 3.5);
        assertEquals(0, catalogManager.getSampleManager().count(studyFqn, query, token).getNumMatches());
    }

    @Test
    public void updateQualityControlTest2() throws CatalogException {
        Sample sample = new Sample().setId("sample");
        catalogManager.getSampleManager().create(studyFqn, sample, QueryOptions.empty(), token);

        List<SampleQcVariantStats> sampleQcVariantStats = new ArrayList<>();
        SampleVariantStats sampleVariantStats = new SampleVariantStats();
        sampleVariantStats.setVariantCount(20);
        sampleVariantStats.setTiTvRatio((float) 13.2);
        sampleQcVariantStats.add(new SampleQcVariantStats("v1", "", null, sampleVariantStats));

        sampleVariantStats = new SampleVariantStats();
        sampleVariantStats.setVariantCount(10);
        sampleVariantStats.setTiTvRatio((float) 15.2);
        sampleQcVariantStats.add(new SampleQcVariantStats("v2", "", null, sampleVariantStats));

        SampleVariantQualityControlMetrics metrics = new SampleVariantQualityControlMetrics(sampleQcVariantStats, null, null);
        SampleQualityControl qualityControl = new SampleQualityControl(null, null, null, metrics);

        OpenCGAResult<Sample> result = catalogManager.getSampleManager().update(studyFqn, "sample",
                new SampleUpdateParams().setQualityControl(qualityControl), QueryOptions.empty(), token);
        assertEquals(1, result.getNumUpdated());

        Query query = new Query(Constants.ANNOTATION, "opencga_sample_variant_stats__v1@opencga_sample_variant_stats:variantCount=20");
        assertEquals(1, catalogManager.getSampleManager().count(studyFqn, query, token).getNumMatches());

        query = new Query(Constants.ANNOTATION, "opencga_sample_variant_stats__v2@opencga_sample_variant_stats:variantCount=20");
        assertEquals(0, catalogManager.getSampleManager().count(studyFqn, query, token).getNumMatches());

        query = new Query(Constants.ANNOTATION, "opencga_sample_variant_stats__v2@opencga_sample_variant_stats:variantCount=10");
        assertEquals(1, catalogManager.getSampleManager().count(studyFqn, query, token).getNumMatches());

        query = new Query(Constants.ANNOTATION, "opencga_sample_variant_stats:variantCount=15");
        assertEquals(0, catalogManager.getSampleManager().count(studyFqn, query, token).getNumMatches());

        query = new Query(Constants.ANNOTATION, "opencga_sample_variant_stats:tiTvRatio=13.2");
        assertEquals(1, catalogManager.getSampleManager().count(studyFqn, query, token).getNumMatches());

        query = new Query(Constants.ANNOTATION, "opencga_sample_variant_stats:tiTvRatio=15.2");
        assertEquals(1, catalogManager.getSampleManager().count(studyFqn, query, token).getNumMatches());

        query = new Query(Constants.ANNOTATION, "opencga_sample_variant_stats:tiTvRatio=3.5");
        assertEquals(0, catalogManager.getSampleManager().count(studyFqn, query, token).getNumMatches());

        // Change values
        sampleQcVariantStats = new ArrayList<>();
        sampleVariantStats = new SampleVariantStats();
        sampleVariantStats.setVariantCount(15);
        sampleVariantStats.setTiTvRatio((float) 3.5);
        sampleQcVariantStats.add(new SampleQcVariantStats("v1", "", null, sampleVariantStats));
        metrics = new SampleVariantQualityControlMetrics(sampleQcVariantStats, null, null);
        qualityControl = new SampleQualityControl(null, null, null, metrics);

        // And update sample
        result = catalogManager.getSampleManager().update(studyFqn, "sample", new SampleUpdateParams().setQualityControl(qualityControl), QueryOptions.empty(), token);
        assertEquals(1, result.getNumUpdated());

        // Check same values as before but the results should be now different
        query = new Query(Constants.ANNOTATION, "opencga_sample_variant_stats:variantCount=20");
        assertEquals(0, catalogManager.getSampleManager().count(studyFqn, query, token).getNumMatches());

        query = new Query(Constants.ANNOTATION, "opencga_sample_variant_stats:variantCount=10");
        assertEquals(0, catalogManager.getSampleManager().count(studyFqn, query, token).getNumMatches());

        query = new Query(Constants.ANNOTATION, "opencga_sample_variant_stats:variantCount=15");
        assertEquals(1, catalogManager.getSampleManager().count(studyFqn, query, token).getNumMatches());

        query = new Query(Constants.ANNOTATION, "opencga_sample_variant_stats:tiTvRatio=13.2");
        assertEquals(0, catalogManager.getSampleManager().count(studyFqn, query, token).getNumMatches());

        query = new Query(Constants.ANNOTATION, "opencga_sample_variant_stats:tiTvRatio=15.2");
        assertEquals(0, catalogManager.getSampleManager().count(studyFqn, query, token).getNumMatches());

        query = new Query(Constants.ANNOTATION, "opencga_sample_variant_stats:tiTvRatio=3.5");
        assertEquals(1, catalogManager.getSampleManager().count(studyFqn, query, token).getNumMatches());
    }

    @Test
    public void distinctTest() throws CatalogException {
        OpenCGAResult<?> distinct = catalogManager.getSampleManager().distinct(studyFqn, SampleDBAdaptor.QueryParams.ID.key(), null, token);
        assertEquals(String.class.getName(), distinct.getResultType());
        assertEquals(9, distinct.getNumResults());
        assertEquals(9, distinct.getResults().size());

        distinct = catalogManager.getSampleManager().distinct(studyFqn, SampleDBAdaptor.QueryParams.UID.key(), null, token);
        assertEquals(Long.class.getName(), distinct.getResultType());
        assertEquals(9, distinct.getNumResults());
        assertEquals(9, distinct.getResults().size());

        distinct = catalogManager.getSampleManager().distinct(studyFqn, SampleDBAdaptor.QueryParams.SOMATIC.key(), null, token);
        assertEquals(Boolean.class.getName(), distinct.getResultType());
        assertEquals(1, distinct.getNumResults());
        assertEquals(1, distinct.getResults().size());
    }

    @Test
    public void updateProcessingField() throws CatalogException {
        catalogManager.getSampleManager().create(studyFqn,
                new Sample().setId("testSample").setDescription("description"), null, token);

        SampleProcessing processing = new SampleProcessing("product", "preparationMethod", "extractionMethod", "labSampleId", "quantity",
                "date", Collections.emptyMap());
        catalogManager.getSampleManager().update(studyFqn, "testSample",
                new SampleUpdateParams().setProcessing(processing), new QueryOptions(Constants.INCREMENT_VERSION, true), token);

        DataResult<Sample> testSample = catalogManager.getSampleManager().get(studyFqn, "testSample", new QueryOptions(), token);
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
                new Sample().setId("testSample").setDescription("description"), null, token);

        SampleCollection collection = new SampleCollection("tissue", "organ", "quantity", "method", "date", Collections.emptyMap());
        CustomStatusParams statusParams = new CustomStatusParams("status1", "my description");
        catalogManager.getSampleManager().update(studyFqn, "testSample",
                new SampleUpdateParams().setCollection(collection).setStatus(statusParams),
                new QueryOptions(Constants.INCREMENT_VERSION, true), token);

        DataResult<Sample> testSample = catalogManager.getSampleManager().get(studyFqn, "testSample", new QueryOptions(), token);
        assertEquals("tissue", testSample.first().getCollection().getTissue());
        assertEquals("organ", testSample.first().getCollection().getOrgan());
        assertEquals("quantity", testSample.first().getCollection().getQuantity());
        assertEquals("method", testSample.first().getCollection().getMethod());
        assertEquals("date", testSample.first().getCollection().getDate());
        assertEquals("status1", testSample.first().getStatus().getName());
        assertEquals("my description", testSample.first().getStatus().getDescription());
        assertNotNull(testSample.first().getStatus().getDate());
        assertTrue(testSample.first().getCollection().getAttributes().isEmpty());
    }

    @Test
    public void updateQualityControlField() throws CatalogException {
        catalogManager.getSampleManager().create(studyFqn,
                new Sample().setId("testSample").setDescription("description"), null, token);

        SampleQualityControl qualityControl = new SampleQualityControl();

        SampleAlignmentQualityControlMetrics metrics = new SampleAlignmentQualityControlMetrics();
        metrics.setFastQc(new FastQc().setSummary(new Summary("basicStatistics", "perBaseSeqQuality", "perTileSeqQuality",
                "perSeqQualityScores", "perBaseSeqContent", "perSeqGcContent", "perBaseNContent", "seqLengthDistribution",
                "seqDuplicationLevels", "overrepresentedSeqs", "adapterContent", "kmerContent")));

        qualityControl.getAlignmentMetrics().add(metrics);

        catalogManager.getSampleManager().update(studyFqn, "testSample", new SampleUpdateParams().setQualityControl(qualityControl),
                new QueryOptions(Constants.INCREMENT_VERSION, true), token);

        DataResult<Sample> testSample = catalogManager.getSampleManager().get(studyFqn, "testSample", new QueryOptions(), token);
        assertEquals("basicStatistics", testSample.first().getQualityControl().getAlignmentMetrics().get(0).getFastQc().getSummary().getBasicStatistics());
        assertEquals("perBaseSeqQuality", testSample.first().getQualityControl().getAlignmentMetrics().get(0).getFastQc().getSummary().getPerBaseSeqQuality());
        assertEquals("perTileSeqQuality", testSample.first().getQualityControl().getAlignmentMetrics().get(0).getFastQc().getSummary().getPerTileSeqQuality());
        assertEquals("perSeqQualityScores", testSample.first().getQualityControl().getAlignmentMetrics().get(0).getFastQc().getSummary().getPerSeqQualityScores());
        assertEquals("perBaseSeqContent", testSample.first().getQualityControl().getAlignmentMetrics().get(0).getFastQc().getSummary().getPerBaseSeqContent());
    }

    @Test
    public void testCreateSample() throws CatalogException {
        String time = TimeUtils.getTime();

        DataResult<Sample> sampleDataResult = catalogManager.getSampleManager().create(studyFqn,
                new Sample()
                        .setId("HG007")
                        .setStatus(new CustomStatus("stat1", "my description", time)),
                null, token);
        assertEquals(1, sampleDataResult.getNumResults());
        assertEquals("stat1", sampleDataResult.first().getStatus().getName());
        assertEquals(time, sampleDataResult.first().getStatus().getDate());
        assertEquals("my description", sampleDataResult.first().getStatus().getDescription());
    }

//    @Test
//    public void testUpdateSampleStats() throws CatalogException {
//        catalogManager.getSampleManager().create(studyFqn, new Sample().setId("HG007"), null, sessionIdUser);
//        DataResult<Sample> update = catalogManager.getSampleManager().update(studyFqn, "HG007",
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
    public void testUpdateWithLockedClinicalAnalysis() throws CatalogException {
        Sample sample = new Sample().setId("sample1");
        catalogManager.getSampleManager().create(studyFqn, sample, QueryOptions.empty(), token);

        sample = new Sample().setId("sample2");
        catalogManager.getSampleManager().create(studyFqn, sample, QueryOptions.empty(), token);

        sample = new Sample().setId("sample3");
        catalogManager.getSampleManager().create(studyFqn, sample, QueryOptions.empty(), token);

        sample = new Sample().setId("sample4");
        catalogManager.getSampleManager().create(studyFqn, sample, QueryOptions.empty(), token);

        Individual individual = new Individual()
                .setId("proband")
                .setDisorders(Collections.singletonList(new Disorder().setId("disorder")));
        catalogManager.getIndividualManager().create(studyFqn, individual, Arrays.asList("sample1", "sample2"), QueryOptions.empty(), token);

        individual = new Individual().setId("father");
        catalogManager.getIndividualManager().create(studyFqn, individual, Arrays.asList("sample3", "sample4"), QueryOptions.empty(), token);

        Family family = new Family().setId("family");
        catalogManager.getFamilyManager().create(studyFqn, family, Arrays.asList("proband", "father"), QueryOptions.empty(), token);

        family.setMembers(Arrays.asList(
                new Individual().setId("proband").setSamples(Collections.singletonList(new Sample().setId("sample2"))),
                new Individual().setId("father").setSamples(Collections.singletonList(new Sample().setId("sample3")))
        ));

        ClinicalAnalysis clinicalAnalysis = new ClinicalAnalysis()
                .setId("clinical")
                .setProband(new Individual().setId("proband"))
                .setFamily(family)
                .setType(ClinicalAnalysis.Type.FAMILY);
        catalogManager.getClinicalAnalysisManager().create(studyFqn, clinicalAnalysis, QueryOptions.empty(), token);

        // We will create another clinical analysis with the same information. In this test, we will not lock clinical2
        clinicalAnalysis = new ClinicalAnalysis()
                .setId("clinical2")
                .setProband(new Individual().setId("proband"))
                .setFamily(family)
                .setType(ClinicalAnalysis.Type.FAMILY);
        catalogManager.getClinicalAnalysisManager().create(studyFqn, clinicalAnalysis, QueryOptions.empty(), token);

        // Update sample1 from proband not used in Clinical Analysis
        catalogManager.getSampleManager().update(studyFqn, "sample1", new SampleUpdateParams(),
                new QueryOptions(Constants.INCREMENT_VERSION, true), token);

        Sample sampleResult = catalogManager.getSampleManager().get(studyFqn, "sample1", QueryOptions.empty(), token).first();
        assertEquals(2, sampleResult.getVersion());

        Individual individualResult = catalogManager.getIndividualManager().get(studyFqn, "proband", QueryOptions.empty(), token).first();
        assertEquals(2, individualResult.getVersion());
        assertEquals(2, individualResult.getSamples().size());
        assertEquals(2, individualResult.getSamples().get(0).getVersion());
        assertEquals(1, individualResult.getSamples().get(1).getVersion());

        Family familyResult = catalogManager.getFamilyManager().get(studyFqn, "family", QueryOptions.empty(), token).first();
        assertEquals(2, familyResult.getVersion());
        assertEquals(2, familyResult.getMembers().size());
        assertEquals(2, familyResult.getMembers().get(0).getVersion());
        assertEquals(1, familyResult.getMembers().get(1).getVersion());

        ClinicalAnalysis clinicalResult = catalogManager.getClinicalAnalysisManager().get(studyFqn, "clinical", QueryOptions.empty(), token).first();
        assertEquals(2, clinicalResult.getProband().getVersion());
        assertEquals(1, clinicalResult.getProband().getSamples().get(0).getVersion());  // sample1 version
        assertEquals(2, clinicalResult.getFamily().getVersion());
        assertEquals(2, clinicalResult.getFamily().getMembers().get(0).getVersion());   // proband version
        assertEquals(1, clinicalResult.getFamily().getMembers().get(1).getVersion());   // father version

        clinicalResult = catalogManager.getClinicalAnalysisManager().get(studyFqn, "clinical2", QueryOptions.empty(), token).first();
        assertEquals(2, clinicalResult.getProband().getVersion());
        assertEquals(1, clinicalResult.getProband().getSamples().get(0).getVersion());  // sample1 version
        assertEquals(2, clinicalResult.getFamily().getVersion());
        assertEquals(2, clinicalResult.getFamily().getMembers().get(0).getVersion());   // proband version
        assertEquals(1, clinicalResult.getFamily().getMembers().get(1).getVersion());   // father version

        // Update sample used in Clinical Analysis from father
        catalogManager.getSampleManager().update(studyFqn, "sample3", new SampleUpdateParams(),
                new QueryOptions(Constants.INCREMENT_VERSION, true), token);

        sampleResult = catalogManager.getSampleManager().get(studyFqn, "sample3", QueryOptions.empty(), token).first();
        assertEquals(2, sampleResult.getVersion());

        individualResult = catalogManager.getIndividualManager().get(studyFqn, "father", QueryOptions.empty(), token).first();
        assertEquals(2, individualResult.getVersion());
        assertEquals(2, individualResult.getSamples().size());
        assertEquals(2, individualResult.getSamples().get(0).getVersion());
        assertEquals(1, individualResult.getSamples().get(1).getVersion());

        familyResult = catalogManager.getFamilyManager().get(studyFqn, "family", QueryOptions.empty(), token).first();
        assertEquals(3, familyResult.getVersion());
        assertEquals(2, familyResult.getMembers().size());
        assertEquals(2, familyResult.getMembers().get(0).getVersion());
        assertEquals(2, familyResult.getMembers().get(1).getVersion());

        clinicalResult = catalogManager.getClinicalAnalysisManager().get(studyFqn, "clinical", QueryOptions.empty(), token).first();
        assertEquals(2, clinicalResult.getProband().getVersion());
        assertEquals(1, clinicalResult.getProband().getSamples().get(0).getVersion());  // sample1 version
        assertEquals(3, clinicalResult.getFamily().getVersion());
        assertEquals(2, clinicalResult.getFamily().getMembers().get(0).getVersion());   // proband version
        assertEquals(2, clinicalResult.getFamily().getMembers().get(1).getVersion());   // father version
        assertEquals(2, clinicalResult.getFamily().getMembers().get(1).getSamples().get(0).getVersion());   // father sample3 version

        clinicalResult = catalogManager.getClinicalAnalysisManager().get(studyFqn, "clinical2", QueryOptions.empty(), token).first();
        assertEquals(2, clinicalResult.getProband().getVersion());
        assertEquals(1, clinicalResult.getProband().getSamples().get(0).getVersion());  // sample1 version
        assertEquals(3, clinicalResult.getFamily().getVersion());
        assertEquals(2, clinicalResult.getFamily().getMembers().get(0).getVersion());   // proband version
        assertEquals(2, clinicalResult.getFamily().getMembers().get(1).getVersion());   // father version
        assertEquals(2, clinicalResult.getFamily().getMembers().get(1).getSamples().get(0).getVersion());   // father sample3 version

        // LOCK CLINICAL ANALYSIS
        catalogManager.getClinicalAnalysisManager().update(studyFqn, "clinical", new ClinicalAnalysisUpdateParams().setLocked(true),
                QueryOptions.empty(), token);
        clinicalResult = catalogManager.getClinicalAnalysisManager().get(studyFqn, "clinical", QueryOptions.empty(), token).first();
        assertTrue(clinicalResult.isLocked());

        SampleUpdateParams updateParams = new SampleUpdateParams().setDescription("something so it doesn't complain because there "
                + "is nothing to be updated");
//        try {
//            catalogManager.getSampleManager().update(studyFqn, "sample1", updateParams, QueryOptions.empty(), token);
//            fail("Although sample1 is not directly in use in ClinicalAnalysis, we should not be able to update information that would "
//                    + "affect an individual or family from a locked clinical analysis unless the version is incremented");
//        } catch (CatalogException e) {
//            // Check nothing changed
//            checkNothingChanged("sample1");
//        }

        try {
            catalogManager.getSampleManager().update(studyFqn, "sample2", updateParams, QueryOptions.empty(), token);
            fail("We should not be able to update information that is in use in a locked clinical analysis unless the version is incremented");
        } catch (CatalogException e) {
            // Check nothing changed
            checkNothingChanged("sample2", 1);
        }

        try {
            catalogManager.getSampleManager().update(studyFqn, "sample3", updateParams, QueryOptions.empty(), token);
            fail("We should not be able to update information that is in use in a locked clinical analysis unless the version is incremented");
        } catch (CatalogException e) {
            // Check nothing changed
            checkNothingChanged("sample3", 2);
        }

//        try {
//            catalogManager.getSampleManager().update(studyFqn, "sample4", updateParams, QueryOptions.empty(), token);
//            fail("Although sample4 is not directly in use in ClinicalAnalysis, we should not be able to update information that would "
//                    + "affect an individual or family from a locked clinical analysis unless the version is incremented");
//        } catch (CatalogException e) {
//            // Check nothing changed
//            checkNothingChanged("sample4");
//        }

        // Update sample 2 from proband
        catalogManager.getSampleManager().update(studyFqn, "sample2", new SampleUpdateParams(),
                new QueryOptions(Constants.INCREMENT_VERSION, true), token);

        sampleResult = catalogManager.getSampleManager().get(studyFqn, "sample2", QueryOptions.empty(), token).first();
        assertEquals(2, sampleResult.getVersion());

        individualResult = catalogManager.getIndividualManager().get(studyFqn, "proband", QueryOptions.empty(), token).first();
        assertEquals(3, individualResult.getVersion());
        assertEquals(2, individualResult.getSamples().size());
        assertEquals(2, individualResult.getSamples().get(0).getVersion());
        assertEquals(2, individualResult.getSamples().get(1).getVersion());

        familyResult = catalogManager.getFamilyManager().get(studyFqn, "family", QueryOptions.empty(), token).first();
        assertEquals(4, familyResult.getVersion());
        assertEquals(2, familyResult.getMembers().size());
        assertEquals(3, familyResult.getMembers().get(0).getVersion());
        assertEquals(2, familyResult.getMembers().get(1).getVersion());

        clinicalResult = catalogManager.getClinicalAnalysisManager().get(studyFqn, "clinical", QueryOptions.empty(), token).first();
        assertEquals(2, clinicalResult.getProband().getVersion());
        assertEquals(1, clinicalResult.getProband().getSamples().get(0).getVersion());  // sample2 version
        assertEquals(3, clinicalResult.getFamily().getVersion());
        assertEquals(2, clinicalResult.getFamily().getMembers().get(0).getVersion());   // proband version
        assertEquals(2, clinicalResult.getFamily().getMembers().get(1).getVersion());   // father version
        assertEquals(1, clinicalResult.getFamily().getMembers().get(0).getSamples().get(0).getVersion());   // proband sample2 version

        clinicalResult = catalogManager.getClinicalAnalysisManager().get(studyFqn, "clinical2", QueryOptions.empty(), token).first();
        assertEquals(3, clinicalResult.getProband().getVersion());
        assertEquals(2, clinicalResult.getProband().getSamples().get(0).getVersion());  // sample2 version
        assertEquals(4, clinicalResult.getFamily().getVersion());
        assertEquals(3, clinicalResult.getFamily().getMembers().get(0).getVersion());   // proband version
        assertEquals(2, clinicalResult.getFamily().getMembers().get(1).getVersion());   // father version
        assertEquals(2, clinicalResult.getFamily().getMembers().get(0).getSamples().get(0).getVersion());   // proband sample2 version


        // Update sample3 from father
        catalogManager.getSampleManager().update(studyFqn, "sample3", new SampleUpdateParams(),
                new QueryOptions(Constants.INCREMENT_VERSION, true), token);

        sampleResult = catalogManager.getSampleManager().get(studyFqn, "sample3", QueryOptions.empty(), token).first();
        assertEquals(3, sampleResult.getVersion());

        individualResult = catalogManager.getIndividualManager().get(studyFqn, "proband", QueryOptions.empty(), token).first();
        assertEquals(3, individualResult.getVersion());
        assertEquals(2, individualResult.getSamples().size());
        assertEquals(2, individualResult.getSamples().get(0).getVersion());
        assertEquals(2, individualResult.getSamples().get(1).getVersion());

        familyResult = catalogManager.getFamilyManager().get(studyFqn, "family", QueryOptions.empty(), token).first();
        assertEquals(5, familyResult.getVersion());
        assertEquals(2, familyResult.getMembers().size());
        assertEquals(3, familyResult.getMembers().get(0).getVersion());
        assertEquals(3, familyResult.getMembers().get(1).getVersion());

        clinicalResult = catalogManager.getClinicalAnalysisManager().get(studyFqn, "clinical", QueryOptions.empty(), token).first();
        assertEquals(2, clinicalResult.getProband().getVersion());
        assertEquals(1, clinicalResult.getProband().getSamples().get(0).getVersion());  // sample2 version
        assertEquals(3, clinicalResult.getFamily().getVersion());
        assertEquals(2, clinicalResult.getFamily().getMembers().get(0).getVersion());   // proband version
        assertEquals(2, clinicalResult.getFamily().getMembers().get(1).getVersion());   // father version
        assertEquals(1, clinicalResult.getFamily().getMembers().get(0).getSamples().get(0).getVersion());   // proband sample2 version
        assertEquals(2, clinicalResult.getFamily().getMembers().get(1).getSamples().get(0).getVersion());   // father sample3 version

        clinicalResult = catalogManager.getClinicalAnalysisManager().get(studyFqn, "clinical2", QueryOptions.empty(), token).first();
        assertEquals(3, clinicalResult.getProband().getVersion());
        assertEquals(2, clinicalResult.getProband().getSamples().get(0).getVersion());  // sample2 version
        assertEquals(5, clinicalResult.getFamily().getVersion());
        assertEquals(3, clinicalResult.getFamily().getMembers().get(0).getVersion());   // proband version
        assertEquals(3, clinicalResult.getFamily().getMembers().get(1).getVersion());   // father version
        assertEquals(2, clinicalResult.getFamily().getMembers().get(0).getSamples().get(0).getVersion());   // proband sample2 version
        assertEquals(3, clinicalResult.getFamily().getMembers().get(1).getSamples().get(0).getVersion());   // father sample3 version
    }

    void checkNothingChanged(String sampleId, int sampleVersion) throws CatalogException {
        Sample sampleResult = catalogManager.getSampleManager().get(studyFqn, sampleId, QueryOptions.empty(), token).first();
        assertEquals(sampleVersion, sampleResult.getVersion());

        Individual individualResult = catalogManager.getIndividualManager().get(studyFqn, "proband", QueryOptions.empty(), token).first();
        assertEquals(2, individualResult.getVersion());
        assertEquals(2, individualResult.getSamples().size());
        assertEquals(2, individualResult.getSamples().get(0).getVersion());
        assertEquals(1, individualResult.getSamples().get(1).getVersion());

        Family familyResult = catalogManager.getFamilyManager().get(studyFqn, "family", QueryOptions.empty(), token).first();
        assertEquals(3, familyResult.getVersion());
        assertEquals(2, familyResult.getMembers().size());
        assertEquals(2, familyResult.getMembers().get(0).getVersion());
        assertEquals(2, familyResult.getMembers().get(1).getVersion());

        ClinicalAnalysis clinicalResult = catalogManager.getClinicalAnalysisManager().get(studyFqn, "clinical", QueryOptions.empty(), token).first();
        assertEquals(2, clinicalResult.getProband().getVersion());
        assertEquals(1, clinicalResult.getProband().getSamples().get(0).getVersion());  // sample1 version
        assertEquals(3, clinicalResult.getFamily().getVersion());
        assertEquals(2, clinicalResult.getFamily().getMembers().get(0).getVersion());   // proband version
        assertEquals(2, clinicalResult.getFamily().getMembers().get(1).getVersion());   // father version
        assertEquals(2, clinicalResult.getFamily().getMembers().get(1).getSamples().get(0).getVersion());   // father sample3 version

        clinicalResult = catalogManager.getClinicalAnalysisManager().get(studyFqn, "clinical2", QueryOptions.empty(), token).first();
        assertEquals(2, clinicalResult.getProband().getVersion());
        assertEquals(1, clinicalResult.getProband().getSamples().get(0).getVersion());  // sample1 version
        assertEquals(3, clinicalResult.getFamily().getVersion());
        assertEquals(2, clinicalResult.getFamily().getMembers().get(0).getVersion());   // proband version
        assertEquals(2, clinicalResult.getFamily().getMembers().get(1).getVersion());   // father version
        assertEquals(2, clinicalResult.getFamily().getMembers().get(1).getSamples().get(0).getVersion());   // father sample3 version
    }

    @Test
    public void testCreateSampleWithDotInName() throws CatalogException {
        String name = "HG007.sample";
        DataResult<Sample> sampleDataResult = catalogManager.getSampleManager().create(studyFqn, new Sample().setId(name), null,
                token);
        assertEquals(name, sampleDataResult.first().getId());
    }

    @Test
    public void testAnnotate() throws CatalogException {
        List<Variable> variables = new ArrayList<>();
        variables.add(new Variable("NAME", "", Variable.VariableType.STRING, "", true, false, Collections.emptyList(), null, 0, "", "",
                null, Collections.emptyMap()));
        variables.add(new Variable("AGE", "", Variable.VariableType.INTEGER, "", false, false, Collections.emptyList(), null, 0, "", "",
                null, Collections.emptyMap()));
        variables.add(new Variable("HEIGHT", "", Variable.VariableType.DOUBLE, "", false, false, Collections.emptyList(), null, 0, "",
                "", null, Collections.emptyMap()));
        variables.add(new Variable("MAP", "", Variable.VariableType.OBJECT, new HashMap<>(), false, false, Collections.emptyList(), null, 0, "", "", null,
                Collections.emptyMap()));
        VariableSet vs1 = catalogManager.getStudyManager().createVariableSet(studyFqn, "vs1", "vs1", false, false, "", null, variables,
                Collections.singletonList(VariableSet.AnnotableDataModels.SAMPLE), token).first();

        HashMap<String, Object> annotations = new HashMap<>();
        annotations.put("NAME", "Joe");
        annotations.put("AGE", 25);
        annotations.put("HEIGHT", 180);
        annotations.put("MAP", new ObjectMap("unknownKey1", "value1").append("unknownKey2", 42));

        catalogManager.getSampleManager().update(studyFqn, s_1, new SampleUpdateParams()
                        .setAnnotationSets(Collections.singletonList(new AnnotationSet("annotation1", vs1.getId(), annotations))),
                QueryOptions.empty(), token);

        DataResult<Sample> sampleDataResult = catalogManager.getSampleManager().get(studyFqn, s_1,
                new QueryOptions(QueryOptions.INCLUDE, Constants.ANNOTATION_SET_NAME + ".annotation1"), token);
        assertEquals(1, sampleDataResult.getNumResults());
        assertEquals(1, sampleDataResult.first().getAnnotationSets().size());

//        DataResult<AnnotationSet> annotationSetDataResult = catalogManager.getSampleManager().getAnnotationSet(s_1,
//                studyFqn, "annotation1", sessionIdUser);
//        assertEquals(1, annotationSetQueryResult.getNumResults());
        Map<String, Object> map = sampleDataResult.first().getAnnotationSets().get(0).getAnnotations();
        assertEquals(4, map.size());

        assertEquals("Joe", map.get("NAME"));
        assertEquals(25, map.get("AGE"));
        assertEquals(180.0, map.get("HEIGHT"));
        assertEquals(2, ((Map) map.get("MAP")).size());
        assertEquals("value1", ((Map) map.get("MAP")).get("unknownKey1"));
        assertEquals(42, ((Map) map.get("MAP")).get("unknownKey2"));
    }

    @Test
    public void testDynamicAnnotationsCreation() throws CatalogException, IOException {
        List<Variable> variables = new ArrayList<>();
        variables.add(new Variable("a", "a", "", Variable.VariableType.MAP_STRING, null, true, false, null, null, 0, "", "",
                Collections.emptySet(), Collections.emptyMap()));
        variables.add(new Variable("a1", "a1", "", Variable.VariableType.OBJECT, null, true, true, null, null, 0, "", "",
                new HashSet<>(Arrays.asList(
                        new Variable("b", "b", "", Variable.VariableType.MAP_STRING, null, true, false, null, null, 0, "", "",
                                Collections.emptySet(), Collections.emptyMap()))),
                Collections.emptyMap()));
        variables.add(new Variable("a2", "a2", "", Variable.VariableType.OBJECT, null, true, true, null, null, 0, "", "",
                new HashSet<>(Arrays.asList(
                        new Variable("b", "b", "", Variable.VariableType.OBJECT, null, true, false, null, null, 0, "", "",
                                new HashSet<>(Arrays.asList(
                                        new Variable("c", "c", "", Variable.VariableType.MAP_STRING, null, true, true, null, null, 0, "", "",
                                                Collections.emptySet(), Collections.emptyMap()))),
                                Collections.emptyMap()))),
                Collections.emptyMap()));
        variables.add(new Variable("a3", "a3", "", Variable.VariableType.OBJECT, null, true, true, null, null, 0, "", "",
                new HashSet<>(Arrays.asList(
                        new Variable("b", "b", "", Variable.VariableType.OBJECT, null, true, true, null, null, 0, "", "",
                                new HashSet<>(Arrays.asList(
                                        new Variable("c", "c", "", Variable.VariableType.MAP_STRING, null, true, false, null, null, 0, "", "",
                                                Collections.emptySet(), Collections.emptyMap()))),
                                Collections.emptyMap()))),
                Collections.emptyMap()));
        VariableSet vs1 = catalogManager.getStudyManager().createVariableSet(studyFqn, "vs1", "vs1", false, false, "", null, variables,
                Collections.singletonList(VariableSet.AnnotableDataModels.SAMPLE), token).first();

        InputStream inputStream = this.getClass().getClassLoader().getResource("annotation_sets/complete_annotation.json").openStream();
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectMap annotations = objectMapper.readValue(inputStream, ObjectMap.class);

        catalogManager.getSampleManager().update(studyFqn, s_1, new SampleUpdateParams()
                        .setAnnotationSets(Collections.singletonList(new AnnotationSet("annotation1", vs1.getId(), annotations))),
                QueryOptions.empty(), token);

        DataResult<Sample> sampleDataResult = catalogManager.getSampleManager().get(studyFqn, s_1,
                new QueryOptions(QueryOptions.INCLUDE, Constants.ANNOTATION_SET_NAME + ".annotation1"), token);

        assertEquals(objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(annotations),
                objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(sampleDataResult.first().getAnnotationSets().get(0).getAnnotations()));

        inputStream = this.getClass().getClassLoader().getResource("annotation_sets/incomplete_annotation.json").openStream();
        objectMapper = new ObjectMapper();
        annotations = objectMapper.readValue(inputStream, ObjectMap.class);

        thrown.expect(CatalogException.class);
        thrown.expectMessage("Missing required variable");
        catalogManager.getSampleManager().update(studyFqn, s_2, new SampleUpdateParams()
                        .setAnnotationSets(Collections.singletonList(new AnnotationSet("annotation1", vs1.getId(), annotations))),
                QueryOptions.empty(), token);
    }

    @Test
    public void testWrongAnnotation() throws CatalogException {
        List<Variable> variables = new ArrayList<>();
        variables.add(new Variable("a", "a", "", Variable.VariableType.MAP_INTEGER, null, true, false, null, null, 0, "", "",
                Collections.emptySet(), Collections.emptyMap()));
        variables.add(new Variable("b", "b", "", Variable.VariableType.STRING, null, true, false, null, null, 0, "", "",
                Collections.emptySet(), Collections.emptyMap()));
        VariableSet vs1 = catalogManager.getStudyManager().createVariableSet(studyFqn, "vs1", "vs1", false, false, "", null, variables,
                Collections.singletonList(VariableSet.AnnotableDataModels.SAMPLE), token).first();

        ObjectMap annotation = new ObjectMap()
                .append("a", 5)
                .append("b", "my_string");

        try {
            catalogManager.getSampleManager().update(studyFqn, s_1, new SampleUpdateParams()
                            .setAnnotationSets(Collections.singletonList(new AnnotationSet("annotation1", vs1.getId(), annotation))),
                    QueryOptions.empty(), token);
            fail("Annotation '5' should not be valid for variable MAP_INTEGER");
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains("does not seem appropriate for variable"));
        }

        annotation.put("a", new ObjectMap("b", "test"));
        try {
            catalogManager.getSampleManager().update(studyFqn, s_1, new SampleUpdateParams()
                            .setAnnotationSets(Collections.singletonList(new AnnotationSet("annotation1", vs1.getId(), annotation))),
                    QueryOptions.empty(), token);
            fail("Annotation 'a.b' should not be INTEGER instead of STRING");
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains("Expected an integer map"));
        }

        annotation.put("a", new ObjectMap("b", "46"));
        try {
            catalogManager.getSampleManager().update(studyFqn, s_1, new SampleUpdateParams()
                            .setAnnotationSets(Collections.singletonList(new AnnotationSet("annotation1", vs1.getId(), annotation))),
                    QueryOptions.empty(), token);
            fail("Annotation 'a.b' should not be INTEGER instead of STRING");
        } catch (CatalogException e) {
            assertTrue(e.getMessage().contains("Expected an integer map"));
        }

        annotation.put("a", new ObjectMap("b", 46));
        catalogManager.getSampleManager().update(studyFqn, s_1, new SampleUpdateParams()
                        .setAnnotationSets(Collections.singletonList(new AnnotationSet("annotation1", vs1.getId(), annotation))),
                QueryOptions.empty(), token);
    }

    @Test
    public void testVariableAllowedKeys() throws CatalogException, IOException {
        List<String> allowedKeys = Arrays.asList("x", "y", "z");

        List<Variable> variables = new ArrayList<>();
        variables.add(new Variable("a", "a", "", Variable.VariableType.MAP_STRING, null, true, false, null, null, 0, "", "",
                Collections.emptySet(), Collections.emptyMap()));
        variables.add(new Variable("a1", "a1", "", Variable.VariableType.OBJECT, null, true, true, null, null, 0, "", "",
                new HashSet<>(Arrays.asList(
                        new Variable("b", "b", "", Variable.VariableType.MAP_STRING, null, true, false, null, null, 0, "", "",
                                Collections.emptySet(), Collections.emptyMap()))),
                Collections.emptyMap()));
        variables.add(new Variable("a2", "a2", "", Variable.VariableType.OBJECT, null, true, true, null, null, 0, "", "",
                new HashSet<>(Arrays.asList(
                        new Variable("b", "b", "", Variable.VariableType.OBJECT, null, true, false, null, null, 0, "", "",
                                new HashSet<>(Arrays.asList(
                                        new Variable("c", "c", "", Variable.VariableType.MAP_STRING, null, true, true, null, allowedKeys, 0,
                                                "", "", Collections.emptySet(), Collections.emptyMap()))),
                                Collections.emptyMap()))),
                Collections.emptyMap()));
        variables.add(new Variable("a3", "a3", "", Variable.VariableType.OBJECT, null, true, true, null, null, 0, "", "",
                new HashSet<>(Arrays.asList(
                        new Variable("b", "b", "", Variable.VariableType.OBJECT, null, true, true, null, null, 0, "", "",
                                new HashSet<>(Arrays.asList(
                                        new Variable("c", "c", "", Variable.VariableType.MAP_STRING, null, true, false, null, null, 0, "", "",
                                                Collections.emptySet(), Collections.emptyMap()))),
                                Collections.emptyMap()))),
                Collections.emptyMap()));
        VariableSet vs1 = catalogManager.getStudyManager().createVariableSet(studyFqn, "vs1", "vs1", false, false, "", null, variables,
                Collections.singletonList(VariableSet.AnnotableDataModels.SAMPLE), token).first();

        InputStream inputStream = this.getClass().getClassLoader().getResource("annotation_sets/complete_annotation.json").openStream();
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectMap annotations = objectMapper.readValue(inputStream, ObjectMap.class);

        catalogManager.getSampleManager().update(studyFqn, s_1, new SampleUpdateParams()
                        .setAnnotationSets(Collections.singletonList(new AnnotationSet("annotation1", vs1.getId(), annotations))),
                QueryOptions.empty(), token);


        // The only keys covered in the example annotationset are x, y, z; so we will change the allowedKeys so the annotation should be
        // no longer valid
        allowedKeys = Arrays.asList("a", "b", "c");

        variables = new ArrayList<>();
        variables.add(new Variable("a", "a", "", Variable.VariableType.MAP_STRING, null, true, false, null, null, 0, "", "",
                Collections.emptySet(), Collections.emptyMap()));
        variables.add(new Variable("a1", "a1", "", Variable.VariableType.OBJECT, null, true, true, null, null, 0, "", "",
                new HashSet<>(Arrays.asList(
                        new Variable("b", "b", "", Variable.VariableType.MAP_STRING, null, true, false, null, null, 0, "", "",
                                Collections.emptySet(), Collections.emptyMap()))),
                Collections.emptyMap()));
        variables.add(new Variable("a2", "a2", "", Variable.VariableType.OBJECT, null, true, true, null, null, 0, "", "",
                new HashSet<>(Arrays.asList(
                        new Variable("b", "b", "", Variable.VariableType.OBJECT, null, true, false, null, null, 0, "", "",
                                new HashSet<>(Arrays.asList(
                                        new Variable("c", "c", "", Variable.VariableType.MAP_STRING, null, true, true, null, allowedKeys, 0,
                                                "", "", Collections.emptySet(), Collections.emptyMap()))),
                                Collections.emptyMap()))),
                Collections.emptyMap()));
        variables.add(new Variable("a3", "a3", "", Variable.VariableType.OBJECT, null, true, true, null, null, 0, "", "",
                new HashSet<>(Arrays.asList(
                        new Variable("b", "b", "", Variable.VariableType.OBJECT, null, true, true, null, null, 0, "", "",
                                new HashSet<>(Arrays.asList(
                                        new Variable("c", "c", "", Variable.VariableType.MAP_STRING, null, true, false, null, null, 0, "", "",
                                                Collections.emptySet(), Collections.emptyMap()))),
                                Collections.emptyMap()))),
                Collections.emptyMap()));
        VariableSet vs2 = catalogManager.getStudyManager().createVariableSet(studyFqn, "vs2", "vs2", false, false, "", null, variables,
                Collections.singletonList(VariableSet.AnnotableDataModels.SAMPLE), token).first();

        thrown.expect(CatalogException.class);
        thrown.expectMessage("not an accepted key");
        catalogManager.getSampleManager().update(studyFqn, s_1, new SampleUpdateParams()
                        .setAnnotationSets(Collections.singletonList(new AnnotationSet("annotation2", vs2.getId(), annotations))),
                QueryOptions.empty(), token);
    }

    @Test
    public void testDynamicAnnotationsSearch() throws CatalogException, IOException {
        List<Variable> variables = new ArrayList<>();
        variables.add(new Variable("a", "a", "", Variable.VariableType.MAP_STRING, null, true, false, null, null, 0, "", "",
                Collections.emptySet(), Collections.emptyMap()));
        variables.add(new Variable("a1", "a1", "", Variable.VariableType.OBJECT, null, true, true, null, null, 0, "", "",
                new HashSet<>(Arrays.asList(
                        new Variable("b", "b", "", Variable.VariableType.MAP_STRING, null, true, false, null, null, 0, "", "",
                                Collections.emptySet(), Collections.emptyMap()))),
                Collections.emptyMap()));
        variables.add(new Variable("a2", "a2", "", Variable.VariableType.OBJECT, null, true, true, null, null, 0, "", "",
                new HashSet<>(Arrays.asList(
                        new Variable("b", "b", "", Variable.VariableType.OBJECT, null, true, false, null, null, 0, "", "",
                                new HashSet<>(Arrays.asList(
                                        new Variable("c", "c", "", Variable.VariableType.MAP_STRING, null, true, true, null, null, 0, "", "",
                                                Collections.emptySet(), Collections.emptyMap()))),
                                Collections.emptyMap()))),
                Collections.emptyMap()));
        variables.add(new Variable("a3", "a3", "", Variable.VariableType.OBJECT, null, true, true, null, null, 0, "", "",
                new HashSet<>(Arrays.asList(
                        new Variable("b", "b", "", Variable.VariableType.OBJECT, null, true, true, null, null, 0, "", "",
                                new HashSet<>(Arrays.asList(
                                        new Variable("c", "c", "", Variable.VariableType.MAP_STRING, null, true, false, null, null, 0, "", "",
                                                Collections.emptySet(), Collections.emptyMap()))),
                                Collections.emptyMap()))),
                Collections.emptyMap()));
        VariableSet vs1 = catalogManager.getStudyManager().createVariableSet(studyFqn, "vs1", "vs1", false, false, "", null, variables,
                Collections.singletonList(VariableSet.AnnotableDataModels.SAMPLE), token).first();

        InputStream inputStream = this.getClass().getClassLoader().getResource("annotation_sets/complete_annotation.json").openStream();
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectMap annotations = objectMapper.readValue(inputStream, ObjectMap.class);

        catalogManager.getSampleManager().update(studyFqn, s_1, new SampleUpdateParams()
                        .setAnnotationSets(Collections.singletonList(new AnnotationSet("annotation1", vs1.getId(), annotations))),
                QueryOptions.empty(), token);

        Query query = new Query(Constants.ANNOTATION, "a3.b.c.z=z2;a2.b.c.z=z3");
        assertEquals(0 , catalogManager.getSampleManager().count(studyFqn, query, token).getNumMatches());

        query = new Query(Constants.ANNOTATION, "a3.b.c.z=z2;a2.b.c.z=z");
        OpenCGAResult<Sample> result = catalogManager.getSampleManager().search(studyFqn, query, null, token);
        assertEquals(1, result.getNumResults());
        assertEquals(s_1, result.first().getId());
    }

    @Test
    public void searchSamples() throws CatalogException {
        catalogManager.getStudyManager().createGroup(studyFqn, "myGroup", Arrays.asList("user2", "user3"), token);
        catalogManager.getStudyManager().createGroup(studyFqn, "myGroup2", Arrays.asList("user2", "user3"), token);
        catalogManager.getStudyManager().updateAcl(Arrays.asList(studyFqn), "@myGroup",
                new StudyAclParams("", null), SET, token);

        catalogManager.getSampleManager().updateAcl(studyFqn, Arrays.asList("s_1"), "@myGroup",
                new SampleAclParams(null, null, null, "VIEW"), SET, false, token);

        DataResult<Sample> search = catalogManager.getSampleManager().search(studyFqn, new Query(), new QueryOptions(),
                sessionIdUser2);
        assertEquals(1, search.getNumResults());
    }

    @Test
    public void testDeleteAnnotationset() throws CatalogException, JsonProcessingException {
        List<Variable> variables = new ArrayList<>();
        variables.add(new Variable("var_name", "", "", Variable.VariableType.STRING, "", true, false, Collections.emptyList(), null, 0, "", "",
                null, Collections.emptyMap()));
        variables.add(new Variable("AGE", "", "", Variable.VariableType.INTEGER, "", false, false, Collections.emptyList(), null, 0, "", "",
                null, Collections.emptyMap()));
        variables.add(new Variable("HEIGHT", "", "", Variable.VariableType.DOUBLE, "", false, false, Collections.emptyList(), null, 0, "",
                "", null, Collections.emptyMap()));
        VariableSet vs1 = catalogManager.getStudyManager().createVariableSet(studyFqn, "vs1", "vs1", false, false, "", null, variables,
                Collections.singletonList(VariableSet.AnnotableDataModels.SAMPLE), token).first();

        ObjectMap annotations = new ObjectMap()
                .append("var_name", "Joe")
                .append("AGE", 25)
                .append("HEIGHT", 180);
        AnnotationSet annotationSet = new AnnotationSet("annotation1", vs1.getId(), annotations);
        AnnotationSet annotationSet1 = new AnnotationSet("annotation2", vs1.getId(), annotations);

        DataResult<Sample> update = catalogManager.getSampleManager().update(studyFqn, s_1, new SampleUpdateParams()
                .setAnnotationSets(Arrays.asList(annotationSet, annotationSet1)), QueryOptions.empty(), token);
        assertEquals(1, update.getNumUpdated());

        Sample sample = catalogManager.getSampleManager().get(studyFqn, s_1, QueryOptions.empty(), token).first();
        assertEquals(3, sample.getAnnotationSets().size());

        catalogManager.getSampleManager().removeAnnotationSet(studyFqn, s_1, "annotation1", QueryOptions.empty(), token);
        update = catalogManager.getSampleManager()
                .removeAnnotationSet(studyFqn, s_1, "annotation2", QueryOptions.empty(), token);
        assertEquals(1, update.getNumUpdated());

        sample = catalogManager.getSampleManager().get(studyFqn, s_1, QueryOptions.empty(), token).first();
        assertEquals(1, sample.getAnnotationSets().size());

        thrown.expect(CatalogDBException.class);
        thrown.expectMessage("not found");
        catalogManager.getSampleManager().removeAnnotationSet(studyFqn, s_1, "non_existing", QueryOptions.empty(), token);
    }

    @Test
    public void testSearchAnnotation() throws CatalogException, JsonProcessingException {
        List<Variable> variables = new ArrayList<>();
        variables.add(new Variable("var_name", "", "", Variable.VariableType.STRING, "", true, false, Collections.emptyList(), null, 0, "", "",
                null, Collections.emptyMap()));
        variables.add(new Variable("AGE", "", "", Variable.VariableType.INTEGER, "", false, false, Collections.emptyList(), null, 0, "", "",
                null, Collections.emptyMap()));
        variables.add(new Variable("HEIGHT", "", "", Variable.VariableType.DOUBLE, "", false, false, Collections.emptyList(), null, 0, "",
                "", null, Collections.emptyMap()));
        variables.add(new Variable("OTHER", "", "", Variable.VariableType.OBJECT, null, false, false, null, null, 1, "", "", null,
                Collections.emptyMap()));
        VariableSet vs1 = catalogManager.getStudyManager().createVariableSet(studyFqn, "vs1", "vs1", false, false, "", null, variables,
                Collections.singletonList(VariableSet.AnnotableDataModels.SAMPLE), token).first();

        ObjectMap annotations = new ObjectMap()
                .append("var_name", "Joe")
                .append("AGE", 25)
                .append("HEIGHT", 180);
        AnnotationSet annotationSet = new AnnotationSet("annotation1", vs1.getId(), annotations);

        catalogManager.getSampleManager().update(studyFqn, s_1, new SampleUpdateParams()
                .setAnnotationSets(Collections.singletonList(annotationSet)), QueryOptions.empty(), token);

        Query query = new Query(Constants.ANNOTATION, "var_name=Joe;" + vs1.getId() + ":AGE=25");
        DataResult<Sample> annotDataResult = catalogManager.getSampleManager().search(studyFqn, query, QueryOptions.empty(),
                token);
        assertEquals(1, annotDataResult.getNumResults());

        query.put(Constants.ANNOTATION, "var_name=Joe;" + vs1.getId() + ":AGE=23");
        annotDataResult = catalogManager.getSampleManager().search(studyFqn, query, QueryOptions.empty(), token);
        assertEquals(0, annotDataResult.getNumResults());

        query.put(Constants.ANNOTATION, "var_name=Joe;" + vs1.getId() + ":AGE=25;variableSet!=" + vs1.getId());
        annotDataResult = catalogManager.getSampleManager().search(studyFqn, query, QueryOptions.empty(), token);
        assertEquals(1, annotDataResult.getNumResults());

        query.put(Constants.ANNOTATION, "var_name=Joe;" + vs1.getId() + ":AGE=25;variableSet!==" + vs1.getId());
        annotDataResult = catalogManager.getSampleManager().search(studyFqn, query, QueryOptions.empty(), token);
        assertEquals(0, annotDataResult.getNumResults());

        query.put(Constants.ANNOTATION, "var_name=Joe;" + vs1.getId() + ":AGE=25;variableSet==" + vs1.getId());
        annotDataResult = catalogManager.getSampleManager().search(studyFqn, query, QueryOptions.empty(), token);
        assertEquals(1, annotDataResult.getNumResults());

        query.put(Constants.ANNOTATION, "var_name=Joe;" + vs1.getId() + ":AGE=25;variableSet===" + vs1.getId());
        annotDataResult = catalogManager.getSampleManager().search(studyFqn, query, QueryOptions.empty(), token);
        assertEquals(0, annotDataResult.getNumResults());

        VariableSet vs = catalogManager.getStudyManager().getVariableSet(studyFqn, "vs", null, token).first();
        query.put(Constants.ANNOTATION, "variableSet===" + vs.getId());
        annotDataResult = catalogManager.getSampleManager().search(studyFqn, query, QueryOptions.empty(), token);
        assertEquals(7, annotDataResult.getNumResults());

        query.put(Constants.ANNOTATION, "variableSet!=" + vs1.getId());
        annotDataResult = catalogManager.getSampleManager().search(studyFqn, query, QueryOptions.empty(), token);
        assertEquals(9, annotDataResult.getNumResults());

        query.put(Constants.ANNOTATION, "variableSet!==" + vs1.getId());
        annotDataResult = catalogManager.getSampleManager().search(studyFqn, query, QueryOptions.empty(), token);
        assertEquals(8, annotDataResult.getNumResults());

        query.put(Constants.ANNOTATION, "variableSet=" + vs1.getId());
        annotDataResult = catalogManager.getSampleManager().search(studyFqn, query,
                new QueryOptions(QueryOptions.INCLUDE, Constants.VARIABLE_SET + "." + vs1.getId()), token);
        assertEquals(1, annotDataResult.getNumResults());
        assertEquals(1, annotDataResult.first().getAnnotationSets().size());
        assertEquals(vs1.getId(), annotDataResult.first().getAnnotationSets().get(0).getVariableSetId());
    }

    @Test
    public void testProjections() throws CatalogException {
        VariableSet variableSet = catalogManager.getStudyManager().getVariableSet("1000G:phase1", "vs", null, token).first();

        Query query = new Query(Constants.ANNOTATION, "variableSet===" + variableSet.getId());
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, "annotationSets");
        DataResult<Sample> annotDataResult = catalogManager.getSampleManager().search(studyFqn, query, options,
                token);
        assertEquals(8, annotDataResult.getNumResults());

        for (Sample sample : annotDataResult.getResults()) {
            assertEquals(null, sample.getId());
            assertTrue(!sample.getAnnotationSets().isEmpty());
        }
    }

    @Test
    public void testAnnotateMulti() throws CatalogException {
        String sampleId = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_1"), new QueryOptions(),
                token).first().getId();

        List<Variable> variables = new ArrayList<>();
        variables.add(new Variable("NAME", "NAME", "", Variable.VariableType.STRING, "", true, false, Collections.emptyList(), null, 0, "", "",
                null, Collections.emptyMap()));
        VariableSet vs1 = catalogManager.getStudyManager().createVariableSet(studyFqn, "vs1", "vs1", false, false, "", null, variables,
                Collections.singletonList(VariableSet.AnnotableDataModels.SAMPLE), token).first();


        HashMap<String, Object> annotations = new HashMap<>();
        annotations.put("NAME", "Luke");

        catalogManager.getSampleManager().update(studyFqn, sampleId, new SampleUpdateParams()
                        .setAnnotationSets(Collections.singletonList(new AnnotationSet("annotation1", vs1.getId(), annotations))),
                QueryOptions.empty(), token);
        DataResult<Sample> sampleDataResult = catalogManager.getSampleManager().get(studyFqn, sampleId,
                new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.ANNOTATION_SETS.key()), token);
        assertEquals(1, sampleDataResult.first().getAnnotationSets().size());

        annotations = new HashMap<>();
        annotations.put("NAME", "Lucas");
        catalogManager.getSampleManager().update(studyFqn, sampleId, new SampleUpdateParams()
                        .setAnnotationSets(Collections.singletonList(new AnnotationSet("annotation2", vs1.getId(), annotations))),
                QueryOptions.empty(), token);
        sampleDataResult = catalogManager.getSampleManager().get(studyFqn, sampleId,
                new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.ANNOTATION_SETS.key()), token);
        assertEquals(2, sampleDataResult.first().getAnnotationSets().size());

        assertTrue(Arrays.asList("annotation1", "annotation2")
                .containsAll(sampleDataResult.first().getAnnotationSets().stream().map(AnnotationSet::getId).collect(Collectors.toSet())));
    }

    @Test
    public void testAnnotateUnique() throws CatalogException {
        String sampleId = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_1"), new QueryOptions(),
                token).first().getId();

        List<Variable> variables = new ArrayList<>();
        variables.add(new Variable("NAME", "NAME", "", Variable.VariableType.STRING, "", true, false, Collections.emptyList(), null, 0, "", "",
                null, Collections.emptyMap()));
        VariableSet vs1 = catalogManager.getStudyManager().createVariableSet(studyFqn, "vs1", "vs1", true, false, "", null, variables,
                Collections.singletonList(VariableSet.AnnotableDataModels.SAMPLE), token).first();


        HashMap<String, Object> annotations = new HashMap<>();
        annotations.put("NAME", "Luke");

        catalogManager.getSampleManager().update(studyFqn, sampleId, new SampleUpdateParams()
                        .setAnnotationSets(Collections.singletonList(new AnnotationSet("annotation1", vs1.getId(), annotations))),
                QueryOptions.empty(), token);
        DataResult<Sample> sampleDataResult = catalogManager.getSampleManager().get(studyFqn, sampleId,
                new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.ANNOTATION_SETS.key()), token);
        assertEquals(1, sampleDataResult.first().getAnnotationSets().size());

        annotations.put("NAME", "Lucas");
        thrown.expect(CatalogException.class);
        thrown.expectMessage("unique");
        catalogManager.getSampleManager().update(studyFqn, sampleId, new SampleUpdateParams()
                        .setAnnotationSets(Collections.singletonList(new AnnotationSet("annotation2", vs1.getId(), annotations))),
                QueryOptions.empty(), token);
    }

    @Test
    public void testAnnotateIndividualUnique() throws CatalogException {
        String individualId = catalogManager.getIndividualManager().create(studyFqn, new Individual().setId("INDIVIDUAL_1"),
                new QueryOptions(), token).first().getId();

        List<Variable> variables = new ArrayList<>();
        variables.add(new Variable("NAME", "NAME", "", Variable.VariableType.STRING, "", true, false, Collections.emptyList(), null, 0, "", "",
                null, Collections.emptyMap()));
        VariableSet vs1 = catalogManager.getStudyManager().createVariableSet(studyFqn, "vs1", "vs1", true, false, "", null, variables,
                Collections.singletonList(VariableSet.AnnotableDataModels.INDIVIDUAL), token).first();


        HashMap<String, Object> annotations = new HashMap<>();
        annotations.put("NAME", "Luke");
        catalogManager.getIndividualManager().update(studyFqn, individualId, new IndividualUpdateParams()
                        .setAnnotationSets(Collections.singletonList(new AnnotationSet("annotation1", vs1.getId(), annotations))),
                QueryOptions.empty(), token);
        DataResult<Individual> individualDataResult = catalogManager.getIndividualManager().get(studyFqn, individualId,
                new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.ANNOTATION_SETS.key()), token);
        assertEquals(1, individualDataResult.first().getAnnotationSets().size());

        annotations.put("NAME", "Lucas");
        thrown.expect(CatalogException.class);
        thrown.expectMessage("unique");
        catalogManager.getIndividualManager().update(studyFqn, individualId, new IndividualUpdateParams()
                        .setAnnotationSets(Collections.singletonList(new AnnotationSet("annotation2", vs1.getId(), annotations))),
                QueryOptions.empty(), token);
    }

    @Test
    public void testAnnotateIncorrectType() throws CatalogException {
        String sampleId = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_1"), new QueryOptions(),
                token).first().getId();

        List<Variable> variables = new ArrayList<>();
        variables.add(new Variable("NUM", "NUM", "", Variable.VariableType.DOUBLE, "", true, false, null, null, 0, "", "", null,
                Collections.emptyMap()));
        VariableSet vs1 = catalogManager.getStudyManager().createVariableSet(studyFqn, "vs1", "vs1", false, false, "", null, variables,
                Collections.singletonList(VariableSet.AnnotableDataModels.SAMPLE), token).first();


        HashMap<String, Object> annotations = new HashMap<>();
        annotations.put("NUM", "5");
        catalogManager.getSampleManager().update(studyFqn, sampleId, new SampleUpdateParams()
                        .setAnnotationSets(Collections.singletonList(new AnnotationSet("annotation1", vs1.getId(), annotations))),
                QueryOptions.empty(), token);
        DataResult<Sample> sampleDataResult = catalogManager.getSampleManager().get(studyFqn, sampleId,
                new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.ANNOTATION_SETS.key()), token);
        assertEquals(1, sampleDataResult.first().getAnnotationSets().size());

        annotations.put("NUM", "6.8");
        catalogManager.getSampleManager().update(studyFqn, sampleId, new SampleUpdateParams()
                        .setAnnotationSets(Collections.singletonList(new AnnotationSet("annotation2", vs1.getId(), annotations))),
                QueryOptions.empty(), token);
        sampleDataResult = catalogManager.getSampleManager().get(studyFqn, sampleId,
                new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.ANNOTATION_SETS.key()), token);
        assertEquals(2, sampleDataResult.first().getAnnotationSets().size());

        annotations.put("NUM", "five polong five");
        thrown.expect(CatalogException.class);
        catalogManager.getSampleManager().update(studyFqn, sampleId, new SampleUpdateParams()
                        .setAnnotationSets(Collections.singletonList(new AnnotationSet("annotation3", vs1.getId(), annotations))),
                QueryOptions.empty(), token);
    }

    @Test
    public void testAnnotateRange() throws CatalogException {
        String sampleId = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_1"), new QueryOptions(),
                token).first().getId();

        List<Variable> variables = new ArrayList<>();
        variables.add(new Variable("RANGE_NUM", "RANGE_NUM", "", Variable.VariableType.DOUBLE, "", true, false, Arrays.asList("1:14",
                "16:22", "50:"), null, 0, "", "", null, Collections.<String, Object>emptyMap()));
        VariableSet vs1 = catalogManager.getStudyManager().createVariableSet(studyFqn, "vs1", "vs1", false, false, "", null, variables,
                Collections.singletonList(VariableSet.AnnotableDataModels.SAMPLE), token).first();

        HashMap<String, Object> annotations = new HashMap<>();
        annotations.put("RANGE_NUM", "1");  // 1:14
        catalogManager.getSampleManager().update(studyFqn, sampleId, new SampleUpdateParams()
                        .setAnnotationSets(Collections.singletonList(new AnnotationSet("annotation1", vs1.getId(), annotations))),
                QueryOptions.empty(), token);
        DataResult<Sample> sampleDataResult = catalogManager.getSampleManager().get(studyFqn, sampleId,
                new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.ANNOTATION_SETS.key()), token);
        assertEquals(1, sampleDataResult.first().getAnnotationSets().size());

        annotations.put("RANGE_NUM", "14"); // 1:14
        catalogManager.getSampleManager().update(studyFqn, sampleId, new SampleUpdateParams()
                        .setAnnotationSets(Collections.singletonList(new AnnotationSet("annotation2", vs1.getId(), annotations))),
                QueryOptions.empty(), token);
        sampleDataResult = catalogManager.getSampleManager().get(studyFqn, sampleId,
                new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.ANNOTATION_SETS.key()), token);
        assertEquals(2, sampleDataResult.first().getAnnotationSets().size());

        annotations.put("RANGE_NUM", "20");  // 16:20
        catalogManager.getSampleManager().update(studyFqn, sampleId, new SampleUpdateParams()
                        .setAnnotationSets(Collections.singletonList(new AnnotationSet("annotation3", vs1.getId(), annotations))),
                QueryOptions.empty(), token);
        sampleDataResult = catalogManager.getSampleManager().get(studyFqn, sampleId,
                new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.ANNOTATION_SETS.key()), token);
        assertEquals(3, sampleDataResult.first().getAnnotationSets().size());

        annotations.put("RANGE_NUM", "100000"); // 50:
        catalogManager.getSampleManager().update(studyFqn, sampleId, new SampleUpdateParams()
                        .setAnnotationSets(Collections.singletonList(new AnnotationSet("annotation4", vs1.getId(), annotations))),
                QueryOptions.empty(), token);
        sampleDataResult = catalogManager.getSampleManager().get(studyFqn, sampleId,
                new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.ANNOTATION_SETS.key()), token);
        assertEquals(4, sampleDataResult.first().getAnnotationSets().size());

        annotations.put("RANGE_NUM", "14.1");
        thrown.expect(CatalogException.class);
        catalogManager.getSampleManager().update(studyFqn, sampleId, new SampleUpdateParams()
                        .setAnnotationSets(Collections.singletonList(new AnnotationSet("annotation5", vs1.getId(), annotations))),
                QueryOptions.empty(), token);
    }

    @Test
    public void testAnnotateCategorical() throws CatalogException {
        String sampleId = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_1"), new QueryOptions(),
                token).first().getId();

        List<Variable> variables = new ArrayList<>();
        variables.add(new Variable("COOL_NAME", "COOL_NAME", "", Variable.VariableType.CATEGORICAL, "", true, false, Arrays.asList("LUKE",
                "LEIA", "VADER", "YODA"), null, 0, "", "", null, Collections.<String, Object>emptyMap()));
        VariableSet vs1 = catalogManager.getStudyManager().createVariableSet(studyFqn, "vs1", "vs1", false, false, "", null, variables,
                Collections.singletonList(VariableSet.AnnotableDataModels.SAMPLE), token).first();

        Map<String, Object> actionMap = new HashMap<>();
        actionMap.put(AnnotationSetManager.ANNOTATION_SETS, ParamUtils.BasicUpdateAction.ADD);
        QueryOptions options = new QueryOptions(Constants.ACTIONS, actionMap);

        HashMap<String, Object> annotations = new HashMap<>();
        annotations.put("COOL_NAME", "LUKE");
        catalogManager.getSampleManager().update(studyFqn, sampleId, new SampleUpdateParams()
                        .setAnnotationSets(Collections.singletonList(new AnnotationSet("annotation1", vs1.getId(), annotations))),
                options, token);
        DataResult<Sample> sampleDataResult = catalogManager.getSampleManager().get(studyFqn, sampleId,
                new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.ANNOTATION_SETS.key()), token);
        assertEquals(1, sampleDataResult.first().getAnnotationSets().size());

        annotations.put("COOL_NAME", "LEIA");
        catalogManager.getSampleManager().update(studyFqn, sampleId, new SampleUpdateParams()
                        .setAnnotationSets(Collections.singletonList(new AnnotationSet("annotation2", vs1.getId(), annotations))),
                options, token);
        sampleDataResult = catalogManager.getSampleManager().get(studyFqn, sampleId,
                new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.ANNOTATION_SETS.key()), token);
        assertEquals(2, sampleDataResult.first().getAnnotationSets().size());

        annotations.put("COOL_NAME", "VADER");
        catalogManager.getSampleManager().update(studyFqn, sampleId, new SampleUpdateParams()
                        .setAnnotationSets(Collections.singletonList(new AnnotationSet("annotation3", vs1.getId(), annotations))),
                options, token);
        sampleDataResult = catalogManager.getSampleManager().get(studyFqn, sampleId,
                new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.ANNOTATION_SETS.key()), token);
        assertEquals(3, sampleDataResult.first().getAnnotationSets().size());

        annotations.put("COOL_NAME", "YODA");
        catalogManager.getSampleManager().update(studyFqn, sampleId, new SampleUpdateParams()
                        .setAnnotationSets(Collections.singletonList(new AnnotationSet("annotation4", vs1.getId(), annotations))),
                options, token);
        sampleDataResult = catalogManager.getSampleManager().get(studyFqn, sampleId,
                new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.ANNOTATION_SETS.key()), token);
        assertEquals(4, sampleDataResult.first().getAnnotationSets().size());

        annotations.put("COOL_NAME", "SPOCK");
        thrown.expect(CatalogException.class);
        catalogManager.getSampleManager().update(studyFqn, sampleId, new SampleUpdateParams()
                        .setAnnotationSets(Collections.singletonList(new AnnotationSet("annotation5", vs1.getId(), annotations))),
                options, token);
    }

    @Test
    public void testAnnotateNested() throws CatalogException {
        String sampleId1 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_1"),
                new QueryOptions(), token).first().getId();
        String sampleId2 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_2"),
                new QueryOptions(), token).first().getId();

        VariableSet vs1 = catalogManager.getStudyManager().createVariableSet(studyFqn, "vs1", "vs1", false, false, "", null,
                Collections.singletonList(CatalogAnnotationsValidatorTest.nestedObject),
                Collections.singletonList(VariableSet.AnnotableDataModels.SAMPLE), token).first();

        HashMap<String, Object> annotations = new HashMap<>();
        annotations.put("nestedObject", new ObjectMap()
                .append("stringList", Arrays.asList("li", "lu"))
                .append("object", new ObjectMap()
                        .append("string", "my value")
                        .append("numberList", Arrays.asList(2, 3, 4))));
        catalogManager.getSampleManager().update(studyFqn, sampleId1, new SampleUpdateParams()
                        .setAnnotationSets(Collections.singletonList(new AnnotationSet("annotation1", vs1.getId(), annotations))),
                QueryOptions.empty(), token);
        DataResult<Sample> sampleDataResult = catalogManager.getSampleManager().get(studyFqn, sampleId1,
                new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.ANNOTATION_SETS.key()), token);
        assertEquals(1, sampleDataResult.first().getAnnotationSets().size());

        annotations.put("nestedObject", new ObjectMap()
                .append("stringList", Arrays.asList("lo", "lu"))
                .append("object", new ObjectMap()
                        .append("string", "stringValue")
                        .append("numberList", Arrays.asList(3, 4, 5))));
        catalogManager.getSampleManager().update(studyFqn, sampleId2, new SampleUpdateParams()
                        .setAnnotationSets(Collections.singletonList(new AnnotationSet("annotation1", vs1.getId(), annotations))),
                QueryOptions.empty(), token);
        sampleDataResult = catalogManager.getSampleManager().get(studyFqn, sampleId2,
                new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.ANNOTATION_SETS.key()), token);
        assertEquals(1, sampleDataResult.first().getAnnotationSets().size());

        List<Sample> samples;
        Query query = new Query(SampleDBAdaptor.QueryParams.ANNOTATION.key(), vs1.getId() + ":nestedObject.stringList=li");
        samples = catalogManager.getSampleManager().search(studyFqn, query, null, token).getResults();
        assertEquals(1, samples.size());

        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key(), vs1.getId() + ":nestedObject.stringList=lo");
        samples = catalogManager.getSampleManager().search(studyFqn, query, null, token).getResults();
        assertEquals(1, samples.size());

        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key(), vs1.getId() + ":nestedObject.stringList=LL");
        samples = catalogManager.getSampleManager().search(studyFqn, query, null, token).getResults();
        assertEquals(0, samples.size());

        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key(), vs1.getId() + ":nestedObject.stringList=lo,li,LL");
        samples = catalogManager.getSampleManager().search(studyFqn, query, null, token).getResults();
        assertEquals(2, samples.size());

        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key(), vs1.getId() + ":nestedObject.object.string=my value");
        samples = catalogManager.getSampleManager().search(studyFqn, query, null, token).getResults();
        assertEquals(1, samples.size());

        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key(), vs1.getId() + ":nestedObject.stringList=lo,lu,LL;" + vs1.getId()
                + ":nestedObject.object.string=my value");
        samples = catalogManager.getSampleManager().search(studyFqn, query, null, token).getResults();
        assertEquals(1, samples.size());

        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key(), vs1.getId() + ":nestedObject.stringList=lo,lu,LL;" + vs1.getId()
                + ":nestedObject.object.numberList=7");
        samples = catalogManager.getSampleManager().search(studyFqn, query, null, token).getResults();
        assertEquals(0, samples.size());

        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key(), vs1.getId() + ":nestedObject.stringList=lo,lu,LL;"
                + vs1.getId() + ":nestedObject.object.numberList=3");
        samples = catalogManager.getSampleManager().search(studyFqn, query, null, token).getResults();
        assertEquals(2, samples.size());

        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key(), vs1.getId() + ":nestedObject.stringList=lo,lu,LL;" + vs1.getId()
                + ":nestedObject.object.numberList=5;" + vs1.getId() + ":nestedObject.object.string=stringValue");
        samples = catalogManager.getSampleManager().search(studyFqn, query, null, token).getResults();
        assertEquals(1, samples.size());

        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key(), vs1.getId() + ":nestedObject.stringList=lo,lu,LL;" + vs1.getId()
                + ":nestedObject.object.numberList=2,5");
        samples = catalogManager.getSampleManager().search(studyFqn, query, null, token).getResults();
        assertEquals(2, samples.size());

        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key(), vs1.getId() + ":nestedObject.stringList=lo,lu,LL;" + vs1.getId()
                + ":nestedObject.object.numberList=0");
        samples = catalogManager.getSampleManager().search(studyFqn, query, null, token).getResults();
        assertEquals(0, samples.size());


        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key(), vs1.getId() + ":unexisting=lo,lu,LL");
        thrown.expect(CatalogException.class);
        thrown.expectMessage("does not exist");
        catalogManager.getSampleManager().search(studyFqn, query, null, token).getResults();
    }

//    @Test
//    public void testQuerySampleAnnotationFail1() throws CatalogException {
//        Query query = new Query();
//        query.put(SampleDBAdaptor.QueryParams.ANNOTATION.key() + ":nestedObject.stringList", "lo,lu,LL");
//
//        thrown.expect(CatalogDBException.class);
//        thrown.expectMessage("annotation:nestedObject does not exist");
//        DataResult<Sample> search = catalogManager.getSampleManager().search(studyFqn, query, null, sessionIdUser);
//        catalogManager.getAllSamples(studyId, query, null, sessionIdUser).getResults();
//    }

//    @Test
//    public void testQuerySampleAnnotationFail2() throws CatalogException {
//        Query query = new Query();
//        query.put(CatalogSampleDBAdaptor.QueryParams.ANNOTATION.key(), "nestedObject.stringList:lo,lu,LL");
//
//        thrown.expect(CatalogDBException.class);
//        thrown.expectMessage("Wrong annotation query");
//        catalogManager.getAllSamples(studyId, query, null, sessionIdUser).getResults();
//    }

    @Test
    public void testGroupByAnnotations() throws Exception {
        AbstractManager.MyResourceId vs1 = catalogManager.getStudyManager().getVariableSetId("vs", studyFqn, token);

        DataResult queryResult = catalogManager.getSampleManager().groupBy(studyFqn, new Query(),
                Collections.singletonList(Constants.ANNOTATION + ":" + vs1.getResourceId() + ":annot1:PHEN"), QueryOptions.empty(),
                token);

        assertEquals(3, queryResult.getNumResults());
        for (Document document : (List<Document>) queryResult.getResults()) {
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

        DBIterator<Sample> iterator = catalogManager.getSampleManager().iterator(studyFqn, query, null, token);
        int count = 0;
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }
        assertEquals(9, count);
    }

    @Test
    public void testQuerySamples() throws CatalogException {
        VariableSet variableSet = catalogManager.getStudyManager().getVariableSet(studyFqn, "vs", QueryOptions.empty(), token).first();

        List<Sample> samples;
        Query query = new Query();

        samples = catalogManager.getSampleManager().search(studyFqn, query, null, token).getResults();
        assertEquals(9, samples.size());

        query = new Query(ANNOTATION.key(), Constants.VARIABLE_SET + "=" + variableSet.getId());
        samples = catalogManager.getSampleManager().search(studyFqn, query, null, token).getResults();
        assertEquals(8, samples.size());

        query = new Query(ANNOTATION.key(), Constants.ANNOTATION_SET_NAME + "=annot2");
        samples = catalogManager.getSampleManager().search(studyFqn, query, null, token).getResults();
        assertEquals(3, samples.size());

        query = new Query(ANNOTATION.key(), Constants.ANNOTATION_SET_NAME + "=noExist");
        samples = catalogManager.getSampleManager().search(studyFqn, query, null, token).getResults();
        assertEquals(0, samples.size());

        query = new Query(ANNOTATION.key(), variableSet.getId() + ":NAME=s_1,s_2,s_3");
        samples = catalogManager.getSampleManager().search(studyFqn, query, null, token).getResults();
        assertEquals(3, samples.size());

        query = new Query(ANNOTATION.key(), variableSet.getId() + ":AGE>30;" + Constants.VARIABLE_SET + "=" + variableSet.getId());
        samples = catalogManager.getSampleManager().search(studyFqn, query, null, token).getResults();
        assertEquals(3, samples.size());

        query = new Query(ANNOTATION.key(), variableSet.getId() + ":AGE>30;" + Constants.VARIABLE_SET + "=" + variableSet.getId());
        samples = catalogManager.getSampleManager().search(studyFqn, query, null, token).getResults();
        assertEquals(3, samples.size());

        query = new Query(ANNOTATION.key(), variableSet.getId() + ":AGE>30;" + variableSet.getId() + ":ALIVE=true;"
                + Constants.VARIABLE_SET + "=" + variableSet.getId());
        samples = catalogManager.getSampleManager().search(studyFqn, query, null, token).getResults();
        assertEquals(2, samples.size());
    }

    @Test
    public void testUpdateAnnotation() throws CatalogException {
        Sample sample = catalogManager.getSampleManager().get(studyFqn, s_1, null, token).first();
        AnnotationSet annotationSet = sample.getAnnotationSets().get(0);

        Individual ind = new Individual()
                .setId("INDIVIDUAL_1")
                .setSex(IndividualProperty.Sex.UNKNOWN);
        ind.setAnnotationSets(Collections.singletonList(annotationSet));
        ind = catalogManager.getIndividualManager().create(studyFqn, ind, QueryOptions.empty(), token).first();

        // First update
        annotationSet.getAnnotations().put("NAME", "SAMPLE1");
        annotationSet.getAnnotations().put("AGE", 38);
        annotationSet.getAnnotations().put("EXTRA", "extra");
        annotationSet.getAnnotations().remove("HEIGHT");

        // Update annotation set
        catalogManager.getIndividualManager().updateAnnotations(studyFqn, ind.getId(), annotationSet.getId(),
                annotationSet.getAnnotations(), ParamUtils.CompleteUpdateAction.SET, new QueryOptions(Constants.INCREMENT_VERSION, true),
                token);
        catalogManager.getSampleManager().updateAnnotations(studyFqn, s_1, annotationSet.getId(), annotationSet.getAnnotations(),
                ParamUtils.CompleteUpdateAction.SET, new QueryOptions(Constants.INCREMENT_VERSION, true), token);

        Consumer<AnnotationSet> check = as -> {
            Map<String, Object> auxAnnotations = as.getAnnotations();

            assertEquals(5, auxAnnotations.size());
            assertEquals("SAMPLE1", auxAnnotations.get("NAME"));
            assertEquals(38, auxAnnotations.get("AGE"));
            assertEquals("extra", auxAnnotations.get("EXTRA"));
        };

        sample = catalogManager.getSampleManager().get(studyFqn, s_1, null, token).first();
        ind = catalogManager.getIndividualManager().get(studyFqn, ind.getId(), null, token).first();
        check.accept(sample.getAnnotationSets().get(0));
        check.accept(ind.getAnnotationSets().get(0));

        // Call again to the update to check that nothing changed
        catalogManager.getIndividualManager().updateAnnotations(studyFqn, ind.getId(), annotationSet.getId(),
                annotationSet.getAnnotations(), ParamUtils.CompleteUpdateAction.SET, new QueryOptions(Constants.INCREMENT_VERSION, true),
                token);
        check.accept(ind.getAnnotationSets().get(0));

        // Update mandatory annotation
        annotationSet.getAnnotations().put("NAME", "SAMPLE 1");
        annotationSet.getAnnotations().remove("EXTRA");

        catalogManager.getIndividualManager().updateAnnotations(studyFqn, ind.getId(), annotationSet.getId(),
                annotationSet.getAnnotations(), ParamUtils.CompleteUpdateAction.SET, new QueryOptions(Constants.INCREMENT_VERSION, true),
                token);
        catalogManager.getSampleManager().updateAnnotations(studyFqn, s_1, annotationSet.getId(), annotationSet.getAnnotations(),
                ParamUtils.CompleteUpdateAction.SET, new QueryOptions(Constants.INCREMENT_VERSION, true), token);

        check = as -> {
            Map<String, Object> auxAnnotations = as.getAnnotations();

            assertEquals(4, auxAnnotations.size());
            assertEquals("SAMPLE 1", auxAnnotations.get("NAME"));
            assertEquals(false, auxAnnotations.containsKey("EXTRA"));
        };

        sample = catalogManager.getSampleManager().get(studyFqn, s_1, null, token).first();
        ind = catalogManager.getIndividualManager().get(studyFqn, ind.getId(), null, token).first();
        check.accept(sample.getAnnotationSets().get(0));
        check.accept(ind.getAnnotationSets().get(0));

        // Update non-mandatory annotation
        annotationSet.getAnnotations().put("EXTRA", "extra");
        catalogManager.getIndividualManager().updateAnnotations(studyFqn, ind.getId(), annotationSet.getId(),
                annotationSet.getAnnotations(), ParamUtils.CompleteUpdateAction.SET, new QueryOptions(Constants.INCREMENT_VERSION, true),
                token);
        catalogManager.getSampleManager().updateAnnotations(studyFqn, s_1, annotationSet.getId(), annotationSet.getAnnotations(),
                ParamUtils.CompleteUpdateAction.SET, new QueryOptions(Constants.INCREMENT_VERSION, true), token);

        check = as -> {
            Map<String, Object> auxAnnotations = as.getAnnotations();

            assertEquals(5, auxAnnotations.size());
            assertEquals("SAMPLE 1", auxAnnotations.get("NAME"));
            assertEquals("extra", auxAnnotations.get("EXTRA"));
        };

        sample = catalogManager.getSampleManager().get(studyFqn, s_1, null, token).first();
        ind = catalogManager.getIndividualManager().get(studyFqn, ind.getId(), null, token).first();
        check.accept(sample.getAnnotationSets().get(0));
        check.accept(ind.getAnnotationSets().get(0));

        // Update non-mandatory annotation
        Map<String, Object> annotationUpdate = new ObjectMap("EXTRA", "extraa");
        // Action now is ADD, we only want to change that annotation
        catalogManager.getIndividualManager().updateAnnotations(studyFqn, ind.getId(), annotationSet.getId(), annotationUpdate,
                ParamUtils.CompleteUpdateAction.ADD, new QueryOptions(Constants.INCREMENT_VERSION, true), token);
        catalogManager.getSampleManager().updateAnnotations(studyFqn, s_1, annotationSet.getId(), annotationUpdate,
                ParamUtils.CompleteUpdateAction.ADD, new QueryOptions(Constants.INCREMENT_VERSION, true), token);

        check = as -> {
            Map<String, Object> auxAnnotations = as.getAnnotations();

            assertEquals(5, auxAnnotations.size());
            assertEquals("SAMPLE 1", auxAnnotations.get("NAME"));
            assertEquals("extraa", auxAnnotations.get("EXTRA"));
        };

        sample = catalogManager.getSampleManager().get(studyFqn, s_1, null, token).first();
        ind = catalogManager.getIndividualManager().get(studyFqn, ind.getId(), null, token).first();
        check.accept(sample.getAnnotationSets().get(0));
        check.accept(ind.getAnnotationSets().get(0));

        thrown.expect(CatalogException.class);
        thrown.expectMessage("not found");
        catalogManager.getIndividualManager().updateAnnotations(studyFqn, ind.getId(), "blabla", annotationUpdate,
                ParamUtils.CompleteUpdateAction.ADD, new QueryOptions(Constants.INCREMENT_VERSION, true), token);
    }

    @Test
    public void testUpdateAnnotationFail() throws CatalogException {
        Sample sample = catalogManager.getSampleManager().get(studyFqn, s_1, null, token).first();
        AnnotationSet annotationSet = sample.getAnnotationSets().get(0);

        thrown.expect(CatalogException.class); //Can not delete required fields
        thrown.expectMessage("required variable");
        catalogManager.getSampleManager().removeAnnotations(studyFqn, s_1, annotationSet.getId(), Collections.singletonList("NAME"),
                QueryOptions.empty(), token);
    }

    @Test
    public void testDeleteAnnotation() throws CatalogException {
        // We add one of the non mandatory annotations

        // First update
        catalogManager.getSampleManager().updateAnnotations(studyFqn, s_1, "annot1", new ObjectMap("EXTRA", "extra"),
                ParamUtils.CompleteUpdateAction.ADD, QueryOptions.empty(), token);

        Sample sample = catalogManager.getSampleManager().get(studyFqn, s_1, null, token).first();
        AnnotationSet annotationSet = sample.getAnnotationSets().get(0);
        assertEquals("extra", annotationSet.getAnnotations().get("EXTRA"));

        // Now we remove that non mandatory annotation
        catalogManager.getSampleManager().removeAnnotations(studyFqn, s_1, annotationSet.getId(), Collections.singletonList("EXTRA"),
                QueryOptions.empty(), token);

        sample = catalogManager.getSampleManager().get(studyFqn, s_1, null, token).first();
        annotationSet = sample.getAnnotationSets().get(0);
        assertTrue(!annotationSet.getAnnotations().containsKey("EXTRA"));

        // Now we attempt to remove one mandatory annotation
        thrown.expect(CatalogException.class); //Can not delete required fields
        thrown.expectMessage("required variable");
        catalogManager.getSampleManager().removeAnnotations(studyFqn, s_1, annotationSet.getId(), Collections.singletonList("AGE"),
                QueryOptions.empty(), token);
    }

    @Test
    public void testDeleteAnnotationSet() throws CatalogException {
        catalogManager.getSampleManager().removeAnnotationSet(studyFqn, s_1, "annot1", QueryOptions.empty(), token);

        DataResult<Sample> sampleDataResult = catalogManager.getSampleManager().get(studyFqn, s_1,
                new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.ANNOTATION_SETS.key()), token);
        assertEquals(0, sampleDataResult.first().getAnnotationSets().size());
    }

    @Test
    public void getVariableSetSummary() throws CatalogException {
        VariableSet variableSet = catalogManager.getStudyManager().getVariableSet(studyFqn, "vs", null, token).first();

        DataResult<VariableSetSummary> variableSetSummary = catalogManager.getStudyManager()
                .getVariableSetSummary(studyFqn, variableSet.getId(), token);

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
                .create(studyFqn, new Sample().setId("SAMPLE_1"), new QueryOptions(), token).first().getId();
        String individualId = catalogManager.getIndividualManager().create(studyFqn, new Individual().setId("Individual1"),
                new QueryOptions(), token).first().getId();

        DataResult<Sample> updateResult = catalogManager.getSampleManager()
                .update(studyFqn, sampleId1, new SampleUpdateParams().setIndividualId(individualId), null, token);
        assertEquals(1, updateResult.getNumUpdated());

        Sample sample = catalogManager.getSampleManager().get(studyFqn, sampleId1, QueryOptions.empty(), token).first();
        assertEquals(individualId, sample.getIndividualId());

        sample = catalogManager.getSampleManager().get(studyFqn, sampleId1, new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key()), token).first();
        assertNull(sample.getAttributes());

        sample = catalogManager.getSampleManager().get(studyFqn, sampleId1, new QueryOptions()
                .append(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key())
                .append(SAMPLE_INCLUDE_INDIVIDUAL_PARAM, true), token).first();
        assertNotNull(sample.getAttributes());
    }

    @Test
    public void testGetSampleAndIndividualWithPermissionsChecked() throws CatalogException {
        String sampleId1 = catalogManager.getSampleManager()
                .create(studyFqn, new Sample().setId("SAMPLE_1"), new QueryOptions(), token).first().getId();
        String individualId = catalogManager.getIndividualManager().create(studyFqn, new Individual().setId("Individual1"),
                new QueryOptions(), token).first().getId();

        DataResult<Sample> updateResult = catalogManager.getSampleManager()
                .update(studyFqn, sampleId1, new SampleUpdateParams().setIndividualId(individualId), null, token);
        assertEquals(1, updateResult.getNumUpdated());

        Sample sample = catalogManager.getSampleManager().get(studyFqn, sampleId1, QueryOptions.empty(), token).first();
        assertEquals(individualId, sample.getIndividualId());

        catalogManager.getSampleManager().updateAcl(studyFqn, Collections.singletonList("SAMPLE_1"), "user2",
                new SampleAclParams(null, null, null, SampleAclEntry.SamplePermissions.VIEW.name()), SET, false, token);

        sample = catalogManager.getSampleManager().get(studyFqn, "SAMPLE_1", new QueryOptions(SAMPLE_INCLUDE_INDIVIDUAL_PARAM, true), sessionIdUser2).first();
        assertEquals(null, sample.getAttributes().get("OPENCGA_INDIVIDUAL"));

        catalogManager.getSampleManager().updateAcl(studyFqn, Collections.singletonList("SAMPLE_1"), "user2",
                new SampleAclParams(null, null, null, SampleAclEntry.SamplePermissions.VIEW.name()), SET, true, token);
        sample = catalogManager.getSampleManager().get(studyFqn, "SAMPLE_1", new QueryOptions(SAMPLE_INCLUDE_INDIVIDUAL_PARAM, true), sessionIdUser2).first();
        assertEquals(individualId, ((Individual) sample.getAttributes().get("OPENCGA_INDIVIDUAL")).getId());
        assertEquals(sampleId1, sample.getId());

        sample = catalogManager.getSampleManager().search(studyFqn, new Query(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key(), "Individual1"),
                new QueryOptions(SAMPLE_INCLUDE_INDIVIDUAL_PARAM, true), sessionIdUser2).first();
        assertEquals(individualId, ((Individual) sample.getAttributes().get("OPENCGA_INDIVIDUAL")).getId());
        assertEquals(sampleId1, sample.getId());

    }

    @Test
    public void searchSamplesByIndividual() throws CatalogException {
        catalogManager.getIndividualManager().create(studyFqn, new Individual().setId("Individual1")
                .setSamples(Arrays.asList(new Sample().setId("sample1"), new Sample().setId("sample2"))), new QueryOptions(), token);

        DataResult<Sample> sampleDataResult = catalogManager.getSampleManager().search(studyFqn,
                new Query(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key(), "Individual1"), QueryOptions.empty(), token);

        assertEquals(2, sampleDataResult.getNumResults());

        sampleDataResult = catalogManager.getSampleManager().search(studyFqn,
                new Query().append(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key(), "Individual1")
                        .append(SampleDBAdaptor.QueryParams.ID.key(), "sample1"), QueryOptions.empty(), token);
        assertEquals(1, sampleDataResult.getNumResults());

        catalogManager.getIndividualManager().create(studyFqn, new Individual().setId("Individual2"), new QueryOptions(), token);
        sampleDataResult = catalogManager.getSampleManager().search(studyFqn,
                new Query().append(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key(), "Individual2"), QueryOptions.empty(), token);
        assertEquals(0, sampleDataResult.getNumResults());
    }

    @Test
    public void searchIndividualsBySample() throws CatalogException {
        catalogManager.getIndividualManager().create(studyFqn, new Individual().setId("Individual1")
                .setSamples(Arrays.asList(new Sample().setId("sample1"), new Sample().setId("sample2"))), new QueryOptions(), token);

        Individual individual = catalogManager.getIndividualManager().search(studyFqn,
                new Query(IndividualDBAdaptor.QueryParams.SAMPLES.key(), "sample1"), QueryOptions.empty(), token).first();
        assertEquals("Individual1", individual.getId());
    }

    @Test
    public void searchSamplesDifferentVersions() throws CatalogException {
        catalogManager.getSampleManager().create(studyFqn, new Sample().setId("sample1"), QueryOptions.empty(), token);
        catalogManager.getSampleManager().create(studyFqn, new Sample().setId("sample2"), QueryOptions.empty(), token);
        catalogManager.getSampleManager().create(studyFqn, new Sample().setId("sample3"), QueryOptions.empty(), token);

        // Generate 4 versions of sample1
        catalogManager.getSampleManager().update(studyFqn, "sample1", new SampleUpdateParams(),
                new QueryOptions(Constants.INCREMENT_VERSION, true), token);
        catalogManager.getSampleManager().update(studyFqn, "sample1", new SampleUpdateParams(),
                new QueryOptions(Constants.INCREMENT_VERSION, true), token);
        catalogManager.getSampleManager().update(studyFqn, "sample1", new SampleUpdateParams(),
                new QueryOptions(Constants.INCREMENT_VERSION, true), token);

        // Generate 3 versions of sample2
        catalogManager.getSampleManager().update(studyFqn, "sample2", new SampleUpdateParams(),
                new QueryOptions(Constants.INCREMENT_VERSION, true), token);
        catalogManager.getSampleManager().update(studyFqn, "sample2", new SampleUpdateParams(),
                new QueryOptions(Constants.INCREMENT_VERSION, true), token);

        // Generate 1 versions of sample3
        catalogManager.getSampleManager().update(studyFqn, "sample3", new SampleUpdateParams(),
                new QueryOptions(Constants.INCREMENT_VERSION, true), token);

        Query query = new Query()
                .append(SampleDBAdaptor.QueryParams.ID.key(), "sample1,sample2,sample3")
                .append(SampleDBAdaptor.QueryParams.VERSION.key(), "3,2,1");
        DataResult<Sample> sampleDataResult = catalogManager.getSampleManager().search(studyFqn, query, QueryOptions.empty(), token);
        assertEquals(3, sampleDataResult.getNumResults());
        for (Sample sample : sampleDataResult.getResults()) {
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
        sampleDataResult = catalogManager.getSampleManager().search(studyFqn, query, QueryOptions.empty(), token);
        assertEquals(3, sampleDataResult.getNumResults());
        sampleDataResult.getResults().forEach(
                s -> assertEquals(2, s.getVersion())
        );

        query.put(SampleDBAdaptor.QueryParams.VERSION.key(), "1,2");
        thrown.expect(CatalogException.class);
        thrown.expectMessage("size of the array");
        catalogManager.getSampleManager().search(studyFqn, query, QueryOptions.empty(), token);
    }

    @Test
    public void getSharedProject() throws CatalogException, IOException {
        catalogManager.getUserManager().create("dummy", "dummy", "asd@asd.asd", "dummy", "", 50000L,
                Account.AccountType.GUEST, null);
        catalogManager.getStudyManager().updateGroup(studyFqn, "@members", ParamUtils.BasicUpdateAction.ADD,
                new GroupUpdateParams(Collections.singletonList("dummy")), token);

        String token = catalogManager.getUserManager().login("dummy", "dummy").getToken();
        DataResult<Project> queryResult = catalogManager.getProjectManager().getSharedProjects("dummy", QueryOptions.empty(), token);
        assertEquals(1, queryResult.getNumResults());

        catalogManager.getStudyManager().updateGroup(studyFqn, "@members", ParamUtils.BasicUpdateAction.ADD,
                new GroupUpdateParams(Collections.singletonList("*")), this.token);
        queryResult = catalogManager.getProjectManager().getSharedProjects("*", QueryOptions.empty(), null);
        assertEquals(1, queryResult.getNumResults());
    }

    @Test
    public void smartResolutorStudyAliasFromAnonymousUser() throws CatalogException {
        catalogManager.getStudyManager().updateGroup(studyFqn, "@members", ParamUtils.BasicUpdateAction.ADD,
                new GroupUpdateParams(Collections.singletonList("*")), token);
        Study study = catalogManager.getStudyManager().resolveId(studyFqn, "*");
        assertTrue(study != null);
    }

    @Test
    public void testCreateSampleWithIndividual() throws CatalogException {
        String individualId = catalogManager.getIndividualManager().create(studyFqn, new Individual().setId("Individual1"),
                new QueryOptions(), token).first().getId();
        String sampleId1 = catalogManager.getSampleManager().create(studyFqn, new Sample()
                        .setId("SAMPLE_1")
                        .setIndividualId(individualId),
                new QueryOptions(), token).first().getId();

        DataResult<Individual> individualDataResult = catalogManager.getIndividualManager().get(studyFqn, individualId,
                QueryOptions.empty(), token);
        assertEquals(sampleId1, individualDataResult.first().getSamples().get(0).getId());

        // Create sample linking to individual based on the individual name
        String sampleId2 = catalogManager.getSampleManager().create(studyFqn, new Sample()
                        .setId("SAMPLE_2")
                        .setIndividualId("Individual1"),
                new QueryOptions(), token).first().getId();

        individualDataResult = catalogManager.getIndividualManager().get(studyFqn, individualId, QueryOptions.empty(), token);
        assertEquals(2, individualDataResult.first().getSamples().size());
        assertTrue(individualDataResult.first().getSamples().stream().map(Sample::getId).collect(Collectors.toSet()).containsAll(
                Arrays.asList(sampleId1, sampleId2)
        ));
    }

    @Test
    public void testModifySampleBadIndividual() throws CatalogException {
        String sampleId1 = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_1"), new QueryOptions(),
                token).first().getId();

        thrown.expect(CatalogException.class);
        thrown.expectMessage("not found");
        catalogManager.getSampleManager().update(studyFqn, sampleId1, new SampleUpdateParams().setIndividualId("ind"), null, token);
    }

    @Test
    public void testDeleteSample() throws CatalogException {
        long sampleUid = catalogManager.getSampleManager().create(studyFqn, new Sample().setId("SAMPLE_1"), new QueryOptions(),
                token).first().getUid();

        Query query = new Query(SampleDBAdaptor.QueryParams.ID.key(), "SAMPLE_1");
        DataResult delete = catalogManager.getSampleManager().delete("1000G:phase1", query, null, token);
        assertEquals(1, delete.getNumDeleted());

        query = new Query()
                .append(SampleDBAdaptor.QueryParams.UID.key(), sampleUid)
                .append(SampleDBAdaptor.QueryParams.DELETED.key(), true);

        DataResult<Sample> sampleDataResult = catalogManager.getSampleManager().search("1000G:phase1", query, new QueryOptions(), token);
//        DataResult<Sample> sample = catalogManager.getSample(sampleId, new QueryOptions(), sessionIdUser);
        assertEquals(1, sampleDataResult.getNumResults());
        assertEquals(Status.DELETED, sampleDataResult.first().getInternal().getStatus().getName());
    }

    @Test
    public void testAssignPermissionsWithPropagationAndNoIndividual() throws CatalogException {
        Sample sample = new Sample().setId("sample");
        catalogManager.getSampleManager().create(studyFqn, sample, QueryOptions.empty(), token);

        DataResult<Map<String, List<String>>> dataResult = catalogManager.getSampleManager().updateAcl(studyFqn,
                Arrays.asList("sample"), "user2", new SampleAclParams(null, null, null, "VIEW"), SET, true, token);
        assertEquals(1, dataResult.getNumResults());
        assertEquals(1, dataResult.first().size());
        assertEquals(1, dataResult.first().get("user2").size());
        assertTrue(dataResult.first().get("user2").contains(SampleAclEntry.SamplePermissions.VIEW.name()));
    }

    // Two samples, one related to one individual and the other does not have any individual associated
    @Test
    public void testAssignPermissionsWithPropagationWithIndividualAndNoIndividual() throws CatalogException {
        Individual individual = new Individual().setId("individual").setSamples(Collections.singletonList(new Sample().setId("sample")));
        catalogManager.getIndividualManager().create(studyFqn, individual, QueryOptions.empty(), token);

        Sample sample2 = new Sample().setId("sample2");
        catalogManager.getSampleManager().create(studyFqn, sample2, QueryOptions.empty(), token);

        DataResult<Map<String, List<String>>> dataResult = catalogManager.getSampleManager().updateAcl(studyFqn,
                Arrays.asList("sample", "sample2"), "user2", new SampleAclParams(null, null, null, "VIEW"), SET, true, token);
        assertEquals(2, dataResult.getNumResults());
        assertEquals(1, dataResult.first().size());
        assertEquals(1, dataResult.first().get("user2").size());
        assertTrue(dataResult.getResults().get(0).get("user2").contains(SampleAclEntry.SamplePermissions.VIEW.name()));
        assertTrue(dataResult.getResults().get(1).get("user2").contains(SampleAclEntry.SamplePermissions.VIEW.name()));

        DataResult<Map<String, List<String>>> individualAcl = catalogManager.getIndividualManager().getAcls(studyFqn,
                Collections.singletonList("individual"), "user2", false, token);
        assertEquals(1, individualAcl.getNumResults());
        assertEquals(1, individualAcl.first().size());
        assertEquals(1, individualAcl.first().get("user2").size());
        assertTrue(individualAcl.first().get("user2").contains(IndividualAclEntry.IndividualPermissions.VIEW.name()));
    }

}