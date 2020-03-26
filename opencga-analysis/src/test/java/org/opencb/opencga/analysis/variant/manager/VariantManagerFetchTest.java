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

package org.opencb.opencga.analysis.variant.manager;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.metadata.Aggregation;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.variant.manager.operations.AbstractVariantOperationManagerTest;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.AclParams;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.sample.SampleAclEntry;
import org.opencb.opencga.core.models.sample.SampleAclParams;
import org.opencb.opencga.core.models.study.GroupUpdateParams;
import org.opencb.opencga.core.models.study.StudyAclEntry;
import org.opencb.opencga.core.models.study.StudyAclParams;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Created on 16/12/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantManagerFetchTest extends AbstractVariantOperationManagerTest {

    @Before
    public void setUp() throws Exception {
        indexFile(getSmallFile(), new QueryOptions(), outputId);
    }

    @Override
    protected Aggregation getAggregation() {
        return Aggregation.NONE;
    }

    @Test
    public void testCount() throws Exception {
        Query query = new Query(VariantQueryParam.STUDY.key(), studyId)
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), Collections.singletonList("NA19600"));
        variantManager.count(query, sessionId);
    }

    @Test
    public void testQuery() throws Exception {
        Query query = new Query(VariantQueryParam.STUDY.key(), studyId);
        DataResult<Variant> result = variantManager.get(query, new QueryOptions(), sessionId);
        Assert.assertNotEquals(0, result.getNumResults());
        for (Variant variant : result.getResults()) {
            Assert.assertEquals(4, variant.getStudies().get(0).getSamples().size());
        }
    }

    @Test
    public void testQueryProject() throws Exception {
        Query query = new Query(VariantCatalogQueryUtils.PROJECT.key(), projectId);
        DataResult<Variant> result = variantManager.get(query, new QueryOptions(), sessionId);
        Assert.assertNotEquals(0, result.getNumResults());
        for (Variant variant : result.getResults()) {
            Assert.assertEquals(4, variant.getStudies().get(0).getSamples().size());
        }
    }

    @Test
    public void testCheckSamplePermissionNonIndexedSamples() throws Exception {
        QueryOptions queryOptions = new QueryOptions();
        Query query = new Query();

        // Without studies
        Map<String, List<String>> map = variantManager.checkSamplesPermissions(query, queryOptions, mockVariantDBAdaptor().getMetadataManager(), sessionId);
        Assert.assertEquals(Collections.singleton(studyFqn), map.keySet());
        Assert.assertEquals(Arrays.asList("NA19600", "NA19660", "NA19661", "NA19685"), map.get(studyFqn));

        catalogManager.getSampleManager().create(studyFqn, new Sample().setId("newSample"), new QueryOptions(), sessionId);

        queryOptions = new QueryOptions();
        query = new Query();

        map = variantManager.checkSamplesPermissions(query, queryOptions, mockVariantDBAdaptor().getMetadataManager(), sessionId);
        Assert.assertEquals(Collections.singleton(studyFqn), map.keySet());
        Assert.assertEquals(Arrays.asList("NA19600", "NA19660", "NA19661", "NA19685"), map.get(studyFqn));
    }

    @Test
    public void testQueryExcludeSamples() throws Exception {
        QueryOptions queryOptions = new QueryOptions(QueryOptions.EXCLUDE, VariantField.STUDIES_SAMPLES);
        Query query = new Query();

        // Without studies
        Map<String, List<String>> longListMap = variantManager.checkSamplesPermissions(query, queryOptions, mockVariantDBAdaptor().getMetadataManager(), sessionId);
        Assert.assertEquals(Collections.singletonMap(studyFqn, Collections.emptyList()), longListMap);

        // With studies
        query.append(VariantQueryParam.STUDY.key(), studyFqn);
        longListMap = variantManager.checkSamplesPermissions(query, queryOptions, mockVariantDBAdaptor().getMetadataManager(), sessionId);
        Assert.assertEquals(Collections.singletonMap(studyFqn, Collections.emptyList()), longListMap);
    }

    @Test
    public void testQueryExcludeStudies() throws Exception {
        Query query = new Query(VariantQueryParam.STUDY.key(), studyId);
        QueryOptions queryOptions = new QueryOptions(QueryOptions.EXCLUDE, VariantField.STUDIES);

        Map<String, List<String>> longListMap = variantManager.checkSamplesPermissions(query, queryOptions, mockVariantDBAdaptor().getMetadataManager(), sessionId);
        Assert.assertEquals(Collections.emptyMap(), longListMap);
    }

    @Test
    public void testQueryAnonymousOnlyAggregated() throws Exception {
        // Only Aggregated studies
        catalogManager.getStudyManager().updateGroup(studyFqn, "@members", ParamUtils.UpdateAction.ADD,
                new GroupUpdateParams(Collections.singletonList("*")), sessionId);
        catalogManager.getStudyManager().updateAcl(Collections.singletonList(studyFqn), "*",
                new StudyAclParams(StudyAclEntry.StudyPermissions.VIEW_AGGREGATED_VARIANTS.name(), AclParams.Action.ADD, null), sessionId);

        Query query = new Query(VariantQueryParam.STUDY.key(), userId + "@p1:s1");
        DataResult<Variant> result = variantManager.get(query, new QueryOptions(), null);
        Assert.assertNotEquals(0, result.getNumResults());
        for (Variant variant : result.getResults()) {
            Assert.assertEquals(0, variant.getStudies().get(0).getSamples().size());
        }
    }

    @Test
    public void testQueryAnonymousViewSampleVariants() throws Exception {
        catalogManager.getStudyManager().updateGroup(studyFqn, "@members", ParamUtils.UpdateAction.ADD,
                new GroupUpdateParams(Collections.singletonList("*")), sessionId);
        catalogManager.getStudyManager().updateAcl(Collections.singletonList(studyFqn), "*",
                new StudyAclParams(StudyAclEntry.StudyPermissions.VIEW_AGGREGATED_VARIANTS.name(), AclParams.Action.ADD, null), sessionId);

        catalogManager.getSampleManager().updateAcl(studyFqn, Arrays.asList("NA19600"), "*",
                new SampleAclParams()
                        .setPermissions(SampleAclEntry.SamplePermissions.VIEW.name()) // View is not enough
                        .setAction(AclParams.Action.ADD), sessionId);
        catalogManager.getSampleManager().updateAcl(studyFqn, Arrays.asList("NA19660"), "*",
                new SampleAclParams()
                        .setPermissions(SampleAclEntry.SamplePermissions.VIEW_VARIANTS.name()) // ViewVariants without VIEW should be enough
                        .setAction(AclParams.Action.ADD), sessionId);

        Query query = new Query(VariantQueryParam.STUDY.key(), userId + "@p1:s1");
        DataResult<Variant> result = variantManager.get(query, new QueryOptions(), null);
        Assert.assertNotEquals(0, result.getNumResults());
        for (Variant variant : result.getResults()) {
            Assert.assertEquals(1, variant.getStudies().get(0).getSamples().size());
            Assert.assertEquals(Collections.singleton("NA19660"), variant.getStudies().get(0).getSamplesName());
        }

    }

    @Test
    public void testQueryAnonymousViewSampleVariantsWithoutAggregatedVariants() throws Exception {
        // Only 2 samples
        catalogManager.getSampleManager().updateAcl(studyFqn, Arrays.asList("NA19600", "NA19660"), "*",
                new SampleAclParams()
                        .setPermissions(SampleAclEntry.SamplePermissions.VIEW + "," + SampleAclEntry.SamplePermissions.VIEW_VARIANTS)
                        .setAction(AclParams.Action.ADD), sessionId);

        Query query = new Query(VariantQueryParam.STUDY.key(), userId + "@p1:s1");
        DataResult<Variant> result = variantManager.get(query, new QueryOptions(), null);
        Assert.assertNotEquals(0, result.getNumResults());
        for (Variant variant : result.getResults()) {
            Assert.assertEquals(2, variant.getStudies().get(0).getSamples().size());
        }

        query = new Query(VariantQueryParam.STUDY.key(), userId + "@p1:s1")
                .append(VariantQueryParam.SAMPLE.key(), "NA19600")
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), ParamConstants.ALL);
        result = variantManager.get(query, new QueryOptions(), null);
        Assert.assertNotEquals(0, result.getNumResults());
        for (Variant variant : result.getResults()) {
            Assert.assertEquals(2, variant.getStudies().get(0).getSamples().size());
        }
    }

    @Test
    public void testQueryAnonymousViewSampleVariantsWithoutAggregatedVariantsFail1() throws Exception {
        catalogManager.getStudyManager().updateAcl(Collections.singletonList(studyFqn), "*",
                new StudyAclParams(StudyAclEntry.StudyPermissions.VIEW_SAMPLES.name(), AclParams.Action.ADD, null), sessionId);
        // Only 2 samples
        catalogManager.getSampleManager().updateAcl(studyFqn, Arrays.asList("NA19600", "NA19660"), "*",
                new SampleAclParams()
                        .setPermissions(SampleAclEntry.SamplePermissions.VIEW + "," + SampleAclEntry.SamplePermissions.VIEW_VARIANTS)
                        .setAction(AclParams.Action.ADD), sessionId);

        // Filter sample "NA19601" is unauthorized, even if the result is not returned
        Query query = new Query(VariantQueryParam.STUDY.key(), userId + "@p1:s1")
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), "NA19600,NA19660")
                .append(VariantQueryParam.SAMPLE.key(), "NA19661");

        thrown.expectMessage("'sample'");
        thrown.expect(CatalogAuthorizationException.class);
        variantManager.get(query, new QueryOptions(), null);
    }

    @Test
    public void testQueryAnonymousViewSampleVariantsWithoutAggregatedVariantsFail2() throws Exception {
        catalogManager.getStudyManager().updateAcl(Collections.singletonList(studyFqn), "*",
                new StudyAclParams(StudyAclEntry.StudyPermissions.VIEW_SAMPLES.name(), AclParams.Action.ADD, null), sessionId);
        // Only 2 samples
        catalogManager.getSampleManager().updateAcl(studyFqn, Arrays.asList("NA19600", "NA19660"), "*",
                new SampleAclParams()
                        .setPermissions(SampleAclEntry.SamplePermissions.VIEW + "," + SampleAclEntry.SamplePermissions.VIEW_VARIANTS)
                        .setAction(AclParams.Action.ADD), sessionId);

        // Include sample "NA19601" is unauthorized
        Query query = new Query(VariantQueryParam.STUDY.key(), userId + "@p1:s1")
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), "NA19600,NA19660,NA19601");

        thrown.expectMessage("'includeSample'");
        thrown.expect(CatalogAuthorizationException.class);
        variantManager.get(query, new QueryOptions(), null);
    }

    @Test
    public void testQueryAnonymousViewSampleVariantsStudyLevel() throws Exception {
        // All samples
        catalogManager.getStudyManager().updateAcl(Collections.singletonList(studyFqn), "*",
                new StudyAclParams()
                        // VIEW_SAMPLE_VARIANTS without VIEW_SAMPLES should be enough
                        .setPermissions(StudyAclEntry.StudyPermissions.VIEW_SAMPLE_VARIANTS.name())
                        .setAction(AclParams.Action.ADD), sessionId);

        Query query = new Query(VariantQueryParam.STUDY.key(), userId + "@p1:s1");
        DataResult<Variant> result = variantManager.get(query, new QueryOptions(), null);
        Assert.assertNotEquals(0, result.getNumResults());
        for (Variant variant : result.getResults()) {
            Assert.assertEquals(4, variant.getStudies().get(0).getSamples().size());
        }
    }

    @Test
    public void testQueryAnonymousWithoutPermissions() throws Exception {
        Query query = new Query(VariantQueryParam.STUDY.key(), studyId);
        thrown.expectMessage("cannot view study");
        thrown.expect(CatalogAuthorizationException.class);
        variantManager.get(query, new QueryOptions(), null);
    }

    @Test
    public void testQueryAnonymousInMembersWithoutPermissions() throws Exception {
        catalogManager.getStudyManager().updateGroup(studyFqn, "@members", ParamUtils.UpdateAction.ADD,
                new GroupUpdateParams(Collections.singletonList("*")), sessionId);
        // Missing VIEW_AGGREGATED_VARIANTS
        Query query = new Query(VariantQueryParam.STUDY.key(), studyId);
        thrown.expectMessage("Permission denied");
        thrown.expect(CatalogAuthorizationException.class);
        variantManager.get(query, new QueryOptions(), null);
    }

}
