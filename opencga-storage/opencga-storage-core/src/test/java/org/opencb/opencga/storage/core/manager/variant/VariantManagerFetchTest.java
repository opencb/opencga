package org.opencb.opencga.storage.core.manager.variant;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.models.Sample;
import org.opencb.opencga.catalog.models.acls.permissions.StudyAclEntry;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Created on 16/12/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantManagerFetchTest extends AbstractVariantStorageOperationTest {

    @Before
    public void setUp() throws Exception {
        indexFile(getSmallFile(), new QueryOptions(), outputId);

    }

    @Override
    protected VariantSource.Aggregation getAggregation() {
        return VariantSource.Aggregation.NONE;
    }

    @Test
    public void testCount() throws Exception {
        Query query = new Query(VariantQueryParam.STUDIES.key(), studyId)
                .append(VariantQueryParam.RETURNED_SAMPLES.key(), Collections.singletonList("NA19600"));
        variantManager.count(query, sessionId);
    }


    @Test
    public void testQuery() throws Exception {
        Query query = new Query(VariantQueryParam.STUDIES.key(), studyId);
        QueryResult<Variant> result = variantManager.get(query, new QueryOptions(), sessionId);
        System.out.println("result.getNumResults() = " + result.getNumResults());
        System.out.println("result.getResult().size() = " + result.getResult().size());
        for (Variant variant : result.getResult()) {
            System.out.println("variant = " + variant);
        }
    }

    @Test
    public void testQueryExcludeSamples() throws Exception {
        QueryOptions queryOptions = new QueryOptions(QueryOptions.EXCLUDE, VariantField.STUDIES_SAMPLES_DATA);
        Query query = new Query();

        // Without studies
        Map<Long, List<Sample>> longListMap = variantManager.checkSamplesPermissions(query, queryOptions, mockVariantDBAdaptor().getStudyConfigurationManager(), sessionId);
        Assert.assertEquals(Collections.singletonMap(studyId, Collections.emptyList()), longListMap);

        // With studies
        query.append(VariantQueryParam.STUDIES.key(), studyId);
        longListMap = variantManager.checkSamplesPermissions(query, queryOptions, mockVariantDBAdaptor().getStudyConfigurationManager(), sessionId);
        Assert.assertEquals(Collections.singletonMap(studyId, Collections.emptyList()), longListMap);
    }

    @Test
    public void testQueryExcludeStudies() throws Exception {
        Query query = new Query(VariantQueryParam.STUDIES.key(), studyId);
        QueryOptions queryOptions = new QueryOptions(QueryOptions.EXCLUDE, VariantField.STUDIES);

        Map<Long, List<Sample>> longListMap = variantManager.checkSamplesPermissions(query, queryOptions, mockVariantDBAdaptor().getStudyConfigurationManager(), sessionId);
        Assert.assertEquals(Collections.emptyMap(), longListMap);
    }

    @Test
    public void testQueryAnonymous() throws Exception {
        catalogManager.createStudyAcls(studyStr, "anonymous", StudyAclEntry.StudyPermissions.VIEW_STUDY.name(), null, sessionId);

//        Query query = new Query(VariantDBAdaptor.VariantQueryParams.STUDIES.key(), "s1");
//        Query query = new Query(VariantDBAdaptor.VariantQueryParams.STUDIES.key(), "p1:s1");
        Query query = new Query(VariantQueryParam.STUDIES.key(), userId + "@p1:s1");
//        Query query = new Query(VariantDBAdaptor.VariantQueryParams.STUDIES.key(), studyId);
        QueryResult<Variant> result = variantManager.get(query, new QueryOptions(), null);
        System.out.println("result.getNumResults() = " + result.getNumResults());
        System.out.println("result.getResult().size() = " + result.getResult().size());
        for (Variant variant : result.getResult()) {
            System.out.println("variant = " + variant);
        }
    }

    @Test
    public void testQueryAnonymousWithoutPermissions() throws Exception {
        Query query = new Query(VariantQueryParam.STUDIES.key(), studyId);
        CatalogAuthorizationException exception = CatalogAuthorizationException
                .deny("anonymous", StudyAclEntry.StudyPermissions.VIEW_STUDY.toString(), "Study", studyId, null);
        thrown.expectMessage(exception.getMessage());
        thrown.expect(exception.getClass());
        variantManager.get(query, new QueryOptions(), null);
    }

}
