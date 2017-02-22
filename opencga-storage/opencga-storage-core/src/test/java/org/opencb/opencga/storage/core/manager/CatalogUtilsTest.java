package org.opencb.opencga.storage.core.manager;

import org.junit.Before;
import org.junit.Test;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.managers.CatalogManager;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor.VariantQueryParams.STUDIES;

/**
 * Created by pfurio on 02/12/16.
 */
public class CatalogUtilsTest {

    private CatalogManager catalogManager;

    private Map<String, Long> studyNameMap;
    private CatalogUtils catalogUtils;

    @Before
    public void setUp() throws Exception {
        studyNameMap = new HashMap<>();
        addStudyId(1L);
        addStudyId(2L);
        addStudyId(3L);

        catalogManager = mock(CatalogManager.class);
        doAnswer(invocation -> studyNameMap.get(invocation.getArgument(0).toString()))
                .when(catalogManager).getStudyId(anyString(), anyString());
        catalogUtils = new CatalogUtils(catalogManager);
    }

    private void addStudyId(long studyId) {
        studyNameMap.put(String.valueOf(studyId), studyId);
        studyNameMap.put("s"+studyId, studyId);
        studyNameMap.put("p1:s"+studyId, studyId);
        studyNameMap.put("u@p1:s"+studyId, studyId);
    }

    @Test
    public void parseQuery() throws Exception {
        assertEquals("1", catalogUtils.parseQuery(new Query(STUDIES.key(), "s1"), "sessionId").getString(STUDIES.key()));
        assertEquals("1,2", catalogUtils.parseQuery(new Query(STUDIES.key(), "s1,s2"), "sessionId").getString(STUDIES.key()));
        assertEquals("!1,2", catalogUtils.parseQuery(new Query(STUDIES.key(), "!s1,2"), "sessionId").getString(STUDIES.key()));
        assertEquals("2;!1;3", catalogUtils.parseQuery(new Query(STUDIES.key(), "u@p1:s2;!s1;p1:s3"), "sessionId").getString(STUDIES.key()));
    }

    @Test
    public void parseSampleAnnotationQuery() throws Exception {
        Query query = CatalogUtils.parseSampleAnnotationQuery("age>20;ontologies=hpo:123,hpo:456;name=smith", SampleDBAdaptor.QueryParams::getParam);

        assertEquals(3, query.size());

        assertTrue(query.containsKey(SampleDBAdaptor.QueryParams.NAME.key()));
        assertEquals("=smith", query.getString(SampleDBAdaptor.QueryParams.NAME.key()));

        assertTrue(query.containsKey(SampleDBAdaptor.QueryParams.ANNOTATION.key()));
        assertEquals("annotation.age>20", query.getString(SampleDBAdaptor.QueryParams.ANNOTATION.key()));

        assertTrue(query.containsKey(SampleDBAdaptor.QueryParams.ONTOLOGIES.key()));
        assertEquals("=hpo:123,hpo:456", query.getString(SampleDBAdaptor.QueryParams.ONTOLOGIES.key()));
    }

}