package org.opencb.opencga.storage.core.manager;

import org.junit.Test;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;

import static org.junit.Assert.*;

/**
 * Created by pfurio on 02/12/16.
 */
public class CatalogUtilsTest {

    @Test
    public void parseQuery() throws Exception {
        Query query = CatalogUtils.parseQuery("age>20;ontologies=hpo:123,hpo:456;name=smith", SampleDBAdaptor.QueryParams::getParam);

        assertEquals(3, query.size());

        assertTrue(query.containsKey(SampleDBAdaptor.QueryParams.NAME.key()));
        assertEquals("=smith", query.getString(SampleDBAdaptor.QueryParams.NAME.key()));

        assertTrue(query.containsKey(SampleDBAdaptor.QueryParams.ANNOTATION.key()));
        assertEquals("annotation.age>20", query.getString(SampleDBAdaptor.QueryParams.ANNOTATION.key()));

        assertTrue(query.containsKey(SampleDBAdaptor.QueryParams.ONTOLOGIES.key()));
        assertEquals("=hpo:123,hpo:456", query.getString(SampleDBAdaptor.QueryParams.ONTOLOGIES.key()));
    }

}