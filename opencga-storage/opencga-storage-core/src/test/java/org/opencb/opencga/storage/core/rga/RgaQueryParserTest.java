package org.opencb.opencga.storage.core.rga;

import org.apache.solr.client.solrj.SolrQuery;
import org.junit.Before;
import org.junit.Test;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.exceptions.RgaException;

import static org.junit.Assert.*;

public class RgaQueryParserTest {

    private RgaQueryParser parser;

    @Before
    public void setUp() throws Exception {
        parser = new RgaQueryParser();
    }

    @Test
    public void parse() throws RgaException {
        Query query = new Query(RgaQueryParams.POPULATION_FREQUENCY.key(), RgaUtils.THOUSAND_GENOMES_STUDY + "<0.001");
        SolrQuery parse = parser.parse(query, QueryOptions.empty());
        assertNotNull(parse);

    }
}