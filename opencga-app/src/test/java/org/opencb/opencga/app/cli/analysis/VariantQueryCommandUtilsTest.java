package org.opencb.opencga.app.cli.analysis;

import org.junit.Test;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;

import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Created on 07/06/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantQueryCommandUtilsTest {

    @Test
    public void parseQueryTest() throws Exception {

        AnalysisCliOptionsParser cliOptionsParser = new AnalysisCliOptionsParser();
        AnalysisCliOptionsParser.QueryVariantCommandOptions queryVariantsOptions = cliOptionsParser.getVariantCommandOptions().queryVariantCommandOptions;

        queryVariantsOptions.hpo = "HP:0002812";
        queryVariantsOptions.returnStudy = "1";
        Map<Long, String> studyIds = Collections.singletonMap(1L, "study");

        Query query = VariantQueryCommandUtils.parseQuery(queryVariantsOptions, studyIds);

//        System.out.println("query = " + query.toJson());
        assertEquals("HP:0002812", query.get(VariantDBAdaptor.VariantQueryParams.ANNOT_HPO.key()));
    }

}