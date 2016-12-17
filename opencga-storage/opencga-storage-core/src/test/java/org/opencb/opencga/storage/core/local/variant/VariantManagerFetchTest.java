package org.opencb.opencga.storage.core.local.variant;

import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;

import java.util.Collections;

/**
 * Created on 16/12/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantManagerFetchTest extends AbstractVariantStorageOperationTest {

    @Before
    public void setUp() throws Exception {
        indexFile(getSmallFileFile(), new QueryOptions(), outputId);

    }

    @Override
    protected VariantSource.Aggregation getAggregation() {
        return VariantSource.Aggregation.NONE;
    }

    @Test
    public void testCount() throws Exception {
        Query query = new Query(VariantDBAdaptor.VariantQueryParams.STUDIES.key(), studyId)
                .append(VariantDBAdaptor.VariantQueryParams.RETURNED_SAMPLES.key(), Collections.singletonList("NA19600"));
        variantManager.count(query, sessionId);
    }
}
